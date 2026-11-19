"""Withings OAuth2.

Withings rotates the refresh token on every use (the old one stops working 8 hours
later), so the current token has one home: the Prefect Secret block
`withings-refresh-token`. Laptop runs and Prefect Cloud runs both read it from
there and write the new token back, so they never drift apart. Local runs need
`prefect cloud login`.

One-time authorisation (opens a browser, then stores the first refresh token):

    uv run python -m ingest.withings_auth
"""

from __future__ import annotations

import http.server
import os
import secrets
import shutil
import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

from dlt.sources.helpers import requests

AUTHORIZE_URL = "https://account.withings.com/oauth2_user/authorize2"
TOKEN_URL = "https://wbsapi.withings.net/v2/oauth2"
SCOPE = "user.metrics"
REFRESH_TOKEN_BLOCK = "withings-refresh-token"


def _client() -> tuple[str, str]:
    return os.environ["WITHINGS_CLIENT_ID"], os.environ["WITHINGS_CLIENT_SECRET"]


def _request_token(**params) -> dict:
    client_id, client_secret = _client()
    resp = requests.post(TOKEN_URL, data={
        "action": "requesttoken", "client_id": client_id, "client_secret": client_secret, **params,
    }, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    # Withings reports errors as HTTP 200 with a non-zero status.
    if payload.get("status") != 0:
        raise RuntimeError(f"Withings token request failed: status={payload.get('status')} error={payload.get('error')}")
    return payload["body"]


def _load_refresh_token() -> str:
    from prefect.blocks.system import Secret

    return Secret.load(REFRESH_TOKEN_BLOCK).get()


def _save_refresh_token(token: str) -> None:
    from prefect.blocks.system import Secret

    Secret(value=token).save(REFRESH_TOKEN_BLOCK, overwrite=True)


def get_access_token() -> str:
    """Swap the stored refresh token for a 3-hour access token and store the rotated refresh token."""
    body = _request_token(grant_type="refresh_token", refresh_token=_load_refresh_token())
    _save_refresh_token(body["refresh_token"])
    return body["access_token"]


def _wait_for_code_on_localhost(redirect_uri: str, expected_state: str) -> str:
    """Serve the redirect URI once and return the `code` Withings sends to it."""
    target = urlparse(redirect_uri)
    received: dict[str, str] = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            query = parse_qs(urlparse(self.path).query)
            received.update({k: v[0] for k, v in query.items()})
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"Withings authorised. You can close this tab and go back to the terminal.")

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer((target.hostname, target.port or 80), Handler)
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()
    thread.join(timeout=300)
    server.server_close()
    if received.get("state") != expected_state or "code" not in received:
        raise RuntimeError(f"No valid authorisation code received (got: {sorted(received)})")
    return received["code"]


def _code_from_url(url: str, expected_state: str) -> str | None:
    query = parse_qs(urlparse(url.strip()).query)
    if query.get("state", [None])[0] == expected_state and query.get("code"):
        return query["code"][0]
    return None


def _wait_for_code_on_clipboard(expected_state: str, timeout_s: int = 300) -> str:
    """Withings won't redirect to localhost, so the redirect lands on a hosted page.

    The code expires 30 seconds after approval, which is tight for pasting by hand, so
    on macOS this watches the clipboard and exchanges the code as soon as the callback
    address is copied. Elsewhere it asks for the address to be pasted.
    """
    if shutil.which("pbpaste"):
        print("After approving, copy the address of the page you land on (or click its 'Copy address' button).")
        print("Watching the clipboard...")
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            clip = subprocess.run(["pbpaste"], capture_output=True, text=True).stdout
            code = _code_from_url(clip, expected_state)
            if code:
                return code
            time.sleep(0.3)
        sys.exit("Timed out waiting for the callback address on the clipboard. Run the command again.")

    pasted = input("After approving, paste the full address you were redirected to, then press Enter:\n> ")
    code = _code_from_url(pasted, expected_state)
    if not code:
        sys.exit("That address doesn't match this login attempt (missing code or state mismatch). Run the command again.")
    return code


def authorise() -> None:
    redirect_uri = os.environ["WITHINGS_REDIRECT_URI"]
    state = secrets.token_urlsafe(16)
    url = f"{AUTHORIZE_URL}?" + urlencode({
        "response_type": "code", "client_id": _client()[0], "scope": SCOPE,
        "redirect_uri": redirect_uri, "state": state,
    })
    on_localhost = urlparse(redirect_uri).hostname in ("localhost", "127.0.0.1")

    print(f"Opening Withings in your browser. If it doesn't open, visit:\n\n  {url}\n")
    webbrowser.open(url)
    if on_localhost:
        code = _wait_for_code_on_localhost(redirect_uri, state)
    else:
        code = _wait_for_code_on_clipboard(state)

    body = _request_token(grant_type="authorization_code", code=code, redirect_uri=redirect_uri)
    _save_refresh_token(body["refresh_token"])
    print(f"Done. Refresh token saved to the Prefect Secret block '{REFRESH_TOKEN_BLOCK}'.")


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    authorise()
