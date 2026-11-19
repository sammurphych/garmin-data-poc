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
import sys
import threading
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
        # The code expires 30 seconds after the redirect, so paste straight away.
        pasted = input("After approving, paste the full URL you were redirected to, then press Enter:\n> ").strip()
        query = parse_qs(urlparse(pasted).query)
        if query.get("state", [None])[0] != state:
            sys.exit("That URL doesn't match this login attempt (state mismatch). Run the command again.")
        code = query["code"][0]

    body = _request_token(grant_type="authorization_code", code=code, redirect_uri=redirect_uri)
    _save_refresh_token(body["refresh_token"])
    print(f"Done. Refresh token saved to the Prefect Secret block '{REFRESH_TOKEN_BLOCK}'.")


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    authorise()
