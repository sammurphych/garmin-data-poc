"""The health-cube MCP server over HTTPS, for claude.ai's custom connectors, behind a one-person OAuth sign-in.

Who gets in:
- One OAuth client, registered ahead of time: MCP_OAUTH_CLIENT_ID / MCP_OAUTH_CLIENT_SECRET, entered in the
  connector's Advanced settings. Dynamic client registration is off, so nobody else can register a client.
- It may only redirect to claude.ai's callback (MCP_REDIRECT_URIS overrides the list, e.g. for local testing).
- Signing in means typing MCP_LOGIN_PASSWORD. After 5 wrong passwords in 15 minutes the sign-in locks for 15 minutes.

Nothing is stored. The sign-in request, authorization codes and tokens are JWTs signed with MCP_SIGNING_KEY, so they
survive Render restarting the service, and changing MCP_SIGNING_KEY revokes every token at once. Access tokens last
an hour and are bound to this server's /mcp address; refresh tokens last 30 days and rotate on every use.

Run locally:  MCP_PUBLIC_URL=http://localhost:8000 uv run --group mcp python semantic/mcp_remote.py
"""

from __future__ import annotations

import asyncio
import contextvars
import hmac
import html
import logging
import os
import secrets
import sys
import time
from collections import deque
from pathlib import Path
from urllib.parse import urlparse

import jwt
import uvicorn
from mcp.server.auth.provider import (
    AccessToken,
    AuthorizationCode,
    AuthorizationParams,
    AuthorizeError,
    RefreshToken,
    TokenError,
    construct_redirect_uri,
)
from mcp.server.auth.settings import AuthSettings
from mcp.server.transport_security import TransportSecuritySettings
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken
from pydantic import AnyUrl
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

sys.path.insert(0, str(Path(__file__).resolve().parent))
import mcp_server  # noqa: E402  (the tools, and .env loading for local runs)

log = logging.getLogger("health-cube.auth")

# Render sets RENDER_EXTERNAL_URL to the service's own https address.
PUBLIC_URL = (os.getenv("MCP_PUBLIC_URL") or os.environ["RENDER_EXTERNAL_URL"]).rstrip("/")
RESOURCE_URL = f"{PUBLIC_URL}/mcp"
CLIENT_ID = os.environ["MCP_OAUTH_CLIENT_ID"]
CLIENT_SECRET = os.environ["MCP_OAUTH_CLIENT_SECRET"]
LOGIN_PASSWORD = os.environ["MCP_LOGIN_PASSWORD"]
SIGNING_KEY = os.environ["MCP_SIGNING_KEY"]
REDIRECT_URIS = os.getenv(
    "MCP_REDIRECT_URIS", "https://claude.ai/api/mcp/auth_callback,https://claude.com/api/mcp/auth_callback"
).split(",")

SCOPE = "health:read"
OWNER = "owner"  # the one person who can sign in
LOGIN_TTL = 600  # seconds to type the password
CODE_TTL = 300
ACCESS_TTL = 3600
REFRESH_TTL = 30 * 86400
MAX_FAILURES, LOCKOUT_SECONDS = 5, 900

for name, value in [("MCP_LOGIN_PASSWORD", LOGIN_PASSWORD), ("MCP_SIGNING_KEY", SIGNING_KEY),
                    ("MCP_OAUTH_CLIENT_SECRET", CLIENT_SECRET)]:
    if len(value) < 32:
        sys.exit(f"{name} must be at least 32 characters; generate one with: openssl rand -base64 32")

# The token endpoint authenticates the client by the method on its record, and claude.ai may send the secret
# either in the form body or in a Basic header. The ASGI wrapper at the bottom notes which one this request uses.
_token_request_uses_basic = contextvars.ContextVar("token_request_uses_basic", default=False)


def _sign(use: str, ttl: int, **claims) -> str:
    now = int(time.time())
    payload = {"iss": PUBLIC_URL, "use": use, "iat": now, "exp": now + ttl, "jti": secrets.token_urlsafe(16), **claims}
    return jwt.encode(payload, SIGNING_KEY, algorithm="HS256")


def _verify(token: str, use: str) -> dict | None:
    """The claims of a token this server signed for `use`, or None if it is forged, expired or meant for another use."""
    try:
        claims = jwt.decode(token, SIGNING_KEY, algorithms=["HS256"], issuer=PUBLIC_URL,
                            options={"require": ["exp", "iss", "jti"], "verify_aud": False})
    except jwt.InvalidTokenError:
        return None
    return claims if claims.get("use") == use else None


class _SpentJtis:
    """IDs of single-use tokens already redeemed. Kept in memory: a restart forgets them, but codes last 5 minutes."""

    def __init__(self) -> None:
        self._expiry: dict[str, int] = {}

    def spend(self, jti: str, exp: int) -> bool:
        """Mark `jti` used; False if it already was."""
        now = time.time()
        self._expiry = {j: e for j, e in self._expiry.items() if e > now}
        if jti in self._expiry:
            return False
        self._expiry[jti] = exp
        return True


class OnePersonProvider:
    """OAuth authorization server for one person and one pre-registered client (claude.ai)."""

    def __init__(self) -> None:
        self._spent_codes = _SpentJtis()
        self._spent_refresh_tokens = _SpentJtis()

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        if not hmac.compare_digest(client_id.encode(), CLIENT_ID.encode()):
            return None
        return OAuthClientInformationFull(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            redirect_uris=[AnyUrl(u) for u in REDIRECT_URIS],
            grant_types=["authorization_code", "refresh_token"],
            response_types=["code"],
            scope=SCOPE,
            token_endpoint_auth_method="client_secret_basic" if _token_request_uses_basic.get()
            else "client_secret_post",
        )

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        raise NotImplementedError("dynamic client registration is disabled")

    async def authorize(self, client: OAuthClientInformationFull, params: AuthorizationParams) -> str:
        if params.resource and params.resource.rstrip("/") != RESOURCE_URL:
            raise AuthorizeError("invalid_target", f"this server only issues tokens for {RESOURCE_URL}")
        login_request = _sign(
            "login", LOGIN_TTL,
            client_id=client.client_id,
            redirect_uri=str(params.redirect_uri),
            redirect_uri_provided_explicitly=params.redirect_uri_provided_explicitly,
            code_challenge=params.code_challenge,
            state=params.state,
            scopes=params.scopes or [SCOPE],
        )
        return construct_redirect_uri(f"{PUBLIC_URL}/login", request=login_request)

    async def load_authorization_code(self, client: OAuthClientInformationFull, code: str) -> AuthorizationCode | None:
        claims = _verify(code, "code")
        if not claims or claims["client_id"] != client.client_id:
            return None
        return AuthorizationCode(
            code=code, scopes=claims["scopes"], expires_at=claims["exp"], client_id=claims["client_id"],
            code_challenge=claims["code_challenge"], redirect_uri=AnyUrl(claims["redirect_uri"]),
            redirect_uri_provided_explicitly=claims["redirect_uri_provided_explicitly"],
            resource=RESOURCE_URL, subject=OWNER,
        )

    async def exchange_authorization_code(self, client: OAuthClientInformationFull, code: AuthorizationCode) -> OAuthToken:
        claims = _verify(code.code, "code")
        if not claims or not self._spent_codes.spend(claims["jti"], claims["exp"]):
            raise TokenError("invalid_grant", "authorization code already used")
        log.info("signed in: tokens issued to client %s", client.client_id[:8])
        return self._issue_tokens(client.client_id, code.scopes)

    async def load_refresh_token(self, client: OAuthClientInformationFull, refresh_token: str) -> RefreshToken | None:
        claims = _verify(refresh_token, "refresh")
        if not claims or claims["client_id"] != client.client_id:
            return None
        return RefreshToken(token=refresh_token, client_id=claims["client_id"], scopes=claims["scopes"],
                            expires_at=claims["exp"], resource=RESOURCE_URL, subject=OWNER)

    async def exchange_refresh_token(
        self, client: OAuthClientInformationFull, refresh_token: RefreshToken, scopes: list[str]
    ) -> OAuthToken:
        claims = _verify(refresh_token.token, "refresh")
        if not claims or not self._spent_refresh_tokens.spend(claims["jti"], claims["exp"]):
            log.warning("refused a refresh token that was already used")
            raise TokenError("invalid_grant", "refresh token already used")
        return self._issue_tokens(client.client_id, scopes)

    async def load_access_token(self, token: str) -> AccessToken | None:
        claims = _verify(token, "access")
        if not claims or claims.get("aud") != RESOURCE_URL:
            return None
        return AccessToken(token=token, client_id=claims["client_id"], scopes=claims["scopes"],
                           expires_at=claims["exp"], resource=RESOURCE_URL, subject=OWNER)

    async def revoke_token(self, token: AccessToken | RefreshToken) -> None:
        pass  # revocation endpoint is off; rotate MCP_SIGNING_KEY to revoke everything

    def _issue_tokens(self, client_id: str, scopes: list[str]) -> OAuthToken:
        return OAuthToken(
            access_token=_sign("access", ACCESS_TTL, aud=RESOURCE_URL, sub=OWNER, client_id=client_id, scopes=scopes),
            token_type="Bearer",
            expires_in=ACCESS_TTL,
            scope=" ".join(scopes),
            refresh_token=_sign("refresh", REFRESH_TTL, sub=OWNER, client_id=client_id, scopes=scopes),
        )


class _Lockout:
    """Counts wrong passwords across everyone, since there is only one account to guess."""

    def __init__(self) -> None:
        self._failures: deque[float] = deque()

    def locked(self) -> bool:
        cutoff = time.time() - LOCKOUT_SECONDS
        while self._failures and self._failures[0] < cutoff:
            self._failures.popleft()
        return len(self._failures) >= MAX_FAILURES

    def fail(self) -> None:
        self._failures.append(time.time())


provider = OnePersonProvider()
lockout = _Lockout()
server = mcp_server.build_server(
    auth_server_provider=provider,
    auth=AuthSettings(
        issuer_url=PUBLIC_URL,
        resource_server_url=RESOURCE_URL,
        validate_token_resource=True,
        required_scopes=[SCOPE],
    ),
)

_callback_origins = sorted({f"{urlparse(u).scheme}://{urlparse(u).netloc}" for u in REDIRECT_URIS})
_PAGE_HEADERS = {
    # form-action must also allow the redirect back to claude.ai that follows a successful sign-in.
    "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; "
                               f"form-action 'self' {' '.join(_callback_origins)}; frame-ancestors 'none'",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Cache-Control": "no-store",
}


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "")
    return forwarded.split(",")[0].strip() or (request.client.host if request.client else "?")


def _login_page(login_request: str, message: str = "", status: int = 200) -> HTMLResponse:
    body = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>health-cube sign-in</title>
<style>body{{font:16px system-ui,sans-serif;max-width:22rem;margin:15vh auto;padding:0 1rem}}
input,button{{font:inherit;width:100%;box-sizing:border-box;padding:.6rem;margin:.4rem 0}}
.msg{{color:#b00020}}</style></head><body>
<h1>health-cube</h1><p>Claude is asking to read your health data.</p>
<p class="msg">{html.escape(message)}</p>
<form method="post" action="/login">
<input type="hidden" name="request" value="{html.escape(login_request)}">
<label>Password <input type="password" name="password" autocomplete="current-password" required autofocus></label>
<button type="submit">Allow</button></form></body></html>"""
    return HTMLResponse(body, status_code=status, headers=_PAGE_HEADERS)


@server.custom_route("/login", methods=["GET"])
async def login_form(request: Request) -> Response:
    login_request = request.query_params.get("request", "")
    if not _verify(login_request, "login"):
        return HTMLResponse("This sign-in link has expired. Start again from Claude.", 400, headers=_PAGE_HEADERS)
    return _login_page(login_request)


@server.custom_route("/login", methods=["POST"])
async def login_submit(request: Request) -> Response:
    form = await request.form()
    login_request, password = str(form.get("request", "")), str(form.get("password", ""))
    claims = _verify(login_request, "login")
    if not claims:
        return HTMLResponse("This sign-in link has expired. Start again from Claude.", 400, headers=_PAGE_HEADERS)
    if lockout.locked():
        log.warning("sign-in refused while locked, from %s", _client_ip(request))
        return _login_page(login_request, "Too many wrong passwords. Try again in 15 minutes.", 429)
    if not hmac.compare_digest(password.encode(), LOGIN_PASSWORD.encode()):
        lockout.fail()
        log.warning("wrong password from %s", _client_ip(request))
        await asyncio.sleep(1)
        return _login_page(login_request, "Wrong password.", 401)

    log.info("password accepted from %s", _client_ip(request))
    code = _sign("code", CODE_TTL, **{k: claims[k] for k in (
        "client_id", "redirect_uri", "redirect_uri_provided_explicitly", "code_challenge", "scopes")})
    return RedirectResponse(construct_redirect_uri(claims["redirect_uri"], code=code, state=claims["state"]),
                            status_code=303, headers={"Cache-Control": "no-store"})


@server.custom_route("/healthz", methods=["GET"])
async def healthz(_: Request) -> Response:
    return JSONResponse({"ok": True})


def _note_basic_auth(app):
    async def wrapped(scope, receive, send):
        if scope["type"] == "http" and scope["path"] == "/token":
            auth = dict(scope["headers"]).get(b"authorization", b"")
            _token_request_uses_basic.set(auth[:6].lower() == b"basic ")
        await app(scope, receive, send)
    return wrapped


app = _note_basic_auth(server.streamable_http_app(
    stateless_http=True,  # no sessions to lose when Render puts the service to sleep
    json_response=True,
    host="0.0.0.0",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=[urlparse(PUBLIC_URL).netloc],
        allowed_origins=_callback_origins,
    ),
))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")),
                proxy_headers=True, forwarded_allow_ips="*")
