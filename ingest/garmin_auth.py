"""Garmin Connect Personal API auth.

The personal API key can't call data endpoints directly. It is exchanged for an
OAuth2 access token valid for 24h. There are no refresh tokens, so we just
request a new token at the start of every run.
"""

from __future__ import annotations

from dlt.sources.helpers import requests

BASE_URL = "https://personal.connectapi.garmin.com"
TOKEN_URL = f"{BASE_URL}/personalapitoken-service/personal-api/personal-api-key/token"


def get_access_token(api_key: str) -> str:
    if not api_key:
        raise ValueError("GARMIN_API_KEY is empty")
    resp = requests.post(TOKEN_URL, headers={"Authorization": f"ApiKey {api_key}"}, timeout=30)
    resp.raise_for_status()

    # Garmin's docs don't publish the response shape, so accept the common ones.
    try:
        body = resp.json()
    except ValueError:
        body = resp.text.strip()

    if isinstance(body, str) and body:
        return body.strip('"')
    if isinstance(body, dict):
        for key in ("access_token", "accessToken", "token"):
            if body.get(key):
                return body[key]
    raise RuntimeError(f"Unrecognised token response from Garmin (keys: {list(body) if isinstance(body, dict) else type(body)})")
