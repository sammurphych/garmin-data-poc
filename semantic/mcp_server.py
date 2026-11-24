"""MCP server exposing the Cube semantic layer (health, training, body views).

Run directly, it serves Claude Code over stdio: .mcp.json at the repo root registers it, and CUBE_API_URL in .env
points at Cube. semantic/mcp_remote.py serves the same tools over HTTPS for claude.ai, and semantic/local_chat.py
calls the two functions directly.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import httpx
import jwt
from dotenv import load_dotenv
from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
logging.getLogger("httpx").setLevel(logging.WARNING)  # Cube long-polls; per-request logs are noise
log = logging.getLogger("health-cube")

CUBE_API_URL = os.getenv("CUBE_API_URL", "http://localhost:4000/cubejs-api/v1")
INSTRUCTIONS = (
    "Personal health data (Garmin wellness and workouts, Withings weight). "
    "Call describe_semantic_layer first, then query with the exact member names it returns. "
    "For individual workouts, include training.activity_id in dimensions: activity names "
    "repeat (e.g. two \"Austin Running\" on one day), so grouping by name or date alone "
    "merges them into one row."
)
READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, openWorldHint=False)


class CubeError(Exception):
    """Cube couldn't be reached or refused the request; the message says which, for the model to relay."""


def _headers() -> dict:
    secret = os.getenv("CUBEJS_API_SECRET")
    if not secret:
        raise CubeError("CUBEJS_API_SECRET is not set on the MCP server")
    token = jwt.encode({"exp": int(time.time()) + 600}, secret, algorithm="HS256")
    return {"Authorization": token, "Content-Type": "application/json"}


def _cube(method: str, path: str, **kwargs: Any) -> httpx.Response:
    """Call Cube, turning connection failures and auth/URL mistakes into a CubeError that names the cause."""
    url = f"{CUBE_API_URL}{path}"
    try:
        resp = httpx.request(method, url, headers=_headers(), timeout=120, **kwargs)
    except httpx.TimeoutException as e:
        raise CubeError(f"Cube at {CUBE_API_URL} didn't answer within 120 s (it may be waking up; try again)") from e
    except httpx.HTTPError as e:
        raise CubeError(f"couldn't reach Cube at {CUBE_API_URL}: {type(e).__name__}: {e}") from e
    if resp.status_code in (401, 403):
        raise CubeError(f"Cube refused the token ({resp.status_code}): CUBEJS_API_SECRET differs from Cube's")
    if resp.status_code == 404:
        raise CubeError(f"{url} returned 404: CUBE_API_URL should end in /cubejs-api/v1")
    return resp


def _error(e: Exception) -> str:
    log.error("Cube call failed: %s", e)
    return json.dumps({"error": str(e), "cube_api_url": CUBE_API_URL})


def describe_semantic_layer() -> str:
    """List the views (health, training, body) with their measures, dimensions and descriptions.
    Call this first to learn which member names exist before calling `query`."""
    try:
        resp = _cube("GET", "/meta")
        if resp.status_code >= 400:
            raise CubeError(f"Cube /meta returned {resp.status_code}: {resp.text[:300]}")
        meta = resp.json()
    except (CubeError, ValueError) as e:
        return _error(e)
    out = []
    for c in meta.get("cubes", []):
        if c.get("type") != "view":
            continue
        out.append({
            "view": c["name"],
            "description": c.get("description"),
            "measures": [{"name": m["name"], "description": m.get("description")} for m in c.get("measures", [])],
            "dimensions": [{"name": d["name"], "type": d["type"], "description": d.get("description")}
                           for d in c.get("dimensions", [])],
        })
    return json.dumps(out, indent=2)


def query(
    measures: list[str],
    dimensions: list[str] | None = None,
    time_dimension: str | None = None,
    granularity: str | None = None,
    date_range: list[str] | str | None = None,
    filters: list[dict] | None = None,
    order: dict | None = None,
    limit: int = 500,
) -> str:
    """Run a Cube query and return rows as JSON.

    Member names are `view.member`, e.g. `health.avg_resting_hr`, `training.activity_type`.
    - time_dimension: e.g. `health.calendar_date` or `training.activity_date`
    - granularity: day | week | month | quarter | year (omit to aggregate the whole range)
    - date_range: ["2026-10-01", "2026-10-31"] or a phrase like "last 7 days", "this month"
    - filters: Cube filters, e.g. [{"member": "training.activity_type", "operator": "equals", "values": ["running"]}]
    - order: e.g. {"health.calendar_date": "asc"}
    - one row per workout: add `training.activity_id` to dimensions, or same-day workouts with
      the same name are merged into one row
    """
    q: dict[str, Any] = {"measures": measures, "dimensions": dimensions or [], "limit": limit}
    if time_dimension:
        td: dict = {"dimension": time_dimension}
        if granularity:
            td["granularity"] = granularity
        if date_range:
            td["dateRange"] = date_range
        q["timeDimensions"] = [td]
    if filters:
        q["filters"] = filters
    if order:
        q["order"] = order
    log.info("query %s", json.dumps(q))  # the question, never the rows that come back

    # Cube answers "Continue wait" while a query is still being prepared.
    # A Render instance waking from sleep can take ~60 s, hence the long timeout.
    for _ in range(30):
        try:
            resp = _cube("POST", "/load", json={"query": q})
            body = resp.json()
        except (CubeError, ValueError) as e:
            return _error(e)
        if body.get("error") == "Continue wait":
            time.sleep(1)
            continue
        if resp.status_code >= 400 or "error" in body:
            return json.dumps({"error": body.get("error", resp.text), "query": q})
        return json.dumps(body.get("data", []), default=str)
    return json.dumps({"error": "Cube query timed out", "query": q})


def build_server(**kwargs: Any) -> MCPServer:
    """The health-cube server with both read-only tools; kwargs (e.g. auth settings) go to MCPServer."""
    server = MCPServer("health-cube", instructions=INSTRUCTIONS, **kwargs)
    for tool in (describe_semantic_layer, query):
        server.add_tool(tool, annotations=READ_ONLY)
    return server


if __name__ == "__main__":
    build_server().run()
