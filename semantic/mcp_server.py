"""MCP server exposing the Cube semantic layer (health, training, body views).

Register with Claude Code (from the repo root). CUBE_API_URL in .env points at Cube:
    claude mcp add health-cube -- uv run --group mcp python semantic/mcp_server.py

The same two functions back the local-model chat in semantic/local_chat.py.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

import httpx
import jwt
from dotenv import load_dotenv
from mcp.server.mcpserver import MCPServer

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
logging.getLogger("httpx").setLevel(logging.WARNING)  # Cube long-polls; per-request logs are noise

CUBE_API_URL = os.getenv("CUBE_API_URL", "http://localhost:4000/cubejs-api/v1")
mcp = MCPServer(
    "health-cube",
    instructions="Personal health data (Garmin wellness and workouts, Withings weight). "
                 "Call describe_semantic_layer first, then query with the exact member names it returns.",
)


def _headers() -> dict:
    token = jwt.encode({"exp": int(time.time()) + 600}, os.environ["CUBEJS_API_SECRET"], algorithm="HS256")
    return {"Authorization": token, "Content-Type": "application/json"}


@mcp.tool()
def describe_semantic_layer() -> str:
    """List the views (health, training, body) with their measures, dimensions and descriptions.
    Call this first to learn which member names exist before calling `query`."""
    meta = httpx.get(f"{CUBE_API_URL}/meta", headers=_headers(), timeout=120).raise_for_status().json()
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


@mcp.tool()
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
    """
    q: dict = {"measures": measures, "dimensions": dimensions or [], "limit": limit}
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

    # Cube answers "Continue wait" while a query is still being prepared.
    # A Render instance waking from sleep can take ~60 s, hence the long timeout.
    for _ in range(30):
        resp = httpx.post(f"{CUBE_API_URL}/load", headers=_headers(), json={"query": q}, timeout=120)
        body = resp.json()
        if body.get("error") == "Continue wait":
            time.sleep(1)
            continue
        if resp.status_code >= 400 or "error" in body:
            return json.dumps({"error": body.get("error", resp.text), "query": q})
        return json.dumps(body.get("data", []), default=str)
    return json.dumps({"error": "Cube query timed out", "query": q})


if __name__ == "__main__":
    mcp.run()
