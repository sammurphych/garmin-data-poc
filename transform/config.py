"""SQLMesh config. Gateway is chosen by WAREHOUSE (local | motherduck), same as dlt.

Both the local file and the MotherDuck db are named `health`, so models
resolve the same catalog either way.

The motherduck gateway keeps SQLMesh state in Postgres when SQLMESH_STATE_DB_URL
is set (e.g. a Neon connection string), and in MotherDuck otherwise.
"""

import os
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from dotenv import load_dotenv
from sqlmesh.core.config import Config, GatewayConfig, ModelDefaultsConfig
from sqlmesh.core.config.connection import (
    DuckDBConnectionConfig,
    MotherDuckConnectionConfig,
    PostgresConnectionConfig,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")


def postgres_from_url(url: str) -> PostgresConnectionConfig:
    """postgresql://user:password@host[:port]/database?sslmode=require"""
    u = urlparse(url)
    return PostgresConnectionConfig(
        host=u.hostname,
        port=u.port or 5432,
        user=unquote(u.username or ""),
        password=unquote(u.password or ""),
        database=u.path.lstrip("/"),
        sslmode=parse_qs(u.query).get("sslmode", ["require"])[0],
    )


gateways = {
    "local": GatewayConfig(connection=DuckDBConnectionConfig(database=str(REPO_ROOT / "health.duckdb"))),
}
if os.getenv("MOTHERDUCK_TOKEN"):
    state_url = os.getenv("SQLMESH_STATE_DB_URL")
    gateways["motherduck"] = GatewayConfig(
        connection=MotherDuckConnectionConfig(
            database=os.getenv("MOTHERDUCK_DATABASE", "health"),
            token=os.environ["MOTHERDUCK_TOKEN"],
        ),
        state_connection=postgres_from_url(state_url) if state_url else None,
    )

config = Config(
    gateways=gateways,
    default_gateway=os.getenv("WAREHOUSE", "local"),
    model_defaults=ModelDefaultsConfig(dialect="duckdb", start=os.getenv("GARMIN_START_DATE", "2009-12-01")),
)
