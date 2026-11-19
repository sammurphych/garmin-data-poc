"""SQLMesh config. Gateway is chosen by WAREHOUSE (local | motherduck), same as dlt.

Both the local file and the MotherDuck db are named `garmin`, so models
resolve the same catalog either way.
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlmesh.core.config import Config, GatewayConfig, ModelDefaultsConfig
from sqlmesh.core.config.connection import DuckDBConnectionConfig, MotherDuckConnectionConfig

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

gateways = {
    "local": GatewayConfig(connection=DuckDBConnectionConfig(database=str(REPO_ROOT / "garmin.duckdb"))),
}
if os.getenv("MOTHERDUCK_TOKEN"):
    gateways["motherduck"] = GatewayConfig(
        # State also lives in MotherDuck. Fine for a POC; use Postgres for state in production.
        connection=MotherDuckConnectionConfig(
            database=os.getenv("MOTHERDUCK_DATABASE", "garmin"),
            token=os.environ["MOTHERDUCK_TOKEN"],
        ),
    )

config = Config(
    gateways=gateways,
    default_gateway=os.getenv("WAREHOUSE", "local"),
    model_defaults=ModelDefaultsConfig(dialect="duckdb", start=os.getenv("GARMIN_START_DATE", "2009-12-01")),
)
