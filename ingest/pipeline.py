"""Run the Garmin -> DuckDB/MotherDuck load.

    uv run python -m ingest.pipeline            # uses WAREHOUSE from .env
"""

from __future__ import annotations

import os
from pathlib import Path

import dlt
from dotenv import load_dotenv

from ingest.garmin_source import garmin_source

REPO_ROOT = Path(__file__).resolve().parents[1]
DATASET = "raw_garmin"


def destination():
    if os.getenv("WAREHOUSE", "local") == "motherduck":
        return dlt.destinations.motherduck(credentials={
            "database": os.getenv("MOTHERDUCK_DATABASE", "health"),
            "password": os.environ["MOTHERDUCK_TOKEN"],
        })
    return dlt.destinations.duckdb(str(REPO_ROOT / "health.duckdb"))


def run_pipeline():
    pipeline = dlt.pipeline(
        # One pipeline per warehouse. Each tracks its own load progress, so a fresh warehouse backfills in full.
        pipeline_name=f"garmin_{os.getenv('WAREHOUSE', 'local')}",
        destination=destination(),
        dataset_name=DATASET,
        pipelines_dir=os.getenv("DLT_PIPELINES_DIR"),  # None -> ~/.dlt; state is also kept in the destination
    )
    source = garmin_source(
        api_key=os.environ["GARMIN_API_KEY"],
        start_date=os.getenv("GARMIN_START_DATE", "2009-12-01"),
    )
    info = pipeline.run(source)
    print(info)
    return info


if __name__ == "__main__":
    load_dotenv(REPO_ROOT / ".env")
    run_pipeline()
