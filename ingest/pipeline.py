"""Run the source loads into DuckDB/MotherDuck.

    uv run python -m ingest.pipeline                # every configured source, WAREHOUSE from .env
    uv run python -m ingest.pipeline withings       # one source

Each source is its own dlt pipeline and raw schema (raw_garmin, raw_withings),
named per warehouse so each warehouse tracks its own load progress and a fresh
warehouse backfills in full.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import dlt
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]


def destination():
    if os.getenv("WAREHOUSE", "local") == "motherduck":
        return dlt.destinations.motherduck(credentials={
            "database": os.getenv("MOTHERDUCK_DATABASE", "health"),
            "password": os.environ["MOTHERDUCK_TOKEN"],
        })
    return dlt.destinations.duckdb(str(REPO_ROOT / "health.duckdb"))


def _run(source_name: str, source):
    pipeline = dlt.pipeline(
        pipeline_name=f"{source_name}_{os.getenv('WAREHOUSE', 'local')}",
        destination=destination(),
        dataset_name=f"raw_{source_name}",
        pipelines_dir=os.getenv("DLT_PIPELINES_DIR"),  # None -> ~/.dlt; state is also kept in the destination
    )
    info = pipeline.run(source)
    print(info)
    return info


def run_garmin():
    from ingest.garmin_source import garmin_source

    return _run("garmin", garmin_source(
        api_key=os.environ["GARMIN_API_KEY"],
        start_date=os.getenv("GARMIN_START_DATE", "2009-12-01"),
    ))


def withings_configured() -> bool:
    return bool(os.getenv("WITHINGS_CLIENT_ID") and os.getenv("WITHINGS_CLIENT_SECRET"))


def run_withings():
    from ingest.withings_source import withings_source

    return _run("withings", withings_source())


SOURCES = {"garmin": run_garmin, "withings": run_withings}


if __name__ == "__main__":
    load_dotenv(REPO_ROOT / ".env")
    names = sys.argv[1:] or [n for n in SOURCES if n != "withings" or withings_configured()]
    for name in names:
        SOURCES[name]()
