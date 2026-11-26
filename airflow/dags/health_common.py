"""Shared by the health DAGs: the Assets they pass between them, the pools, and the shared load tasks.

How the DAGs fit together (Airflow's data-aware scheduling):

    garmin_poll    (every 30 min, only if a new activity)  ─┐
    garmin_daily   (07:00 UTC: sleep, HRV, steps)          ─┼─> raw_garmin ─> raw_weather ─┐
    withings_poll  (every 30 min, only if a new weigh-in)  ──────────────────> raw_withings ┴─> transform
    rebuild_dashboards (Mon + Thu 08:00 UTC)

A task with `outlets=[asset]` records an asset event when it succeeds, and a DAG with `schedule=asset`
runs after it. raw_garmin has no DAG of its own: weather loads straight after Garmin, and the transform
waits for weather so new activities have their weather too.

The tasks run the pipeline in the project's own venv with `uv run`, so Airflow's pinned dependencies never
meet dlt's, SQLMesh's or Prefect's. The pipeline and SQLMesh read their secrets from the repo's .env.
"""

from __future__ import annotations

import json
import shutil
from datetime import timedelta
from pathlib import Path

from airflow.sdk import Asset, Variable, task

REPO_ROOT = Path(__file__).resolve().parents[2]
UV = shutil.which("uv") or "uv"

RAW_GARMIN = Asset("raw_garmin")
RAW_WEATHER = Asset("raw_weather")
RAW_WITHINGS = Asset("raw_withings")

# One slot each, so two DAGs never load the same source at once: dlt keeps one state per source, and
# Withings rotates its token on every login. Created by airflow/setup.sh.
GARMIN_POOL = "garmin_api"
WITHINGS_POOL = "withings_api"

# The Garmin activity ids already loaded, so garmin_poll can tell when a new one appears.
SEEN_ACTIVITIES = "garmin_seen_activity_ids"

DEFAULT_ARGS = {"retries": 1, "retry_delay": timedelta(minutes=5)}


def pipeline(source: str) -> str:
    return f"{UV} run python -m ingest.pipeline {source}"


@task.bash(cwd=str(REPO_ROOT), pool=GARMIN_POOL)
def list_recent_activities() -> str:
    """The ids of the last few days' activities, as JSON. A bash task's last line of output is its XCom."""
    return f"{UV} run python -m ingest.changes garmin"


@task.bash(cwd=str(REPO_ROOT), pool=GARMIN_POOL, retries=2, outlets=[RAW_GARMIN])
def load_garmin() -> str:
    return pipeline("garmin")


@task.bash(cwd=str(REPO_ROOT), retries=2, outlets=[RAW_WEATHER])
def load_weather() -> str:
    """After Garmin, because it looks up the activities Garmin just loaded."""
    return pipeline("weather")


@task
def remember_activities(ids_json: str) -> None:
    """Runs once Garmin has loaded, so a weather failure doesn't make garmin_poll reload Garmin every
    half hour; the weather is fetched again on the next load."""
    Variable.set(SEEN_ACTIVITIES, ids_json)


def garmin_then_weather(ids):
    """list_recent_activities' output -> load Garmin -> load weather, and remember the ids once Garmin loaded.
    Returns the first task, so a DAG can put a check in front of it."""
    garmin = load_garmin()
    garmin >> load_weather()
    garmin >> remember_activities(ids)
    return garmin


def seen_activities() -> set[int]:
    return set(json.loads(Variable.get(SEEN_ACTIVITIES, default="[]")))
