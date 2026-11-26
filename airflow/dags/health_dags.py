"""The health pipeline as five DAGs that hand over through Assets; see health_common.py for the picture.

Everything starts paused. While Prefect still runs the daily job, switching these on means both load
every day (harmless: loads merge on keys) and both rebuild the dashboards on Mondays and Thursdays
(costs Netlify credits), so pause the Prefect deployment first.
"""

from __future__ import annotations

import json

import pendulum
from airflow.sdk import dag, task

from health_common import (
    DEFAULT_ARGS, RAW_WEATHER, RAW_WITHINGS, REPO_ROOT, UV, WITHINGS_POOL, garmin_then_weather,
    list_recent_activities, pipeline, seen_activities,
)

START = pendulum.datetime(2026, 11, 1, tz="UTC")
POLL_EVERY = "*/30 * * * *"


@dag(schedule=POLL_EVERY, start_date=START, catchup=False, max_active_runs=1, default_args=DEFAULT_ARGS,
     tags=["health", "garmin"])
def garmin_poll():
    """Loads Garmin (and the weather) only when an activity has appeared since the last load. Garmin's
    Personal API can't notify us, so this asks every half hour, which is one small API call."""
    ids = list_recent_activities()

    @task.short_circuit
    def new_activity(ids_json: str) -> bool:
        """False skips everything after it, and the run still counts as a success."""
        new = set(json.loads(ids_json)) - seen_activities()
        print(f"New activities: {sorted(new) or 'none'}")
        return bool(new)

    new_activity(ids) >> garmin_then_weather(ids)


@dag(schedule="0 7 * * *", start_date=START, catchup=False, max_active_runs=1, default_args=DEFAULT_ARGS,
     tags=["health", "garmin"])
def garmin_daily():
    """Loads Garmin once a day whatever happened, for the data that arrives without an activity (sleep, HRV,
    steps, resting heart rate) and for Garmin's own late corrections to the last 3 days."""
    ids = list_recent_activities()
    ids >> garmin_then_weather(ids)


@dag(schedule=POLL_EVERY, start_date=START, catchup=False, max_active_runs=1, default_args=DEFAULT_ARGS,
     tags=["health", "withings"])
def withings_poll():
    """Loads Withings only when a weigh-in was added or edited. Withings could call a webhook instead, but
    it needs a public URL, so that waits for the server."""

    @task.bash(cwd=str(REPO_ROOT), pool=WITHINGS_POOL)
    def count_new_weigh_ins() -> str:
        return f"{UV} run python -m ingest.changes withings"

    @task.short_circuit
    def new_weigh_in(count: str) -> bool:
        print(f"New or edited weigh-ins: {count}")
        return int(count) > 0

    @task.bash(cwd=str(REPO_ROOT), pool=WITHINGS_POOL, outlets=[RAW_WITHINGS])
    def load_withings() -> str:
        return pipeline("withings")

    new_weigh_in(count_new_weigh_ins()) >> load_withings()


@dag(schedule=(RAW_WEATHER | RAW_WITHINGS), start_date=START, catchup=False, max_active_runs=1,
     default_args={**DEFAULT_ARGS, "retries": 0}, tags=["health"])
def transform():
    """Runs whenever new Garmin (with its weather) or Withings data has loaded. --ignore-cron with the
    models' allow_partials processes today so far, so a run shows up within the hour; events that arrive
    while it runs are picked up by one more run straight after."""

    @task.bash(cwd=str(REPO_ROOT))
    def sqlmesh_run() -> str:
        return f"{UV} run sqlmesh -p transform run prod --ignore-cron"

    sqlmesh_run()


@dag(schedule="0 8 * * 1,4", start_date=START, catchup=False, default_args=DEFAULT_ARGS, tags=["health"])
def rebuild_dashboards():
    """Rebuilds the Evidence site on Netlify on Mondays and Thursdays. Each deploy costs Netlify credits,
    so it's on a fixed schedule rather than after every transform. Trigger it by hand for an extra build."""

    @task.bash(cwd=str(REPO_ROOT), retry_delay=pendulum.duration(minutes=1))
    def refresh_bi() -> str:
        # The build hook URL is in .env; -f makes curl fail on an HTTP error.
        return 'set -a && . ./.env && set +a && curl -fsS -X POST "$EVIDENCE_BUILD_HOOK"'

    refresh_bi()


garmin_poll()
garmin_daily()
withings_poll()
transform()
rebuild_dashboards()
