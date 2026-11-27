"""The health pipeline as five DAGs that hand over through Assets; see health_common.py for the picture.

Everything starts paused. Run them in one place at a time (the laptop or the server): two copies would
both load and both rebuild the dashboards, which costs Netlify credits.
"""

from __future__ import annotations

import json

import pendulum
from airflow.sdk import Variable, dag, task

from health_common import (
    DEFAULT_ARGS, RAW_WEATHER, RAW_WITHINGS, REPO_ROOT, UV, WITHINGS_POOL, garmin_then_weather,
    list_recent_activities, pipeline, seen_activities, send_ntfy,
)

START = pendulum.datetime(2026, 11, 1, tz="UTC")
POLL_EVERY = "*/30 * * * *"
# The data_checks keys already notified, so each flag is sent once.
ALERTED_FLAGS = "data_checks_alerted"


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

    @task.short_circuit
    def on_schedule(dag_run=None) -> bool:
        """With catchup=False Airflow still runs the latest missed slot straight away, e.g. on switching the
        DAG on or after the laptop wakes. Skip the build then: the next slot rebuilds anyway."""
        if dag_run.run_type != "scheduled":
            return True
        late = pendulum.now("UTC") - dag_run.run_after
        print(f"{late} after the scheduled time")
        return late < pendulum.duration(hours=2)

    @task.bash(cwd=str(REPO_ROOT), retry_delay=pendulum.duration(minutes=1))
    def refresh_bi() -> str:
        # The build hook URL is in .env; -f makes curl fail on an HTTP error.
        return 'set -a && . ./.env && set +a && curl -fsS -X POST "$EVIDENCE_BUILD_HOOK"'

    on_schedule() >> refresh_bi()


@dag(schedule="0 9 * * *", start_date=START, catchup=False, max_active_runs=1, default_args=DEFAULT_ARGS,
     tags=["health"])
def data_checks():
    """Once a day, after the morning load: a phone notification for each newly flagged activity or weigh-in
    (transform/data_checks.py says what's flagged), and one if Garmin data has stopped arriving. Flags are
    suspect data, not failures, so they're normal-priority notifications and the run still succeeds."""

    @task.bash(cwd=str(REPO_ROOT))
    def check_marts() -> str:
        return f"{UV} run python transform/data_checks.py"

    @task
    def notify(result_json: str) -> None:
        result = json.loads(result_json)
        behind = result["garmin_days_behind"]
        if behind is not None and behind > 2:
            send_ntfy("No new Garmin data", f"The latest day with Garmin data is {behind} days ago. Is the "
                      "Personal API key still valid? Check the garmin_daily runs.", priority="high", tags="warning")
        sent = set(json.loads(Variable.get(ALERTED_FLAGS, default="[]")))
        new = [f for f in result["flagged"] if f["key"] not in sent]
        for flag in new:
            send_ntfy(flag["title"], flag["detail"], tags="mag")
        print(f"Garmin {behind} days behind; {len(new)} new flags of {len(result['flagged'])}")
        # Keep only the keys still in the 14-day window, so the Variable doesn't grow forever.
        Variable.set(ALERTED_FLAGS, json.dumps(sorted({f["key"] for f in result["flagged"]})))

    notify(check_marts())


garmin_poll()
garmin_daily()
withings_poll()
transform()
rebuild_dashboards()
data_checks()
