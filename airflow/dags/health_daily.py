"""Daily health pipeline in Airflow, the same steps as flows/garmin_daily.py in Prefect.

    extract_garmin ──> extract_weather ──┐
    extract_withings ────────────────────┴─> loads_finished ─┐
    extract_garmin ──────────────────────────────────────────┴─> transform ─> rebuild_today ─> refresh_bi
    extract_withings, extract_weather ─> fail_if_a_load_failed

- Weather waits for Garmin because it looks up the activities Garmin just loaded.
- A Withings or weather failure doesn't stop the transform (loads_finished waits for them whatever happens),
  but fail_if_a_load_failed then fails, so the run ends Failed and gets noticed. A Garmin failure stops the
  transform.
- The dashboards rebuild only on Mondays and Thursdays (UTC), because each Netlify deploy costs credits;
  the rebuild_dashboards param forces it on or off for a manual run.

The tasks run the pipeline in the project's own venv with `uv run`, so Airflow's pinned dependencies never
meet dlt's, SQLMesh's or Prefect's. The pipeline and SQLMesh read their secrets from the repo's .env, and
Withings still keeps its rotating token in the Prefect Secret, so `prefect cloud login` must be done.
"""

from __future__ import annotations

import shutil
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Param, dag, task

REPO_ROOT = Path(__file__).resolve().parents[2]
UV = shutil.which("uv") or "uv"
DASHBOARD_REBUILD_WEEKDAYS = {0, 3}  # Monday, Thursday


@dag(
    schedule="0 7 * * *",  # an hour after Prefect's 06:00 UTC run, so the two never load Withings at once
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,  # don't run every missed day since start_date when the DAG is switched on
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    params={
        "rebuild_dashboards": Param(
            None, type=["null", "boolean"],
            description="true or false forces the Netlify rebuild on or off; empty follows the Mon/Thu schedule",
        ),
    },
    tags=["health"],
    doc_md=__doc__,
)
def health_daily():
    @task.bash(cwd=str(REPO_ROOT), retries=2)
    def extract_garmin() -> str:
        return f"{UV} run python -m ingest.pipeline garmin"

    @task.bash(cwd=str(REPO_ROOT))
    def extract_withings() -> str:
        return f"{UV} run python -m ingest.pipeline withings"

    @task.bash(cwd=str(REPO_ROOT))
    def extract_weather() -> str:
        return f"{UV} run python -m ingest.pipeline weather"

    # Succeeds once both optional loads have finished, whether they worked or not.
    loads_finished = EmptyOperator(task_id="loads_finished", trigger_rule="all_done")

    @task.bash(cwd=str(REPO_ROOT), retries=0)
    def transform() -> str:
        return f"{UV} run sqlmesh -p transform run prod"

    @task.short_circuit
    def rebuild_today(params: dict, dag_run) -> bool:
        """False skips refresh_bi."""
        forced = params["rebuild_dashboards"]
        if forced is not None:
            return forced
        return dag_run.run_after.weekday() in DASHBOARD_REBUILD_WEEKDAYS

    @task.bash(cwd=str(REPO_ROOT), retries=2, retry_delay=timedelta(minutes=1))
    def refresh_bi() -> str:
        # The build hook URL is in .env; -f makes curl fail on an HTTP error.
        return 'set -a && . ./.env && set +a && curl -fsS -X POST "$EVIDENCE_BUILD_HOOK"'

    # Runs only if an upstream load failed, and then fails, so the run ends Failed.
    @task(trigger_rule="one_failed", retries=0)
    def fail_if_a_load_failed() -> None:
        raise RuntimeError("Withings or weather load failed; Garmin, SQLMesh and dashboards still updated")

    garmin, withings, weather = extract_garmin(), extract_withings(), extract_weather()
    garmin >> weather
    [weather, withings] >> loads_finished
    [garmin, loads_finished] >> transform() >> rebuild_today() >> refresh_bi()
    [withings, weather] >> fail_if_a_load_failed()


health_daily()
