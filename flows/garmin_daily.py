"""Daily health pipeline: dlt loads (Garmin, Withings) -> SQLMesh run -> Evidence (Netlify) rebuild.

Local:  uv run python flows/garmin_daily.py        (reads .env)
Cloud:  deployed via prefect.yaml to a Prefect Managed work pool, secrets from Secret blocks.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))  # so `ingest` imports when run from the cloned repo

# Prefect Secret block name -> env var the code reads.
SECRETS = {
    "garmin-api-key": "GARMIN_API_KEY",
    "motherduck-token": "MOTHERDUCK_TOKEN",
    "evidence-build-hook": "EVIDENCE_BUILD_HOOK",
    "withings-client-id": "WITHINGS_CLIENT_ID",
    "withings-client-secret": "WITHINGS_CLIENT_SECRET",
    "sqlmesh-state-db-url": "SQLMESH_STATE_DB_URL",
}


def load_secrets() -> None:
    """Prefer Prefect Secret blocks; fall back to env / .env for local runs."""
    load_dotenv(REPO_ROOT / ".env")
    for block, env in SECRETS.items():
        if os.getenv(env):
            continue
        try:
            os.environ[env] = Secret.load(block).get()
        except Exception:  # block missing, or local run not connected to Cloud
            pass


@task(retries=2, retry_delay_seconds=300)
def extract_garmin() -> str:
    from ingest.pipeline import run_garmin

    info = run_garmin()
    get_run_logger().info(str(info))
    return str(info)


@task(retries=1, retry_delay_seconds=300)
def extract_withings() -> str:
    from ingest.pipeline import run_withings, withings_configured

    if not withings_configured():
        get_run_logger().warning("Withings client id/secret not set; skipping Withings")
        return "skipped"
    info = run_withings()
    get_run_logger().info(str(info))
    return str(info)


@task
def transform() -> None:
    from sqlmesh import Context

    ctx = Context(paths=str(REPO_ROOT / "transform"))
    result = ctx.run(environment="prod")
    # Newer SQLMesh returns a CompletionStatus, older returns bool.
    failed = getattr(result, "is_failure", None)
    if failed is True or result is False:
        raise RuntimeError("SQLMesh run failed (see logs; audits may have failed)")


@task(retries=2, retry_delay_seconds=60)
def refresh_bi() -> None:
    hook = os.getenv("EVIDENCE_BUILD_HOOK")
    if not hook:
        get_run_logger().warning("EVIDENCE_BUILD_HOOK not set; skipping dashboard rebuild")
        return
    httpx.post(hook, timeout=30).raise_for_status()


@flow(name="garmin-daily", log_prints=True)
def garmin_daily() -> None:
    load_secrets()
    extract_garmin()
    # A Withings failure (e.g. an expired login) shouldn't stop Garmin data reaching the
    # dashboards, but the run still ends Failed so it gets noticed.
    withings = extract_withings(return_state=True)
    transform()
    refresh_bi()
    if withings.is_failed():
        raise RuntimeError("Withings load failed; Garmin, SQLMesh and dashboards still updated")


if __name__ == "__main__":
    garmin_daily()
