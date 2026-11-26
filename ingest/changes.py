"""Cheap checks for new data, so an orchestrator can poll often and load only when something arrived.

    uv run python -m ingest.changes garmin     # JSON list of activity ids started in the last few days
    uv run python -m ingest.changes withings   # number of weigh-ins added or edited since the last load

Both make one small read-only API call (plus a token request) and never touch the warehouse, so
polling every half hour costs no MotherDuck compute. The result is the last line printed, which is
what Airflow's bash tasks pass on to the next task.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
RECENT_DAYS = 3  # activities recorded offline can sync a day or two late


def recent_garmin_activity_ids() -> list[int]:
    """Ids of the activities that started in the last RECENT_DAYS days, as Garmin lists them now."""
    from ingest.garmin_source import ACTIVITY_PAGE_SIZE, GarminClient

    today = date.today()
    page = GarminClient(os.environ["GARMIN_API_KEY"]).get("/activitylist-service/activities/search/activities", {
        "startDate": (today - timedelta(days=RECENT_DAYS)).isoformat(), "endDate": today.isoformat(),
        "start": 0, "limit": ACTIVITY_PAGE_SIZE, "sortOrder": "asc",
    }) or []
    return sorted(a["activityId"] for a in page)


def new_withings_weigh_ins() -> int:
    """Weigh-ins Withings has added or changed since the last load's `lastupdate`."""
    import dlt
    from dlt.sources.helpers import requests

    from ingest.withings_auth import get_access_token
    from ingest.withings_source import CATEGORY_REAL, MEASURE_URL

    try:
        state = dlt.attach(f"withings_{os.getenv('WAREHOUSE', 'local')}",
                           pipelines_dir=os.getenv("DLT_PIPELINES_DIR")).state
        last_update = state["sources"]["withings"]["resources"]["measures"]["last_update"]
    except Exception:  # no local load yet (a fresh machine): let the load run and restore its state
        return 1
    # last_update is the time of the newest change already loaded, and Withings returns changes at or after
    # `lastupdate`, so +1 leaves out the weigh-in that set it.
    resp = requests.post(MEASURE_URL, headers={"Authorization": f"Bearer {get_access_token()}"}, data={
        "action": "getmeas", "meastype": 1, "category": CATEGORY_REAL, "lastupdate": last_update + 1,
    }, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("status") != 0:
        raise RuntimeError(f"Withings getmeas failed: status={payload.get('status')} error={payload.get('error')}")
    return len(payload["body"].get("measuregrps", []))


if __name__ == "__main__":
    load_dotenv(REPO_ROOT / ".env")
    check = sys.argv[1] if len(sys.argv) > 1 else ""
    if check == "garmin":
        print(json.dumps(recent_garmin_activity_ids()))
    elif check == "withings":
        print(new_withings_weigh_ins())
    else:
        sys.exit("usage: python -m ingest.changes garmin|withings")
