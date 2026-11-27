"""Checks on the modelled data that are worth a phone notification, for Airflow's data_checks DAG.

    uv run python transform/data_checks.py

Prints one line of JSON (the last line is what Airflow's bash task passes on):
    {"garmin_days_behind": 1, "flagged": [{"key": "activity:123", "title": "...", "detail": "..."}]}

- garmin_days_behind: days since the latest day with Garmin wellness data. More than 2 usually means the
  loads are failing quietly or the Personal API key expired (it lasts a year).
- flagged: activities and weigh-ins from the last 14 days that marts.fct_activity_quality or
  marts.fct_weight_daily flag. The DAG remembers the keys it has already sent, so each is sent once.

The SQLMesh audits catch data that's wrong (and stop the run); these catch data that's real but suspect.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
RECENT_DAYS = 14


def run_checks(con: duckdb.DuckDBPyConnection) -> dict:
    behind = con.sql("""
        select current_date - max(calendar_date)
        from marts.fct_daily_health where steps is not null
    """).fetchone()[0]
    activities = con.sql(f"""
        select activity_id, activity_date, activity_name, activity_type, likely_type, is_type_mismatch,
               gps_jumps, vehicle_speed_samples
        from marts.fct_activity_quality
        where activity_date >= current_date - {RECENT_DAYS} and (is_type_mismatch or is_gps_suspect)
        order by activity_date
    """).fetchall()
    weigh_ins = con.sql(f"""
        select calendar_date, weight_kg, change_kg
        from marts.fct_weight_daily
        where calendar_date >= current_date - {RECENT_DAYS} and is_suspect
        order by calendar_date
    """).fetchall()

    flagged = []
    for activity_id, day, name, recorded, likely, mismatch, jumps, vehicle in activities:
        if mismatch and vehicle >= 30:
            detail = f"Recorded as {recorded}, but {vehicle} s faster than 8 m/s: a vehicle?"
        elif mismatch:
            detail = f"Recorded as {recorded}, but it looks like a {likely}. Change the type in Garmin Connect?"
        else:
            detail = f"{jumps} GPS jumps; best efforts skip those stretches."
        flagged.append({"key": f"activity:{activity_id}", "title": f"Check {name} ({day})", "detail": detail})
    for day, weight, change in weigh_ins:
        change_text = f"{change:+.1f} kg on the previous weigh-in" if change is not None else "far from the day's lowest"
        flagged.append({"key": f"weight:{day}", "title": f"Check the weigh-in on {day}",
                        "detail": f"{weight:.1f} kg, {change_text}. Someone else on the scale?"})
    return {"garmin_days_behind": behind, "flagged": flagged}


if __name__ == "__main__":
    load_dotenv(REPO_ROOT / ".env")
    if os.getenv("WAREHOUSE", "local") == "motherduck":
        database = f"md:{os.getenv('MOTHERDUCK_DATABASE', 'health')}?motherduck_token={os.environ['MOTHERDUCK_TOKEN']}"
    else:
        database = str(REPO_ROOT / "health.duckdb")
    with duckdb.connect(database, read_only=not database.startswith("md:")) as con:
        print(json.dumps(run_checks(con), default=str))
