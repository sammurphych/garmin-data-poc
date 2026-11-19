"""dlt source for Withings body measurements (scales).

One row per weigh-in ("measure group"), with each measurement type as its own
column. Incremental on Withings' `lastupdate`, which returns groups created or
changed since the last run; `merge` on `grpid` keeps edited weigh-ins unique.
"""

from __future__ import annotations

from datetime import datetime, timezone

import dlt
from dlt.sources.helpers import requests

from ingest.withings_auth import get_access_token

MEASURE_URL = "https://wbsapi.withings.net/measure"
CATEGORY_REAL = 1  # 2 = user goals, which we skip

# Withings meastype code -> column name.
MEASURE_TYPES = {
    1: "weight_kg",
    5: "fat_free_mass_kg",
    6: "fat_ratio_pct",
    8: "fat_mass_kg",
    76: "muscle_mass_kg",
    77: "hydration_kg",
    88: "bone_mass_kg",
}


def _to_row(group: dict, tz: str | None) -> dict:
    row = {
        "grpid": group["grpid"],
        "measured_at": datetime.fromtimestamp(group["date"], tz=timezone.utc),
        "timezone": tz,
        "attrib": group.get("attrib"),
        "deviceid": group.get("deviceid"),
        "created_at": datetime.fromtimestamp(group["created"], tz=timezone.utc) if group.get("created") else None,
    }
    for m in group.get("measures", []):
        if m["type"] in MEASURE_TYPES:
            # Withings sends value and a power-of-ten unit: 72350, -3 -> 72.35 kg.
            unit = m["unit"]
            row[MEASURE_TYPES[m["type"]]] = m["value"] / 10 ** -unit if unit < 0 else m["value"] * 10 ** unit
    return row


@dlt.source(name="withings")
def withings_source():
    headers = {"Authorization": f"Bearer {get_access_token()}"}

    @dlt.resource(
        name="measures",
        primary_key="grpid",
        write_disposition="merge",
        columns={name: {"data_type": "double"} for name in MEASURE_TYPES.values()},
    )
    def measures():
        state = dlt.current.resource_state()
        params = {
            "action": "getmeas",
            "meastypes": ",".join(map(str, MEASURE_TYPES)),
            "category": CATEGORY_REAL,
            "lastupdate": state.get("last_update", 0),
        }
        while True:
            resp = requests.post(MEASURE_URL, headers=headers, data=params, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            if payload.get("status") != 0:
                raise RuntimeError(f"Withings getmeas failed: status={payload.get('status')} error={payload.get('error')}")
            body = payload["body"]
            rows = [_to_row(g, body.get("timezone")) for g in body.get("measuregrps", [])
                    if g.get("category") == CATEGORY_REAL]
            if rows:
                yield rows
            if not body.get("more"):
                break
            params["offset"] = body["offset"]
        state["last_update"] = body.get("updatetime", params["lastupdate"])

    return measures
