"""dlt source for hourly weather at the start of each activity, from Open-Meteo's historical API.

Open-Meteo is free for non-commercial use and needs no key. It is sent only the activity's start
point rounded to 0.1 degrees (about 11 km, roughly the city), never the route or the exact spot.

The weather for an area is fetched in windows of at most 14 days around the days with
activities there, since Open-Meteo counts a longer range as several calls. Fetched days are
remembered in dlt state; the last week is fetched again on later runs, because Open-Meteo
replaces its first estimate for recent days with measured data a few days later.
"""

from __future__ import annotations

import time
from collections import defaultdict
from datetime import date, timedelta
from typing import Iterable, Iterator

import dlt
from dlt.sources.helpers import requests

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY = ["temperature_2m", "apparent_temperature", "relative_humidity_2m", "wind_speed_10m"]
MAX_WINDOW_DAYS = 14
REVISED_DAYS = 7  # Open-Meteo's recent days are provisional for about this long
REQUEST_PAUSE_S = 0.2  # Open-Meteo allows 600 calls a minute
GRID_DEGREES = 0.1  # rounding of the start point: about 11 km


def area(lat: float, lon: float) -> tuple[float, float]:
    """The rounded start point that stands for an activity's area."""
    return round(round(lat / GRID_DEGREES) * GRID_DEGREES, 1), round(round(lon / GRID_DEGREES) * GRID_DEGREES, 1)


def _windows(days: Iterable[date]) -> Iterator[tuple[date, date]]:
    """Group sorted days into ranges of at most MAX_WINDOW_DAYS, each ending a day after its last
    activity day so activities that run past midnight UTC still have their weather."""
    start = prev = None
    for d in sorted(days):
        if start is None:
            start = prev = d
        elif (d - start).days + 2 <= MAX_WINDOW_DAYS:
            prev = d
        else:
            yield start, prev + timedelta(days=1)
            start = prev = d
    if start is not None:
        yield start, prev + timedelta(days=1)


@dlt.source(name="weather")
def weather_source(activity_starts: list[tuple[int, float, float, date]]):
    """activity_starts: (activity id, start latitude, start longitude, UTC start date) of every activity
    with a location."""

    # Which area's weather applies to each activity. Only the rounded point is kept.
    @dlt.resource(name="activity_areas", primary_key="activity_id", write_disposition="merge",
                  columns={"activity_id": {"data_type": "bigint"}})
    def activity_areas():
        rows = []
        for activity_id, lat, lon, _ in activity_starts:
            a_lat, a_lon = area(lat, lon)
            rows.append({"activity_id": activity_id, "latitude": a_lat, "longitude": a_lon})
        yield rows

    @dlt.resource(name="hourly", primary_key=("latitude", "longitude", "time_utc"), write_disposition="merge",
                  columns={h: {"data_type": "double"} for h in HOURLY} | {"time_utc": {"data_type": "timestamp"}})
    def hourly():
        state = dlt.current.resource_state()
        fetched = set(state.get("fetched_days", []))  # "lat|lon|YYYY-MM-DD"
        settled_before = date.today() - timedelta(days=REVISED_DAYS)

        days_by_area: dict[tuple[float, float], set[date]] = defaultdict(set)
        for _, lat, lon, d in activity_starts:
            a = area(lat, lon)
            if f"{a[0]}|{a[1]}|{d.isoformat()}" not in fetched:
                days_by_area[a].add(d)

        for (lat, lon), days in sorted(days_by_area.items()):
            for start, end in _windows(days):
                end = min(end, date.today())
                resp = requests.get(ARCHIVE_URL, params={
                    "latitude": lat, "longitude": lon, "timezone": "GMT",
                    "start_date": start.isoformat(), "end_date": end.isoformat(), "hourly": ",".join(HOURLY),
                }, timeout=60)
                resp.raise_for_status()
                time.sleep(REQUEST_PAUSE_S)
                h = resp.json()["hourly"]
                yield [{"latitude": lat, "longitude": lon, "time_utc": t, **{v: h[v][i] for v in HOURLY}}
                       for i, t in enumerate(h["time"]) if h["temperature_2m"][i] is not None]
                fetched.update(f"{lat}|{lon}|{d.isoformat()}" for d in days
                               if start <= d <= end and d < settled_before)
                state["fetched_days"] = sorted(fetched)

    return activity_areas, hourly
