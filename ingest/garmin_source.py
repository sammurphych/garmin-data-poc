"""dlt source for the Garmin Connect Personal API (read-only, own data).

Each resource keeps the last loaded date in dlt state (stored in the destination,
so it survives ephemeral Prefect Managed workers). Every run re-pulls the last
`lookback_days` because Garmin keeps updating recent days (late syncs, sleep
scoring), and `merge` on the natural key keeps rows unique.
"""

from __future__ import annotations

import time
from datetime import date, timedelta
from typing import Iterator

import dlt
from dlt.sources.helpers import requests

from ingest.garmin_auth import BASE_URL, get_access_token

# Max date range per request, from Garmin's Personal API spec.
MAX_DAYS_DAILY_SUMMARY = 365
MAX_DAYS_SLEEP = 31
MAX_DAYS_HRV = 28  # not documented; kept conservative
MAX_DAYS_ACTIVITIES = 365
MAX_DAYS_VO2MAX = 365  # not documented; a full year is accepted
ACTIVITY_PAGE_SIZE = 99
# Per-second recordings are ~1 MB and ~2 s each, so a run fetches at most this many days of them.
# The first backfill (~2,500 runs since 2009) then spreads over a couple of weeks of daily runs,
# and an interrupted run loses at most one window.
SAMPLES_DAYS_PER_RUN = 366
DETAIL_MAX_SAMPLES = 100_000  # above any real activity (a 5 h run at 1 Hz is 18,000), so nothing is downsampled
# Left out of the recordings: exact location shows where you live and run, and nothing downstream needs it.
GPS_METRICS = {"directLatitude", "directLongitude"}
REQUEST_PAUSE_S = 0.5  # rate limits aren't published, so be polite


class GarminClient:
    def __init__(self, api_key: str):
        self._headers = {"Authorization": f"Bearer {get_access_token(api_key)}", "Accept": "application/json"}

    def get(self, path: str, params: dict | None = None):
        resp = requests.get(f"{BASE_URL}{path}", headers=self._headers, params=params, timeout=60)
        time.sleep(REQUEST_PAUSE_S)
        if resp.status_code == 204 or not resp.content:
            return None
        resp.raise_for_status()
        return resp.json()


def _chunks(start: date, end: date, max_days: int) -> Iterator[tuple[date, date]]:
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=max_days - 1), end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def _window(start_date: str, lookback_days: int, max_days: int | None = None) -> tuple[date, date]:
    """Date range to fetch for the current resource, based on its dlt state.

    With max_days, the range stops max_days after its start, so a long backfill
    advances one window per run instead of fetching everything at once.
    """
    state = dlt.current.resource_state()
    first = date.fromisoformat(start_date)
    last = state.get("last_loaded_date")
    begin = max(first, date.fromisoformat(last) - timedelta(days=lookback_days)) if last else first
    end = date.today()
    if max_days is not None:
        end = min(end, begin + timedelta(days=max_days - 1))
    return begin, end


def _mark_loaded(end: date) -> None:
    dlt.current.resource_state()["last_loaded_date"] = end.isoformat()


# Nested objects (heartRate.restingValue, sleepScores.overall.value, ...) are flattened by dlt
# into columns like heart_rate__resting_value; remaining lists become child tables.
@dlt.source(name="garmin")
def garmin_source(
    api_key: str = dlt.secrets.value,
    start_date: str = "2009-12-01",
    lookback_days: int = 3,
):
    client = GarminClient(api_key)

    @dlt.resource(name="daily_summaries", primary_key="calendarDate", write_disposition="merge")
    def daily_summaries():
        begin, end = _window(start_date, lookback_days)
        for s, e in _chunks(begin, end, MAX_DAYS_DAILY_SUMMARY):
            body = client.get("/usersummary-service/v2/dailysummaries",
                              {"startDate": s.isoformat(), "endDate": e.isoformat()})
            if body:
                yield body.get("data", body) if isinstance(body, dict) else body
        _mark_loaded(end)

    def activity_pages(begin: date, end: date) -> Iterator[list[dict]]:
        for s, e in _chunks(begin, end, MAX_DAYS_ACTIVITIES):
            offset = 0
            while True:
                page = client.get("/activitylist-service/activities/search/activities", {
                    "startDate": s.isoformat(), "endDate": e.isoformat(),
                    "start": offset, "limit": ACTIVITY_PAGE_SIZE, "sortOrder": "asc",
                }) or []
                if page:
                    yield page
                if len(page) < ACTIVITY_PAGE_SIZE:
                    break
                offset += ACTIVITY_PAGE_SIZE

    # splitSummaries (per-segment totals: warm-up, intervals, run/walk) arrives with each activity and lands
    # in the child table activities__split_summaries.
    @dlt.resource(name="activities", primary_key="activityId", write_disposition="merge")
    def activities():
        begin, end = _window(start_date, lookback_days)
        for page in activity_pages(begin, end):
            # Drop social/large nested lists we don't model.
            for a in page:
                a.pop("comments", None)
                a.pop("summarizedExerciseSets", None)
            yield page
        _mark_loaded(end)

    # One row per recorded sample (about one a second) of each activity: elapsed/timer/moving time, distance,
    # heart rate, speed, grade-adjusted speed, elevation, cadence, power, running dynamics. Column names follow
    # Garmin's metric keys (directHeartRate -> direct_heart_rate), and older watches record fewer of them.
    # An activity's rows are replaced as a whole when it is fetched again (lookback days).
    @dlt.resource(name="activity_samples", merge_key="activity_id",
                  write_disposition={"disposition": "merge", "strategy": "delete-insert"},
                  columns={"activity_id": {"data_type": "bigint"}, "sample_index": {"data_type": "bigint"}})
    def activity_samples():
        begin, end = _window(start_date, lookback_days, max_days=SAMPLES_DAYS_PER_RUN)
        for page in activity_pages(begin, end):
            for a in page:
                body = client.get(f"/activity-service/activity/{a['activityId']}/details",
                                  {"maxChartSize": DETAIL_MAX_SAMPLES, "maxPolylineSize": 0})
                samples = (body or {}).get("activityDetailMetrics") or []
                if not samples:
                    continue  # manual entries and some indoor activities have no recording
                keys = {d["metricsIndex"]: d["key"] for d in body["metricDescriptors"]}
                yield [
                    {"activity_id": a["activityId"], "sample_index": i,
                     **{keys[j]: v for j, v in enumerate(s["metrics"]) if keys.get(j) not in GPS_METRICS}}
                    for i, s in enumerate(samples)
                ]
        _mark_loaded(end)

    @dlt.resource(name="sleep", primary_key="calendarDate", write_disposition="merge")
    def sleep():
        begin, end = _window(start_date, lookback_days)
        for s, e in _chunks(begin, end, MAX_DAYS_SLEEP):
            body = client.get("/sleep-service/sleep/dailySleepSummariesByDate",
                              {"startDate": s.isoformat(), "endDate": e.isoformat()})
            # Nights with no recorded sleep come back as placeholder rows with every field null.
            nights = [n for n in body or [] if n.get("calendarDate")]
            if nights:
                yield nights
        _mark_loaded(end)

    @dlt.resource(name="hrv", primary_key="calendarDate", write_disposition="merge")
    def hrv():
        begin, end = _window(start_date, lookback_days)
        for s, e in _chunks(begin, end, MAX_DAYS_HRV):
            body = client.get(f"/hrv-service/hrv/daily/{s.isoformat()}/{e.isoformat()}")
            if body and body.get("hrvSummaries"):
                yield body["hrvSummaries"]
        _mark_loaded(end)

    # Flattened here rather than by dlt, so the cycling column exists even when no cycling estimate has arrived yet.
    @dlt.resource(name="vo2max", primary_key="calendarDate", write_disposition="merge",
                  columns={"runningVo2max": {"data_type": "double"}, "cyclingVo2max": {"data_type": "double"}})
    def vo2max():
        begin, end = _window(start_date, lookback_days)
        for s, e in _chunks(begin, end, MAX_DAYS_VO2MAX):
            days = client.get(f"/metrics-service/metrics/maxmet/daily/{s.isoformat()}/{e.isoformat()}") or []
            # Each day has a running ("generic") and a cycling estimate; days with neither are skipped.
            rows = [{"calendarDate": (d.get("generic") or d.get("cycling"))["calendarDate"],
                     "runningVo2max": (d.get("generic") or {}).get("vo2MaxPreciseValue"),
                     "cyclingVo2max": (d.get("cycling") or {}).get("vo2MaxPreciseValue")}
                    for d in days if d.get("generic") or d.get("cycling")]
            if rows:
                yield rows
        _mark_loaded(end)

    return daily_summaries, activities, activity_samples, sleep, hrv, vo2max
