MODEL (
  name marts.fct_daily_health,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column calendar_date,
    lookback 3  -- Garmin keeps revising recent days; matches dlt lookback
  ),
  cron '@daily',
  grain calendar_date,
  audits (
    unique_values(columns := (calendar_date)),
    accepted_range(column := resting_hr, min_v := 25, max_v := 120),
    accepted_range(column := sleep_score, min_v := 0, max_v := 100),
    -- Warnings, not failures: a resting heart rate this far from the usual (illness, a bad night, a
    -- watch worn loose) is worth a look, but it's real data.
    z_score_non_blocking(column := resting_hr, threshold := 4)
  )
);

SELECT
  d.calendar_date,
  d.steps,
  d.floors_ascended,
  d.calories_total,
  d.calories_active,
  d.resting_hr,
  d.min_hr,
  d.max_hr,
  d.intensity_minutes_moderate + 2 * d.intensity_minutes_vigorous AS intensity_minutes,
  d.stress_avg,
  d.body_battery_max,
  d.body_battery_min,
  d.body_battery_charged,
  d.body_battery_drained,
  d.respiration_avg,
  s.sleep_hours,
  s.deep_sleep_hours,
  s.rem_sleep_hours,
  s.sleep_score,
  h.hrv_last_night_avg,
  h.hrv_weekly_avg,
  h.hrv_status,
  v.running_vo2max,
  v.cycling_vo2max
FROM staging.stg_daily_summary AS d
LEFT JOIN staging.stg_sleep AS s USING (calendar_date)
LEFT JOIN staging.stg_hrv AS h USING (calendar_date)
LEFT JOIN staging.stg_vo2max AS v USING (calendar_date)
WHERE d.calendar_date BETWEEN @start_ds AND @end_ds
