MODEL (
  name staging.stg_sleep,
  kind VIEW,
  grain calendar_date,
  audits (not_null(columns := (calendar_date)), unique_values(columns := (calendar_date)))
);

SELECT
  CAST(calendar_date AS DATE)  AS calendar_date,  -- the date you woke up
  sleep_time_seconds / 3600.0  AS sleep_hours,
  deep_sleep_seconds / 3600.0  AS deep_sleep_hours,
  light_sleep_seconds / 3600.0 AS light_sleep_hours,
  rem_sleep_seconds / 3600.0   AS rem_sleep_hours,
  awake_sleep_seconds / 3600.0 AS awake_hours,
  awake_count,
  avg_sleep_stress,
  avg_heart_rate               AS sleep_avg_hr,
  sleep_scores__overall__value AS sleep_score
FROM raw_garmin.sleep
WHERE sleep_time_seconds IS NOT NULL
