MODEL (
  name marts.fct_activities,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key activity_id,
    lookback 3
  ),
  cron '@daily',
  grain activity_id,
  audits (
    unique_values(columns := (activity_id)),
    accepted_range(column := distance_km, min_v := 0, max_v := 500)
  )
);

SELECT
  activity_id,
  activity_name,
  activity_type,
  event_type,
  activity_date,
  start_time_local,
  start_time_utc,
  distance_km,
  duration_minutes,
  moving_minutes,
  elevation_gain_m,
  CASE WHEN distance_km > 0 AND activity_type ILIKE '%running%'
       THEN moving_minutes / distance_km END AS pace_min_per_km,
  avg_hr,
  max_hr,
  calories,
  training_load,
  aerobic_training_effect,
  anaerobic_training_effect
FROM staging.stg_activities
WHERE activity_date BETWEEN @start_ds AND @end_ds
