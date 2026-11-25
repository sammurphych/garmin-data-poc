MODEL (
  name marts.fct_activity_segments,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key (activity_id, segment_type),
    lookback 3
  ),
  cron '@daily',
  grain (activity_id, segment_type),
  audits (unique_combination_of_columns(columns := (activity_id, segment_type)))
);

SELECT
  activity_id,
  activity_date,
  segment_type,
  segment_count,
  distance_km,
  duration_minutes,
  CASE WHEN distance_km > 0 THEN duration_minutes / distance_km END AS pace_min_per_km,
  avg_speed_mps,
  max_speed_mps,
  elevation_gain_m,
  elevation_loss_m
FROM staging.stg_activity_segments
WHERE activity_date BETWEEN @start_ds AND @end_ds
