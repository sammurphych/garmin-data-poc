MODEL (
  name staging.stg_activities,
  kind VIEW,
  grain activity_id,
  audits (not_null(columns := (activity_id, start_time_local)), unique_values(columns := (activity_id)))
);

SELECT
  activity_id,
  activity_name,
  activity_type__type_key                  AS activity_type,
  event_type__type_key                     AS event_type,
  CAST(start_time_local AS TIMESTAMP)      AS start_time_local,
  CAST(start_time_gmt AS TIMESTAMP)        AS start_time_utc,
  CAST(start_time_local AS DATE)           AS activity_date,
  distance / 1000.0                        AS distance_km,
  duration / 60.0                          AS duration_minutes,
  moving_duration / 60.0                   AS moving_minutes,
  elevation_gain                           AS elevation_gain_m,
  average_speed                            AS avg_speed_mps,
  average_hr                               AS avg_hr,
  max_hr,
  calories,
  activity_training_load                   AS training_load,
  aerobic_training_effect,
  anaerobic_training_effect
FROM raw_garmin.activities
