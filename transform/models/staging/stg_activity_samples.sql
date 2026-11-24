MODEL (
  name staging.stg_activity_samples,
  kind VIEW,
  grain (activity_id, sample_index),
  audits (not_null(columns := (activity_id, sample_index)))
);

-- One row per recorded sample of an activity, about one a second. Older watches record fewer metrics.
SELECT
  activity_id,
  sample_index,
  sum_duration                  AS timer_s,     -- timer time: excludes auto-pause and manual pauses
  sum_elapsed_duration          AS elapsed_s,
  sum_moving_duration           AS moving_s,
  sum_distance                  AS distance_m,
  direct_heart_rate             AS heart_rate,
  direct_speed                  AS speed_mps,
  direct_grade_adjusted_speed   AS gap_speed_mps,
  direct_elevation              AS elevation_m,
  direct_double_cadence         AS cadence_spm,  -- steps per minute, both feet
  direct_power                  AS power_w,
  direct_stride_length          AS stride_length_cm,
  direct_ground_contact_time    AS ground_contact_ms,
  direct_vertical_oscillation   AS vertical_oscillation_cm,
  direct_respiration_rate       AS respiration_rate,
  direct_performance_condition  AS performance_condition,
  direct_available_stamina      AS available_stamina
FROM raw_garmin.activity_samples
