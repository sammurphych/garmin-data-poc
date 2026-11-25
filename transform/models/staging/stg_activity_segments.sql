MODEL (
  name staging.stg_activity_segments,
  kind VIEW,
  grain (activity_id, segment_type),
  audits (
    not_null(columns := (activity_id, segment_type)),
    unique_combination_of_columns(columns := (activity_id, segment_type))
  )
);

-- Garmin's splitSummaries: totals per segment type of an activity. INTERVAL_* are the steps of a structured
-- workout (warm-up, active, recovery, cool-down); RWD_* are run/walk/stand detection.
SELECT
  a.activity_id,
  CAST(a.start_time_local AS DATE) AS activity_date,
  ss.split_type                    AS segment_type,
  ss.no_of_splits                  AS segment_count,
  ss.distance / 1000.0             AS distance_km,
  ss.duration / 60.0               AS duration_minutes,
  ss.average_speed                 AS avg_speed_mps,
  ss.max_speed                     AS max_speed_mps,
  ss.total_ascent                  AS elevation_gain_m,
  ss.elevation_loss                AS elevation_loss_m
FROM raw_garmin.activities__split_summaries AS ss
INNER JOIN raw_garmin.activities AS a ON ss._dlt_parent_id = a._dlt_id
