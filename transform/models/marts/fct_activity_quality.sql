MODEL (
  name marts.fct_activity_quality,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key activity_id,
    lookback 3
  ),
  cron '@daily',
  grain activity_id,
  audits (
    unique_values(columns := (activity_id)),
    accepted_values(column := likely_type, is_in := ('run', 'ride', 'walk')),
    accepted_range(column := gps_jumps, min_v := 0)
  )
);

-- One row per activity with a recording and at least 1 km: what the recording says it really was, and how
-- far its GPS can be trusted. Best efforts leave out activities that weren't runs and stretches with a GPS
-- jump; the dashboards' Data quality page lists the rest.
--
-- Likely type, from the recording while moving (faster than 1 m/s):
-- - since 2018 the watch records cadence, and a run's median is 150+ steps a minute; under 130 it was a
--   ride (faster than 4 m/s) or a walk
-- - the watch's own speed above 7 m/s (2:23/km) for 5% of the time is a bike, a car or a train
-- - earlier watches recorded no cadence, and the activities averaging faster than 3:50/km over 8 km or more
--   were bike commutes (checked with the owner); slower ones are taken as recorded
WITH activities AS (
  SELECT
    activity_id,
    activity_date,
    activity_name,
    activity_type,
    distance_km,
    duration_minutes
  FROM staging.stg_activities
  WHERE distance_km >= 1
    AND activity_date BETWEEN @start_ds AND @end_ds
),

recordings AS (
  SELECT
    s.activity_id,
    MEDIAN(s.cadence_spm) FILTER (WHERE s.speed_mps > 1) AS median_cadence_spm,
    MEDIAN(s.speed_mps) FILTER (WHERE s.speed_mps > 1)   AS median_speed_mps,
    QUANTILE_CONT(s.speed_mps, 0.95)                     AS p95_speed_mps,
    COUNT(*) FILTER (WHERE s.is_gps_jump)                AS gps_jumps,
    COUNT(*) FILTER (WHERE s.speed_mps > 8)              AS vehicle_speed_samples,
    COUNT(*)                                             AS samples
  FROM staging.stg_activity_samples AS s
  INNER JOIN activities USING (activity_id)
  GROUP BY s.activity_id
),

typed AS (
  SELECT
    a.activity_id,
    a.activity_date,
    a.activity_name,
    a.activity_type,
    a.distance_km,
    r.median_cadence_spm,
    r.median_speed_mps,
    r.p95_speed_mps,
    r.gps_jumps,
    r.vehicle_speed_samples,
    r.samples,
    CASE
      WHEN r.p95_speed_mps > 7 THEN 'ride'
      WHEN r.median_cadence_spm >= 130 THEN 'run'
      WHEN r.median_cadence_spm < 130 AND r.median_speed_mps > 4 THEN 'ride'
      WHEN r.median_cadence_spm < 130 THEN 'walk'
      WHEN a.distance_km >= 8 AND a.duration_minutes / a.distance_km < 3.83 THEN 'ride'
    END                               AS likely_type,
    a.activity_type ILIKE '%running%' AS recorded_as_run
  FROM activities AS a
  INNER JOIN recordings AS r USING (activity_id)
)

SELECT
  activity_id,
  activity_date,
  activity_name,
  activity_type,
  distance_km,
  median_cadence_spm,
  median_speed_mps,
  p95_speed_mps,
  gps_jumps,
  vehicle_speed_samples,
  samples,
  likely_type,
  -- A run recorded as something else only counts when the cadence says so; walks and hikes are often run
  -- in parts, so they're only flagged when the median is a run's.
  COALESCE(
    (recorded_as_run AND likely_type IN ('ride', 'walk'))
    -- half a minute faster than 8 m/s isn't running, whatever the rest was
    OR (recorded_as_run AND vehicle_speed_samples >= 30)
    OR (
      NOT recorded_as_run
      AND likely_type = 'run'
      AND (activity_type ILIKE '%walking%' OR activity_type ILIKE '%hiking%' OR activity_type ILIKE '%cycling%')
    ),
    FALSE
  )
    AS is_type_mismatch,
  -- Runs only: a ride's speed is normal at 8 m/s. A jump or two happens in clean runs too (a tunnel, tall
  -- buildings); best efforts skip just those stretches.
  COALESCE(
    recorded_as_run AND likely_type IS DISTINCT FROM 'ride' AND likely_type IS DISTINCT FROM 'walk' AND gps_jumps >= 3,
    FALSE
  ) AS is_gps_suspect
FROM typed
