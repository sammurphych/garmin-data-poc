MODEL (
  name marts.fct_best_efforts,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key (activity_id, effort),
    lookback 3
  ),
  cron '@daily',
  grain (activity_id, effort),
  audits (
    unique_combination_of_columns(columns := (activity_id, effort)),
    accepted_range(column := best_seconds, min_v := 1, max_v := 36000)
  )
);

-- The fastest stretch of each standard distance inside each run (so a half marathon also yields a best
-- 5 km and 10 km), from the per-second samples. For every sample, an ASOF join finds the first later
-- sample at least that distance further on, and the time between them is scaled to the exact distance.
--
-- Rides and drives recorded as runs are left out, or they'd hold every record:
-- - since 2018 the watch records cadence, and a real run's median while moving is 150+ steps a minute
-- - earlier watches recorded none, and the activities averaging faster than 3:50/km over 8 km or more
--   were bike commutes (checked with the owner)
WITH recorded_runs AS (
  SELECT
    activity_id,
    activity_date,
    distance_km,
    duration_minutes
  FROM staging.stg_activities
  WHERE activity_type ILIKE '%running%'
    AND activity_date BETWEEN @start_ds AND @end_ds
),

samples AS (
  SELECT
    s.activity_id,
    s.timer_s,
    s.distance_m,
    s.cadence_spm,
    s.speed_mps
  FROM staging.stg_activity_samples AS s
  INNER JOIN recorded_runs USING (activity_id)
  WHERE s.distance_m IS NOT NULL AND s.timer_s IS NOT NULL
),

moving_cadence AS (
  SELECT
    activity_id,
    MEDIAN(cadence_spm) FILTER (WHERE speed_mps > 1) AS median_cadence_spm
  FROM samples
  GROUP BY activity_id
),

runs AS (
  SELECT
    r.activity_id,
    r.activity_date,
    r.distance_km
  FROM recorded_runs AS r
  LEFT JOIN moving_cadence AS c USING (activity_id)
  WHERE CASE
      WHEN c.median_cadence_spm IS NOT NULL THEN c.median_cadence_spm >= 130
      ELSE NOT (r.distance_km >= 8 AND r.duration_minutes / r.distance_km < 3.83)
    END
),

-- The alias names the VALUES columns (SQLFluff's AL05 would delete it), and the ASOF join's
-- inequality stays in this order (ST09 would flip it).
targets AS (
  SELECT * FROM (
    VALUES
    ('1 km', 1000.0), ('1 mile', 1609.344), ('5 km', 5000.0), ('10 km', 10000.0),
    ('Half marathon', 21097.5), ('30 km', 30000.0), ('Marathon', 42195.0)
  ) AS t (effort, effort_m)  -- noqa: AL05
),

starts AS (
  SELECT
    s.activity_id,
    s.timer_s,
    s.distance_m,
    t.effort,
    t.effort_m,
    s.distance_m + t.effort_m AS end_m
  FROM samples AS s
  INNER JOIN runs AS r USING (activity_id)
  INNER JOIN targets AS t ON t.effort_m <= r.distance_km * 1000.0
),

efforts AS (
  SELECT
    st.activity_id,
    st.effort,
    st.effort_m,
    st.timer_s                                                              AS start_timer_s,
    (e.timer_s - st.timer_s) * st.effort_m / (e.distance_m - st.distance_m) AS seconds
  FROM starts AS st
  ASOF JOIN samples AS e ON st.activity_id = e.activity_id AND e.distance_m >= st.end_m  -- noqa: ST09
)

SELECT
  e.activity_id,
  ANY_VALUE(r.activity_date)                               AS activity_date,
  e.effort,
  ANY_VALUE(e.effort_m) / 1000.0                           AS effort_km,
  MIN(e.seconds)                                           AS best_seconds,
  MIN(e.seconds) / 60.0 / (ANY_VALUE(e.effort_m) / 1000.0) AS pace_min_per_km,
  ARG_MIN(e.start_timer_s, e.seconds)                      AS starts_at_timer_s
FROM efforts AS e
INNER JOIN runs AS r USING (activity_id)
-- Faster than 6.5 m/s (2:34/km) is beyond world-record pace for these distances: a GPS jump, not a run.
WHERE e.seconds > 0 AND e.effort_m / e.seconds <= 6.5
GROUP BY e.activity_id, e.effort
