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
    accepted_range(column := best_seconds, min_v := 1, max_v := 36000),
    -- 2:11/km is the 1000 m world record; anything faster is a recording problem the filters missed.
    accepted_range(column := pace_min_per_km, min_v := 2.18)
  )
);

-- The fastest stretch of each standard distance inside each run (so a half marathon also yields a best
-- 5 km and 10 km), from the per-second samples. For every sample, an ASOF join finds the first later
-- sample at least that distance further on, and the time between them is scaled to the exact distance.
--
-- Left out, or they'd hold the records:
-- - activities recorded as runs that were rides or walks (marts.fct_activity_quality.likely_type)
-- - stretches with a GPS jump or a vehicle's speed in them (the watch reading over 8 m/s, as on a day out
--   that included a train): a running count of those samples is the same at both ends of a clean stretch
WITH runs AS (
  SELECT
    a.activity_id,
    a.activity_date,
    a.distance_km
  FROM staging.stg_activities AS a
  LEFT JOIN marts.fct_activity_quality AS q USING (activity_id)
  WHERE a.activity_type ILIKE '%running%'
    AND COALESCE(q.likely_type, 'run') = 'run'
    AND a.activity_date BETWEEN @start_ds AND @end_ds
),

samples AS (
  SELECT
    s.activity_id,
    s.timer_s,
    s.distance_m,
    COUNT(*) FILTER (WHERE s.is_gps_jump OR s.speed_mps > 8) OVER (
      PARTITION BY s.activity_id ORDER BY s.sample_index
    ) AS jumps_so_far
  FROM staging.stg_activity_samples AS s
  INNER JOIN runs USING (activity_id)
  WHERE s.distance_m IS NOT NULL AND s.timer_s IS NOT NULL
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
    s.jumps_so_far,
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
    (e.timer_s - st.timer_s) * st.effort_m / (e.distance_m - st.distance_m) AS seconds,
    e.jumps_so_far - st.jumps_so_far                                        AS jumps
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
WHERE e.seconds > 0 AND e.jumps = 0
GROUP BY e.activity_id, e.effort
