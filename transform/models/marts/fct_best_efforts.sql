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
WITH runs AS (
  SELECT activity_id, activity_date, distance_km
  FROM staging.stg_activities
  WHERE activity_type ILIKE '%running%'
    AND activity_date BETWEEN @start_ds AND @end_ds
), samples AS (
  SELECT s.activity_id, s.timer_s, s.distance_m
  FROM staging.stg_activity_samples AS s
  JOIN runs USING (activity_id)
  WHERE s.distance_m IS NOT NULL AND s.timer_s IS NOT NULL
), targets AS (
  SELECT * FROM (VALUES
    ('1 km', 1000.0), ('1 mile', 1609.344), ('5 km', 5000.0), ('10 km', 10000.0),
    ('Half marathon', 21097.5), ('30 km', 30000.0), ('Marathon', 42195.0)
  ) AS t(effort, effort_m)
), starts AS (
  SELECT s.activity_id, s.timer_s, s.distance_m, t.effort, t.effort_m, s.distance_m + t.effort_m AS end_m
  FROM samples AS s
  JOIN runs AS r USING (activity_id)
  JOIN targets AS t ON t.effort_m <= r.distance_km * 1000.0
), efforts AS (
  SELECT
    st.activity_id,
    st.effort,
    st.effort_m,
    st.timer_s                                                                  AS start_timer_s,
    (e.timer_s - st.timer_s) * st.effort_m / (e.distance_m - st.distance_m)    AS seconds
  FROM starts AS st
  ASOF JOIN samples AS e ON st.activity_id = e.activity_id AND e.distance_m >= st.end_m
)
SELECT
  e.activity_id,
  ANY_VALUE(r.activity_date)                       AS activity_date,
  e.effort,
  ANY_VALUE(e.effort_m) / 1000.0                   AS effort_km,
  MIN(e.seconds)                                   AS best_seconds,
  MIN(e.seconds) / 60.0 / (ANY_VALUE(e.effort_m) / 1000.0)  AS pace_min_per_km,
  ARG_MIN(e.start_timer_s, e.seconds)              AS starts_at_timer_s
FROM efforts AS e
JOIN runs AS r USING (activity_id)
-- Faster than 6.5 m/s (2:34/km) is beyond world-record pace for these distances: a GPS jump, not a run.
WHERE e.seconds > 0 AND e.effort_m / e.seconds <= 6.5
GROUP BY e.activity_id, e.effort
