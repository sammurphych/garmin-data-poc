MODEL (
  name marts.fct_activity_km_splits,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key (activity_id, km),
    lookback 3
  ),
  cron '@daily',
  grain (activity_id, km),
  audits (
    unique_combination_of_columns(columns := (activity_id, km)),
    not_null(columns := (activity_id, km, split_seconds))
  )
);

-- One row per kilometre of each run, from the per-second samples. Each sample's change in timer time,
-- distance and elevation since the previous sample counts towards the kilometre it ends in, so the last
-- row of a run is usually a partial kilometre (split_km < 1).
WITH runs AS (
  SELECT activity_id, activity_date
  FROM staging.stg_activities
  WHERE activity_type ILIKE '%running%'
    AND activity_date BETWEEN @start_ds AND @end_ds
), deltas AS (
  SELECT
    s.activity_id,
    r.activity_date,
    GREATEST(CAST(CEIL(s.distance_m / 1000.0) AS INT), 1)                         AS km,
    s.timer_s - LAG(s.timer_s, 1, 0) OVER w                                        AS dt,
    s.distance_m - LAG(s.distance_m, 1, 0) OVER w                                  AS dd,
    s.elevation_m - LAG(s.elevation_m) OVER w                                      AS de,
    s.heart_rate,
    s.gap_speed_mps,
    s.cadence_spm,
    s.power_w
  FROM staging.stg_activity_samples AS s
  JOIN runs AS r USING (activity_id)
  WHERE s.distance_m IS NOT NULL AND s.timer_s IS NOT NULL
  WINDOW w AS (PARTITION BY s.activity_id ORDER BY s.sample_index)
)
SELECT
  activity_id,
  ANY_VALUE(activity_date)                                                          AS activity_date,
  km,
  SUM(dd) / 1000.0                                                                  AS split_km,
  SUM(dt)                                                                           AS split_seconds,
  SUM(dt) / 60.0 / NULLIF(SUM(dd) / 1000.0, 0)                                      AS pace_min_per_km,
  -- Grade-adjusted pace: what the pace would have been on the flat.
  1000.0 / 60.0 / NULLIF(SUM(gap_speed_mps * dt) / NULLIF(SUM(CASE WHEN gap_speed_mps IS NOT NULL THEN dt END), 0), 0)
                                                                                    AS gap_pace_min_per_km,
  SUM(heart_rate * dt) / NULLIF(SUM(CASE WHEN heart_rate IS NOT NULL THEN dt END), 0)  AS avg_hr,
  MAX(heart_rate)                                                                   AS max_hr,
  SUM(GREATEST(de, 0))                                                              AS elevation_gain_m,
  -SUM(LEAST(de, 0))                                                                AS elevation_loss_m,
  SUM(cadence_spm * dt) / NULLIF(SUM(CASE WHEN cadence_spm IS NOT NULL THEN dt END), 0)  AS avg_cadence_spm,
  SUM(power_w * dt) / NULLIF(SUM(CASE WHEN power_w IS NOT NULL THEN dt END), 0)     AS avg_power_w
FROM deltas
GROUP BY activity_id, km
