MODEL (
  name marts.agg_weekly_training,
  kind FULL,
  cron '@daily',
  grain (week_start, activity_type)
);

SELECT
  CAST(date_trunc('week', activity_date) AS DATE) AS week_start,
  activity_type,
  count(*)                       AS activity_count,
  round(sum(distance_km), 2)     AS distance_km,
  round(sum(moving_minutes), 1)  AS moving_minutes,
  round(sum(elevation_gain_m))   AS elevation_gain_m,
  round(sum(training_load), 1)   AS training_load,
  round(avg(avg_hr), 1)          AS avg_hr
FROM marts.fct_activities
GROUP BY ALL
