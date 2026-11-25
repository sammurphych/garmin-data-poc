MODEL (
  name marts.agg_weekly_training,
  kind FULL,
  cron '@daily',
  grain (week_start, activity_type),
  audits (unique_combination_of_columns(columns := (week_start, activity_type)))
);

SELECT
  CAST(DATE_TRUNC('week', activity_date) AS DATE) AS week_start,
  activity_type,
  COUNT(*)                                        AS activity_count,
  ROUND(SUM(distance_km), 2)                      AS distance_km,
  ROUND(SUM(moving_minutes), 1)                   AS moving_minutes,
  ROUND(SUM(elevation_gain_m))                    AS elevation_gain_m,
  ROUND(SUM(training_load), 1)                    AS training_load,
  ROUND(AVG(avg_hr), 1)                           AS avg_hr
FROM marts.fct_activities
GROUP BY ALL
