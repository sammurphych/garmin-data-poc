MODEL (
  name marts.dim_date,
  kind FULL,
  cron '@daily',
  grain calendar_date
);

SELECT
  CAST(d AS DATE)                    AS calendar_date,
  CAST(date_trunc('week', d) AS DATE)  AS week_start,
  CAST(date_trunc('month', d) AS DATE) AS month_start,
  year(d)                            AS year,
  isodow(d)                          AS iso_day_of_week,
  dayname(d)                         AS day_name,
  isodow(d) IN (6, 7)                AS is_weekend
FROM generate_series(
  (SELECT CAST(min(calendar_date) AS TIMESTAMP) FROM staging.stg_daily_summary),
  CAST(current_date AS TIMESTAMP),
  INTERVAL 1 DAY
) AS t(d)
