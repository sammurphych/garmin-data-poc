MODEL (
  name marts.dim_date,
  kind FULL,
  cron '@daily',
  grain calendar_date,
  audits (unique_values(columns := (calendar_date)), not_null(columns := (calendar_date)))
);

SELECT
  CAST(d AS DATE)                      AS calendar_date,
  CAST(DATE_TRUNC('week', d) AS DATE)  AS week_start,
  CAST(DATE_TRUNC('month', d) AS DATE) AS month_start,
  YEAR(d)                              AS year,
  ISODOW(d)                            AS iso_day_of_week,
  DAYNAME(d)                           AS day_name,
  ISODOW(d) IN (6, 7)                  AS is_weekend
FROM GENERATE_SERIES(
    (SELECT CAST(MIN(calendar_date) AS TIMESTAMP) FROM staging.stg_daily_summary),
    CAST(CURRENT_DATE AS TIMESTAMP),
    INTERVAL 1 DAY
  ) AS t (d)
