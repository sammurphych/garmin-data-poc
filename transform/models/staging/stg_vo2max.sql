MODEL (
  name staging.stg_vo2max,
  kind VIEW,
  grain calendar_date,
  audits (unique_values(columns := (calendar_date)))
);

SELECT
  CAST(calendar_date AS DATE) AS calendar_date,
  running_vo2max,
  cycling_vo2max
FROM raw_garmin.vo2max
