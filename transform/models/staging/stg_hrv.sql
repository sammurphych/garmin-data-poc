MODEL (
  name staging.stg_hrv,
  kind VIEW,
  grain calendar_date,
  audits (not_null(columns := (calendar_date)), unique_values(columns := (calendar_date)))
);

SELECT
  CAST(calendar_date AS DATE) AS calendar_date,
  last_night_avg              AS hrv_last_night_avg,
  weekly_avg                  AS hrv_weekly_avg,
  status                      AS hrv_status
FROM raw_garmin.hrv
