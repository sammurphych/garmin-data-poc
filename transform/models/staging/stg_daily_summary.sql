MODEL (
  name staging.stg_daily_summary,
  kind VIEW,
  grain calendar_date,
  audits (not_null(columns := (calendar_date)), unique_values(columns := (calendar_date)))
);

SELECT
  CAST(calendar_date AS DATE)            AS calendar_date,
  movement__steps                        AS steps,
  movement__floors_ascended              AS floors_ascended,
  calories__burned_total                 AS calories_total,
  calories__burned_active                AS calories_active,
  heart_rate__resting_value              AS resting_hr,
  heart_rate__min_value                  AS min_hr,
  heart_rate__max_value                  AS max_hr,
  intensity_minutes__moderate            AS intensity_minutes_moderate,
  intensity_minutes__vigorous            AS intensity_minutes_vigorous,
  stress__avg_level                      AS stress_avg,
  stress__max_level                      AS stress_max,
  body_battery__min_value                AS body_battery_min,
  body_battery__max_value                AS body_battery_max,
  body_battery__charged_value            AS body_battery_charged,
  body_battery__drained_value            AS body_battery_drained,
  respiration__avg_value                 AS respiration_avg
FROM raw_garmin.daily_summaries
