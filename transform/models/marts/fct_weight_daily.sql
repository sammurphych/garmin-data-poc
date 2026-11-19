MODEL (
  name marts.fct_weight_daily,
  kind FULL,  -- a few weigh-ins a day at most, so rebuilding is cheap
  cron '@daily',
  grain calendar_date,
  audits (
    unique_values(columns := (calendar_date)),
    accepted_range(column := weight_kg, min_v := 20, max_v := 300)
  )
);

-- One row per day with a weigh-in. Values are the day's last reading; the 7-day
-- average covers the calendar week ending that day, so gaps between weigh-ins don't stretch it.
WITH daily AS (
  SELECT
    measured_date                                                                       AS calendar_date,
    ARG_MAX(weight_kg, measured_at) FILTER (WHERE weight_kg IS NOT NULL)               AS weight_kg,
    MIN(weight_kg)                                                                      AS weight_kg_min,
    ARG_MAX(fat_ratio_pct, measured_at) FILTER (WHERE fat_ratio_pct IS NOT NULL)       AS fat_ratio_pct,
    ARG_MAX(fat_mass_kg, measured_at) FILTER (WHERE fat_mass_kg IS NOT NULL)           AS fat_mass_kg,
    ARG_MAX(muscle_mass_kg, measured_at) FILTER (WHERE muscle_mass_kg IS NOT NULL)     AS muscle_mass_kg,
    ARG_MAX(bone_mass_kg, measured_at) FILTER (WHERE bone_mass_kg IS NOT NULL)         AS bone_mass_kg,
    ARG_MAX(hydration_kg, measured_at) FILTER (WHERE hydration_kg IS NOT NULL)         AS hydration_kg,
    COUNT(*)                                                                            AS weigh_ins
  FROM staging.stg_withings_measures
  GROUP BY measured_date
)
SELECT
  *,
  AVG(weight_kg) OVER (
    ORDER BY calendar_date RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW
  ) AS weight_kg_7d_avg
FROM daily
