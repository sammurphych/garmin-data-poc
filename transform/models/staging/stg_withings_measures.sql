MODEL (
  name staging.stg_withings_measures,
  kind VIEW,
  grain grpid,
  audits (
    not_null(columns := (grpid, measured_at)),
    unique_values(columns := (grpid)),
    accepted_range(column := weight_kg, min_v := 20, max_v := 300)
  )
);

-- One row per Withings weigh-in. measured_date is the local date of the weigh-in.
SELECT
  grpid,
  measured_at,
  COALESCE(timezone, 'UTC')                                        AS timezone,
  CAST(measured_at AT TIME ZONE COALESCE(timezone, 'UTC') AS DATE) AS measured_date,
  weight_kg,
  fat_ratio_pct,
  fat_mass_kg,
  fat_free_mass_kg,
  muscle_mass_kg,
  bone_mass_kg,
  hydration_kg,
  deviceid                                                         AS device_id
FROM raw_withings.measures
