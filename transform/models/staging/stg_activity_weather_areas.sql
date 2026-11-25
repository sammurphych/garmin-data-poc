MODEL (
  name staging.stg_activity_weather_areas,
  kind VIEW,
  grain activity_id,
  audits (unique_values(columns := (activity_id)), not_null(columns := (activity_id, latitude, longitude)))
);

-- The area whose weather applies to each activity: its start point rounded to 0.1 degrees (about 11 km).
SELECT
  activity_id,
  latitude,
  longitude
FROM raw_weather.activity_areas
