MODEL (
  name staging.stg_weather_hourly,
  kind VIEW,
  grain (latitude, longitude, time_utc),
  audits (not_null(columns := (latitude, longitude, time_utc)))
);

-- Hourly weather for each area activities start in (start point rounded to about 11 km), from Open-Meteo.
-- Each value is for the hour starting at time_utc; precipitation is the total over the hour before it.
SELECT
  latitude,
  longitude,
  CAST(time_utc AS TIMESTAMP)        AS time_utc,
  temperature_2m                     AS temperature_c,
  apparent_temperature               AS feels_like_c,
  relative_humidity_2m               AS humidity_pct,
  dew_point_2m                       AS dew_point_c,
  precipitation                      AS precipitation_mm,
  CAST(weather_code AS INTEGER)      AS weather_code,   -- WMO code
  cloud_cover                        AS cloud_cover_pct,
  wind_speed_10m                     AS wind_speed_kmh,
  wind_gusts_10m                     AS wind_gust_kmh
FROM raw_weather.hourly
