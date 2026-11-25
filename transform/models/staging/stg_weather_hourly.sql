MODEL (
  name staging.stg_weather_hourly,
  kind VIEW,
  grain (latitude, longitude, time_utc),
  audits (not_null(columns := (latitude, longitude, time_utc)))
);

-- Hourly weather for each area activities start in (start point rounded to about 11 km), from Open-Meteo.
-- Each value is for the hour starting at time_utc.
SELECT
  latitude,
  longitude,
  CAST(time_utc AS TIMESTAMP) AS time_utc,
  temperature_2m              AS temperature_c,
  apparent_temperature        AS feels_like_c,   -- with wind chill and humidity
  relative_humidity_2m        AS humidity_pct,
  wind_speed_10m              AS wind_speed_kmh
FROM raw_weather.hourly
