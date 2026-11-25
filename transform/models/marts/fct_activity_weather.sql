MODEL (
  name marts.fct_activity_weather,
  kind FULL,  -- one row per activity with a location, so rebuilding is cheap
  cron '@daily',
  grain activity_id,
  audits (
    unique_values(columns := (activity_id)),
    accepted_range(column := start_temperature_c, min_v := -50, max_v := 60)
  )
);

-- The weather during each activity with a start location: the hour it started in, and the hours it ran through.
WITH activities AS (
  SELECT
    a.activity_id,
    a.start_time_utc,
    a.start_time_utc + TO_SECONDS(CAST(COALESCE(a.duration_minutes, 0) * 60 AS BIGINT)) AS end_time_utc,
    w.latitude,
    w.longitude
  FROM staging.stg_activities AS a
  JOIN staging.stg_activity_weather_areas AS w ON w.activity_id = a.activity_id
), hours AS (
  SELECT
    a.activity_id,
    h.time_utc = DATE_TRUNC('hour', a.start_time_utc) AS is_start_hour,
    h.temperature_c,
    h.feels_like_c,
    h.humidity_pct,
    h.dew_point_c,
    h.precipitation_mm,
    h.weather_code,
    h.cloud_cover_pct,
    h.wind_speed_kmh,
    h.wind_gust_kmh
  FROM activities AS a
  JOIN staging.stg_weather_hourly AS h
    ON h.latitude = a.latitude AND h.longitude = a.longitude
    AND h.time_utc BETWEEN DATE_TRUNC('hour', a.start_time_utc) AND a.end_time_utc
), per_activity AS (
  SELECT
    activity_id,
    ANY_VALUE(temperature_c) FILTER (WHERE is_start_hour)    AS start_temperature_c,
    ANY_VALUE(feels_like_c) FILTER (WHERE is_start_hour)     AS start_feels_like_c,
    ANY_VALUE(weather_code) FILTER (WHERE is_start_hour)     AS start_weather_code,
    AVG(temperature_c)                                       AS avg_temperature_c,
    MAX(feels_like_c)                                        AS max_feels_like_c,
    AVG(humidity_pct)                                        AS avg_humidity_pct,
    AVG(dew_point_c)                                         AS avg_dew_point_c,
    SUM(precipitation_mm)                                    AS precipitation_mm,
    AVG(cloud_cover_pct)                                     AS avg_cloud_cover_pct,
    AVG(wind_speed_kmh)                                      AS avg_wind_speed_kmh,
    MAX(wind_gust_kmh)                                       AS max_wind_gust_kmh,
    COUNT(*)                                                 AS weather_hours
  FROM hours
  GROUP BY activity_id
)
SELECT
  activity_id,
  start_temperature_c,
  start_feels_like_c,
  start_weather_code,
  CASE  -- WMO weather codes
    WHEN start_weather_code = 0 THEN 'Clear'
    WHEN start_weather_code IN (1, 2) THEN 'Partly cloudy'
    WHEN start_weather_code = 3 THEN 'Overcast'
    WHEN start_weather_code IN (45, 48) THEN 'Fog'
    WHEN start_weather_code BETWEEN 51 AND 57 THEN 'Drizzle'
    WHEN start_weather_code BETWEEN 61 AND 67 OR start_weather_code BETWEEN 80 AND 82 THEN 'Rain'
    WHEN start_weather_code BETWEEN 71 AND 77 OR start_weather_code IN (85, 86) THEN 'Snow'
    WHEN start_weather_code BETWEEN 95 AND 99 THEN 'Thunderstorm'
  END                                                        AS start_conditions,
  avg_temperature_c,
  max_feels_like_c,
  avg_humidity_pct,
  avg_dew_point_c,
  precipitation_mm,
  avg_cloud_cover_pct,
  avg_wind_speed_kmh,
  max_wind_gust_kmh,
  weather_hours
FROM per_activity
