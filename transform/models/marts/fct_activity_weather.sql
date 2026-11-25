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
  INNER JOIN staging.stg_activity_weather_areas AS w ON a.activity_id = w.activity_id
),

hours AS (
  SELECT
    a.activity_id,
    h.time_utc = DATE_TRUNC('hour', a.start_time_utc) AS is_start_hour,
    h.temperature_c,
    h.feels_like_c,
    h.humidity_pct,
    h.wind_speed_kmh
  FROM activities AS a
  INNER JOIN staging.stg_weather_hourly AS h
    ON a.latitude = h.latitude AND a.longitude = h.longitude
      AND h.time_utc BETWEEN DATE_TRUNC('hour', a.start_time_utc) AND a.end_time_utc
)

SELECT
  activity_id,
  ANY_VALUE(temperature_c) FILTER (WHERE is_start_hour) AS start_temperature_c,
  ANY_VALUE(feels_like_c) FILTER (WHERE is_start_hour)  AS start_feels_like_c,
  AVG(temperature_c)                                    AS avg_temperature_c,
  AVG(feels_like_c)                                     AS avg_feels_like_c,
  AVG(humidity_pct)                                     AS avg_humidity_pct,
  AVG(wind_speed_kmh)                                   AS avg_wind_speed_kmh
FROM hours
GROUP BY activity_id
