-- Each outdoor run with the weather while it ran (Open-Meteo, hourly, for the start area). Only runs in
-- fct_best_efforts, which leaves out rides logged as runs.
select
  a.activity_id, a.activity_date, a.activity_name, a.distance_km, a.pace_min_per_km, a.avg_hr,
  w.start_temperature_c, w.avg_temperature_c, w.avg_feels_like_c, w.avg_humidity_pct, w.avg_wind_speed_kmh
from health.marts.fct_activities a
join health.marts.fct_activity_weather w using (activity_id)
where a.activity_id in (select activity_id from health.marts.fct_best_efforts)
  and w.avg_feels_like_c is not null
order by a.activity_date desc
