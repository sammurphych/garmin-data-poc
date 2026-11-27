select
  activity_id, activity_date, activity_name, activity_type, distance_km, median_cadence_spm, median_speed_mps,
  gps_jumps, vehicle_speed_samples, likely_type, is_type_mismatch, is_gps_suspect
from health.marts.fct_activity_quality
order by activity_date desc
