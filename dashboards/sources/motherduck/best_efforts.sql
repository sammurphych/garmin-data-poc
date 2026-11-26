-- The fastest stretch of each standard distance in each run (rides logged as runs are already excluded).
select e.*, a.activity_name
from health.marts.fct_best_efforts e
join health.marts.fct_activities a using (activity_id)
order by e.activity_date desc
