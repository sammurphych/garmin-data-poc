---
title: Running
---

Best efforts are the fastest stretch of each distance inside any run, from the per-second recordings,
so a half marathon also sets a best 5 km and 10 km. Rides recorded as runs are left out.

## Personal bests

```sql pbs
with best as (
  select effort, effort_km, arg_min(activity_id, best_seconds) as activity_id,
    round(min(best_seconds))::int as secs, round(min(best_seconds) / effort_km)::int as pace_secs
  from motherduck.best_efforts
  group by effort, effort_km
)
select
  b.effort,
  -- Whole seconds first: DuckDB rounds when casting to an integer, so 2.6 minutes would show as 3.
  case when b.secs >= 3600
    then printf('%d:%02d:%02d', b.secs // 3600, b.secs % 3600 // 60, b.secs % 60)
    else printf('%d:%02d', b.secs // 60, b.secs % 60) end                           as time,
  printf('%d:%02d', b.pace_secs // 60, b.pace_secs % 60)                            as pace_per_km,
  e.activity_date                                                                as date,
  e.activity_name                                                                as run
from best b
join motherduck.best_efforts e on e.activity_id = b.activity_id and e.effort = b.effort
order by b.effort_km
```

<DataTable data={pbs} rows=all>
  <Column id=effort />
  <Column id=time align=right />
  <Column id=pace_per_km title="Pace /km" align=right />
  <Column id=date fmt="d mmm yyyy" />
  <Column id=run />
</DataTable>

## Best each year

```sql efforts
select effort from motherduck.best_efforts group by effort order by min(effort_km)
```

<Dropdown data={efforts} name=effort value=effort defaultValue="5 km" title="Distance" />

```sql yearly
select
  year(activity_date)        as year,
  min(best_seconds) / 60.0   as best_minutes,
  min(pace_min_per_km)       as best_pace,
  count(*)                   as runs
from motherduck.best_efforts
where effort = '${inputs.effort.value}'
group by 1
order by 1
```

<BarChart data={yearly} x=year y=best_minutes xType=category yAxisTitle="minutes" title="Fastest {inputs.effort.value} each year" />

## Pace and heat

```sql heat
-- Steady 5-15 km runs only, so long runs and short intervals don't skew a band.
select
  floor(avg_feels_like_c / 5) * 5                                             as band_start,
  printf('%d to %d °C', (floor(avg_feels_like_c / 5) * 5)::int, (floor(avg_feels_like_c / 5) * 5 + 5)::int) as feels_like,
  median(pace_min_per_km)                                                     as median_pace,
  median(avg_hr)                                                              as median_hr,
  count(*)                                                                    as runs
from motherduck.run_weather
where distance_km between 5 and 15
group by 1, 2
having count(*) >= 5
order by 1
```

Median pace of 5–15 km runs by the feels-like temperature while running (bands with at least 5 runs).
Pace is in minutes per km, so higher is slower. The axes don't start at zero: the differences are small.

<LineChart data={heat} x=feels_like y=median_pace sort=false markers=true yScale=true yFmt="0.00" yAxisTitle="min/km" title="Median pace by feels-like temperature" />
<LineChart data={heat} x=feels_like y=median_hr sort=false markers=true yScale=true yFmt="0" yAxisTitle="bpm" title="Median heart rate by feels-like temperature" />

```sql runs_weather
select activity_date, avg_feels_like_c, pace_min_per_km, distance_km
from motherduck.run_weather
where distance_km between 5 and 15 and pace_min_per_km < 8
```

<ScatterPlot data={runs_weather} x=avg_feels_like_c y=pace_min_per_km xAxisTitle="feels-like °C" yAxisTitle="min/km" title="Every 5-15 km run" />

## Splits

```sql recent_runs
select
  s.activity_id,
  strftime(any_value(s.activity_date), '%Y-%m-%d') || ' · ' || any_value(a.activity_name)
    || ' · ' || max(s.km)::int || ' km' as label,
  any_value(s.activity_date) as activity_date
from motherduck.km_splits s
join motherduck.activities a using (activity_id)
group by s.activity_id
order by activity_date desc, s.activity_id desc
limit 100
```

<Dropdown data={recent_runs} name=run value=activity_id label=label title="Run (latest 100)" />

```sql splits
select
  km,
  pace_min_per_km      as pace,
  gap_pace_min_per_km  as grade_adjusted_pace,
  avg_hr,
  elevation_gain_m,
  elevation_loss_m,
  avg_cadence_spm,
  printf('%d:%02d', round(split_seconds / split_km)::int // 60, round(split_seconds / split_km)::int % 60) as pace_per_km
from motherduck.km_splits
where activity_id = '${inputs.run.value}'
order by km
```

<BarChart data={splits} x=km y={['pace', 'grade_adjusted_pace']} type=grouped xType=category yAxisTitle="min/km" title="Pace per km" />
<LineChart data={splits} x=km y=avg_hr xType=category yScale=true yAxisTitle="bpm" title="Heart rate per km" />

<DataTable data={splits} rows=all>
  <Column id=km />
  <Column id=pace_per_km title="Pace /km" align=right />
  <Column id=grade_adjusted_pace title="Grade-adjusted" fmt="0.00" />
  <Column id=avg_hr title="HR" fmt="0" />
  <Column id=elevation_gain_m title="Up (m)" fmt="0" />
  <Column id=elevation_loss_m title="Down (m)" fmt="0" />
  <Column id=avg_cadence_spm title="Cadence" fmt="0" />
</DataTable>
