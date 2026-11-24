---
title: Health & Training
---

```sql kpis
with w as (
  select
    calendar_date >= current_date - interval 30 day as is_last_30,
    resting_hr, sleep_score, hrv_last_night_avg as hrv, steps
  from motherduck.daily_health
  where calendar_date >= current_date - interval 60 day
)
select
  avg(resting_hr)  filter (where is_last_30)                                         as resting_hr,
  avg(resting_hr)  filter (where is_last_30) - avg(resting_hr) filter (where not is_last_30) as resting_hr_delta,
  avg(sleep_score) filter (where is_last_30)                                         as sleep_score,
  avg(sleep_score) filter (where is_last_30) - avg(sleep_score) filter (where not is_last_30) as sleep_score_delta,
  avg(hrv)         filter (where is_last_30)                                         as hrv,
  avg(hrv)         filter (where is_last_30) - avg(hrv) filter (where not is_last_30)        as hrv_delta,
  avg(steps)       filter (where is_last_30)                                         as steps,
  avg(steps)       filter (where is_last_30) - avg(steps) filter (where not is_last_30)      as steps_delta
from w
```

Last 30 days, with the change against the 30 days before.

<BigValue data={kpis} value=resting_hr comparison=resting_hr_delta comparisonTitle="vs prev 30d" title="Resting HR" fmt="0.0" downIsGood=true />
<BigValue data={kpis} value=sleep_score comparison=sleep_score_delta comparisonTitle="vs prev 30d" title="Sleep score" fmt="0" />
<BigValue data={kpis} value=hrv comparison=hrv_delta comparisonTitle="vs prev 30d" title="HRV (ms)" fmt="0" />
<BigValue data={kpis} value=steps comparison=steps_delta comparisonTitle="vs prev 30d" title="Steps / day" fmt="#,##0" />

## Recovery

```sql daily
select
  calendar_date,
  resting_hr,
  hrv_last_night_avg as hrv,
  sleep_score,
  sleep_hours,
  body_battery_max
from motherduck.daily_health
where calendar_date >= current_date - interval 90 day
```

<LineChart data={daily} x=calendar_date y={['resting_hr', 'hrv']} title="Resting HR and HRV (90 days)" />
<BarChart data={daily} x=calendar_date y=sleep_hours title="Sleep hours" />
<LineChart data={daily} x=calendar_date y=sleep_score title="Sleep score" />

## Training volume

```sql weekly
select week_start, activity_type, distance_km, moving_minutes, training_load
from motherduck.weekly_training
where week_start >= current_date - interval 26 week
```

<BarChart data={weekly} x=week_start y=moving_minutes series=activity_type title="Weekly moving minutes by activity type" />
<BarChart data={weekly} x=week_start y=training_load series=activity_type title="Weekly training load" />

[All activities →](/activities) · [Weight →](/weight)
