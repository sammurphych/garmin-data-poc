---
title: Activities
---

```sql types
select distinct activity_type from motherduck.activities order by 1
```

<Dropdown data={types} name=activity_type value=activity_type multiple=true selectAllByDefault=true title="Activity type" />

```sql filtered
select
  start_time_local,
  activity_name,
  activity_type,
  round(distance_km, 2)       as distance_km,
  round(moving_minutes, 1)    as moving_minutes,
  round(pace_min_per_km, 2)   as pace_min_per_km,
  avg_hr,
  training_load
from motherduck.activities
where activity_type in ${inputs.activity_type.value}
order by start_time_local desc
```

<DataTable data={filtered} rows=25 search=true />
