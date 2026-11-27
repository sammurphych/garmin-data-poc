---
title: Data quality
---

What the recordings say might be wrong. These are flags, not errors: the data is real, but an activity may
have the wrong type in Garmin Connect, or its GPS may have jumped. Best efforts already leave out activities
that weren't runs and any stretch with a GPS jump or a vehicle's speed in it, and a daily check sends a phone
notification for new flags.

```sql counts
select
  count(*) filter (where is_type_mismatch)                    as type_mismatches,
  count(*) filter (where is_gps_suspect)                      as gps_suspect,
  (select count(*) from motherduck.weight where is_suspect)   as suspect_weigh_ins,
  count(*)                                                    as activities_checked
from motherduck.activity_quality
```

<BigValue data={counts} value=type_mismatches title="Wrong activity type?" />
<BigValue data={counts} value=gps_suspect title="GPS jumped (3+ times)" />
<BigValue data={counts} value=suspect_weigh_ins title="Suspect weigh-ins" />
<BigValue data={counts} value=activities_checked title="Activities checked" />

## Wrong activity type?

The cadence and the watch's speed while moving. A run's median cadence is 150+ steps a minute; under 130 it
was a ride (faster than 4 m/s) or a walk, and the watch reading over 8 m/s for half a minute is a vehicle.

```sql mismatches
select
  activity_date                                                        as date,
  activity_name                                                        as activity,
  activity_type                                                        as recorded_as,
  coalesce(likely_type, 'vehicle')                                     as looks_like,
  distance_km,
  median_cadence_spm                                                   as cadence,
  case when median_speed_mps > 0
    then printf('%d:%02d', (1000 / median_speed_mps)::int // 60, (1000 / median_speed_mps)::int % 60) end as pace,
  vehicle_speed_samples                                                as seconds_over_8_mps
from motherduck.activity_quality
where is_type_mismatch
order by activity_date desc
```

<DataTable data={mismatches} rows=15 search=true>
  <Column id=date />
  <Column id=activity />
  <Column id=recorded_as />
  <Column id=looks_like />
  <Column id=distance_km fmt="0.0" title="km" />
  <Column id=cadence title="Cadence (spm)" />
  <Column id=pace title="Median pace /km" />
  <Column id=seconds_over_8_mps title="Seconds over 8 m/s" />
</DataTable>

## GPS jumps

Runs where the distance jumped 3 or more times: faster than 8 m/s and at least 20 m beyond what the watch's own
speed accounts for, like a lost signal snapping back among tall buildings.

```sql gps
select activity_date as date, activity_name as activity, distance_km, gps_jumps
from motherduck.activity_quality
where is_gps_suspect
order by activity_date desc
```

<DataTable data={gps} rows=10>
  <Column id=date />
  <Column id=activity />
  <Column id=distance_km fmt="0.0" title="km" />
  <Column id=gps_jumps title="Jumps" />
</DataTable>

## Suspect weigh-ins

More than 3 kg from the previous weigh-in within a week, or from the day's lowest reading: more likely someone
else on the scale, or a weigh-in in clothes.

```sql weigh_ins
select calendar_date as date, weight_kg, weight_kg_min, change_kg, weigh_ins
from motherduck.weight
where is_suspect
order by calendar_date desc
```

<DataTable data={weigh_ins}>
  <Column id=date />
  <Column id=weight_kg fmt="0.0" title="Weight (kg)" />
  <Column id=weight_kg_min fmt="0.0" title="Day's lowest (kg)" />
  <Column id=change_kg fmt="+0.0;-0.0" title="Change (kg)" />
  <Column id=weigh_ins title="Weigh-ins that day" />
</DataTable>
