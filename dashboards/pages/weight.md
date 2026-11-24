---
title: Weight
---

```sql latest
-- Changes are measured from the latest weigh-in, not from today, so gaps in weighing don't blank the page.
with lw as (select max(calendar_date) as d from motherduck.weight)
select
  w.calendar_date                                                              as latest_date,
  w.weight_kg                                                                  as weight_kg,
  w.weight_kg_7d_avg                                                           as weight_7d_avg,
  w.weight_kg - (select arg_max(weight_kg, calendar_date) from motherduck.weight, lw
                 where calendar_date <= lw.d - interval 30 day)              as change_30d,
  w.weight_kg - (select arg_max(weight_kg, calendar_date) from motherduck.weight, lw
                 where calendar_date <= lw.d - interval 365 day)             as change_1y,
  current_date - w.calendar_date                                               as days_since
from motherduck.weight w, lw
where w.calendar_date = lw.d
```

Latest weigh-in: **<Value data={latest} column=latest_date fmt="d mmm yyyy" />** (<Value data={latest} column=days_since /> days ago).

<BigValue data={latest} value=weight_kg title="Weight (kg)" fmt="0.0" />
<BigValue data={latest} value=weight_7d_avg title="7-day average (kg)" fmt="0.0" />
<BigValue data={latest} value=change_30d title="Change over 30 days (kg)" fmt="+0.0;-0.0;0.0" downIsGood=true />
<BigValue data={latest} value=change_1y title="Change over 1 year (kg)" fmt="+0.0;-0.0;0.0" downIsGood=true />

## Trend

<ButtonGroup name=range defaultValue="365">
  <ButtonGroupItem valueLabel="3 months" value="90" />
  <ButtonGroupItem valueLabel="1 year" value="365" />
  <ButtonGroupItem valueLabel="3 years" value="1095" />
  <ButtonGroupItem valueLabel="All" value="100000" />
</ButtonGroup>

```sql trend
select calendar_date, weight_kg as weight, weight_kg_7d_avg as "7-day average"
from motherduck.weight
where calendar_date >= (select max(calendar_date) from motherduck.weight) - to_days(cast('${inputs.range}' as integer))
order by calendar_date
```

<LineChart data={trend} x=calendar_date y={['weight', '7-day average']} yScale=true yAxisTitle="kg" title="Weight" />

```sql fat
select calendar_date, fat_ratio_pct as "body fat %"
from motherduck.weight
where fat_ratio_pct is not null
order by calendar_date
```

{#if fat.length > 0}

<ScatterPlot data={fat} x=calendar_date y="body fat %" title="Body fat % (only on weigh-ins that measured it)" />

{/if}

## By year

```sql yearly
select
  year(calendar_date)            as year,
  round(avg(weight_kg), 1)       as avg_kg,
  round(min(weight_kg), 1)       as min_kg,
  round(max(weight_kg), 1)       as max_kg,
  count(*)                       as days_weighed
from motherduck.weight
group by 1
order by 1 desc
```

<DataTable data={yearly} />
