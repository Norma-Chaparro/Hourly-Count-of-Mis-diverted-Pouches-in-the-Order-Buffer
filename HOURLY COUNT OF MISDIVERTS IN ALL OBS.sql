declare @currentdatetime datetime = getdate();
declare @irn int = 1269057110;

with main as (
select event_time, tm_id, to_value, location,
datepart(hour, event_time) as event_hour
from mi_tm
where convert(date, event_time) = convert(date, @currentdatetime)
and _identity_record_number > @irn
),
t1 as (
select min(event_time) as s,
tm_id, to_value, event_hour
from main
where to_value >= 'ob0' and to_value < 'ob1'
and tm_id >= 'ph' and tm_id < 'pi'
group by tm_id, to_value, event_hour
),
t2 as (
select min(event_time) as e,
tm_id, location, event_hour
from main
where location >= 'ob0' and location < 'ob1'
group by tm_id, location, event_hour
),
t3 as (
select distinct
case when a.to_value >= 'ob' and a.to_value < 'oc' then a.tm_id end as missed,
b.event_hour as event_hour,
substring(b.location, 1, 4) as 'order buffer'
from t1 a
join t2 b on a.tm_id = b.tm_id
and a.to_value != b.location
and a.event_hour = b.event_hour
group by a.to_value, b.location, b.event_hour,
case when a.to_value >= 'ob' and a.to_value < 'oc' then a.tm_id end
)
select t3.event_hour as hour,
count(case when [order buffer] = 'ob01' then [order buffer] end) as 'order buffer 1',
count(case when [order buffer] = 'ob02' then [order buffer] end) as 'order buffer 2',
count(case when [order buffer] = 'ob03' then [order buffer] end) as 'order buffer 3',
count(case when [order buffer] = 'ob04' then [order buffer] end) as 'order buffer 4'
from t3
group by t3.event_hour
order by t3.event_hour