with sat as (
select formatDateTime(toDate(pickup_date),'%Y-%m') as month, 
count(distinct(pickup_date)) as unique_days,
round(count(id) / count(distinct(pickup_date))) as avg_trips, 
round(avg(date_diff('minutes',pickup_datetime,dropoff_datetime))) as avg_trip_duration,
round(avg(fare_amount)) avg_fare_amount
from tripdata 
where pickup_date between '2014-01-01' and '2016-12-31'
and toDayOfWeek(pickup_date) in (6)
group by month
order by month desc),

sun as (select formatDateTime(toDate(pickup_date),'%Y-%m') as month, 
count(distinct(pickup_date)) as unique_days,
round(count(id) / count(distinct(pickup_date))) as avg_trips, 
round(avg(date_diff('minutes',pickup_datetime,dropoff_datetime))) as avg_trip_duration,
round(avg(fare_amount)) avg_fare_amount
from tripdata 
where pickup_date between '2014-01-01' and '2016-12-31'
and toDayOfWeek(pickup_date) in (7)
group by month
order by month desc)

select sat.month, sat.avg_trips sat_mean_trip_count, sat.avg_fare_amount sat_mean_fare_per_trip,sat.avg_trip_duration sat_mean_duration_per_trip,
sun.avg_trips sun_mean_trip_count, sun.avg_fare_amount sun_mean_fare_per_trip,sun.avg_trip_duration sun_mean_duration_per_trip 
from sat
left join sun on sun.month = sat.month;