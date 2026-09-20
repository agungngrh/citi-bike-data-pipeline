{{ config(materialized='table') }}

with start_trips as (

    select
        start_station_key as station_key,
        count(*) as start_trip_count
    from {{ ref('fct_trip') }}
    where start_station_key is not null
    group by start_station_key

),

end_trips as (

    select
        end_station_key as station_key,
        count(*) as end_trip_count
    from {{ ref('fct_trip') }}
    where end_station_key is not null
    group by end_station_key

),

station_activity as (

    select
        coalesce(start_trips.station_key, end_trips.station_key) as station_key,
        coalesce(start_trips.start_trip_count, 0) as start_trip_count,
        coalesce(end_trips.end_trip_count, 0) as end_trip_count
    from start_trips
    full outer join end_trips
        on start_trips.station_key = end_trips.station_key

)

select
    station_key,
    start_trip_count,
    end_trip_count,
    start_trip_count + end_trip_count as total_trip_activity
from station_activity