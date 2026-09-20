{{ config(
    materialized='table'
) }}

with trip_activity as (

    select
        start_station_key as station_key,
        start_date_key as date_key,

        count(*) as trips_started,
        sum(duration_minutes) as total_trip_duration_minutes,
        avg(duration_minutes) as avg_trip_duration_minutes

    from {{ ref('fct_trip') }}

    where start_station_key is not null

    group by
        start_station_key,
        start_date_key

),

trip_ends as (

    select
        end_station_key as station_key,
        end_date_key as date_key,

        count(*) as trips_ended

    from {{ ref('fct_trip') }}

    where end_station_key is not null

    group by
        end_station_key,
        end_date_key

),

station_status as (

    select
        station_key,
        observation_date_key as date_key,

        avg(num_bikes_available) as avg_bikes_available,
        min(num_bikes_available) as min_bikes_available,
        max(num_bikes_available) as max_bikes_available,

        avg(num_docks_available) as avg_docks_available,
        min(num_docks_available) as min_docks_available,
        max(num_docks_available) as max_docks_available,

        count(*) as status_observations,

        countif(is_empty) as empty_observations,
        countif(is_full) as full_observations

    from {{ ref('fct_station_status') }}

    where station_key is not null

    group by
        station_key,
        observation_date_key

),

combined as (

    select
        coalesce(
            trip_activity.station_key,
            trip_ends.station_key,
            station_status.station_key
        ) as station_key,

        coalesce(
            trip_activity.date_key,
            trip_ends.date_key,
            station_status.date_key
        ) as date_key,

        trip_activity.trips_started,
        trip_ends.trips_ended,
        trip_activity.total_trip_duration_minutes,
        trip_activity.avg_trip_duration_minutes,

        station_status.avg_bikes_available,
        station_status.min_bikes_available,
        station_status.max_bikes_available,

        station_status.avg_docks_available,
        station_status.min_docks_available,
        station_status.max_docks_available,

        station_status.status_observations,
        station_status.empty_observations,
        station_status.full_observations

    from trip_activity

    full outer join trip_ends
        on trip_activity.station_key = trip_ends.station_key
        and trip_activity.date_key = trip_ends.date_key

    full outer join station_status
        on coalesce(
            trip_activity.station_key,
            trip_ends.station_key
        ) = station_status.station_key
        and coalesce(
            trip_activity.date_key,
            trip_ends.date_key
        ) = station_status.date_key

)

select
    c.date_key,
    d.full_date,

    c.station_key,
    s.station_name,
    s.short_name,
    s.region_id,
    s.latitude,
    s.longitude,
    s.capacity,
    s.station_type,

    coalesce(c.trips_started, 0) as trips_started,
    coalesce(c.trips_ended, 0) as trips_ended,

    coalesce(c.trips_started, 0)
        + coalesce(c.trips_ended, 0) as total_trip_activity,

    c.total_trip_duration_minutes,
    c.avg_trip_duration_minutes,

    c.avg_bikes_available,
    c.min_bikes_available,
    c.max_bikes_available,

    c.avg_docks_available,
    c.min_docks_available,
    c.max_docks_available,

    coalesce(c.status_observations, 0) as status_observations,
    coalesce(c.empty_observations, 0) as empty_observations,
    coalesce(c.full_observations, 0) as full_observations

from combined c

join {{ ref('dim_date') }} d
    on c.date_key = d.date_key

left join {{ ref('dim_station') }} s
    on c.station_key = s.station_key