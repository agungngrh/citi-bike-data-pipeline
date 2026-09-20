{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('raw', 'trip_history') }}
)

select
    trim(ride_id) as ride_id,
    trim(rideable_type) as rideable_type,
    started_at,
    ended_at,
    trim(start_station_name) as start_station_name,
    trim(start_station_id) as start_station_id,
    trim(end_station_name) as end_station_name,
    trim(end_station_id) as end_station_id,

    cast(start_lat as float64) as start_latitude,
    cast(start_lng as float64) as start_longitude,
    cast(end_lat as float64) as end_latitude,
    cast(end_lng as float64) as end_longitude,

    lower(trim(member_casual)) as member_casual,
    source_month,
    batch_id

from source