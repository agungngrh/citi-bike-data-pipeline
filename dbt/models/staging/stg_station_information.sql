{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('raw', 'station_information') }}
)

select
    trim(station_id) as station_id,
    trim(name) as station_name,
    trim(short_name) as short_name,
    trim(region_id) as region_id,
    cast(lat as float64) as latitude,
    cast(lon as float64) as longitude,
    capacity,
    trim(station_type) as station_type,
    snapshot_date,
    batch_id
from source