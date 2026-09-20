{{ config(materialized='table') }}

select
    station_id as station_key,
    station_id,
    station_name,
    short_name,
    region_id,
    latitude,
    longitude,
    capacity,
    station_type,
    snapshot_date as last_snapshot_date
from {{ ref('stg_station_information') }}
qualify row_number() over (
    partition by station_id
    order by snapshot_date desc
) = 1