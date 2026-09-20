{{ config(materialized='table') }}

select
    generate_uuid() as quarantine_id,

    batch_id,

    ride_id,
    started_at,
    ended_at,

    start_station_id,
    end_station_id,

    rejection_reasons as rejections,
    array_length(rejection_reasons) as rejection_count,

    current_timestamp() as quarantined_at

from {{ ref('int_trip_history') }}

where not is_valid_trip