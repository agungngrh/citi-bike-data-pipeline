{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        "field": "started_at",
        "data_type": "datetime",
        "granularity": "day"
    }
) }}

select
    ride_id,
    rideable_type,

    started_at,
    ended_at,

    start_station_id,
    end_station_id,

    start_station_key,
    end_station_key,

    duration_minutes,

    start_date,
    end_date,

    cast(
        format_date('%Y%m%d', start_date)
        as int64
    ) as start_date_key,

    cast(
        format_date('%Y%m%d', end_date)
        as int64
    ) as end_date_key,

    start_hour,

    start_latitude,
    start_longitude,
    end_latitude,
    end_longitude,

    member_casual,

    source_month,
    batch_id

from {{ ref('int_trip_history') }}

where is_valid_trip

{% if is_incremental() %}
    and source_month = date('{{ var("source_month") }}')
{% endif %}