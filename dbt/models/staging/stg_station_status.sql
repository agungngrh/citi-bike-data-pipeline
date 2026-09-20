{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('raw', 'station_status') }}

)

select
    trim(station_id) as station_id,

    cast(num_bikes_available as int64) as num_bikes_available,
    cast(num_bikes_disabled as int64) as num_bikes_disabled,

    cast(num_docks_available as int64) as num_docks_available,
    cast(num_docks_disabled as int64) as num_docks_disabled,

    cast(is_installed as int64) as is_installed,
    cast(is_renting as int64) as is_renting,
    cast(is_returning as int64) as is_returning,

    cast(last_reported as int64) as last_reported,

    trim(snapshot_id) as snapshot_id,
    cast(poll_time as timestamp) as poll_time,

    cast(feed_last_updated as int64) as feed_last_updated,
    cast(feed_ttl as int64) as feed_ttl,

    kafka_topic,
    cast(kafka_partition as int64) as kafka_partition,
    cast(kafka_offset as int64) as kafka_offset,
    cast(kafka_timestamp as timestamp) as kafka_timestamp,

    ingested_at

from source