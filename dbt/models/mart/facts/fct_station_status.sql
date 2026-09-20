{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        "field": "poll_time",
        "data_type": "timestamp",
        "granularity": "day"
    }
) }}

select
    i.station_id,
    s.station_key,

    i.snapshot_id,
    i.poll_time,

    cast(
        format_date('%Y%m%d', date(i.poll_time))
        as int64
    ) as observation_date_key,

    i.last_reported,
    i.num_bikes_available,
    i.num_bikes_disabled,
    i.num_docks_available,
    i.num_docks_disabled,

    i.is_installed,
    i.is_renting,
    i.is_returning,
    i.is_active_station,
    i.is_empty,
    i.is_full,

    i.feed_last_updated,
    i.feed_ttl,

    i.kafka_topic,
    i.kafka_partition,
    i.kafka_offset,
    i.kafka_timestamp,
    i.ingested_at

from {{ ref('int_station_status') }} i

left join {{ ref('dim_station') }} s
    on i.station_id = s.station_id

where i.is_valid

{% if is_incremental() %}
  and i.poll_time >= _dbt_max_partition
{% endif %}