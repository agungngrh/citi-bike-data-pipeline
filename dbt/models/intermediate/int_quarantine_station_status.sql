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
    to_hex(
        md5(
            concat(
                coalesce(station_id, '<NULL>'),
                '|',
                coalesce(snapshot_id, '<NULL>'),
                '|',
                coalesce(kafka_topic, '<NULL>'),
                '|',
                cast(coalesce(kafka_partition, -1) as string),
                '|',
                cast(coalesce(kafka_offset, -1) as string)
            )
        )
    ) as quarantine_id,

    station_id,
    snapshot_id,
    last_reported,
    poll_time,

    kafka_topic,
    kafka_partition,
    kafka_offset,

    rejection_reasons as rejections,
    array_length(rejection_reasons) as rejection_count,

    poll_time as invalid_at

from {{ ref('int_station_status') }}

where not is_valid

{% if is_incremental() %}
  and poll_time >= _dbt_max_partition
{% endif %}