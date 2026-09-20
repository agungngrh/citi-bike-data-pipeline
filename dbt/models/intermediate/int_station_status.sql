{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        "field": "poll_time",
        "data_type": "timestamp",
        "granularity": "day"
    }
) }}

with staged as (

    select *
    from {{ ref('stg_station_status') }}

    {% if is_incremental() %}
    where poll_time >= _dbt_max_partition
    {% endif %}

),

technical_dedup as (

    -- Remove technical Kafka replays of the same message.
    select *
    from staged
    qualify row_number() over (
        partition by
            kafka_topic,
            kafka_partition,
            kafka_offset
        order by ingested_at asc
    ) = 1

),

logical_dedup as (

    -- Keep one observation for each station and polling snapshot.
    select *
    from technical_dedup
    qualify row_number() over (
        partition by
            station_id,
            snapshot_id
        order by
            poll_time asc,
            kafka_timestamp asc,
            ingested_at asc
    ) = 1

),

classified as (

    select
        *,

        array(
            select reason
            from unnest([
                if(
                    station_id is null,
                    struct(
                        'INVALID_STATION_ID' as reason_code,
                        'station_id' as failed_field,
                        'station_id must not be null' as reason_detail
                    ),
                    null
                ),

                if(
                    snapshot_id is null,
                    struct(
                        'INVALID_SNAPSHOT_ID',
                        'snapshot_id',
                        'snapshot_id must not be null'
                    ),
                    null
                ),

                if(
                    num_bikes_available is null,
                    struct(
                        'MISSING_BIKES_AVAILABLE',
                        'num_bikes_available',
                        'num_bikes_available must not be null'
                    ),
                    null
                ),

                if(
                    num_bikes_available is not null
                    and num_bikes_available < 0,
                    struct(
                        'NEGATIVE_BIKES_AVAILABLE',
                        'num_bikes_available',
                        'num_bikes_available must be >= 0'
                    ),
                    null
                ),

                if(
                    num_docks_available is null,
                    struct(
                        'MISSING_DOCKS_AVAILABLE',
                        'num_docks_available',
                        'num_docks_available must not be null'
                    ),
                    null
                ),

                if(
                    num_docks_available is not null
                    and num_docks_available < 0,
                    struct(
                        'NEGATIVE_DOCKS_AVAILABLE',
                        'num_docks_available',
                        'num_docks_available must be >= 0'
                    ),
                    null
                ),

                if(
                    is_installed is not null
                    and is_installed not in (0, 1),
                    struct(
                        'INVALID_IS_INSTALLED',
                        'is_installed',
                        'is_installed must be 0 or 1'
                    ),
                    null
                ),

                if(
                    is_renting is not null
                    and is_renting not in (0, 1),
                    struct(
                        'INVALID_IS_RENTING',
                        'is_renting',
                        'is_renting must be 0 or 1'
                    ),
                    null
                ),

                if(
                    is_returning is not null
                    and is_returning not in (0, 1),
                    struct(
                        'INVALID_IS_RETURNING',
                        'is_returning',
                        'is_returning must be 0 or 1'
                    ),
                    null
                )
            ]) as reason
            where reason is not null
        ) as rejection_reasons

    from logical_dedup

)

select
    station_id,

    num_bikes_available,
    num_bikes_disabled,

    num_docks_available,
    num_docks_disabled,

    is_installed,
    is_renting,
    is_returning,

    last_reported,
    snapshot_id,
    poll_time,

    feed_last_updated,
    feed_ttl,

    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    ingested_at,

    rejection_reasons,

    array_length(rejection_reasons) = 0 as is_valid,

    (is_installed = 1) as is_active_station,
    (num_bikes_available = 0) as is_empty,
    (num_docks_available = 0) as is_full

from classified