{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        "field": "start_date",
        "data_type": "date",
        "granularity": "day"
    }
) }}

with trips as (

    select
        ride_id,
        rideable_type,
        started_at,
        ended_at,

        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,

        start_latitude,
        start_longitude,
        end_latitude,
        end_longitude,

        member_casual,

        source_month,
        batch_id,

        datetime_diff(
            ended_at,
            started_at,
            minute
        ) as duration_minutes,

        date(started_at) as start_date,

        date(ended_at) as end_date,

        extract(hour from started_at) as start_hour

    from {{ ref('stg_trip_history') }}

    {% if is_incremental() %}
    where source_month = date('{{ var("source_month") }}')
    {% endif %}

),

classified as (

    select
        *,

        array(
            select reason
            from unnest([
                if(
                    ride_id is null,
                    struct(
                        'INVALID_RIDE_ID' as reason_code,
                        'ride_id' as failed_field,
                        'ride_id must not be null' as reason_detail
                    ),
                    null
                ),

                if(
                    started_at is null,
                    struct(
                        'INVALID_START_TIME',
                        'started_at',
                        'started_at must not be null'
                    ),
                    null
                ),

                if(
                    ended_at is null,
                    struct(
                        'INVALID_END_TIME',
                        'ended_at',
                        'ended_at must not be null'
                    ),
                    null
                ),

                if(
                    started_at is not null
                    and ended_at is not null
                    and datetime_diff(
                        ended_at,
                        started_at,
                        second
                    ) <= 0,
                    struct(
                        'INVALID_DURATION',
                        'started_at,ended_at',
                        'started_at must be earlier than ended_at'
                    ),
                    null
                ),

                if(
                    start_latitude is not null
                    and start_latitude not between -90 and 90,
                    struct(
                        'INVALID_START_LATITUDE',
                        'start_latitude',
                        'start_latitude must be between -90 and 90'
                    ),
                    null
                ),

                if(
                    start_longitude is not null
                    and start_longitude not between -180 and 180,
                    struct(
                        'INVALID_START_LONGITUDE',
                        'start_longitude',
                        'start_longitude must be between -180 and 180'
                    ),
                    null
                ),

                if(
                    end_latitude is not null
                    and end_latitude not between -90 and 90,
                    struct(
                        'INVALID_END_LATITUDE',
                        'end_latitude',
                        'end_latitude must be between -90 and 90'
                    ),
                    null
                ),

                if(
                    end_longitude is not null
                    and end_longitude not between -180 and 180,
                    struct(
                        'INVALID_END_LONGITUDE',
                        'end_longitude',
                        'end_longitude must be between -180 and 180'
                    ),
                    null
                ),

                if(
                    member_casual is not null
                    and member_casual not in ('member', 'casual'),
                    struct(
                        'INVALID_RIDER_TYPE',
                        'member_casual',
                        'member_casual must be member or casual'
                    ),
                    null
                )
            ]) as reason
            where reason is not null
        ) as rejection_reasons

    from trips

)

select
    t.ride_id,
    t.rideable_type,

    t.started_at,
    t.ended_at,

    t.start_station_id,
    t.end_station_id,

    start_station.station_key as start_station_key,
    end_station.station_key as end_station_key,

    start_station.mapping_status as start_station_mapping_status,
    end_station.mapping_status as end_station_mapping_status,

    t.duration_minutes,
    t.start_date,
    t.end_date,
    t.start_hour,

    t.start_latitude,
    t.start_longitude,
    t.end_latitude,
    t.end_longitude,

    t.member_casual,

    t.rejection_reasons,

    array_length(t.rejection_reasons) = 0 as is_valid_trip,

    t.source_month,
    t.batch_id

from classified t

left join {{ ref('int_station_identity') }} start_station
    on trim(t.start_station_id)
        = start_station.source_station_code

left join {{ ref('int_station_identity') }} end_station
    on trim(t.end_station_id)
        = end_station.source_station_code