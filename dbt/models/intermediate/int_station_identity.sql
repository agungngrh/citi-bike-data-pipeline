{{ config(materialized='table') }}

with source_station_rows as (

    select
        trim(start_station_id) as source_station_code,
        trim(start_station_name) as source_station_name,
        start_latitude as source_latitude,
        start_longitude as source_longitude,
        count(*) as trip_rows

    from {{ ref('stg_trip_history') }}

    where start_station_id is not null

    group by
        start_station_id,
        start_station_name,
        start_latitude,
        start_longitude

    union all

    select
        trim(end_station_id) as source_station_code,
        trim(end_station_name) as source_station_name,
        end_latitude as source_latitude,
        end_longitude as source_longitude,
        count(*) as trip_rows

    from {{ ref('stg_trip_history') }}

    where end_station_id is not null

    group by
        end_station_id,
        end_station_name,
        end_latitude,
        end_longitude

),

source_stations as (

    select
        source_station_code,
        source_station_name,
        source_latitude,
        source_longitude,
        sum(trip_rows) as trip_rows

    from source_station_rows

    group by
        source_station_code,
        source_station_name,
        source_latitude,
        source_longitude

    qualify row_number() over (
        partition by source_station_code
        order by trip_rows desc
    ) = 1

),

station_reference as (

    select
        station_id as station_key,
        trim(short_name) as short_name,
        station_name,
        latitude,
        longitude,

        safe_cast(
            regexp_replace(trim(short_name), r'_+$', '')
            as numeric
        ) as short_name_numeric,

        array_to_string(
            array(
                select trim(part)
                from unnest(
                    split(
                        regexp_replace(
                            lower(trim(station_name)),
                            r'\s*\([^)]*\)',
                            ''
                        ),
                        '&'
                    )
                ) as part
                where trim(part) != ''
                order by trim(part)
            ),
            ' & '
        ) as canonical_name

    from {{ ref('stg_station_information') }}

    where station_id is not null
      and latitude is not null
      and longitude is not null

    qualify row_number() over (
        partition by station_id
        order by snapshot_date desc
    ) = 1

),

source_prepared as (

    select
        *,
        safe_cast(
            regexp_replace(source_station_code, r'_+$', '')
            as numeric
        ) as source_station_code_numeric,

        array_to_string(
            array(
                select trim(part)
                from unnest(
                    split(
                        regexp_replace(
                            lower(trim(source_station_name)),
                            r'\s*\([^)]*\)',
                            ''
                        ),
                        '&'
                    )
                ) as part
                where trim(part) != ''
                order by trim(part)
            ),
            ' & '
        ) as canonical_name

    from source_stations

),

exact_candidates as (

    select
        s.source_station_code,
        r.station_key,
        count(*) over (
            partition by s.source_station_code
        ) as candidate_count

    from source_prepared s
    join station_reference r
        on s.source_station_code = r.short_name

),

exact_matches as (

    select
        source_station_code,
        station_key,
        'EXACT_SHORT_NAME' as mapping_method

    from exact_candidates

    where candidate_count = 1

),

numeric_candidates as (

    select
        s.source_station_code,
        r.station_key,
        count(*) over (
            partition by s.source_station_code
        ) as candidate_count

    from source_prepared s
    join station_reference r
        on s.source_station_code_numeric = r.short_name_numeric

    where s.source_station_code not in (
        select source_station_code
        from exact_matches
    )

),

numeric_matches as (

    select
        source_station_code,
        station_key,
        'NUMERIC_SHORT_NAME' as mapping_method

    from numeric_candidates

    where candidate_count = 1

),

geo_candidates as (

    select
        s.source_station_code,
        r.station_key,

        st_distance(
            st_geogpoint(
                s.source_longitude,
                s.source_latitude
            ),
            st_geogpoint(
                r.longitude,
                r.latitude
            )
        ) as distance_m,

        count(*) over (
            partition by s.source_station_code
        ) as candidate_count

    from source_prepared s
    join station_reference r
        on s.canonical_name = r.canonical_name

    where s.source_station_code not in (
        select source_station_code
        from exact_matches

        union distinct

        select source_station_code
        from numeric_matches
    )

    and s.source_latitude is not null
    and s.source_longitude is not null

),

geo_matches as (

    select
        source_station_code,
        station_key,
        'NAME_COORDINATE' as mapping_method

    from geo_candidates

    where distance_m <= 100
      and candidate_count = 1

),

matches as (

    select * from exact_matches

    union all

    select * from numeric_matches

    union all

    select * from geo_matches

)

select
    s.source_station_code,
    s.source_station_name,
    s.source_latitude,
    s.source_longitude,
    s.trip_rows,

    m.station_key,
    m.mapping_method,

    case
        when m.station_key is not null
            then 'MATCHED'
        else 'UNRESOLVED'
    end as mapping_status

from source_prepared s

left join matches m
    on s.source_station_code = m.source_station_code