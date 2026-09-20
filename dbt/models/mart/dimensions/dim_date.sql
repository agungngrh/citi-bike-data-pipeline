{{ config(materialized='table') }}

with dates as (

    select date_value
    from unnest(
        generate_date_array(
            date('2026-01-01'),
            date_add(current_date(), interval 1 year)
        )
    ) as date_value

)

select
    cast(format_date('%Y%m%d', date_value) as int64) as date_key,
    date_value as full_date,

    extract(year from date_value) as year,
    extract(quarter from date_value) as quarter,
    extract(month from date_value) as month,
    format_date('%B', date_value) as month_name,

    extract(isoyear from date_value) as iso_year,
    extract(isoweek from date_value) as week,

    extract(day from date_value) as day,
    format_date('%A', date_value) as day_name,
    extract(dayofweek from date_value) as day_of_week,

    extract(dayofweek from date_value) in (1, 7) as is_weekend

from dates