{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        "field": "full_date",
        "data_type": "date",
        "granularity": "day"
    }
) }}

select
    f.start_date_key as date_key,
    d.full_date,
    f.member_casual,

    count(*) as trip_count,
    sum(f.duration_minutes) as total_duration_minutes

from {{ ref('fct_trip') }} f

join {{ ref('dim_date') }} d
    on f.start_date_key = d.date_key

{% if is_incremental() %}
where f.start_date >= date('{{ var("source_month") }}')
  and f.start_date < date_add(
      date('{{ var("source_month") }}'),
      interval 1 month
  )
{% endif %}

group by
    f.start_date_key,
    d.full_date,
    f.member_casual