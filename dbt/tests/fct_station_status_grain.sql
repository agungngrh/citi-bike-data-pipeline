select
    station_id,
    snapshot_id,
    count(*) as row_count
from {{ ref('fct_station_status') }}
group by
    station_id,
    snapshot_id
having count(*) > 1