CREATE TABLE IF NOT EXISTS `jcdeah-009.citibike_raw.trip_history`
(
    ride_id STRING,
    rideable_type STRING,
    started_at DATETIME,
    ended_at DATETIME,
    start_station_name STRING,
    start_station_id STRING,
    end_station_name STRING,
    end_station_id STRING,
    start_lat FLOAT64,
    start_lng FLOAT64,
    end_lat FLOAT64,
    end_lng FLOAT64,
    member_casual STRING,
    source_month DATE NOT NULL,
    batch_id STRING NOT NULL
)
PARTITION BY DATE(started_at)
CLUSTER BY batch_id, start_station_id, end_station_id;