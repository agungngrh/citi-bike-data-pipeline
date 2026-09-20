CREATE TABLE IF NOT EXISTS `jcdeah-009.citibike_raw.station_status`
(
    station_id STRING,
    num_bikes_available INT64,
    num_bikes_disabled INT64,
    num_docks_available INT64,
    num_docks_disabled INT64,
    is_installed INT64,
    is_renting INT64,
    is_returning INT64,
    last_reported INT64,
    snapshot_id STRING,
    poll_time TIMESTAMP,
    feed_last_updated INT64,
    feed_ttl INT64,
    kafka_topic STRING,
    kafka_partition INT64,
    kafka_offset INT64,
    kafka_timestamp TIMESTAMP,
    ingested_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY station_id;