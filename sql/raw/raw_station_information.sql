CREATE OR REPLACE TABLE `jcdeah-009.citibike_raw.station_information`
(
    station_id STRING,
    name STRING,
    short_name STRING,
    region_id STRING,

    lat FLOAT64,
    lon FLOAT64,

    capacity INT64,

    rental_uris JSON,
    rental_methods ARRAY<STRING>,

    has_kiosk BOOL,
    station_type STRING,
    external_id STRING,

    electric_bike_surcharge_waiver BOOL,

    eightd_station_services JSON,
    eightd_has_key_dispenser BOOL,

    snapshot_date DATE NOT NULL,
    batch_id STRING NOT NULL
)
PARTITION BY snapshot_date
CLUSTER BY station_id;