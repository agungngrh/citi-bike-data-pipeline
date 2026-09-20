CREATE TABLE IF NOT EXISTS `jcdeah-009.citibike_ops.pipeline_run_log`
(
    run_id STRING NOT NULL,
    dag_id STRING NOT NULL,
    batch_id STRING,
    task_id STRING NOT NULL,
    try_number INT64 NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status STRING NOT NULL,
    error_message STRING
)
PARTITION BY DATE(start_time)
CLUSTER BY dag_id, batch_id, task_id;