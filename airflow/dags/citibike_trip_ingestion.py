import re
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.timetables.interval import CronDataIntervalTimetable
from google.cloud import bigquery

from config.settings import settings
from src.batch.pipeline import stage_trip_batch, upload_trip_batch
from src.batch.source import get_trip_source, source_is_available
from src.observability.callbacks import (
    on_task_failure,
    on_task_retry,
    on_task_success,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)

RAW_TRIP_TABLE = f"{settings.gcp_project_id}.{settings.bq_raw_dataset}.trip_history"
LOCAL_TIMEZONE = pendulum.timezone("Asia/Jakarta")

DEFAULT_ARGS = {
    "owner": "agung_nugraha",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(hours=1),
    "on_success_callback": on_task_success,
    "on_retry_callback": on_task_retry,
    "on_failure_callback": on_task_failure,
}


def get_source_month(context) -> str:
    return context["data_interval_start"].in_timezone(LOCAL_TIMEZONE).strftime("%Y-%m")


def trip_source_is_available(**context) -> bool:
    source_month = get_source_month(context)
    source = get_trip_source(source_month)

    return source_is_available(source)


def stage_trip_source(**context) -> dict:
    source_month = get_source_month(context)

    return stage_trip_batch(source_month)


def upload_trip_to_gcs(**context) -> dict:
    result = context["ti"].xcom_pull(task_ids="stage_trip_source")

    if not result:
        raise ValueError("stage_trip_source returned no result")

    source_month = result.get("source_month")

    if not source_month:
        raise ValueError("No source_month returned by stage_trip_source")

    return upload_trip_batch(source_month)


def load_trip_to_bq(**context) -> None:
    result = context["ti"].xcom_pull(task_ids="upload_trip_to_gcs")

    if not result:
        raise ValueError("upload_trip_to_gcs returned no result")

    uploaded_paths = result.get("uploaded_paths", [])

    if not uploaded_paths:
        raise ValueError("No uploaded trip objects found")

    bq_hook = BigQueryHook(
        gcp_conn_id="google_cloud_default",
        use_legacy_sql=False,
    )

    client = bq_hook.get_client(project_id=settings.gcp_project_id)
    raw_table = client.get_table(RAW_TRIP_TABLE)

    loaded_partitions = set()

    logger.info("Starting Raw trip load: %s files", len(uploaded_paths))

    for object_path in sorted(uploaded_paths):
        match = re.search(
            r"/day=\d{2}/(\d{8})-tripdata\.csv$",
            object_path,
        )

        if not match:
            raise ValueError(
                f"Cannot determine partition date from object: {object_path}"
            )

        partition_date = match.group(1)

        if partition_date in loaded_partitions:
            raise ValueError(
                f"Multiple GCS objects found for partition {partition_date}: "
                f"{object_path}"
            )

        loaded_partitions.add(partition_date)

        source_uri = f"gs://{settings.gcs_bucket}/{object_path}"
        destination = f"{RAW_TRIP_TABLE}${partition_date}"

        job_config = bigquery.LoadJobConfig(
            schema=raw_table.schema,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            field_delimiter=",",
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
            max_bad_records=0,
            ignore_unknown_values=False,
            allow_quoted_newlines=False,
            encoding="UTF-8",
        )

        logger.info("Loading %s -> %s", source_uri, destination)

        load_job = client.load_table_from_uri(
            source_uri,
            destination,
            job_config=job_config,
        )

        load_job.result()

        logger.info("Loaded partition %s successfully", partition_date)

    logger.info(
        "Raw trip load completed: %s partitions loaded",
        len(loaded_partitions),
    )


with DAG(
    dag_id="citibike_trip_ingestion",
    schedule=CronDataIntervalTimetable("0 2 1 * *", timezone=LOCAL_TIMEZONE),
    start_date=pendulum.datetime(2026, 2, 1, tz=LOCAL_TIMEZONE),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "batch_prefix": "trip_history",
        "batch_granularity": "month",
    },
    tags=["citibike", "trip", "batch"],
) as dag:

    wait_for_trip_source = PythonSensor(
        task_id="wait_for_trip_source",
        python_callable=trip_source_is_available,
        mode="reschedule",
        poke_interval=300,
        timeout=60 * 60 * 2,
    )

    stage_trip_source_task = PythonOperator(
        task_id="stage_trip_source",
        python_callable=stage_trip_source,
    )

    upload_trip_to_gcs_task = PythonOperator(
        task_id="upload_trip_to_gcs",
        python_callable=upload_trip_to_gcs,
    )

    load_trip_to_bq_task = PythonOperator(
        task_id="load_trip_to_bq",
        python_callable=load_trip_to_bq,
    )

    (
        wait_for_trip_source
        >> stage_trip_source_task
        >> upload_trip_to_gcs_task
        >> load_trip_to_bq_task
    )
