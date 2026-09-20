from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.timetables.trigger import CronTriggerTimetable
from google.cloud import bigquery

from config.settings import DBT_BIN, DBT_PROJECT_DIR, settings
from src.batch.pipeline import stage_station_information
from src.batch.source import get_station_source, source_is_available
from src.observability.callbacks import (
    on_task_failure,
    on_task_retry,
    on_task_success,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)

RAW_TRIP_TABLE = f"{settings.gcp_project_id}.{settings.bq_raw_dataset}.trip_history"
RAW_STATION_INFORMATION_TABLE = (
    f"{settings.gcp_project_id}.{settings.bq_raw_dataset}.station_information"
)

SOURCE_MONTH_VARS = (
    'source_month: "{{ ti.xcom_pull(task_ids="get_latest_trip_source_month") }}"'
)

LOCAL_TIMEZONE = pendulum.timezone("Asia/Jakarta")
NYC_TIMEZONE = pendulum.timezone("America/New_York")

DEFAULT_ARGS = {
    "owner": "agung_nugraha",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_success_callback": on_task_success,
    "on_retry_callback": on_task_retry,
    "on_failure_callback": on_task_failure,
}


def station_source_is_available() -> bool:
    return source_is_available(get_station_source())


def stage_station_snapshot(**context) -> dict:
    snapshot_date = context["logical_date"].in_timezone(NYC_TIMEZONE).date()

    return stage_station_information(snapshot_date)


def load_station_snapshot(**context) -> None:
    snapshot_date = context["logical_date"].in_timezone(NYC_TIMEZONE).date()

    object_path = (
        "raw/station_information/"
        f"year={snapshot_date:%Y}/"
        f"month={snapshot_date:%m}/"
        f"day={snapshot_date:%d}/"
        f"{snapshot_date:%Y%m%d}-station-information.ndjson"
    )

    source_uri = f"gs://{settings.gcs_bucket}/{object_path}"
    destination = f"{RAW_STATION_INFORMATION_TABLE}${snapshot_date:%Y%m%d}"

    bq_hook = BigQueryHook(
        gcp_conn_id="google_cloud_default",
        use_legacy_sql=False,
    )

    client = bq_hook.get_client(project_id=settings.gcp_project_id)
    raw_table = client.get_table(RAW_STATION_INFORMATION_TABLE)

    job_config = bigquery.LoadJobConfig(
        schema=raw_table.schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
        max_bad_records=0,
        ignore_unknown_values=False,
    )

    logger.info("Loading station snapshot: %s -> %s", source_uri, destination)

    load_job = client.load_table_from_uri(
        source_uri,
        destination,
        job_config=job_config,
    )

    load_job.result()

    logger.info("Station snapshot loaded successfully: %s", snapshot_date)


def get_latest_trip_source_month() -> str:
    """Get the latest trip batch available in Raw for dbt processing."""
    bq_hook = BigQueryHook(
        gcp_conn_id="google_cloud_default",
        use_legacy_sql=False,
    )

    client = bq_hook.get_client(project_id=settings.gcp_project_id)

    query = f"""
        select max(source_month) as source_month
        from `{RAW_TRIP_TABLE}`
    """

    result = client.query(query).result()
    row = next(iter(result))

    source_month = row.source_month

    if source_month is None:
        raise ValueError("No trip source month found in raw_trip_history")

    source_month = source_month.replace(day=1)

    logger.info("Latest landed trip source month: %s", source_month)

    return source_month.isoformat()


def create_dbt_layer(
    layer_name: str,
    selector: str,
    dbt_vars: str = "{}",
) -> BashOperator:
    """Create a dbt build task for one model layer."""
    return BashOperator(
        task_id=f"dbt_{layer_name}",
        bash_command=f"""
set -euo pipefail

{DBT_BIN} build \\
    --project-dir {DBT_PROJECT_DIR} \\
    --profiles-dir {DBT_PROJECT_DIR} \\
    --select {selector} \\
    --vars '{dbt_vars}'
""",
    )


with DAG(
    dag_id="citibike_batch_transform",
    description="Daily station refresh and batch warehouse transformation",
    schedule=CronTriggerTimetable("0 2 * * *", timezone=LOCAL_TIMEZONE),
    start_date=pendulum.datetime(2026, 9, 20, tz=LOCAL_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["citibike", "batch", "transform"],
) as dag:

    wait_for_station_source = PythonSensor(
        task_id="wait_for_station_source",
        python_callable=station_source_is_available,
        mode="reschedule",
        poke_interval=300,
        timeout=1800,
        retries=0,
    )

    stage_station_snapshot_task = PythonOperator(
        task_id="stage_station_snapshot",
        python_callable=stage_station_snapshot,
    )

    load_station_to_bq = PythonOperator(
        task_id="load_station_to_bq",
        python_callable=load_station_snapshot,
    )

    get_latest_trip_source_month_task = PythonOperator(
        task_id="get_latest_trip_source_month",
        python_callable=get_latest_trip_source_month,
    )

    dbt_staging = create_dbt_layer(
        layer_name="staging",
        selector="path:models/staging",
    )

    dbt_intermediate = create_dbt_layer(
        layer_name="intermediate",
        selector="path:models/intermediate",
        dbt_vars=SOURCE_MONTH_VARS,
    )

    dbt_mart = create_dbt_layer(
        layer_name="mart",
        selector="path:models/mart",
        dbt_vars=SOURCE_MONTH_VARS,
    )

    (
        wait_for_station_source
        >> stage_station_snapshot_task
        >> load_station_to_bq
        >> dbt_staging
    )

    [
        dbt_staging,
        get_latest_trip_source_month_task,
    ] >> dbt_intermediate

    dbt_intermediate >> dbt_mart
