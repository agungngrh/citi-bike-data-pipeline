import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

CITIBIKE_TRIPDATA_BASE_URL = "https://s3.amazonaws.com/tripdata"
CITIBIKE_STATION_INFORMATION_URL = (
    "https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_information.json"
)
STATION_BASE_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
CITIBIKE_DOWNLOAD_DIR = "/tmp/citibike"

DBT_PROJECT_DIR = "/opt/airflow/app/dbt"
DBT_BIN = "/opt/dbt_venv/bin/dbt"
PRODUCER_ID = "citibike-station-status-producer"


@dataclass(frozen=True)
class Settings:
    gcp_project_id: str
    bq_location: str
    gcs_bucket: str
    gcs_temp_bucket: str

    citibike_gbfs_url: str

    bq_raw_dataset: str
    bq_landing_dataset: str
    bq_staging_dataset: str
    bq_intermediate_dataset: str
    bq_mart_dataset: str
    bq_ops_dataset: str

    kafka_bootstrap_servers: str
    kafka_station_status_topic: str
    kafka_station_status_dlq_topic: str

    spark_master_url: str
    spark_app_name: str

    slack_webhook_url: str

    @property
    def spark_checkpoint_path(self) -> str:
        return f"gs://{self.gcs_bucket}/checkpoint/spark/station_status"

    @property
    def pipeline_run_log_table_id(self) -> str:
        return f"{self.gcp_project_id}." f"{self.bq_ops_dataset}." "pipeline_run_log"


settings = Settings(
    gcp_project_id=os.environ["GCP_PROJECT_ID"],
    bq_location=os.environ["BUCKET_LOCATION"],
    gcs_bucket=os.environ["GCS_BUCKET"],
    gcs_temp_bucket=os.environ["GCS_TEMP_BUCKET"],
    citibike_gbfs_url=os.environ["CITIBIKE_GBFS_URL"],
    bq_raw_dataset=os.environ["BQ_RAW_DATASET"],
    bq_landing_dataset=os.environ["BQ_LANDING_DATASET"],
    bq_staging_dataset=os.environ["BQ_STAGING_DATASET"],
    bq_intermediate_dataset=os.environ["BQ_INTERMEDIATE_DATASET"],
    bq_mart_dataset=os.environ["BQ_MART_DATASET"],
    bq_ops_dataset=os.environ["BQ_OPS_DATASET"],
    kafka_bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    kafka_station_status_topic=os.environ["KAFKA_STATION_STATUS_TOPIC"],
    kafka_station_status_dlq_topic=os.environ["KAFKA_STATION_STATUS_DLQ_TOPIC"],
    spark_master_url=os.environ["SPARK_MASTER_URL"],
    spark_app_name=os.environ["SPARK_APP_NAME"],
    slack_webhook_url=os.environ["SLACK_WEBHOOK_URL"],
)
