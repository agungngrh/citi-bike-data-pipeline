from typing import Any

from google.cloud import bigquery

from config.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

_bq_client: bigquery.Client | None = None


def _get_bq_client() -> bigquery.Client:
    """Get the shared BigQuery client."""
    global _bq_client

    if _bq_client is None:
        _bq_client = bigquery.Client(project=settings.gcp_project_id)

    return _bq_client


def log_pipeline_run(log_data: dict[str, Any]) -> None:
    """Write one pipeline task attempt log to BigQuery."""
    try:
        client = _get_bq_client()

        errors = client.insert_rows_json(
            settings.pipeline_run_log_table_id,
            [log_data],
            timeout=10,
        )

        if errors:
            logger.error(
                "Failed to write pipeline run log: %s",
                errors,
            )
            return

        logger.info(
            "Pipeline run logged: dag=%s, batch=%s, task=%s, " "status=%s, attempt=%s",
            log_data.get("dag_id"),
            log_data.get("batch_id"),
            log_data.get("task_id"),
            log_data.get("status"),
            log_data.get("try_number"),
        )

    except Exception:
        logger.exception("Failed to save pipeline run log")
