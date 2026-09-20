import requests

from config.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

MAX_ERROR_LENGTH = 500


def notify_failure(
    *,
    batch_id: str | None,
    task_id: str,
    dag_run_id: str,
    error_message: str | None = None,
) -> None:
    if not settings.slack_webhook_url:
        logger.warning("Slack webhook URL is not configured; notification skipped.")
        return

    display_batch_id = batch_id or "N/A"

    display_error = error_message or "No error message captured"

    if len(display_error) > MAX_ERROR_LENGTH:
        display_error = f"{display_error[:MAX_ERROR_LENGTH]}..."

    payload = {
        "text": (
            "*Pipeline Task Failed*\n"
            f"Batch: {display_batch_id}\n"
            f"Task: {task_id}\n"
            f"Run: {dag_run_id}\n"
            f"Error:\n{display_error}"
        )
    }

    try:
        response = requests.post(settings.slack_webhook_url, json=payload, timeout=10)
        response.raise_for_status()

        logger.info(
            "Slack failure alert sent: batch=%s, task=%s", display_batch_id, task_id
        )

    except requests.RequestException:
        logger.exception(
            "Failed to send Slack failure alert: batch=%s, task=%s",
            display_batch_id,
            task_id,
        )
