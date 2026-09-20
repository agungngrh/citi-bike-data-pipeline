from datetime import datetime, timezone
from typing import Any

from src.observability.ops_logger import log_pipeline_run
from src.observability.slack_notifier import notify_failure
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _get_try_number(context: dict[str, Any]) -> int:
    """Get the current task attempt number."""
    try_number = context.get("try_number")

    if try_number is not None:
        return int(try_number)

    ti = context.get("ti")

    if ti is not None and hasattr(ti, "try_number"):
        return int(ti.try_number)

    logger.warning("try_number unavailable; defaulting to 1")
    return 1


def _get_batch_id(context: dict[str, Any]) -> str | None:
    """Build batch ID when the DAG defines a batch configuration."""
    dag = context.get("dag")

    if dag is None:
        return None

    prefix = dag.params.get("batch_prefix")
    granularity = dag.params.get("batch_granularity")

    if not prefix or not granularity:
        return None

    params = context.get("params") or {}
    period = params.get("batch_period")

    if not period:
        start = context["data_interval_start"]

        if granularity == "month":
            period = start.strftime("%Y-%m")
        else:
            period = start.strftime("%Y-%m-%d")

    return f"{prefix}_{period}"


def _get_error_message(context: dict[str, Any]) -> str | None:
    exception = context.get("exception")

    return str(exception) if exception else None


def _build_log(
    context: dict[str, Any],
    status: str,
) -> dict[str, Any]:
    ti = context["ti"]
    dag_run = context["dag_run"]

    return {
        "run_id": dag_run.run_id,
        "dag_id": dag_run.dag_id,
        "batch_id": _get_batch_id(context),
        "task_id": ti.task_id,
        "try_number": _get_try_number(context),
        "start_time": ti.start_date.isoformat(),
        "end_time": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "error_message": _get_error_message(context),
    }


def _write_log(
    context: dict[str, Any],
    status: str,
) -> None:
    try:
        log_pipeline_run(
            _build_log(
                context,
                status,
            )
        )
    except Exception:
        logger.exception("Failed to write pipeline run log")


def on_task_success(context: dict[str, Any]) -> None:
    _write_log(context, "SUCCESS")


def on_task_retry(context: dict[str, Any]) -> None:
    _write_log(context, "FAILED")


def on_task_failure(context: dict[str, Any]) -> None:
    _write_log(context, "FAILED")

    try:
        ti = context["ti"]
        dag_run = context["dag_run"]
        batch_id = _get_batch_id(context)

        notify_failure(
            batch_id=batch_id,
            task_id=ti.task_id,
            dag_run_id=dag_run.run_id,
            error_message=_get_error_message(context),
        )
    except Exception:
        logger.exception("Failed to send Slack failure notification")
