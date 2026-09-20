import json
from datetime import date
from pathlib import Path

from google.cloud import storage

from config.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

TRIP_PREFIX = "raw/trip_history"
STATION_PREFIX = "raw/station_information"


def _upload(
    bucket: storage.Bucket,
    local_file: Path,
    object_name: str,
    content_type: str,
) -> None:
    """Upload one local file to GCS."""
    try:
        bucket.blob(object_name).upload_from_filename(
            str(local_file),
            content_type=content_type,
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to upload {local_file.name} "
            f"to gs://{settings.gcs_bucket}/{object_name}"
        ) from exc


def json_to_ndjson(
    source_file: Path,
    output_file: Path,
    snapshot_date: date,
    batch_id: str,
) -> Path:
    """Convert station JSON records to NDJSON."""
    with source_file.open("r", encoding="utf-8") as file:
        source_data = json.load(file)

    stations = source_data.get("data", {}).get("stations", [])

    if not stations:
        raise ValueError(f"No station records found in {source_file.name}")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", encoding="utf-8") as file:
        for station in stations:
            record = {
                **station,
                "snapshot_date": snapshot_date.isoformat(),
                "batch_id": batch_id,
            }

            file.write(json.dumps(record, separators=(",", ":")) + "\n")

    logger.info(
        "Prepared %s station records for landing: %s",
        len(stations),
        output_file,
    )

    return output_file


def build_trip_object_name(trip_date: date, source_month: str) -> str:
    """Build the GCS path for a daily trip file."""
    return (
        f"{TRIP_PREFIX}/"
        f"source_month={source_month}/"
        f"year={trip_date:%Y}/"
        f"month={trip_date:%m}/"
        f"day={trip_date:%d}/"
        f"{trip_date:%Y%m%d}-tripdata.csv"
    )


def _delete_trip_batch(bucket: storage.Bucket, source_month: str) -> None:
    """Remove the existing GCS footprint for a trip batch."""
    prefix = f"{TRIP_PREFIX}/source_month={source_month}/"
    deleted = 0

    for blob in bucket.list_blobs(prefix=prefix):
        blob.delete()
        deleted += 1

    if deleted:
        logger.info(
            "Removed %s existing trip objects for %s",
            deleted,
            source_month,
        )


def upload_tripdata_to_gcs(
    daily_files: list[Path],
    source_month: str,
) -> list[str]:
    """Upload daily trip files to the GCS Raw layer."""
    if not daily_files:
        raise ValueError(f"No daily trip files found for {source_month}")

    client = storage.Client()
    bucket = client.bucket(settings.gcs_bucket)

    _delete_trip_batch(bucket=bucket, source_month=source_month)

    uploaded_objects: list[str] = []

    for daily_file in daily_files:
        trip_date = date.fromisoformat(daily_file.stem)
        object_name = build_trip_object_name(
            trip_date=trip_date,
            source_month=source_month,
        )

        _upload(
            bucket=bucket,
            local_file=daily_file,
            object_name=object_name,
            content_type="text/csv",
        )

        uploaded_objects.append(object_name)

    logger.info(
        "Uploaded %s daily trip files for %s",
        len(uploaded_objects),
        source_month,
    )

    return uploaded_objects


def build_station_object_name(snapshot_date: date) -> str:
    """Build the GCS path for a station snapshot."""
    return (
        f"{STATION_PREFIX}/"
        f"year={snapshot_date:%Y}/"
        f"month={snapshot_date:%m}/"
        f"day={snapshot_date:%d}/"
        f"{snapshot_date:%Y%m%d}-station-information.ndjson"
    )


def upload_station_to_gcs(local_file: Path, snapshot_date: date) -> str:
    """Upload a station snapshot to the GCS Raw layer."""
    client = storage.Client()
    bucket = client.bucket(settings.gcs_bucket)

    object_name = build_station_object_name(snapshot_date)

    _upload(
        bucket=bucket,
        local_file=local_file,
        object_name=object_name,
        content_type="application/x-ndjson",
    )

    logger.info("Uploaded station snapshot: %s", object_name)

    return object_name
