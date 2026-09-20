import shutil
from datetime import date
from pathlib import Path

from config.settings import CITIBIKE_DOWNLOAD_DIR
from src.batch.download import download_source
from src.batch.extract import extract_tripdata
from src.batch.landing import (
    json_to_ndjson,
    upload_station_to_gcs,
    upload_tripdata_to_gcs,
)
from src.batch.partition import partition_daily_files
from src.batch.source import get_station_source, get_trip_source
from src.utils.logger import get_logger

logger = get_logger(__name__)


def stage_trip_batch(source_month: str) -> dict:
    """Stage one monthly trip batch locally."""
    batch_id = f"trip_history_{source_month}"

    source = get_trip_source(source_month)
    archive_path = download_source(source)

    extract_dir = Path(CITIBIKE_DOWNLOAD_DIR) / "extracted" / source_month

    csv_files = extract_tripdata(
        zip_path=archive_path,
        output_dir=extract_dir,
    )

    if not csv_files:
        raise ValueError(f"No trip CSV files extracted for {source_month}")

    partition_dir = Path(CITIBIKE_DOWNLOAD_DIR) / "partitioned" / source_month

    if partition_dir.exists():
        shutil.rmtree(partition_dir)

    daily_files = []

    for csv_file in csv_files:
        logger.info(
            "Partitioning %s for %s",
            csv_file.name,
            source_month,
        )

        daily_files.extend(
            partition_daily_files(
                source_file=csv_file,
                output_dir=partition_dir,
                source_month=source_month,
                batch_id=batch_id,
            )
        )

    daily_files = sorted(set(daily_files))

    if not daily_files:
        raise ValueError(f"No daily trip files produced for {source_month}")

    logger.info(
        "Trip batch staged: %s (%s daily files)",
        batch_id,
        len(daily_files),
    )

    return {
        "batch_id": batch_id,
        "source_month": source_month,
        "file_count": len(daily_files),
    }


def upload_trip_batch(source_month: str) -> dict:
    """Upload a prepared monthly trip batch to GCS."""
    partition_dir = Path(CITIBIKE_DOWNLOAD_DIR) / "partitioned" / source_month

    daily_files = sorted(partition_dir.rglob("*.csv"))

    if not daily_files:
        raise ValueError(f"No partitioned trip files found for {source_month}")

    uploaded_paths = upload_tripdata_to_gcs(
        daily_files=daily_files,
        source_month=source_month,
    )

    batch_id = f"trip_history_{source_month}"

    logger.info(
        "Trip batch uploaded: %s (%s daily files)",
        batch_id,
        len(uploaded_paths),
    )

    return {
        "batch_id": batch_id,
        "source_month": source_month,
        "uploaded_paths": uploaded_paths,
        "file_count": len(uploaded_paths),
    }


def stage_station_information(snapshot_date: date) -> dict:
    """Stage one station information snapshot."""
    batch_id = f"station_information_{snapshot_date.isoformat()}"

    source = get_station_source()
    source_file = download_source(source)

    ndjson_file = (
        Path(CITIBIKE_DOWNLOAD_DIR)
        / "station"
        / f"{snapshot_date:%Y%m%d}-station-information.ndjson"
    )

    json_to_ndjson(
        source_file=source_file,
        output_file=ndjson_file,
        snapshot_date=snapshot_date,
        batch_id=batch_id,
    )

    gcs_path = upload_station_to_gcs(
        local_file=ndjson_file,
        snapshot_date=snapshot_date,
    )

    logger.info("Station batch staged: %s", batch_id)

    return {
        "batch_id": batch_id,
        "snapshot_date": snapshot_date.isoformat(),
        "gcs_path": gcs_path,
    }
