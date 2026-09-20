import csv
from datetime import date
from pathlib import Path
from typing import Any, TextIO

from src.utils.logger import get_logger

logger = get_logger(__name__)


def partition_daily_files(
    source_file: Path,
    output_dir: Path,
    source_month: str,
    batch_id: str,
) -> list[Path]:
    """Split a trip CSV into daily CSV files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    writers: dict[str, Any] = {}
    file_handles: dict[str, TextIO] = {}
    output_paths: dict[str, Path] = {}

    total_rows = 0
    source_month_date = f"{source_month}-01"

    try:
        with source_file.open("r", newline="", encoding="utf-8-sig") as file:
            reader = csv.DictReader(file)

            if not reader.fieldnames:
                raise ValueError(f"CSV file has no header: {source_file.name}")

            if "started_at" not in reader.fieldnames:
                raise ValueError(
                    f"Required column 'started_at' is missing in {source_file.name}"
                )

            output_fields = [*reader.fieldnames, "source_month", "batch_id"]

            for row in reader:
                total_rows += 1
                started_at = row.get("started_at")

                if not started_at:
                    raise ValueError(f"Missing started_at in {source_file.name}")

                try:
                    trip_date = date.fromisoformat(started_at[:10])
                except ValueError as exc:
                    raise ValueError(
                        f"Invalid started_at value: {started_at!r}"
                    ) from exc

                date_key = trip_date.isoformat()
                writer = writers.get(date_key)

                if writer is None:
                    daily_path = (
                        output_dir
                        / f"{trip_date:%Y}"
                        / f"{trip_date:%m}"
                        / f"{trip_date}.csv"
                    )

                    daily_path.parent.mkdir(parents=True, exist_ok=True)

                    file_exists = daily_path.exists()
                    file_has_data = file_exists and daily_path.stat().st_size > 0

                    file_handle = daily_path.open("a", newline="", encoding="utf-8")

                    writer = csv.DictWriter(file_handle, fieldnames=output_fields)

                    if not file_has_data:
                        writer.writeheader()

                    writers[date_key] = writer
                    file_handles[date_key] = file_handle
                    output_paths[date_key] = daily_path

                row["source_month"] = source_month_date
                row["batch_id"] = batch_id

                writer.writerow(row)

    finally:
        for file_handle in file_handles.values():
            file_handle.close()

    daily_files = [output_paths[key] for key in sorted(output_paths)]

    logger.info(
        "Partitioned %s: %s rows into %s daily files",
        source_file.name,
        total_rows,
        len(daily_files),
    )

    return daily_files
