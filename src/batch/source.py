from dataclasses import dataclass
from pathlib import Path

import requests

from config.settings import (
    CITIBIKE_DOWNLOAD_DIR,
    CITIBIKE_TRIPDATA_BASE_URL,
    STATION_BASE_URL,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class Source:
    url: str
    local_path: Path


def get_trip_source(source_month: str) -> Source:
    """Build the Citi Bike trip source for a month."""
    filename = f"{source_month.replace('-', '')}-citibike-tripdata.zip"

    return Source(
        url=f"{CITIBIKE_TRIPDATA_BASE_URL.rstrip('/')}/{filename}",
        local_path=Path(CITIBIKE_DOWNLOAD_DIR) / filename,
    )


def get_station_source() -> Source:
    """Build the Citi Bike station information source."""
    return Source(
        url=STATION_BASE_URL,
        local_path=(Path(CITIBIKE_DOWNLOAD_DIR) / "station_information.json"),
    )


def source_is_available(source: Source) -> bool:
    """Check whether a source is currently available."""
    try:
        response = requests.head(source.url, allow_redirects=True, timeout=30)
        response.raise_for_status()
        return True

    except requests.RequestException as exc:
        logger.info("Source is not available: %s (%s)", source.url, exc)
        return False
