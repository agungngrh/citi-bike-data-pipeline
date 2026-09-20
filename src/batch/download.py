from pathlib import Path

import requests

from src.batch.source import Source
from src.utils.logger import get_logger

logger = get_logger(__name__)


def download_source(source: Source) -> Path:
    """Download a source file to its local path."""
    source.local_path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = source.local_path.with_name(source.local_path.name + ".part")

    logger.info("Downloading source: %s", source.url)

    try:
        with requests.get(source.url, stream=True, timeout=(10, 60)) as response:
            response.raise_for_status()

            with temp_path.open("wb") as file:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        file.write(chunk)

        if temp_path.stat().st_size == 0:
            raise ValueError(f"Downloaded file is empty: {source.url}")

        temp_path.rename(source.local_path)

    except Exception:
        temp_path.unlink(missing_ok=True)
        raise

    logger.info(
        "Source downloaded: %s (%s bytes)",
        source.local_path,
        source.local_path.stat().st_size,
    )

    return source.local_path
