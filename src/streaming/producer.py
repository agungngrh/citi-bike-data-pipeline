import json
import signal
import time
import uuid
from datetime import datetime, timezone
from threading import Event
from typing import Any

import requests
from kafka import KafkaProducer

from config.settings import PRODUCER_ID, settings
from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

shutdown_event = Event()


class FeedEnvelopeError(Exception):
    def __init__(self, error_type: str, message: str, raw_body: str) -> None:
        self.error_type = error_type
        self.message = message
        self.raw_body = raw_body


def get_station_status_url(gbfs_url: str) -> str:
    """Find the station status feed URL from the GBFS feed index."""
    response = requests.get(gbfs_url, timeout=(10, 30))
    response.raise_for_status()

    payload: dict[str, Any] = response.json()
    feeds = payload.get("data", {}).get("en", {}).get("feeds", [])

    for feed in feeds:
        if feed.get("name") == "station_status":
            return feed["url"]

    raise RuntimeError("station_status feed not found")


def fetch_station_status(
    url: str,
) -> tuple[dict[str, Any] | None, int | None, str | None]:
    """Fetch the station status feed with limited retries for transient errors."""
    for attempt in range(3):
        try:
            response = requests.get(url, timeout=(10, 30))

            if 400 <= response.status_code < 500:
                return None, response.status_code, None

            if response.status_code >= 500:
                if attempt == 2:
                    return None, response.status_code, None

                time.sleep(2 ** (attempt + 1))
                continue

            try:
                return response.json(), response.status_code, None
            except ValueError:
                return None, response.status_code, response.text[:1000]

        except requests.RequestException:
            if attempt == 2:
                return None, None, None

            time.sleep(2 ** (attempt + 1))

    return None, None, None


def parse_feed_envelope(payload: dict[str, Any]) -> tuple[list[Any], int, int]:
    """Validate the GBFS response structure and return its station data and metadata."""
    data = payload.get("data")

    if not isinstance(data, dict):
        raise FeedEnvelopeError(
            "INVALID_FEED_STRUCTURE",
            "Response data is not an object.",
            json.dumps(payload)[:1000],
        )

    stations = data.get("stations")

    if not isinstance(stations, list):
        raise FeedEnvelopeError(
            "INVALID_STATION_DATA",
            "data.stations is not a list.",
            json.dumps(data)[:1000],
        )

    feed_last_updated = payload.get("last_updated")
    feed_ttl = payload.get("ttl")

    if type(feed_last_updated) is not int or type(feed_ttl) is not int:
        raise FeedEnvelopeError(
            "INVALID_FEED_METADATA",
            "last_updated or ttl has an invalid type.",
            json.dumps(payload)[:1000],
        )

    return stations, feed_last_updated, feed_ttl


def is_valid_station(station: dict[str, Any]) -> bool:
    station_id = station.get("station_id")

    if not isinstance(station_id, str) or not station_id.strip():
        return False

    for field in (
        "last_reported",
        "num_bikes_available",
        "num_docks_available",
    ):
        if type(station.get(field)) is not int:
            return False

    for field in (
        "is_installed",
        "is_renting",
        "is_returning",
    ):
        if type(station.get(field)) is not int:
            return False

    for field in (
        "num_bikes_disabled",
        "num_docks_disabled",
    ):
        value = station.get(field)

        if value is not None and type(value) is not int:
            return False

    return True


def publish_dlq(
    producer: KafkaProducer,
    error_type: str,
    error_message: str,
    source_url: str,
    raw_body: str | None = None,
    http_status: int | None = None,
) -> None:
    """Send rejected feed or station records to the Kafka DLQ with failure context."""
    event = {
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "source_url": source_url,
        "http_status": http_status,
        "error_type": error_type,
        "error_message": error_message,
        "raw_body_snippet": raw_body,
        "producer_id": PRODUCER_ID,
    }

    producer.send(
        settings.kafka_station_status_dlq_topic,
        value=json.dumps(event).encode("utf-8"),
    )


def reject_feed(
    producer: KafkaProducer,
    error_type: str,
    error_message: str,
    source_url: str,
    raw_body: str | None,
    http_status: int | None,
) -> None:
    # This path returns before the normal poll cycle ends, so flush the DLQ message here.
    publish_dlq(
        producer=producer,
        error_type=error_type,
        error_message=error_message,
        source_url=source_url,
        raw_body=raw_body,
        http_status=http_status,
    )
    producer.flush()


def poll_once(station_status_url: str, producer: KafkaProducer) -> None:
    """Fetch one station snapshot, validate its records, and publish valid events to Kafka."""
    payload, http_status, raw_body = fetch_station_status(station_status_url)

    if payload is None:
        if http_status is not None and 200 <= http_status < 300:
            reject_feed(
                producer,
                "INVALID_JSON",
                "Station status response is not valid JSON.",
                station_status_url,
                raw_body,
                http_status,
            )
            logger.error(
                "Invalid JSON received from station status feed. http_status=%s",
                http_status,
            )
            return

        if http_status is not None and 400 <= http_status < 500:
            logger.warning(
                "Polling skipped because station status returned HTTP %s.",
                http_status,
            )
            return

        logger.warning(
            "Polling failed due to network or server error. Skipping this cycle."
        )
        return

    try:
        stations, feed_last_updated, feed_ttl = parse_feed_envelope(payload)
    except FeedEnvelopeError as exc:
        reject_feed(
            producer,
            exc.error_type,
            exc.message,
            station_status_url,
            exc.raw_body,
            http_status,
        )
        logger.error(
            "Feed rejected: error_type=%s, message=%s",
            exc.error_type,
            exc.message,
        )
        return

    snapshot_id = str(uuid.uuid4())
    poll_time = datetime.now(timezone.utc).isoformat()

    valid_count = 0
    rejected_count = 0

    for station in stations:
        if not isinstance(station, dict) or not is_valid_station(station):
            rejected_count += 1

            publish_dlq(
                producer=producer,
                error_type="INVALID_STATION_RECORD",
                error_message=(
                    "Station record is missing required fields or has invalid types."
                ),
                source_url=station_status_url,
                raw_body=json.dumps(station)[:1000],
                http_status=http_status,
            )
            continue

        event = {
            "station_id": station["station_id"],
            "num_bikes_available": station["num_bikes_available"],
            "num_bikes_disabled": station.get("num_bikes_disabled"),
            "num_docks_available": station["num_docks_available"],
            "num_docks_disabled": station.get("num_docks_disabled"),
            "is_installed": int(station["is_installed"]),
            "is_renting": int(station["is_renting"]),
            "is_returning": int(station["is_returning"]),
            "last_reported": station["last_reported"],
            "snapshot_id": snapshot_id,
            "poll_time": poll_time,
            "feed_last_updated": feed_last_updated,
            "feed_ttl": feed_ttl,
        }

        producer.send(
            settings.kafka_station_status_topic,
            key=station["station_id"].encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
        )

        valid_count += 1

    producer.flush()

    logger.info(
        "Poll completed: stations=%s, valid=%s, rejected=%s",
        len(stations),
        valid_count,
        rejected_count,
    )


def handle_shutdown(_signum: int, _frame: Any) -> None:
    logger.info("Shutdown signal received.")
    shutdown_event.set()


def main() -> None:
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    station_status_url = get_station_status_url(settings.citibike_gbfs_url)
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
    )

    logger.info(
        "Producer started. station_status_url=%s",
        station_status_url,
    )

    try:
        while not shutdown_event.is_set():
            poll_once(
                station_status_url=station_status_url,
                producer=producer,
            )

            # The GBFS feed is a periodic snapshot, so the producer polls it every 5 minutes.
            shutdown_event.wait(300)

    finally:
        producer.flush()
        producer.close()
        logger.info("Producer stopped.")


if __name__ == "__main__":
    main()
