"""
producer.py — Kafka producer that polls the Citi Bike GBFS real-time endpoints
every 30 seconds and publishes station-status messages to a Kafka topic.

Each message is a JSON object representing one station's current state:
  {
    "station_id": "...",
    "num_bikes_available": 5,
    "num_docks_available": 12,
    "num_ebikes_available": 2,
    "is_installed": 1,
    "is_renting": 1,
    "last_reported": 1712000000,
    "ingested_at": 1712000005
  }

Environment variables
---------------------
KAFKA_BOOTSTRAP_SERVERS  Comma-separated broker list (default: localhost:29092)
KAFKA_TOPIC              Topic name (default: citibike-station-status)
POLL_INTERVAL            Seconds between polls (default: 30)
"""

from __future__ import annotations

import json
import logging
import os
import time

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (overridable via environment variables)
# ---------------------------------------------------------------------------

GBFS_STATION_STATUS = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json"
GBFS_STATION_INFO = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092").split(",")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "citibike-station-status")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "30"))

# Retry settings
MAX_RETRIES = 5
RETRY_BACKOFF_SECONDS = 5


# ---------------------------------------------------------------------------
# Producer factory
# ---------------------------------------------------------------------------


def create_producer() -> KafkaProducer:
    """Create and return a KafkaProducer with JSON serialisation."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",                 # strongest delivery guarantee
        retries=3,
        max_block_ms=30_000,        # block up to 30 s waiting for metadata
    )


# ---------------------------------------------------------------------------
# GBFS fetch helpers
# ---------------------------------------------------------------------------


def fetch_station_status(timeout: int = 15) -> tuple[list[dict], int]:
    """
    Fetch current station-status data from the GBFS endpoint.

    Returns
    -------
    stations : list[dict]
        One dict per station.
    last_updated : int
        Unix timestamp from the GBFS feed.
    """
    response = requests.get(GBFS_STATION_STATUS, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    stations = payload["data"]["stations"]
    last_updated = payload["last_updated"]
    return stations, last_updated


# ---------------------------------------------------------------------------
# Main produce loop
# ---------------------------------------------------------------------------


def build_message(station: dict, ingested_at: int) -> dict:
    """Extract only the fields we want to publish."""
    return {
        "station_id": station["station_id"],
        "num_bikes_available": station.get("num_bikes_available", 0),
        "num_docks_available": station.get("num_docks_available", 0),
        "num_ebikes_available": station.get("num_ebikes_available", 0),
        "is_installed": station.get("is_installed", 0),
        "is_renting": station.get("is_renting", 0),
        "last_reported": station.get("last_reported", 0),
        "ingested_at": ingested_at,
    }


def on_send_error(exc: Exception) -> None:
    logger.error("Failed to deliver message: %s", exc)


def run_producer() -> None:
    """
    Main producer loop.  Polls GBFS every POLL_INTERVAL seconds and publishes
    one Kafka message per station.  Retries on transient fetch errors.
    Exits on unrecoverable Kafka errors.
    """
    producer = create_producer()
    logger.info(
        "Producer started — bootstrap=%s  topic=%s  interval=%ds",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        POLL_INTERVAL,
    )

    consecutive_errors = 0

    while True:
        try:
            stations, last_updated = fetch_station_status()
            consecutive_errors = 0  # reset on success

            for station in stations:
                msg = build_message(station, last_updated)
                producer.send(KAFKA_TOPIC, value=msg).add_errback(on_send_error)

            producer.flush()
            logger.info("Published %d station updates (feed_ts=%d)", len(stations), last_updated)

        except requests.RequestException as exc:
            consecutive_errors += 1
            logger.warning(
                "GBFS fetch failed (%d/%d): %s",
                consecutive_errors,
                MAX_RETRIES,
                exc,
            )
            if consecutive_errors >= MAX_RETRIES:
                logger.error("Max retries exceeded — shutting down producer")
                break
            time.sleep(RETRY_BACKOFF_SECONDS)
            continue

        except KafkaError as exc:
            logger.error("Unrecoverable Kafka error: %s", exc)
            break

        time.sleep(POLL_INTERVAL)

    producer.close()
    logger.info("Producer stopped")


if __name__ == "__main__":
    run_producer()
