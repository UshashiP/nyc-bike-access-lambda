"""Tests for src/streaming/producer.py"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from src.streaming.producer import build_message, fetch_station_status


# ---------------------------------------------------------------------------
# build_message
# ---------------------------------------------------------------------------


def test_build_message_all_fields():
    station = {
        "station_id": "66db2085-0aca-11e7-82f6-3863bb44ef7c",
        "num_bikes_available": 5,
        "num_docks_available": 12,
        "num_ebikes_available": 2,
        "is_installed": 1,
        "is_renting": 1,
        "last_reported": 1712000000,
    }
    msg = build_message(station, ingested_at=1712000005)

    assert msg["station_id"] == station["station_id"]
    assert msg["num_bikes_available"] == 5
    assert msg["num_ebikes_available"] == 2
    assert msg["ingested_at"] == 1712000005


def test_build_message_missing_optional_fields_defaults_to_zero():
    station = {"station_id": "abc"}  # all numeric fields missing
    msg = build_message(station, ingested_at=100)
    assert msg["num_bikes_available"] == 0
    assert msg["num_docks_available"] == 0
    assert msg["num_ebikes_available"] == 0


def test_build_message_is_json_serialisable():
    station = {
        "station_id": "xyz",
        "num_bikes_available": 3,
        "num_docks_available": 7,
        "num_ebikes_available": 1,
        "is_installed": 1,
        "is_renting": 1,
        "last_reported": 1712000000,
    }
    msg = build_message(station, ingested_at=1712000001)
    # Must not raise
    serialised = json.dumps(msg)
    recovered = json.loads(serialised)
    assert recovered["station_id"] == "xyz"


# ---------------------------------------------------------------------------
# fetch_station_status
# ---------------------------------------------------------------------------


_FAKE_GBFS_RESPONSE = {
    "last_updated": 1712000000,
    "ttl": 30,
    "data": {
        "stations": [
            {
                "station_id": "A01",
                "num_bikes_available": 4,
                "num_docks_available": 8,
                "num_ebikes_available": 1,
                "is_installed": 1,
                "is_renting": 1,
                "last_reported": 1711999990,
            },
            {
                "station_id": "A02",
                "num_bikes_available": 0,
                "num_docks_available": 15,
                "num_ebikes_available": 0,
                "is_installed": 1,
                "is_renting": 1,
                "last_reported": 1711999985,
            },
        ]
    },
}


@patch("src.streaming.producer.requests.get")
def test_fetch_station_status_returns_stations_and_timestamp(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = _FAKE_GBFS_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    stations, last_updated = fetch_station_status()
    assert len(stations) == 2
    assert last_updated == 1712000000
    assert stations[0]["station_id"] == "A01"


@patch("src.streaming.producer.requests.get")
def test_fetch_station_status_raises_on_http_error(mock_get):
    import requests

    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = requests.HTTPError("503 Service Unavailable")
    mock_get.return_value = mock_resp

    with pytest.raises(requests.HTTPError):
        fetch_station_status()
