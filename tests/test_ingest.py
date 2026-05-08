"""Tests for src/ingest.py"""
from __future__ import annotations

import io
import zipfile
from unittest.mock import MagicMock, patch

import pytest

from src.ingest import (
    _month_keys,
    _trip_data_url,
    download_month,
    extract_csv_from_zip,
    ingest_month,
)


# ---------------------------------------------------------------------------
# _month_keys
# ---------------------------------------------------------------------------


def test_month_keys_single_year():
    keys = list(_month_keys(2023, 2023))
    assert len(keys) == 12
    assert keys[0] == "202301"
    assert keys[-1] == "202312"


def test_month_keys_multi_year():
    keys = list(_month_keys(2022, 2023))
    assert len(keys) == 24
    assert "202212" in keys
    assert "202301" in keys


# ---------------------------------------------------------------------------
# _trip_data_url
# ---------------------------------------------------------------------------


def test_trip_data_url_format_pre2024_uses_annual_zip():
    # 2023 and earlier → annual ZIP (e.g. 2023-citibike-tripdata.zip)
    url = _trip_data_url("202301")
    assert "2023-citibike-tripdata.zip" in url
    assert url.startswith("https://")


def test_trip_data_url_format_2024_onwards_uses_monthly_zip():
    # 2024+ → monthly ZIP (e.g. 202401-citibike-tripdata.zip)
    url = _trip_data_url("202401")
    assert "202401" in url
    assert url.startswith("https://")
    assert url.endswith(".zip")


# ---------------------------------------------------------------------------
# download_month
# ---------------------------------------------------------------------------


def _make_zip_bytes(csv_content: str = "ride_id,started_at\n1,2023-01-01 00:00:00") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("202301-citibike-tripdata.csv", csv_content)
    return buf.getvalue()


@patch("src.ingest.requests.get")
def test_download_month_success(mock_get):
    zip_bytes = _make_zip_bytes()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.content = zip_bytes
    mock_get.return_value = mock_resp

    result = download_month("202301")
    assert result == zip_bytes


@patch("src.ingest.requests.get")
def test_download_month_404_returns_none(mock_get):
    mock_resp = MagicMock()
    mock_resp.status_code = 404
    mock_get.return_value = mock_resp

    result = download_month("202399")
    assert result is None


# ---------------------------------------------------------------------------
# extract_csv_from_zip
# ---------------------------------------------------------------------------


def test_extract_csv_from_zip():
    csv_data = "ride_id,started_at\n1,2023-01-01"
    zip_bytes = _make_zip_bytes(csv_data)
    name, contents = extract_csv_from_zip(zip_bytes)
    assert name.endswith(".csv")
    assert b"ride_id" in contents


def test_extract_csv_from_zip_no_csv_raises():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("readme.txt", "no csv here")
    with pytest.raises(ValueError, match="no CSV"):
        extract_csv_from_zip(buf.getvalue())


# ---------------------------------------------------------------------------
# ingest_month – uses moto for S3
# ---------------------------------------------------------------------------


@pytest.fixture
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("S3_BUCKET", "test-bucket")


@patch("src.ingest.requests.get")
def test_ingest_month_uploads_to_s3(mock_get, aws_credentials):
    from moto import mock_aws
    import boto3

    zip_bytes = _make_zip_bytes()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.content = zip_bytes
    mock_get.return_value = mock_resp

    config = {
        "aws": {"s3_bucket": "test-bucket", "region": "us-east-1"},
        "s3": {"raw_prefix": "raw/citibike"},
    }

    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")
        result = ingest_month("202301", config, s3_client=s3)

    assert result is True
