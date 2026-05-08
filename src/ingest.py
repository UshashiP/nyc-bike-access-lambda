"""
ingest.py — Download historical Citi Bike trip CSVs from the public S3 bucket
and upload raw files to the project's S3 bronze layer.

Citi Bike publishes monthly CSVs at:
  https://s3.amazonaws.com/tripdata/<YYYYMM>-citibike-tripdata.csv.zip

Two schemas exist depending on year:
  - Legacy  (≤2020): trip duration, bike id, birth year, gender, user type
  - Current (≥2021): ride_id, rideable_type, member_or_casual, lat/lng coords
"""

from __future__ import annotations

import io
import logging
import os
import zipfile
from typing import Iterator
from urllib.parse import urljoin

import boto3
import requests
import yaml

logger = logging.getLogger(__name__)

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "pipeline_config.yaml")


def load_config() -> dict:
    # Auto-load .env from the project root so S3_BUCKET etc. are available
    # whether the script is invoked directly, via Airflow, or by pytest.
    try:
        from dotenv import load_dotenv

        env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
        load_dotenv(env_path, override=False)
    except ImportError:
        pass  # python-dotenv optional — env vars may already be in the shell

    with open(_CONFIG_PATH) as f:
        raw = f.read()
    # Expand environment-variable placeholders like ${VAR:-default}
    import re

    def _expand(match):
        expr = match.group(1)
        if ":-" in expr:
            var, default = expr.split(":-", 1)
            return os.environ.get(var, default)
        return os.environ.get(expr, match.group(0))

    expanded = re.sub(r"\$\{([^}]+)\}", _expand, raw)
    cfg = yaml.safe_load(expanded)

    # Fail fast with a clear message if S3_BUCKET is still unresolved
    bucket = cfg.get("aws", {}).get("s3_bucket", "")
    if not bucket or bucket.startswith("${"):
        raise RuntimeError(
            "S3_BUCKET environment variable is not set. "
            "Add it to your .env file or export it in your shell."
        )
    return cfg


def _month_keys(start_year: int, end_year: int) -> Iterator[str]:
    """Yield YYYYMM strings for every month in [start_year, end_year]."""
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            yield f"{year}{month:02d}"


def _trip_data_url(yyyymm: str) -> str:
    """
    Return the canonical Citi Bike public download URL for a given month.

    URL format changed after 2023:
      2023 and earlier → annual ZIP:  2023-citibike-tripdata.zip
      2024 onwards     → monthly ZIP: 202401-citibike-tripdata.zip
    """
    year = int(yyyymm[:4])
    if year <= 2023:
        # Annual ZIP — only valid for January of that year in our iteration,
        # all other months in the same year will 404 and be skipped gracefully.
        return f"https://s3.amazonaws.com/tripdata/{year}-citibike-tripdata.zip"
    return f"https://s3.amazonaws.com/tripdata/{yyyymm}-citibike-tripdata.zip"


def download_month(yyyymm: str, timeout: int = 120) -> bytes | None:
    """
    Download one month of trip data.  Returns raw ZIP bytes, or None if the
    file does not exist yet (404) so callers can skip future months gracefully.
    """
    url = _trip_data_url(yyyymm)
    logger.info("Downloading %s", url)
    response = requests.get(url, timeout=timeout)
    if response.status_code == 404:
        logger.warning("No data for %s (404) — skipping", yyyymm)
        return None
    response.raise_for_status()
    return response.content


def extract_csv_from_zip(zip_bytes: bytes) -> tuple[str, bytes]:
    """
    Unzip in-memory, return (filename, csv_bytes) for the first CSV inside.
    Raises ValueError if no CSV is found.
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv") and not n.startswith("__MACOSX")]
        if not csv_names:
            raise ValueError("ZIP contains no CSV files")
        name = csv_names[0]
        return name, zf.read(name)


def extract_all_csvs_from_zip(zip_bytes: bytes) -> list[tuple[str, bytes]]:
    """Extract all CSVs from a ZIP — used for annual ZIPs (pre-2024)."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv") and not n.startswith("__MACOSX")]
        if not csv_names:
            raise ValueError("ZIP contains no CSV files")
        return [(name, zf.read(name)) for name in csv_names]


def upload_to_s3(
    s3_client,
    bucket: str,
    key: str,
    data: bytes,
    content_type: str = "text/csv",
) -> None:
    logger.info("Uploading s3://%s/%s (%d bytes)", bucket, key, len(data))
    s3_client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)


def ingest_month(yyyymm: str, config: dict, s3_client=None) -> bool:
    """
    Full ingest pipeline for one month:
      1. Download ZIP from Citi Bike public S3
      2. Extract CSV(s)
      3. Upload to project S3 raw layer

    Returns True on success, False if data unavailable.
    """
    zip_bytes = download_month(yyyymm)
    if zip_bytes is None:
        return False

    if s3_client is None:
        s3_client = boto3.client("s3", region_name=config["aws"]["region"])

    bucket = config["aws"]["s3_bucket"]
    year = int(yyyymm[:4])

    if year <= 2023:
        # Annual ZIP — extract all CSVs and upload each one
        csv_files = extract_all_csvs_from_zip(zip_bytes)
        for csv_name, csv_bytes in csv_files:
            key = f"{config['s3']['raw_prefix']}/{yyyymm}/{csv_name}"
            upload_to_s3(s3_client, bucket, key, csv_bytes)
    else:
        # Monthly ZIP — single CSV
        csv_name, csv_bytes = extract_csv_from_zip(zip_bytes)
        key = f"{config['s3']['raw_prefix']}/{yyyymm}/{csv_name}"
        upload_to_s3(s3_client, bucket, key, csv_bytes)

    return True


def ingest_range(start_year: int, end_year: int, config: dict, s3_client=None) -> list[str]:
    """
    Ingest all months in [start_year, end_year].
    Returns list of successfully ingested YYYYMM keys.
    """
    if s3_client is None:
        s3_client = boto3.client("s3", region_name=config["aws"]["region"])

    ingested = []
    for yyyymm in _month_keys(start_year, end_year):
        try:
            ok = ingest_month(yyyymm, config, s3_client=s3_client)
            if ok:
                ingested.append(yyyymm)
        except Exception as exc:
            logger.error("Failed to ingest %s: %s", yyyymm, exc)
    logger.info("Ingested %d month(s)", len(ingested))
    return ingested


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)
    cfg = load_config()

    parser = argparse.ArgumentParser(description="Ingest Citi Bike trip data to S3")
    parser.add_argument("--start-year", type=int, default=cfg["citibike"]["start_year"])
    parser.add_argument("--end-year", type=int, default=cfg["citibike"]["end_year"])
    args = parser.parse_args()

    ingest_range(args.start_year, args.end_year, cfg)
