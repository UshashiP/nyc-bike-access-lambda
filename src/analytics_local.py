"""
analytics_local.py — Download gold-layer Parquet from S3 to a local folder,
then run DuckDB queries on the local files.

Bypasses DuckDB's httpfs/S3 integration, which can be finicky with credentials.
Uses boto3 (which already works via .env) for the download.

Usage:
    PYTHONPATH=. python src/analytics_local.py --query top_stations
    PYTHONPATH=. python src/analytics_local.py --query hourly
    PYTHONPATH=. python src/analytics_local.py --sync-only       # just download
"""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import boto3
import duckdb

from src.ingest import load_config

logger = logging.getLogger(__name__)

LOCAL_GOLD_DIR = Path("data/gold")


# ---------------------------------------------------------------------------
# S3 → local sync
# ---------------------------------------------------------------------------


def sync_gold_from_s3(bucket: str, region: str, dest: Path = LOCAL_GOLD_DIR) -> Path:
    """Download everything under gold/citibike/ to a local folder."""
    s3 = boto3.client("s3", region_name=region)
    prefix = "gold/citibike/"

    dest.mkdir(parents=True, exist_ok=True)
    paginator = s3.get_paginator("list_objects_v2")
    total = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            # Strip the gold/citibike/ prefix so local path mirrors S3 structure
            rel = key[len(prefix):]
            local_path = dest / rel
            local_path.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, key, str(local_path))
            total += 1
            if total % 10 == 0:
                print(f"  downloaded {total} files…")
    print(f"Downloaded {total} parquet files to {dest}/")
    return dest


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------


def top_stations(con: duckdb.DuckDBPyConnection, gold_dir: Path, n: int = 20):
    path = gold_dir / "trips_per_station"
    sql = f"""
        SELECT
            start_station_name,
            start_station_id,
            SUM(trips_started)         AS total_trips,
            ROUND(AVG(avg_duration_seconds) / 60, 1) AS avg_duration_min,
            SUM(ebike_trips)           AS ebike_trips,
            SUM(member_trips)          AS member_trips
        FROM read_parquet('{path}/**/*.parquet', hive_partitioning=true)
        GROUP BY start_station_name, start_station_id
        ORDER BY total_trips DESC
        LIMIT {n}
    """
    return con.execute(sql).df()


def hourly_pattern(con: duckdb.DuckDBPyConnection, gold_dir: Path):
    path = gold_dir / "hourly_pattern"
    sql = f"""
        SELECT
            hour_of_day,
            day_of_week,
            SUM(trip_count) AS total_trips
        FROM read_parquet('{path}/**/*.parquet')
        GROUP BY hour_of_day, day_of_week
        ORDER BY day_of_week, hour_of_day
    """
    return con.execute(sql).df()


def rideable_split(con: duckdb.DuckDBPyConnection, gold_dir: Path):
    path = gold_dir / "rideable_split"
    sql = f"""
        SELECT
            rideable_type,
            SUM(trip_count) AS total_trips,
            ROUND(100.0 * SUM(trip_count) / SUM(SUM(trip_count)) OVER (), 2) AS pct
        FROM read_parquet('{path}/**/*.parquet')
        GROUP BY rideable_type
        ORDER BY total_trips DESC
    """
    return con.execute(sql).df()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    logging.basicConfig(level=logging.INFO)
    cfg = load_config()
    bucket = cfg["aws"]["s3_bucket"]
    region = cfg["aws"]["region"]

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--query",
        choices=["top_stations", "hourly", "rideable_split"],
        default="top_stations",
    )
    parser.add_argument(
        "--sync-only",
        action="store_true",
        help="Just download gold parquet from S3, then exit",
    )
    parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="Skip the S3 download (use existing local files)",
    )
    args = parser.parse_args()

    # Step 1 — sync from S3 unless explicitly skipped
    if not args.skip_sync:
        print(f"Syncing gold layer from s3://{bucket}/gold/citibike/ …")
        sync_gold_from_s3(bucket, region)

    if args.sync_only:
        return

    # Step 2 — run the query on local files
    con = duckdb.connect()
    con.execute(f"SET threads = {cfg['duckdb']['threads']}")
    con.execute(f"SET memory_limit = '{cfg['duckdb']['memory_limit']}'")

    if args.query == "top_stations":
        df = top_stations(con, LOCAL_GOLD_DIR)
    elif args.query == "hourly":
        df = hourly_pattern(con, LOCAL_GOLD_DIR)
    elif args.query == "rideable_split":
        df = rideable_split(con, LOCAL_GOLD_DIR)

    # Save results as CSV in outputs/ folder
    output_dir = Path(__file__).parent.parent / "outputs"
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / f"{args.query}_results.csv"
    df.to_csv(output_path, index=False)
    print(f"Results saved to {output_path}\n")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
