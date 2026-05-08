"""
analytics.py — DuckDB-powered analytics layer.

Queries the gold-layer Parquet files (batch) and streaming Parquet files
(real-time station status) stored in S3 to answer key equity questions:

  1. Which neighborhoods have the fewest available bikes right now?
  2. How does e-bike availability differ by borough?
  3. What are the busiest stations and when?
  4. Combined batch + streaming: stations with high trip volume but frequent
     low-availability events (equity gap indicator).

All queries return pandas DataFrames for downstream use (Airflow reports,
Streamlit dashboards, or notebook exploration).
"""

from __future__ import annotations

import logging
import os
from typing import Any

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DuckDB connection factory
# ---------------------------------------------------------------------------


def get_connection(
    threads: int = 4,
    memory_limit: str = "4GB",
    aws_region: str = "us-east-1",
) -> duckdb.DuckDBPyConnection:
    """
    Create a DuckDB connection with S3 access configured via environment
    variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, or IAM role).
    """
    con = duckdb.connect()
    con.execute(f"SET threads = {threads}")
    con.execute(f"SET memory_limit = '{memory_limit}'")

    # Install / load httpfs extension for S3 access
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute(f"SET s3_region = '{aws_region}'")

    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if access_key and secret_key:
        con.execute(f"SET s3_access_key_id = '{access_key}'")
        con.execute(f"SET s3_secret_access_key = '{secret_key}'")

    return con


# ---------------------------------------------------------------------------
# Gold-layer batch queries
# ---------------------------------------------------------------------------


def top_stations_by_trips(
    con: duckdb.DuckDBPyConnection,
    gold_path: str,
    n: int = 20,
    rideable_type: str | None = None,
) -> pd.DataFrame:
    """
    Return the top-N stations by total trips started.

    Parameters
    ----------
    gold_path : str
        S3 prefix for the gold/trips_per_station Parquet dataset.
    n : int
        Number of rows to return.
    rideable_type : str | None
        Filter by 'electric_bike', 'classic_bike', or None for all.
    """
    # NOTE: rideable_type isn't in the trips_per_station gold table
    # (it's aggregated away), so the filter is ignored here. Left in the
    # signature for backwards compatibility.
    _ = rideable_type
    sql = f"""
        SELECT
            start_station_id,
            start_station_name,
            SUM(trips_started)         AS total_trips,
            AVG(avg_duration_seconds)  AS avg_duration_seconds,
            SUM(ebike_trips)           AS total_ebike_trips,
            SUM(member_trips)          AS total_member_trips
        FROM read_parquet('{gold_path}/**/*.parquet', hive_partitioning = true)
        GROUP BY start_station_id, start_station_name
        ORDER BY total_trips DESC
        LIMIT {n}
    """
    return con.execute(sql).df()


def hourly_usage_pattern(
    con: duckdb.DuckDBPyConnection,
    gold_path: str,
) -> pd.DataFrame:
    """Return average trip counts by hour-of-day and day-of-week."""
    sql = f"""
        SELECT
            hour_of_day,
            day_of_week,
            rideable_type,
            member_casual,
            SUM(trip_count) AS total_trips
        FROM read_parquet('{gold_path}/hourly_pattern/**/*.parquet')
        GROUP BY hour_of_day, day_of_week, rideable_type, member_casual
        ORDER BY day_of_week, hour_of_day
    """
    return con.execute(sql).df()


def neighborhood_trip_summary(
    con: duckdb.DuckDBPyConnection,
    gold_path: str,
    geoparquet_path: str,
) -> pd.DataFrame:
    """
    Join gold trip data with neighborhood accessibility scores from GeoParquet.
    Returns one row per neighborhood with trip and accessibility metrics.
    """
    sql = f"""
        WITH trips AS (
            SELECT
                start_station_id,
                SUM(trips_started)   AS total_trips,
                SUM(ebike_trips)     AS ebike_trips,
                SUM(member_trips)    AS member_trips
            FROM read_parquet('{gold_path}/trips_per_station/**/*.parquet',
                              hive_partitioning = true)
            GROUP BY start_station_id
        ),
        nbhd AS (
            SELECT
                neighborhood_id,
                neighborhood_name,
                borough,
                accessibility_score
            FROM read_parquet('{geoparquet_path}')
        )
        SELECT
            n.neighborhood_name,
            n.borough,
            n.accessibility_score,
            SUM(t.total_trips)  AS total_trips,
            SUM(t.ebike_trips)  AS total_ebike_trips,
            ROUND(100.0 * SUM(t.ebike_trips) / NULLIF(SUM(t.total_trips), 0), 2)
                AS ebike_pct
        FROM nbhd n
        LEFT JOIN trips t ON t.start_station_id = n.neighborhood_id   -- proxy join
        GROUP BY n.neighborhood_name, n.borough, n.accessibility_score
        ORDER BY n.accessibility_score DESC
    """
    return con.execute(sql).df()


# ---------------------------------------------------------------------------
# Real-time streaming layer queries
# ---------------------------------------------------------------------------


def current_station_availability(
    con: duckdb.DuckDBPyConnection,
    streaming_path: str,
    lookback_minutes: int = 5,
) -> pd.DataFrame:
    """
    Query the most recent station-status micro-batches from the streaming layer.

    Returns stations ranked by fewest available bikes — useful for identifying
    empty stations in real time.
    """
    sql = f"""
        WITH latest AS (
            SELECT
                station_id,
                num_bikes_available,
                num_docks_available,
                is_renting,
                processed_at,
                ROW_NUMBER() OVER (
                    PARTITION BY station_id ORDER BY processed_at DESC
                ) AS rn
            FROM read_parquet('{streaming_path}/**/*.parquet')
            WHERE processed_at >= NOW() - INTERVAL '{lookback_minutes}' MINUTE
        )
        SELECT
            station_id,
            num_bikes_available,
            num_docks_available,
            is_renting,
            processed_at AS last_seen
        FROM latest
        WHERE rn = 1
        ORDER BY num_bikes_available ASC
    """
    return con.execute(sql).df()


def equity_gap_stations(
    con: duckdb.DuckDBPyConnection,
    gold_path: str,
    streaming_path: str,
    top_n: int = 10,
) -> pd.DataFrame:
    """
    Lambda architecture query — combine batch and streaming data.

    Identifies stations that are historically high-volume (batch layer) but
    frequently empty (streaming layer) — potential equity gap locations.
    """
    sql = f"""
        WITH batch AS (
            SELECT
                start_station_id AS station_id,
                SUM(trips_started) AS historical_trips
            FROM read_parquet('{gold_path}/trips_per_station/**/*.parquet',
                              hive_partitioning = true)
            GROUP BY start_station_id
            ORDER BY historical_trips DESC
            LIMIT 200
        ),
        streaming AS (
            SELECT
                station_id,
                AVG(num_bikes_available)                       AS avg_bikes_available,
                SUM(CASE WHEN num_bikes_available = 0 THEN 1 ELSE 0 END)
                    AS empty_readings,
                COUNT(*)                                       AS total_readings
            FROM read_parquet('{streaming_path}/**/*.parquet')
            GROUP BY station_id
        )
        SELECT
            b.station_id,
            b.historical_trips,
            s.avg_bikes_available,
            s.empty_readings,
            s.total_readings,
            ROUND(100.0 * s.empty_readings / NULLIF(s.total_readings, 0), 2)
                AS pct_time_empty
        FROM batch b
        JOIN streaming s ON b.station_id = s.station_id
        ORDER BY b.historical_trips DESC, pct_time_empty DESC
        LIMIT {top_n}
    """
    return con.execute(sql).df()


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from src.ingest import load_config

    logging.basicConfig(level=logging.INFO)
    # load_config() auto-loads .env so AWS creds and S3_BUCKET are available
    cfg = load_config()
    bucket = cfg["aws"]["s3_bucket"]

    gold = f"s3://{bucket}/{cfg['s3']['gold_prefix']}"
    streaming = f"s3://{bucket}/{cfg['s3']['streaming_prefix']}"
    geoparquet = f"s3://{bucket}/gold/citibike/neighborhood_scores.parquet"

    con = get_connection(
        threads=cfg["duckdb"]["threads"],
        memory_limit=cfg["duckdb"]["memory_limit"],
        aws_region=cfg["aws"]["region"],
    )

    parser = argparse.ArgumentParser(description="Run DuckDB analytics queries")
    parser.add_argument(
        "--query",
        choices=["top_stations", "hourly", "neighborhoods", "realtime", "equity_gap"],
        required=True,
    )
    args = parser.parse_args()

    if args.query == "top_stations":
        df = top_stations_by_trips(con, f"{gold}/trips_per_station")
    elif args.query == "hourly":
        df = hourly_usage_pattern(con, gold)
    elif args.query == "neighborhoods":
        df = neighborhood_trip_summary(con, gold, geoparquet)
    elif args.query == "realtime":
        df = current_station_availability(con, streaming)
    elif args.query == "equity_gap":
        df = equity_gap_stations(con, gold, streaming)
    else:
        raise ValueError(f"Unknown query: {args.query}")

    # Save results as CSV in outputs/ folder
    output_dir = os.path.join(os.path.dirname(__file__), "..", "outputs")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{args.query}_results.csv")
    df.to_csv(output_path, index=False)
    print(f"Results saved to {output_path}\n")
    print(df.to_string())
