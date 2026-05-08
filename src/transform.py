"""
transform.py — PySpark medallion-architecture transformations.

Bronze  → raw CSV data written to S3 as Parquet (typed, schema-unified)
Silver  → cleaned: nulls dropped, dupes removed, duration outliers filtered,
           schema normalised across legacy/current formats
Gold    → aggregations: trips per station, hourly patterns, rideable-type split

All functions accept a SparkSession and S3 paths via config dict.
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SparkSession factory
# ---------------------------------------------------------------------------


def create_spark_session(app_name: str = "CitiBikeTransform") -> SparkSession:
    # hadoop-aws + aws-java-sdk-bundle provide the S3AFileSystem implementation.
    # Spark downloads these from Maven on first run (~200 MB, cached in ~/.ivy2).
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        # Give the driver more heap — default 1 GB is too small for a full month
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        # Path-style access avoids DNS issues with bucket names containing dots
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Bronze layer — raw CSV → typed Parquet
# ---------------------------------------------------------------------------

# Unified schema for the current (2021+) Citi Bike format
CURRENT_SCHEMA = StructType(
    [
        StructField("ride_id", StringType(), True),
        StructField("rideable_type", StringType(), True),
        StructField("started_at", TimestampType(), True),
        StructField("ended_at", TimestampType(), True),
        StructField("start_station_name", StringType(), True),
        StructField("start_station_id", StringType(), True),
        StructField("end_station_name", StringType(), True),
        StructField("end_station_id", StringType(), True),
        StructField("start_lat", DoubleType(), True),
        StructField("start_lng", DoubleType(), True),
        StructField("end_lat", DoubleType(), True),
        StructField("end_lng", DoubleType(), True),
        StructField("member_casual", StringType(), True),
    ]
)


def raw_to_bronze(
    spark: SparkSession,
    raw_s3_path: str,
    bronze_s3_path: str,
    yyyymm: str,
) -> DataFrame:
    """
    Read raw CSVs from S3, cast to unified schema, write as Parquet.

    Supports both current and legacy Citi Bike schemas by normalising
    legacy columns to the current naming convention before writing.
    """
    logger.info("Bronze: reading raw data from %s", raw_s3_path)

    df = spark.read.option("header", "true").option("inferSchema", "false").csv(raw_s3_path)

    # Detect schema version by column presence
    cols_lower = {c.lower() for c in df.columns}
    is_legacy = "tripduration" in cols_lower

    if is_legacy:
        df = _normalise_legacy_schema(df)
    else:
        df = _cast_current_schema(df)

    # Add partition column
    df = df.withColumn("year_month", F.lit(yyyymm))

    output_path = f"{bronze_s3_path}/year_month={yyyymm}"
    logger.info("Bronze: writing Parquet to %s", output_path)
    df.write.mode("overwrite").parquet(output_path)
    return df


def _cast_current_schema(df: DataFrame) -> DataFrame:
    """Cast columns to typed values for the current (2021+) schema."""
    return (
        df.withColumn("started_at", F.to_timestamp("started_at"))
        .withColumn("ended_at", F.to_timestamp("ended_at"))
        .withColumn("start_lat", F.col("start_lat").cast(DoubleType()))
        .withColumn("start_lng", F.col("start_lng").cast(DoubleType()))
        .withColumn("end_lat", F.col("end_lat").cast(DoubleType()))
        .withColumn("end_lng", F.col("end_lng").cast(DoubleType()))
    )


def _normalise_legacy_schema(df: DataFrame) -> DataFrame:
    """
    Map legacy column names to the current unified schema.
    Synthesise missing modern fields where possible.
    """
    return (
        df.withColumnRenamed("bikeid", "ride_id")
        .withColumn("rideable_type", F.lit("classic_bike"))
        .withColumn("started_at", F.to_timestamp("starttime"))
        .withColumn("ended_at", F.to_timestamp("stoptime"))
        .withColumnRenamed("start station name", "start_station_name")
        .withColumnRenamed("start station id", "start_station_id")
        .withColumnRenamed("end station name", "end_station_name")
        .withColumnRenamed("end station id", "end_station_id")
        .withColumn("start_lat", F.col("start station latitude").cast(DoubleType()))
        .withColumn("start_lng", F.col("start station longitude").cast(DoubleType()))
        .withColumn("end_lat", F.col("end station latitude").cast(DoubleType()))
        .withColumn("end_lng", F.col("end station longitude").cast(DoubleType()))
        .withColumn(
            "member_casual",
            F.when(F.lower(F.col("usertype")) == "subscriber", "member").otherwise("casual"),
        )
        .select(
            "ride_id", "rideable_type", "started_at", "ended_at",
            "start_station_name", "start_station_id",
            "end_station_name", "end_station_id",
            "start_lat", "start_lng", "end_lat", "end_lng",
            "member_casual",
        )
    )


# ---------------------------------------------------------------------------
# Silver layer — cleaning and deduplication
# ---------------------------------------------------------------------------

MIN_TRIP_SECONDS = 60        # trips shorter than 1 min are likely docking errors
MAX_TRIP_SECONDS = 86_400    # trips longer than 24 h are almost always data errors


def bronze_to_silver(
    spark: SparkSession,
    bronze_s3_path: str,
    silver_s3_path: str,
) -> DataFrame:
    """
    Read bronze Parquet, apply cleaning rules, write to silver.

    Cleaning steps:
      - Drop rows with null coordinates or null timestamps
      - Remove duplicate ride_ids
      - Filter trip-duration outliers
      - Derive trip_duration_seconds, hour_of_day, day_of_week columns
    """
    logger.info("Silver: reading bronze data from %s", bronze_s3_path)
    df = spark.read.parquet(bronze_s3_path)

    # Drop rows with null critical fields
    df = df.dropna(subset=["started_at", "ended_at", "start_lat", "start_lng", "start_station_id"])

    # Remove duplicates
    df = df.dropDuplicates(["ride_id"])

    # Derive trip duration
    df = df.withColumn(
        "trip_duration_seconds",
        F.unix_timestamp("ended_at") - F.unix_timestamp("started_at"),
    )

    # Filter outliers
    df = df.filter(
        (F.col("trip_duration_seconds") >= MIN_TRIP_SECONDS)
        & (F.col("trip_duration_seconds") <= MAX_TRIP_SECONDS)
    )

    # Temporal features
    df = (
        df.withColumn("hour_of_day", F.hour("started_at"))
        .withColumn("day_of_week", F.dayofweek("started_at"))
        .withColumn("trip_date", F.to_date("started_at"))
    )

    logger.info("Silver: writing Parquet to %s", silver_s3_path)
    df.write.mode("overwrite").partitionBy("year_month").parquet(silver_s3_path)
    return df


# ---------------------------------------------------------------------------
# Gold layer — aggregations
# ---------------------------------------------------------------------------


def silver_to_gold(
    spark: SparkSession,
    silver_s3_path: str,
    gold_s3_path: str,
) -> dict[str, DataFrame]:
    """
    Produce gold-layer aggregation tables from silver data.

    Returns a dict of {table_name: DataFrame} written to S3.
    """
    logger.info("Gold: reading silver data from %s", silver_s3_path)
    df = spark.read.parquet(silver_s3_path)

    results: dict[str, DataFrame] = {}

    # --- trips per station per day ---
    trips_per_station = (
        df.groupBy("start_station_id", "start_station_name", "trip_date")
        .agg(
            F.count("*").alias("trips_started"),
            F.avg("trip_duration_seconds").alias("avg_duration_seconds"),
            F.sum(F.when(F.col("rideable_type") == "electric_bike", 1).otherwise(0)).alias(
                "ebike_trips"
            ),
            F.sum(F.when(F.col("member_casual") == "member", 1).otherwise(0)).alias(
                "member_trips"
            ),
        )
    )
    _write_gold(trips_per_station, f"{gold_s3_path}/trips_per_station", "trip_date")
    results["trips_per_station"] = trips_per_station

    # --- hourly usage pattern ---
    hourly_pattern = (
        df.groupBy("hour_of_day", "day_of_week", "rideable_type", "member_casual")
        .agg(F.count("*").alias("trip_count"))
        .orderBy("day_of_week", "hour_of_day")
    )
    _write_gold(hourly_pattern, f"{gold_s3_path}/hourly_pattern")
    results["hourly_pattern"] = hourly_pattern

    # --- rideable-type split by station ---
    rideable_split = (
        df.groupBy("start_station_id", "rideable_type")
        .agg(F.count("*").alias("trip_count"))
    )
    _write_gold(rideable_split, f"{gold_s3_path}/rideable_split")
    results["rideable_split"] = rideable_split

    logger.info("Gold: wrote %d table(s)", len(results))
    return results


def _write_gold(df: DataFrame, path: str, partition_col: str | None = None) -> None:
    writer = df.write.mode("overwrite")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.parquet(path)
    logger.info("Gold: wrote %s", path)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from src.ingest import load_config

    logging.basicConfig(level=logging.INFO)
    cfg = load_config()
    bucket = cfg["aws"]["s3_bucket"]

    parser = argparse.ArgumentParser(description="Run medallion transform pipeline")
    parser.add_argument("--yyyymm", required=True, help="Month to process, e.g. 202301")
    parser.add_argument(
        "--stage",
        choices=["bronze", "silver", "gold", "all"],
        default="all",
    )
    args = parser.parse_args()

    spark = create_spark_session()
    yyyymm = args.yyyymm

    raw = f"s3a://{bucket}/{cfg['s3']['raw_prefix']}/{yyyymm}/"
    bronze = f"s3a://{bucket}/{cfg['s3']['bronze_prefix']}/"
    silver = f"s3a://{bucket}/{cfg['s3']['silver_prefix']}/"
    gold = f"s3a://{bucket}/{cfg['s3']['gold_prefix']}/"

    if args.stage in ("bronze", "all"):
        raw_to_bronze(spark, raw, bronze, yyyymm)
    if args.stage in ("silver", "all"):
        bronze_to_silver(spark, bronze, silver)
    if args.stage in ("gold", "all"):
        silver_to_gold(spark, silver, gold)
