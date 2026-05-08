"""
consumer.py — Spark Structured Streaming consumer.

Reads station-status messages from the Kafka topic, parses the JSON payload,
and writes micro-batches to the S3 streaming layer every 5 minutes.

The streaming data is then queryable by DuckDB (analytics.py) alongside the
gold-layer Parquet files, implementing the Lambda architecture pattern:

  Batch layer:    Monthly CSVs → PySpark → S3 medallion (bronze/silver/gold)
  Streaming layer: Kafka → Spark Structured Streaming → S3/streaming

Environment variables
---------------------
KAFKA_BOOTSTRAP_SERVERS  Comma-separated broker list (default: localhost:29092)
KAFKA_TOPIC              Topic name (default: citibike-station-status)
S3_BUCKET                Target S3 bucket (required)
AWS_ACCESS_KEY_ID        AWS credentials (or use IAM role)
AWS_SECRET_ACCESS_KEY    AWS credentials (or use IAM role)
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "citibike-station-status")
S3_BUCKET = os.environ.get("S3_BUCKET")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

STREAMING_PREFIX = "streaming/citibike/station_status"
CHECKPOINT_PREFIX = "checkpoints/citibike/consumer"
MICRO_BATCH_INTERVAL = "5 minutes"


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

STATION_STATUS_SCHEMA = StructType(
    [
        StructField("station_id", StringType(), True),
        StructField("num_bikes_available", IntegerType(), True),
        StructField("num_docks_available", IntegerType(), True),
        StructField("num_ebikes_available", IntegerType(), True),
        StructField("is_installed", IntegerType(), True),
        StructField("is_renting", IntegerType(), True),
        StructField("last_reported", LongType(), True),
        StructField("ingested_at", LongType(), True),
    ]
)


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------


def create_spark_session() -> SparkSession:
    """
    Build a SparkSession with the Kafka connector and S3A support.
    The Kafka connector JAR is resolved at runtime via --packages or
    specified in spark-submit.
    """
    # Bundle all required JARs: Kafka connector + hadoop-aws + aws-sdk-bundle.
    # Spark downloads these from Maven on first run (cached in ~/.ivy2).
    packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ]
    )
    builder = (
        SparkSession.builder.appName("CitiBikeStreamingConsumer")
        .config("spark.jars.packages", packages)
        .config("spark.sql.streaming.checkpointLocation", "spark-checkpoint-fallback")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
    )
    return builder.getOrCreate()


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------


def run_consumer() -> None:
    if not S3_BUCKET:
        raise EnvironmentError("S3_BUCKET environment variable is required")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    output_path = f"s3a://{S3_BUCKET}/{STREAMING_PREFIX}"
    checkpoint_path = f"s3a://{S3_BUCKET}/{CHECKPOINT_PREFIX}"

    logger.info(
        "Consumer starting — topic=%s  output=%s  interval=%s",
        KAFKA_TOPIC,
        output_path,
        MICRO_BATCH_INTERVAL,
    )

    # Read raw bytes from Kafka
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        # Limit records per micro-batch to avoid memory pressure
        .option("maxOffsetsPerTrigger", 50_000)
        .load()
    )

    # Parse JSON payload
    parsed = raw_stream.select(
        F.from_json(F.col("value").cast("string"), STATION_STATUS_SCHEMA).alias("data"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select("data.*", "kafka_timestamp")

    # Enrich with processing timestamp and date partition key
    enriched = (
        parsed.withColumn("processed_at", F.current_timestamp())
        .withColumn("processing_date", F.to_date(F.col("processed_at")))
    )

    # Write micro-batches to S3 as Parquet, partitioned by date
    query = (
        enriched.writeStream.format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("processing_date")
        .trigger(processingTime=MICRO_BATCH_INTERVAL)
        .start()
    )

    logger.info("Streaming query started — awaiting termination")
    query.awaitTermination()


if __name__ == "__main__":
    run_consumer()
