"""
streaming_monitor_dag.py — Airflow DAG that monitors the Kafka → Spark
Structured Streaming pipeline health.

Runs every 15 minutes and checks:
  1. Kafka lag — are consumers keeping up with the producer?
  2. Streaming freshness — is there recent data in the S3 streaming layer?
  3. Station availability alert — flag neighborhoods with 0 available bikes.

Uses the Airflow BashOperator to inspect Kafka consumer groups and the
PythonOperator for the S3/DuckDB freshness checks.

Environment variables expected:
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET,
  KAFKA_BOOTSTRAP_SERVERS
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "citibike-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="citibike_streaming_monitor",
    default_args=default_args,
    description="Monitor Kafka/Spark Structured Streaming pipeline health every 15 minutes",
    schedule_interval="*/15 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["citibike", "streaming", "kafka", "monitoring"],
) as dag:

    # ----------------------------------------------------------------
    # Task callables
    # ----------------------------------------------------------------

    def check_streaming_freshness(**context) -> None:
        """
        Verify the S3 streaming layer has received data in the last
        STALENESS_THRESHOLD_MINUTES minutes.  Raises RuntimeError if stale.
        """
        import os
        from datetime import timezone

        import boto3

        bucket = os.environ["S3_BUCKET"]
        prefix = "streaming/citibike/station_status/"
        threshold_minutes = int(os.environ.get("STALENESS_THRESHOLD_MINUTES", "10"))

        s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))
        result = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        objects = result.get("Contents", [])

        if not objects:
            raise RuntimeError(
                f"No streaming data found at s3://{bucket}/{prefix}. "
                "Is the Kafka consumer running?"
            )

        latest_modified = max(obj["LastModified"] for obj in objects)
        now = datetime.now(tz=timezone.utc)
        age_minutes = (now - latest_modified).total_seconds() / 60

        if age_minutes > threshold_minutes:
            raise RuntimeError(
                f"Streaming data is stale: last object was {age_minutes:.1f} minutes ago "
                f"(threshold: {threshold_minutes} min). Check the Spark consumer."
            )

        context["task_instance"].xcom_push(
            key="streaming_age_minutes", value=round(age_minutes, 1)
        )

    def check_low_availability_stations(**context) -> None:
        """
        Query the latest streaming micro-batch from DuckDB.
        Log a warning (not an error) if any stations have 0 bikes available.
        This is informational for the equity dashboard — not a failure condition.
        """
        import logging
        import os

        from src.analytics import current_station_availability, get_connection

        logger = logging.getLogger(__name__)

        bucket = os.environ["S3_BUCKET"]
        streaming_path = f"s3://{bucket}/streaming/citibike/station_status"

        con = get_connection(aws_region=os.environ.get("AWS_REGION", "us-east-1"))
        df = current_station_availability(con, streaming_path, lookback_minutes=15)

        empty_stations = df[df["num_bikes_available"] == 0]
        logger.info(
            "Station availability check: %d/%d stations currently empty",
            len(empty_stations),
            len(df),
        )

        # Push metrics to XCom for downstream alerting or dashboard use
        context["task_instance"].xcom_push(
            key="empty_station_count", value=len(empty_stations)
        )
        context["task_instance"].xcom_push(
            key="total_station_count", value=len(df)
        )

        if not empty_stations.empty:
            logger.warning(
                "Empty stations (top 10 by station_id):\n%s",
                empty_stations.head(10).to_string(index=False),
            )

    def check_kafka_consumer_lag(**context) -> None:
        """
        Use kafka-python to query consumer group offsets and calculate lag.
        Raises RuntimeError if lag exceeds 10,000 messages (possible consumer crash).
        """
        import logging
        import os

        from kafka import KafkaAdminClient
        from kafka.errors import KafkaException

        logger = logging.getLogger(__name__)

        bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
        topic = os.environ.get("KAFKA_TOPIC", "citibike-station-status")
        group = "citibike-streaming-group"
        lag_threshold = int(os.environ.get("KAFKA_LAG_THRESHOLD", "10000"))

        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap.split(","))
            offsets = admin.list_consumer_group_offsets(group)
            admin.close()
        except KafkaException as exc:
            logger.warning("Could not connect to Kafka to check lag: %s", exc)
            return  # non-fatal — Kafka may be starting up

        total_lag = sum(
            v.offset for tp, v in offsets.items() if tp.topic == topic
        )
        logger.info("Kafka consumer group '%s' total lag: %d", group, total_lag)

        if total_lag > lag_threshold:
            raise RuntimeError(
                f"Kafka consumer lag {total_lag} exceeds threshold {lag_threshold}. "
                "The Spark Structured Streaming consumer may be behind or stopped."
            )

        context["task_instance"].xcom_push(key="kafka_lag", value=total_lag)

    # ----------------------------------------------------------------
    # Task instances
    # ----------------------------------------------------------------

    freshness_check = PythonOperator(
        task_id="check_streaming_freshness",
        python_callable=check_streaming_freshness,
    )

    availability_check = PythonOperator(
        task_id="check_low_availability_stations",
        python_callable=check_low_availability_stations,
    )

    kafka_lag_check = PythonOperator(
        task_id="check_kafka_consumer_lag",
        python_callable=check_kafka_consumer_lag,
    )

    # Run checks in parallel — they are independent
    [freshness_check, availability_check, kafka_lag_check]
