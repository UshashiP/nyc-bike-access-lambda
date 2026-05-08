"""
batch_pipeline_dag.py — Airflow DAG for the monthly Citi Bike batch pipeline.

Schedule: first day of each month at 06:00 UTC (data for the previous month
is usually published by the 5th, so manual backfills may be needed for the
most recent month).

Pipeline stages:
  1. ingest      — download monthly CSV ZIP from Citi Bike public S3 → project S3 raw layer
  2. validate    — schema & quality checks (fail fast on critical errors)
  3. bronze      — PySpark: raw CSV → typed Parquet (EMR or local Spark)
  4. silver      — PySpark: clean, deduplicate, derive features
  5. gold        — PySpark: aggregate → trips_per_station, hourly_pattern, rideable_split
  6. spatial     — GeoPandas: join stations to NYC NTAs, compute accessibility scores
  7. geoparquet  — write neighbourhood scores as GeoParquet to S3
  8. analytics   — DuckDB smoke-test: validate gold tables are queryable

Environment variables expected (set in docker-compose or Airflow Connections):
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------

default_args = {
    "owner": "citibike-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="citibike_batch_pipeline",
    default_args=default_args,
    description="Monthly Citi Bike batch pipeline: ingest → bronze → silver → gold → spatial → DuckDB",
    schedule_interval="0 6 1 * *",  # 1st of every month at 06:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["citibike", "batch", "pyspark"],
    params={
        "yyyymm": "{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m') }}"
    },
) as dag:

    # ----------------------------------------------------------------
    # Task callables
    # ----------------------------------------------------------------

    def task_ingest(**context) -> None:
        from src.ingest import ingest_month, load_config

        cfg = load_config()
        yyyymm = context["params"]["yyyymm"]
        ok = ingest_month(yyyymm, cfg)
        if not ok:
            raise RuntimeError(f"Ingest returned no data for {yyyymm}")

    def task_validate(**context) -> None:
        import io

        import boto3
        import pandas as pd

        from src.ingest import load_config
        from src.validate import validate_dataframe

        cfg = load_config()
        yyyymm = context["params"]["yyyymm"]
        bucket = cfg["aws"]["s3_bucket"]
        prefix = f"{cfg['s3']['raw_prefix']}/{yyyymm}/"

        s3 = boto3.client("s3", region_name=cfg["aws"]["region"])
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
        if not objects:
            raise RuntimeError(f"No raw files found at s3://{bucket}/{prefix}")

        key = objects[0]["Key"]
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        df = pd.read_csv(io.BytesIO(body), low_memory=False)
        report = validate_dataframe(df, yyyymm)
        if not report.is_valid:
            raise RuntimeError(f"Validation failed: {report.errors}")

    def task_bronze(**context) -> None:
        from src.ingest import load_config
        from src.transform import create_spark_session, raw_to_bronze

        cfg = load_config()
        yyyymm = context["params"]["yyyymm"]
        bucket = cfg["aws"]["s3_bucket"]
        spark = create_spark_session("CitiBikeBronze")
        raw_to_bronze(
            spark,
            raw_s3_path=f"s3a://{bucket}/{cfg['s3']['raw_prefix']}/{yyyymm}/",
            bronze_s3_path=f"s3a://{bucket}/{cfg['s3']['bronze_prefix']}/",
            yyyymm=yyyymm,
        )
        spark.stop()

    def task_silver(**context) -> None:
        from src.ingest import load_config
        from src.transform import bronze_to_silver, create_spark_session

        cfg = load_config()
        bucket = cfg["aws"]["s3_bucket"]
        spark = create_spark_session("CitiBikeSilver")
        bronze_to_silver(
            spark,
            bronze_s3_path=f"s3a://{bucket}/{cfg['s3']['bronze_prefix']}/",
            silver_s3_path=f"s3a://{bucket}/{cfg['s3']['silver_prefix']}/",
        )
        spark.stop()

    def task_gold(**context) -> None:
        from src.ingest import load_config
        from src.transform import create_spark_session, silver_to_gold

        cfg = load_config()
        bucket = cfg["aws"]["s3_bucket"]
        spark = create_spark_session("CitiBikeGold")
        silver_to_gold(
            spark,
            silver_s3_path=f"s3a://{bucket}/{cfg['s3']['silver_prefix']}/",
            gold_s3_path=f"s3a://{bucket}/{cfg['s3']['gold_prefix']}/",
        )
        spark.stop()

    def task_spatial(**context) -> None:
        import pandas as pd

        from src.ingest import load_config
        from src.spatial import compute_accessibility_scores, export_geoparquet

        cfg = load_config()
        bucket = cfg["aws"]["s3_bucket"]

        # Load station info from GBFS (live feed)
        import requests

        resp = requests.get(cfg["gbfs"]["station_info_url"], timeout=30)
        resp.raise_for_status()
        station_info_df = pd.DataFrame(resp.json()["data"]["stations"])

        # Load gold trips per station
        trips_df = pd.read_parquet(
            f"s3://{bucket}/{cfg['s3']['gold_prefix']}/trips_per_station/"
        )

        scores = compute_accessibility_scores(trips_df, station_info_df, nta_source="arcgis")
        output_path = f"s3://{bucket}/gold/citibike/neighborhood_scores.parquet"
        export_geoparquet(scores, output_path)

    def task_analytics_smoke_test(**context) -> None:
        from src.analytics import get_connection, top_stations_by_trips
        from src.ingest import load_config

        cfg = load_config()
        bucket = cfg["aws"]["s3_bucket"]
        gold = f"s3://{bucket}/{cfg['s3']['gold_prefix']}"
        con = get_connection(
            threads=cfg["duckdb"]["threads"],
            memory_limit=cfg["duckdb"]["memory_limit"],
            aws_region=cfg["aws"]["region"],
        )
        df = top_stations_by_trips(con, f"{gold}/trips_per_station", n=5)
        if df.empty:
            raise RuntimeError("DuckDB smoke test returned empty results from gold layer")

    # ----------------------------------------------------------------
    # Task instances
    # ----------------------------------------------------------------

    ingest = PythonOperator(task_id="ingest", python_callable=task_ingest)
    validate = PythonOperator(task_id="validate", python_callable=task_validate)
    bronze = PythonOperator(task_id="bronze", python_callable=task_bronze)
    silver = PythonOperator(task_id="silver", python_callable=task_silver)
    gold = PythonOperator(task_id="gold", python_callable=task_gold)
    spatial = PythonOperator(task_id="spatial", python_callable=task_spatial)
    analytics_smoke = PythonOperator(
        task_id="analytics_smoke_test", python_callable=task_analytics_smoke_test
    )

    # ----------------------------------------------------------------
    # Task dependencies
    # ----------------------------------------------------------------

    ingest >> validate >> bronze >> silver >> gold >> spatial >> analytics_smoke
