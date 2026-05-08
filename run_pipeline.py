"""
run_pipeline.py — Local pipeline entry point.

Runs the full analytics + visualisation pipeline against locally-synced
gold-layer Parquet data. No PySpark or Kafka required for this step.

Steps
-----
  1. (optional) Sync gold-layer Parquet from S3 → data/gold/
  2. Run DuckDB analytics queries → outputs/*.csv
  3. Generate visualisation charts     → outputs/plots/*.png

Usage
-----
  # Full run (sync from S3 first):
  python run_pipeline.py

  # Skip S3 sync — use existing local Parquet files:
  python run_pipeline.py --skip-sync

  # Analytics only (no charts):
  python run_pipeline.py --skip-sync --skip-viz

  # Charts only (CSVs already exist):
  python run_pipeline.py --skip-sync --skip-analytics

Environment
-----------
  Reads .env at project root for AWS credentials and S3_BUCKET.
  See .env.example for required variables.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Bootstrap .env before any other imports that may need env vars
# ---------------------------------------------------------------------------

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).parent / ".env", override=False)
except ImportError:
    pass

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _step(msg: str) -> None:
    print(f"\n{'─' * 60}\n▶  {msg}\n{'─' * 60}")


def _ok(msg: str) -> None:
    print(f"✔  {msg}")


def _fail(msg: str, exc: Exception) -> None:
    print(f"✘  {msg}: {exc}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Step 1 — Sync from S3
# ---------------------------------------------------------------------------


def run_sync(cfg: dict) -> None:
    from src.analytics_local import sync_gold_from_s3

    bucket = cfg["aws"]["s3_bucket"]
    region = cfg["aws"]["region"]
    _step(f"Syncing gold layer from s3://{bucket}/gold/citibike/")
    sync_gold_from_s3(bucket, region)
    _ok("Sync complete")


# ---------------------------------------------------------------------------
# Step 2 — Analytics queries
# ---------------------------------------------------------------------------


def run_analytics(cfg: dict) -> dict[str, Path]:
    import duckdb

    from src.analytics_local import (
        LOCAL_GOLD_DIR,
        hourly_pattern,
        rideable_split,
        top_stations,
    )

    _step("Running DuckDB analytics queries")
    output_dir = Path("outputs")
    output_dir.mkdir(exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET threads = {cfg['duckdb']['threads']}")
    con.execute(f"SET memory_limit = '{cfg['duckdb']['memory_limit']}'")

    results: dict[str, Path] = {}

    queries = [
        ("top_stations", lambda: top_stations(con, LOCAL_GOLD_DIR)),
        ("hourly", lambda: hourly_pattern(con, LOCAL_GOLD_DIR)),
        ("rideable_split", lambda: rideable_split(con, LOCAL_GOLD_DIR)),
    ]

    for name, fn in queries:
        t0 = time.time()
        try:
            df = fn()
            out = output_dir / f"{name}_results.csv"
            df.to_csv(out, index=False)
            elapsed = time.time() - t0
            _ok(f"{name:20s} → {out}  ({len(df)} rows, {elapsed:.2f}s)")
            results[name] = out
        except Exception as exc:  # noqa: BLE001
            _fail(f"Query '{name}' failed", exc)
            logger.debug("", exc_info=True)

    return results


# ---------------------------------------------------------------------------
# Step 3 — Visualisations
# ---------------------------------------------------------------------------


def run_visualizations() -> None:
    from src.visualize import (
        OUTPUTS_DIR,
        PLOTS_DIR,
        _setup,
        plot_ebike_share_scatter,
        plot_hourly_heatmap,
        plot_member_casual,
        plot_neighborhood_capacity,
        plot_rideable_donut,
        plot_summary_dashboard,
        plot_top_stations,
    )
    import pandas as pd

    _step("Generating visualisation charts")
    _setup()

    charts = [
        ("top_stations.png", lambda: plot_top_stations(pd.read_csv(OUTPUTS_DIR / "top_stations_results.csv"))),
        ("hourly_heatmap.png", lambda: plot_hourly_heatmap(pd.read_csv(OUTPUTS_DIR / "hourly_results.csv"))),
        ("rideable_donut.png", lambda: plot_rideable_donut(pd.read_csv(OUTPUTS_DIR / "rideable_split_results.csv"))),
        ("member_casual.png", lambda: plot_member_casual(pd.read_csv(OUTPUTS_DIR / "top_stations_results.csv"))),
        ("ebike_share_scatter.png", lambda: plot_ebike_share_scatter(pd.read_csv(OUTPUTS_DIR / "top_stations_results.csv"))),
        ("neighborhood_capacity.png", lambda: plot_neighborhood_capacity(pd.read_csv(OUTPUTS_DIR / "neighborhood_accessibility.csv"))),
        (
            "summary_dashboard.png",
            lambda: plot_summary_dashboard(
                pd.read_csv(OUTPUTS_DIR / "top_stations_results.csv"),
                pd.read_csv(OUTPUTS_DIR / "hourly_results.csv"),
                pd.read_csv(OUTPUTS_DIR / "rideable_split_results.csv"),
            ),
        ),
    ]

    for name, fn in charts:
        csv_needed = name.replace(".png", "_results.csv")
        # Check required CSVs exist
        required = OUTPUTS_DIR / csv_needed
        if not required.exists() and not (OUTPUTS_DIR / "top_stations_results.csv").exists():
            logger.warning("Skipping %s — required CSVs not found", name)
            continue
        t0 = time.time()
        try:
            out = fn()
            elapsed = time.time() - t0
            _ok(f"{name:35s} ({elapsed:.2f}s)")
        except Exception as exc:  # noqa: BLE001
            _fail(f"Chart '{name}' failed", exc)
            logger.debug("", exc_info=True)

    print(f"\n  Charts in: {PLOTS_DIR}/")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="NYC Citi Bike local analytics pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--skip-sync", action="store_true", help="Skip S3 → local sync")
    parser.add_argument("--skip-analytics", action="store_true", help="Skip DuckDB query step")
    parser.add_argument("--skip-viz", action="store_true", help="Skip chart generation")
    args = parser.parse_args()

    t_start = time.time()
    print("\n🚲  NYC Citi Bike Analytics Pipeline")
    print(f"    {'─' * 50}")

    # Load config
    from src.ingest import load_config
    try:
        cfg = load_config()
    except RuntimeError as exc:
        print(f"\n⚠️  Config warning: {exc}")
        print("    S3 sync will be skipped. Set S3_BUCKET in .env to enable.\n")
        cfg = {
            "aws": {"s3_bucket": "", "region": "us-east-1"},
            "duckdb": {"threads": 4, "memory_limit": "4GB"},
        }
        args.skip_sync = True

    # Step 1
    if not args.skip_sync:
        try:
            run_sync(cfg)
        except Exception as exc:  # noqa: BLE001
            _fail("S3 sync failed — continuing with local data", exc)
            logger.debug("", exc_info=True)

    # Step 2
    if not args.skip_analytics:
        run_analytics(cfg)

    # Step 3
    if not args.skip_viz:
        run_visualizations()

    elapsed = time.time() - t_start
    print(f"\n{'─' * 60}")
    print(f"✅  Pipeline finished in {elapsed:.1f}s")
    print(f"    Launch dashboard:  streamlit run src/dashboard.py")
    print(f"{'─' * 60}\n")


if __name__ == "__main__":
    main()
