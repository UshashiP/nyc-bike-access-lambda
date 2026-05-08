"""
validate.py — Schema and data-quality checks for raw Citi Bike trip data.

Runs before the PySpark transform step.  Reports issues without halting the
pipeline unless critical columns are entirely missing.

Two schemas are supported:
  - legacy  (pre-2021): 15 columns including gender, birth year, trip duration
  - current (2021+):    13 columns using ride_id, rideable_type, member_or_casual
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Literal

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

LEGACY_REQUIRED_COLUMNS = {
    "tripduration",
    "starttime",
    "stoptime",
    "start station id",
    "start station name",
    "start station latitude",
    "start station longitude",
    "end station id",
    "end station name",
    "end station latitude",
    "end station longitude",
    "bikeid",
    "usertype",
}

CURRENT_REQUIRED_COLUMNS = {
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_casual",
}

SchemaVersion = Literal["legacy", "current", "unknown"]


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class ValidationReport:
    schema_version: SchemaVersion = "unknown"
    row_count: int = 0
    missing_required_columns: list[str] = field(default_factory=list)
    null_rate: dict[str, float] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def detect_schema(columns: list[str]) -> SchemaVersion:
    """Guess schema version from column names (case-insensitive)."""
    lower = {c.lower() for c in columns}
    if "ride_id" in lower:
        return "current"
    if "tripduration" in lower:
        return "legacy"
    return "unknown"


def _required_for(version: SchemaVersion) -> set[str]:
    if version == "current":
        return CURRENT_REQUIRED_COLUMNS
    if version == "legacy":
        return LEGACY_REQUIRED_COLUMNS
    return set()


# ---------------------------------------------------------------------------
# Core validation
# ---------------------------------------------------------------------------


def validate_dataframe(df: pd.DataFrame, yyyymm: str = "") -> ValidationReport:
    """
    Run quality checks on a raw trip-data DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Raw CSV loaded with pandas.
    yyyymm : str
        Month label for logging context (e.g. '202301').

    Returns
    -------
    ValidationReport
    """
    report = ValidationReport()
    report.row_count = len(df)
    ctx = f"[{yyyymm}]" if yyyymm else ""

    # 1. Detect schema version
    report.schema_version = detect_schema(df.columns.tolist())
    logger.info("%s schema_version=%s  rows=%d", ctx, report.schema_version, report.row_count)

    if report.schema_version == "unknown":
        report.errors.append("Could not detect schema version — unrecognised columns")
        return report

    # 2. Check required columns
    lower_cols = {c.lower(): c for c in df.columns}
    required = _required_for(report.schema_version)
    for req in sorted(required):
        if req not in lower_cols:
            report.missing_required_columns.append(req)

    if report.missing_required_columns:
        report.errors.append(
            f"Missing required columns: {report.missing_required_columns}"
        )
        logger.error("%s %s", ctx, report.errors[-1])

    # 3. Null rates for key columns
    key_cols = ["start_lat", "start_lng", "end_lat", "end_lng"] if report.schema_version == "current" \
        else ["start station latitude", "start station longitude",
              "end station latitude", "end station longitude"]

    for col in key_cols:
        actual_col = lower_cols.get(col)
        if actual_col is not None:
            null_rate = df[actual_col].isna().mean()
            report.null_rate[col] = round(null_rate, 4)
            if null_rate > 0.05:
                report.warnings.append(
                    f"Column '{col}' has {null_rate:.1%} null values"
                )

    # 4. Row count sanity
    if report.row_count == 0:
        report.errors.append("DataFrame is empty")
    elif report.row_count < 1000:
        report.warnings.append(f"Suspiciously low row count: {report.row_count}")

    # 5. Duplicate ride IDs (current schema only)
    if report.schema_version == "current":
        ride_id_col = lower_cols.get("ride_id")
        if ride_id_col:
            dup_count = df[ride_id_col].duplicated().sum()
            if dup_count > 0:
                report.warnings.append(f"{dup_count} duplicate ride_id values detected")

    for w in report.warnings:
        logger.warning("%s %s", ctx, w)

    return report


def validate_csv_bytes(csv_bytes: bytes, yyyymm: str = "") -> ValidationReport:
    """Convenience wrapper — validate raw CSV bytes directly."""
    import io
    df = pd.read_csv(io.BytesIO(csv_bytes), low_memory=False)
    return validate_dataframe(df, yyyymm)


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) < 2:
        print("Usage: validate.py <path/to/file.csv>")
        sys.exit(1)

    df = pd.read_csv(sys.argv[1], low_memory=False)
    rpt = validate_dataframe(df, yyyymm=sys.argv[1])
    print(rpt)
    sys.exit(0 if rpt.is_valid else 1)
