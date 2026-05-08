"""Tests for src/validate.py"""
from __future__ import annotations

import pandas as pd
import pytest

from src.validate import ValidationReport, detect_schema, validate_dataframe


def _make_current_df(n: int = 100, with_nulls: bool = False) -> pd.DataFrame:
    data = {
        "ride_id": [str(i) for i in range(n)],
        "rideable_type": ["classic_bike"] * n,
        "started_at": ["2023-01-01 08:00:00"] * n,
        "ended_at": ["2023-01-01 08:15:00"] * n,
        "start_station_name": ["Station A"] * n,
        "start_station_id": ["S001"] * n,
        "end_station_name": ["Station B"] * n,
        "end_station_id": ["S002"] * n,
        "start_lat": [40.7128] * n,
        "start_lng": [-74.0060] * n,
        "end_lat": [40.7589] * n,
        "end_lng": [-73.9851] * n,
        "member_casual": ["member"] * n,
    }
    if with_nulls:
        data["start_lat"] = [None] * n
    return pd.DataFrame(data)


def _make_legacy_df(n: int = 100) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "tripduration": [900] * n,
            "starttime": ["2019-01-01 08:00:00"] * n,
            "stoptime": ["2019-01-01 08:15:00"] * n,
            "start station id": [72] * n,
            "start station name": ["W 52 St & 11 Ave"] * n,
            "start station latitude": [40.7696] * n,
            "start station longitude": [-73.9936] * n,
            "end station id": [79] * n,
            "end station name": ["Franklin St & W Broadway"] * n,
            "end station latitude": [40.7191] * n,
            "end station longitude": [-74.0063] * n,
            "bikeid": [14969] * n,
            "usertype": ["Subscriber"] * n,
            "birth year": [1969] * n,
            "gender": [1] * n,
        }
    )


# ---------------------------------------------------------------------------
# detect_schema
# ---------------------------------------------------------------------------


def test_detect_schema_current():
    df = _make_current_df(5)
    assert detect_schema(df.columns.tolist()) == "current"


def test_detect_schema_legacy():
    df = _make_legacy_df(5)
    assert detect_schema(df.columns.tolist()) == "legacy"


def test_detect_schema_unknown():
    df = pd.DataFrame({"foo": [1, 2], "bar": [3, 4]})
    assert detect_schema(df.columns.tolist()) == "unknown"


# ---------------------------------------------------------------------------
# validate_dataframe — happy path
# ---------------------------------------------------------------------------


def test_valid_current_dataframe():
    df = _make_current_df(200)
    report = validate_dataframe(df, "202301")
    assert report.is_valid
    assert report.schema_version == "current"
    assert report.row_count == 200
    assert report.missing_required_columns == []


def test_valid_legacy_dataframe():
    df = _make_legacy_df(200)
    report = validate_dataframe(df, "201901")
    assert report.is_valid
    assert report.schema_version == "legacy"


# ---------------------------------------------------------------------------
# validate_dataframe — failure cases
# ---------------------------------------------------------------------------


def test_missing_column_produces_error():
    # Drop a required column that does NOT affect schema detection
    # (ride_id is used to detect schema so dropping it changes version to 'unknown')
    df = _make_current_df(100)
    df = df.drop(columns=["member_casual"])
    report = validate_dataframe(df, "202301")
    assert not report.is_valid
    assert "member_casual" in report.missing_required_columns


def test_empty_dataframe_is_invalid():
    df = _make_current_df(0)
    report = validate_dataframe(df, "202301")
    assert not report.is_valid
    assert any("empty" in e.lower() for e in report.errors)


def test_unknown_schema_produces_error():
    df = pd.DataFrame({"x": [1, 2], "y": [3, 4]})
    report = validate_dataframe(df)
    assert not report.is_valid


# ---------------------------------------------------------------------------
# validate_dataframe — warnings
# ---------------------------------------------------------------------------


def test_high_null_rate_produces_warning():
    df = _make_current_df(100, with_nulls=True)
    report = validate_dataframe(df, "202301")
    # High null rate on start_lat should trigger a warning
    assert any("start_lat" in w for w in report.warnings)


def test_duplicate_ride_ids_produce_warning():
    df = _make_current_df(100)
    # Make all ride_ids the same
    df["ride_id"] = "DUPE"
    report = validate_dataframe(df, "202301")
    assert any("duplicate" in w.lower() for w in report.warnings)
