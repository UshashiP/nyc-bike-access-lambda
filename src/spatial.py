"""
spatial.py — Spatial joins between Citi Bike station data and NYC neighborhood
             boundaries to support equity analysis.

Uses GeoPandas for local/small-scale joins and can operate on PySpark DataFrames
for distributed use via a broadcast join pattern.

Reference data: NYC Neighborhood Tabulation Areas (NTAs) from NYC Open Data
  https://data.cityofnewyork.us/api/geospatial/cpf4-rkhq?method=export&type=GeoJSON
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from io import BytesIO

import boto3
import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Point

logger = logging.getLogger(__name__)


CRS_WGS84 = "EPSG:4326"

# NYC Open Data GeoJSON (default)
NTA_URL = (
    "https://data.cityofnewyork.us/api/geospatial/cpf4-rkhq"
    "?method=export&type=GeoJSON"
)
# ArcGIS REST endpoint (user-provided)
NTA_ARCGIS_URL = (
    "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/NYC_Neighborhood_Tabulation_Areas_2020/FeatureServer/0/query?where=1=1&outFields=*&outSR=4326&f=pgeojson"
)


# ---------------------------------------------------------------------------
# NTA boundary loading
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def load_nta_boundaries(source: str = "remote") -> gpd.GeoDataFrame:
    """
    Load NYC NTA boundaries.  Cached so the HTTP request only fires once
    per process, regardless of how many times the function is called.

    Parameters
    ----------
    source : str
        'remote'  — download from NYC Open Data (default)
        'arcgis'  — download from ArcGIS REST endpoint (GeoJSON)
        Any other value is treated as a local file path.
    """
    if source == "remote":
        logger.info("Downloading NTA boundaries from NYC Open Data …")
        response = requests.get(NTA_URL, timeout=60)
        response.raise_for_status()
        gdf = gpd.read_file(BytesIO(response.content))
    elif source == "arcgis":
        logger.info("Downloading NTA boundaries from ArcGIS REST endpoint …")
        response = requests.get(NTA_ARCGIS_URL, timeout=60)
        response.raise_for_status()
        gdf = gpd.read_file(BytesIO(response.content))
    else:
        logger.info("Loading NTA boundaries from %s", source)
        gdf = gpd.read_file(source)

    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(CRS_WGS84)

    # Normalise key column names
    col_map = {}
    for col in gdf.columns:
        lower = col.lower()
        if lower in ("ntaname", "nta_name"):
            col_map[col] = "neighborhood_name"
        elif lower in ("ntacode", "nta_code", "ntaabbrev"):
            col_map[col] = "neighborhood_id"
        elif lower in ("boroname", "boro_name"):
            col_map[col] = "borough"
    gdf = gdf.rename(columns=col_map)

    logger.info("Loaded %d NTA polygons", len(gdf))
    return gdf[["neighborhood_id", "neighborhood_name", "borough", "geometry"]]


# ---------------------------------------------------------------------------
# Station-level spatial join
# ---------------------------------------------------------------------------


def stations_to_geodataframe(stations_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Convert a station DataFrame (with lat/lng columns) to a GeoDataFrame.
    Accepts both GBFS info column names and gold-layer trip column names.
    """
    # Handle different column naming conventions
    lat_col = next(
        (c for c in stations_df.columns if c.lower() in ("lat", "latitude", "start_lat")),
        None,
    )
    lng_col = next(
        (c for c in stations_df.columns if c.lower() in ("lon", "lng", "longitude", "start_lng")),
        None,
    )
    if lat_col is None or lng_col is None:
        raise ValueError(
            f"Cannot find lat/lng columns in: {list(stations_df.columns)}"
        )

    geometry = [
        Point(row[lng_col], row[lat_col])
        for _, row in stations_df.iterrows()
    ]
    gdf = gpd.GeoDataFrame(stations_df.copy(), geometry=geometry, crs=CRS_WGS84)
    return gdf


def join_stations_to_neighborhoods(
    stations_df: pd.DataFrame,
    nta_source: str = "remote",
) -> pd.DataFrame:
    """
    Spatially join station points to NTA neighborhood polygons.

    Returns the original DataFrame with three new columns:
      - neighborhood_id
      - neighborhood_name
      - borough

    Stations outside any NTA polygon are assigned NaN values.
    """
    stations_gdf = stations_to_geodataframe(stations_df)
    nta_gdf = load_nta_boundaries(nta_source)

    joined = gpd.sjoin(stations_gdf, nta_gdf, how="left", predicate="within")

    # Drop geometry and sjoin artifact columns before returning plain DataFrame
    result = pd.DataFrame(joined.drop(columns=["geometry", "index_right"], errors="ignore"))
    unmatched = result["neighborhood_id"].isna().sum()
    if unmatched > 0:
        logger.warning("%d station(s) could not be matched to an NTA polygon", unmatched)
    return result


# ---------------------------------------------------------------------------
# Accessibility score
# ---------------------------------------------------------------------------


def compute_accessibility_scores(
    trips_per_station: pd.DataFrame,
    station_info: pd.DataFrame,
    nta_source: str = "remote",
) -> pd.DataFrame:
    """
    Compute a neighborhood-level accessibility score:

        score = (station_count / area_km2) * log1p(avg_daily_trips)

    Higher score → more accessible neighborhood for Citi Bike riders.

    Parameters
    ----------
    trips_per_station : pd.DataFrame
        Gold-layer trips aggregated by start_station_id.
    station_info : pd.DataFrame
        GBFS station_information with station_id, lat, lon, capacity.
    nta_source : str
        Source for NTA boundaries (see load_nta_boundaries).

    Returns
    -------
    pd.DataFrame
        One row per neighborhood with accessibility_score column.
    """
    import numpy as np

    # Join stations to neighborhoods
    station_neighborhoods = join_stations_to_neighborhoods(station_info, nta_source)

    # Summarise trips per station
    station_trips = (
        trips_per_station.groupby("start_station_id")["trips_started"]
        .mean()
        .reset_index()
        .rename(columns={"trips_started": "avg_daily_trips"})
    )

    # Merge
    merged = station_neighborhoods.merge(
        station_trips, left_on="station_id", right_on="start_station_id", how="left"
    )
    merged["avg_daily_trips"] = merged["avg_daily_trips"].fillna(0)

    # Aggregate to neighborhood level
    nta_stats = (
        merged.groupby(["neighborhood_id", "neighborhood_name", "borough"])
        .agg(
            station_count=("station_id", "nunique"),
            total_capacity=("capacity", "sum"),
            avg_daily_trips=("avg_daily_trips", "mean"),
        )
        .reset_index()
    )

    # Compute area in km² from NTA geometries
    nta_gdf = load_nta_boundaries(nta_source)
    nta_area = nta_gdf[["neighborhood_id", "geometry"]].copy()
    nta_area = nta_area.to_crs("EPSG:3857")  # metric CRS for area calculation
    nta_area["area_km2"] = nta_area.geometry.area / 1e6  # m² → km²
    nta_area = pd.DataFrame(nta_area[["neighborhood_id", "area_km2"]])

    nta_stats = nta_stats.merge(nta_area, on="neighborhood_id", how="left")
    nta_stats["area_km2"] = nta_stats["area_km2"].fillna(1)  # avoid division by zero

    # Accessibility score
    nta_stats["accessibility_score"] = (
        nta_stats["station_count"] / nta_stats["area_km2"]
    ) * np.log1p(nta_stats["avg_daily_trips"])

    nta_stats["accessibility_score"] = nta_stats["accessibility_score"].round(4)

    return nta_stats.sort_values("accessibility_score", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# GeoParquet export
# ---------------------------------------------------------------------------


def export_geoparquet(
    nta_scores: pd.DataFrame,
    output_path: str,
    nta_source: str = "remote",
) -> gpd.GeoDataFrame:
    """
    Merge accessibility scores back to NTA geometry and write as GeoParquet.
    output_path can be a local path or an s3:// URI (requires s3fs).
    """
    nta_gdf = load_nta_boundaries(nta_source)
    result_gdf = nta_gdf.merge(nta_scores, on="neighborhood_id", how="left")
    result_gdf.to_parquet(output_path, index=False)
    logger.info("GeoParquet written to %s (%d rows)", output_path, len(result_gdf))
    return result_gdf


if __name__ == "__main__":
    import argparse
    import json

    from src.ingest import load_config

    logging.basicConfig(level=logging.INFO)
    cfg = load_config()

    parser = argparse.ArgumentParser(description="Compute neighborhood accessibility scores")
    parser.add_argument("--station-info", required=True, help="Path to GBFS station_info CSV")
    parser.add_argument("--trips", required=True, help="Path to gold trips_per_station Parquet")
    parser.add_argument("--output", required=True, help="Output GeoParquet path")
    args = parser.parse_args()

    station_info_df = pd.read_csv(args.station_info)
    trips_df = pd.read_parquet(args.trips)
    scores = compute_accessibility_scores(trips_df, station_info_df)
    export_geoparquet(scores, args.output)
