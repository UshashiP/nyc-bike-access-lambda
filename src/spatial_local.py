"""
spatial_local.py — Run spatial equity analysis using local gold-layer data and the ArcGIS NTA GeoJSON endpoint.

Usage:
    PYTHONPATH=. python src/spatial_local.py
"""
import pandas as pd
from src.ingest import load_config
from src.spatial import compute_accessibility_scores

cfg = load_config()

# Load station info from GBFS
resp = pd.read_json(cfg['gbfs']['station_info_url'])
stations = pd.DataFrame(resp['data']['stations'])

# Load gold-layer trips per station
trips = pd.read_parquet('data/gold/trips_per_station/')

# Compute accessibility scores using ArcGIS NTA boundaries
scores = compute_accessibility_scores(trips, stations, nta_source="arcgis")

# Save results
scores.to_csv('outputs/neighborhood_accessibility.csv', index=False)
print(f"Saved {len(scores)} neighborhoods → outputs/neighborhood_accessibility.csv")
print(scores.head(20).to_string(index=False))
