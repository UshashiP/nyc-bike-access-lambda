"""
visualize_accessibility.py — Visualize Citi Bike neighborhood accessibility scores on a map.

Usage:
    PYTHONPATH=. ./citibike-env/bin/python src/visualize_accessibility.py
"""
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd

# Load the accessibility scores (CSV output from spatial analysis)
scores = pd.read_csv("outputs/neighborhood_accessibility.csv")

# Load NTA boundaries (GeoJSON from ArcGIS)
nta_gdf = gpd.read_file(
    "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/NYC_Neighborhood_Tabulation_Areas_2020/FeatureServer/0/query?where=1=1&outFields=*&outSR=4326&f=pgeojson"
)

# Merge scores onto geometry
gdf = nta_gdf.merge(scores, on=["neighborhood_id", "neighborhood_name", "borough"], how="left")

# Plot
fig, ax = plt.subplots(1, 1, figsize=(12, 10))
gdf.plot(
    column="accessibility_score",
    cmap="viridis",
    linewidth=0.5,
    edgecolor="gray",
    legend=True,
    ax=ax,
    missing_kwds={"color": "lightgray", "label": "No data"},
)
ax.set_title("NYC Citi Bike Neighborhood Accessibility Scores", fontsize=16)
ax.axis("off")
plt.tight_layout()
plt.show()
