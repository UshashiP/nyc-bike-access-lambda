"""
visualize.py — Generate publication-quality charts from gold-layer analytics outputs.

Charts produced (all saved to outputs/plots/):
  1. top_stations.png         — Top 20 stations by total trips (horizontal bar)
  2. hourly_heatmap.png       — Trips by hour-of-day × day-of-week (heatmap)
  3. rideable_donut.png       — E-bike vs classic bike split (donut)
  4. member_casual.png        — Member vs casual stacked bar (top 15 stations)
  5. ebike_share_scatter.png  — Station volume vs e-bike utilisation (scatter)
  6. neighborhood_capacity.png — Station capacity heatmap by borough
  7. summary_dashboard.png    — One-page 3-panel summary

Usage:
    PYTHONPATH=. python src/visualize.py
    PYTHONPATH=. python src/visualize.py --outputs-dir outputs
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths and theme
# ---------------------------------------------------------------------------

OUTPUTS_DIR = Path("outputs")
PLOTS_DIR = OUTPUTS_DIR / "plots"

PALETTE = {
    "electric_bike": "#F4A736",
    "classic_bike": "#4A90D9",
    "member": "#27AE60",
    "casual": "#E74C3C",
    "primary": "#2C3E50",
    "accent": "#16A085",
    "neutral": "#95A5A6",
}

# Spark SQL dayofweek(): 1 = Sunday … 7 = Saturday
DAY_LABELS = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}
BOROUGH_ORDER = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]


def _setup() -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid", context="talk", font_scale=0.95)
    plt.rcParams.update(
        {
            "figure.dpi": 150,
            "savefig.bbox": "tight",
            "savefig.facecolor": "white",
            "axes.spines.top": False,
            "axes.spines.right": False,
        }
    )


# ---------------------------------------------------------------------------
# Chart 1 — Top stations
# ---------------------------------------------------------------------------


def plot_top_stations(df: pd.DataFrame, n: int = 20) -> Path:
    """Horizontal bar chart of top-N stations, bars coloured by e-bike dominance."""
    top = df.head(n).copy()
    top["ebike_share"] = top["ebike_trips"] / top["total_trips"].replace(0, np.nan)
    colors = [
        PALETTE["electric_bike"] if share >= 0.55 else PALETTE["classic_bike"]
        for share in top["ebike_share"].fillna(0)
    ]

    fig, ax = plt.subplots(figsize=(14, 9))
    bars = ax.barh(top["start_station_name"], top["total_trips"], color=colors, edgecolor="white", linewidth=0.4)

    for bar, val in zip(bars, top["total_trips"]):
        ax.text(
            bar.get_width() + top["total_trips"].max() * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"{int(val):,}",
            va="center",
            ha="left",
            fontsize=8.5,
        )

    ax.invert_yaxis()
    ax.set_xlabel("Total Trips")
    ax.set_title(
        f"Top {n} Citi Bike Stations by Total Trips\n"
        "Amber = e-bike dominant (≥55%),  Blue = classic dominant",
        fontsize=13,
        pad=14,
    )
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.set_xlim(0, top["total_trips"].max() * 1.18)

    from matplotlib.patches import Patch

    ax.legend(
        handles=[
            Patch(facecolor=PALETTE["electric_bike"], label="E-bike dominant (≥55%)"),
            Patch(facecolor=PALETTE["classic_bike"], label="Classic dominant"),
        ],
        loc="lower right",
        fontsize=9,
    )

    plt.tight_layout()
    out = PLOTS_DIR / "top_stations.png"
    fig.savefig(out)
    plt.close(fig)
    logger.info("Saved %s", out)
    return out


# ---------------------------------------------------------------------------
# Chart 2 — Hourly heatmap
# ---------------------------------------------------------------------------


def plot_hourly_heatmap(df: pd.DataFrame) -> Path:
    """Heatmap: trips by hour-of-day (x) and day-of-week (y)."""
    pivot = df.pivot_table(
        index="day_of_week", columns="hour_of_day", values="total_trips", aggfunc="sum"
    )
    # Reorder so Monday is top row, not Sunday
    day_order = [2, 3, 4, 5, 6, 7, 1]  # Mon–Sun
    pivot = pivot.reindex([d for d in day_order if d in pivot.index])
    pivot.index = [DAY_LABELS[d] for d in pivot.index]

    fig, ax = plt.subplots(figsize=(18, 5))
    sns.heatmap(
        pivot,
        cmap="YlOrRd",
        ax=ax,
        linewidths=0.3,
        linecolor="white",
        annot=True,
        fmt=".0f",
        annot_kws={"size": 6.5},
        cbar_kws={"label": "Total Trips", "shrink": 0.8},
    )
    ax.set_title("Citi Bike Usage: Trips by Hour of Day × Day of Week", fontsize=13, pad=14)
    ax.set_xlabel("Hour of Day (0 = midnight)")
    ax.set_ylabel("")
    ax.tick_params(axis="x", labelsize=8)

    plt.tight_layout()
    out = PLOTS_DIR / "hourly_heatmap.png"
    fig.savefig(out)
    plt.close(fig)
    logger.info("Saved %s", out)
    return out


# ---------------------------------------------------------------------------
# Chart 3 — Rideable donut
# ---------------------------------------------------------------------------


def plot_rideable_donut(df: pd.DataFrame) -> Path:
    """Donut chart: e-bike vs classic bike share."""
    fig, ax = plt.subplots(figsize=(7, 7))
    colors = [PALETTE["electric_bike"], PALETTE["classic_bike"]]
    wedge_props = {"width": 0.46, "edgecolor": "white", "linewidth": 2.5}

    _, _, autotexts = ax.pie(
        df["total_trips"],
        labels=df["rideable_type"].str.replace("_", " ").str.title(),
        colors=colors,
        autopct="%1.1f%%",
        startangle=90,
        wedgeprops=wedge_props,
        textprops={"fontsize": 13},
    )
    for at in autotexts:
        at.set_fontsize(12)
        at.set_fontweight("bold")

    total = int(df["total_trips"].sum())
    ax.text(
        0, 0,
        f"{total:,}\nTotal\nTrips",
        ha="center", va="center",
        fontsize=11, fontweight="bold", color=PALETTE["primary"],
    )
    ax.set_title("Rideable Type Split", fontsize=14, pad=18)

    plt.tight_layout()
    out = PLOTS_DIR / "rideable_donut.png"
    fig.savefig(out)
    plt.close(fig)
    logger.info("Saved %s", out)
    return out


# ---------------------------------------------------------------------------
# Chart 4 — Member vs casual (top 15)
# ---------------------------------------------------------------------------


def plot_member_casual(df: pd.DataFrame) -> Path:
    """Stacked horizontal bar: member vs casual riders for top 15 stations."""
    top15 = df.head(15).copy()
    top15["casual_trips"] = top15["total_trips"] - top15["member_trips"]
    top15["member_pct"] = 100 * top15["member_trips"] / top15["total_trips"]

    fig, ax = plt.subplots(figsize=(14, 8))
    ax.barh(top15["start_station_name"], top15["member_trips"],
            color=PALETTE["member"], label="Member")
    ax.barh(top15["start_station_name"], top15["casual_trips"],
            left=top15["member_trips"], color=PALETTE["casual"], label="Casual")

    # Annotate member %
    for _, row in top15.iterrows():
        ax.text(
            row["total_trips"] + top15["total_trips"].max() * 0.01,
            top15.index.get_loc(row.name),
            f"{row['member_pct']:.0f}% mbr",
            va="center", ha="left", fontsize=8,
        )

    ax.invert_yaxis()
    ax.set_xlabel("Total Trips")
    ax.set_title("Member vs Casual Riders — Top 15 Stations", fontsize=13, pad=14)
    ax.legend(loc="lower right", fontsize=10)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.set_xlim(0, top15["total_trips"].max() * 1.2)

    plt.tight_layout()
    out = PLOTS_DIR / "member_casual.png"
    fig.savefig(out)
    plt.close(fig)
    logger.info("Saved %s", out)
    return out


# ---------------------------------------------------------------------------
# Chart 5 — E-bike share scatter
# ---------------------------------------------------------------------------


def plot_ebike_share_scatter(df: pd.DataFrame) -> Path:
    """Scatter: total trips vs e-bike share, coloured by e-bike share."""
    plot_df = df.copy()
    plot_df["ebike_share"] = 100 * plot_df["ebike_trips"] / plot_df["total_trips"].replace(0, np.nan)
    plot_df = plot_df.dropna(subset=["ebike_share"])

    fig, ax = plt.subplots(figsize=(12, 7))
    sc = ax.scatter(
        plot_df["total_trips"],
        plot_df["ebike_share"],
        c=plot_df["ebike_share"],
        cmap="RdYlGn",
        alpha=0.75,
        edgecolors="white",
        linewidths=0.4,
        s=90,
        vmin=0,
        vmax=100,
    )

    # Label top 5 by trip volume
    for _, row in plot_df.nlargest(5, "total_trips").iterrows():
        ax.annotate(
            row["start_station_name"].split("&")[0].strip(),
            (row["total_trips"], row["ebike_share"]),
            xytext=(8, 2),
            textcoords="offset points",
            fontsize=8,
            color=PALETTE["primary"],
        )

    cbar = fig.colorbar(sc, ax=ax, shrink=0.8)
    cbar.set_label("E-bike Share (%)")

    ax.axhline(y=50, color=PALETTE["neutral"], linestyle="--", linewidth=1, label="50% threshold")
    ax.set_xlabel("Total Trips")
    ax.set_ylabel("E-bike Share (%)")
    ax.set_title("Station Volume vs E-bike Utilisation", fontsize=13, pad=14)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.legend(fontsize=9)

    plt.tight_layout()
    out = PLOTS_DIR / "ebike_share_scatter.png"
    fig.savefig(out)
    plt.close(fig)
    logger.info("Saved %s", out)
    return out


# ---------------------------------------------------------------------------
# Chart 6 — Neighborhood capacity heatmap by borough
# ---------------------------------------------------------------------------


def plot_neighborhood_capacity(df: pd.DataFrame) -> Path:
    """
    Grouped bar chart of top neighborhoods by total dock capacity,
    coloured by borough.
    """
    plot_df = df[df["total_capacity"] > 0].copy()
    if plot_df.empty:
        logger.warning("No capacity data — skipping neighborhood capacity chart")
        return None

    top_n = plot_df.nlargest(20, "total_capacity")
    borough_colors = {
        "Manhattan": "#E74C3C",
        "Brooklyn": "#3498DB",
        "Queens": "#2ECC71",
        "Bronx": "#F39C12",
        "Staten Island": "#9B59B6",
    }
    colors = [borough_colors.get(b, PALETTE["neutral"]) for b in top_n["borough"]]

    fig, ax = plt.subplots(figsize=(14, 8))
    bars = ax.barh(
        top_n["neighborhood_name"],
        top_n["total_capacity"],
        color=colors,
        edgecolor="white",
        linewidth=0.4,
    )
    for bar, val in zip(bars, top_n["total_capacity"]):
        ax.text(
            bar.get_width() + top_n["total_capacity"].max() * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"{int(val):,} docks",
            va="center", ha="left", fontsize=8.5,
        )

    ax.invert_yaxis()
    ax.set_xlabel("Total Dock Capacity")
    ax.set_title("Top 20 Neighborhoods by Total Dock Capacity", fontsize=13, pad=14)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.set_xlim(0, top_n["total_capacity"].max() * 1.2)

    from matplotlib.patches import Patch
    legend_els = [
        Patch(facecolor=c, label=b)
        for b, c in borough_colors.items()
        if b in top_n["borough"].values
    ]
    ax.legend(handles=legend_els, loc="lower right", fontsize=9)

    plt.tight_layout()
    out = PLOTS_DIR / "neighborhood_capacity.png"
    fig.savefig(out)
    plt.close(fig)
    logger.info("Saved %s", out)
    return out


# ---------------------------------------------------------------------------
# Chart 7 — Summary dashboard (3 panels)
# ---------------------------------------------------------------------------


def plot_summary_dashboard(
    top_df: pd.DataFrame,
    hourly_df: pd.DataFrame,
    rideable_df: pd.DataFrame,
) -> Path:
    """3-panel summary figure: top stations + rideable split + hourly heatmap."""
    fig = plt.figure(figsize=(22, 14))
    fig.patch.set_facecolor("white")
    fig.suptitle(
        "NYC Citi Bike — Lambda Architecture Pipeline · Analytics Summary",
        fontsize=18,
        fontweight="bold",
        y=0.99,
    )

    # ── Top 10 stations (top-left) ──────────────────────────────────────────
    ax1 = fig.add_subplot(2, 2, 1)
    top10 = top_df.head(10).copy()
    top10["ebike_share"] = top10["ebike_trips"] / top10["total_trips"].replace(0, np.nan)
    colors = [
        PALETTE["electric_bike"] if (s or 0) >= 0.55 else PALETTE["classic_bike"]
        for s in top10["ebike_share"]
    ]
    ax1.barh(top10["start_station_name"], top10["total_trips"], color=colors, edgecolor="white")
    ax1.invert_yaxis()
    ax1.set_title("Top 10 Stations by Trips", fontsize=11, pad=8)
    ax1.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax1.tick_params(axis="y", labelsize=7.5)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    # ── Rideable split donut (top-right) ────────────────────────────────────
    ax2 = fig.add_subplot(2, 2, 2)
    colors2 = [PALETTE["electric_bike"], PALETTE["classic_bike"]]
    _, _, autotexts = ax2.pie(
        rideable_df["total_trips"],
        labels=rideable_df["rideable_type"].str.replace("_", " ").str.title(),
        colors=colors2,
        autopct="%1.1f%%",
        startangle=90,
        wedgeprops={"width": 0.46, "edgecolor": "white", "linewidth": 2},
        textprops={"fontsize": 11},
    )
    for at in autotexts:
        at.set_fontweight("bold")
    total = int(rideable_df["total_trips"].sum())
    ax2.text(0, 0, f"{total:,}\nTrips", ha="center", va="center",
             fontsize=10, fontweight="bold", color=PALETTE["primary"])
    ax2.set_title("Rideable Type Split", fontsize=11, pad=8)

    # ── Hourly heatmap (full bottom row) ────────────────────────────────────
    ax3 = fig.add_subplot(2, 1, 2)
    pivot = hourly_df.pivot_table(
        index="day_of_week", columns="hour_of_day", values="total_trips", aggfunc="sum"
    )
    day_order = [2, 3, 4, 5, 6, 7, 1]
    pivot = pivot.reindex([d for d in day_order if d in pivot.index])
    pivot.index = [DAY_LABELS[d] for d in pivot.index]
    sns.heatmap(
        pivot,
        cmap="YlOrRd",
        ax=ax3,
        linewidths=0.3,
        linecolor="white",
        cbar_kws={"label": "Trips", "shrink": 0.7},
    )
    ax3.set_title("Trips Heatmap: Hour of Day × Day of Week", fontsize=11, pad=8)
    ax3.set_xlabel("Hour of Day (0 = midnight)", fontsize=9)
    ax3.set_ylabel("")
    ax3.tick_params(labelsize=8)

    plt.tight_layout(rect=[0, 0, 1, 0.97])
    out = PLOTS_DIR / "summary_dashboard.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out)
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="Generate Citi Bike visualizations")
    parser.add_argument(
        "--outputs-dir",
        default="outputs",
        help="Path to the outputs/ directory (default: outputs)",
    )
    args = parser.parse_args()

    global OUTPUTS_DIR, PLOTS_DIR
    OUTPUTS_DIR = Path(args.outputs_dir)
    PLOTS_DIR = OUTPUTS_DIR / "plots"
    _setup()

    # Load data
    top_df = pd.read_csv(OUTPUTS_DIR / "top_stations_results.csv")
    hourly_df = pd.read_csv(OUTPUTS_DIR / "hourly_results.csv")
    rideable_df = pd.read_csv(OUTPUTS_DIR / "rideable_split_results.csv")
    nbhd_df = pd.read_csv(OUTPUTS_DIR / "neighborhood_accessibility.csv")

    # Generate all charts
    plot_top_stations(top_df)
    plot_hourly_heatmap(hourly_df)
    plot_rideable_donut(rideable_df)
    plot_member_casual(top_df)
    plot_ebike_share_scatter(top_df)
    plot_neighborhood_capacity(nbhd_df)
    plot_summary_dashboard(top_df, hourly_df, rideable_df)

    print(f"\nAll charts saved to {PLOTS_DIR}/")
    charts = [
        "top_stations.png",
        "hourly_heatmap.png",
        "rideable_donut.png",
        "member_casual.png",
        "ebike_share_scatter.png",
        "neighborhood_capacity.png",
        "summary_dashboard.png",
    ]
    for c in charts:
        print(f"  {c}")


if __name__ == "__main__":
    main()
