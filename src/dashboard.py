"""
dashboard.py — Interactive Streamlit dashboard for NYC Citi Bike analytics.

Reads from outputs/*.csv (produced by analytics_local.py / run_pipeline.py).

Run with:
    streamlit run src/dashboard.py

Requirements: streamlit, plotly, pandas
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="NYC Citi Bike Analytics",
    page_icon="🚲",
    layout="wide",
    initial_sidebar_state="expanded",
)

OUTPUTS_DIR = Path("outputs")
DAY_MAP = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}

# ---------------------------------------------------------------------------
# Data loading (cached)
# ---------------------------------------------------------------------------


@st.cache_data(ttl=300)
def load_data() -> dict[str, pd.DataFrame]:
    data: dict[str, pd.DataFrame] = {}

    top_path = OUTPUTS_DIR / "top_stations_results.csv"
    hourly_path = OUTPUTS_DIR / "hourly_results.csv"
    rideable_path = OUTPUTS_DIR / "rideable_split_results.csv"
    nbhd_path = OUTPUTS_DIR / "neighborhood_accessibility.csv"

    if top_path.exists():
        df = pd.read_csv(top_path)
        df["ebike_share_pct"] = (100 * df["ebike_trips"] / df["total_trips"].replace(0, float("nan"))).round(1)
        df["member_pct"] = (100 * df["member_trips"] / df["total_trips"].replace(0, float("nan"))).round(1)
        data["top"] = df

    if hourly_path.exists():
        df = pd.read_csv(hourly_path)
        df["day_name"] = df["day_of_week"].map(DAY_MAP)
        data["hourly"] = df

    if rideable_path.exists():
        data["rideable"] = pd.read_csv(rideable_path)

    if nbhd_path.exists():
        data["nbhd"] = pd.read_csv(nbhd_path)

    return data


data = load_data()

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/thumb/a/a3/Citi_Bike_logo.svg/320px-Citi_Bike_logo.svg.png",
        width=140,
    )
    st.title("NYC Citi Bike")
    st.caption("Lambda Architecture · Analytics Dashboard")
    st.divider()

    top_n = st.slider("Top N stations", min_value=5, max_value=20, value=10, step=5)

    if "top" in data:
        rideable_filter = st.multiselect(
            "Filter by rideable type",
            options=["All", "Electric Bike", "Classic Bike"],
            default=["All"],
        )
    else:
        rideable_filter = ["All"]

    st.divider()
    st.markdown(
        """
        **Pipeline layers**
        - 🟤 Bronze — raw Parquet
        - 🥈 Silver — cleaned & typed
        - 🥇 Gold — aggregated
        - 📡 Streaming — live GBFS

        **Stack:** PySpark · DuckDB · Kafka  
        GeoPandas · Airflow · Docker · S3
        """
    )

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _missing_data_msg(name: str) -> None:
    st.warning(
        f"**{name}** data not found. "
        f"Run `python run_pipeline.py --skip-sync` to generate CSV outputs.",
        icon="⚠️",
    )


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_overview, tab_stations, tab_temporal, tab_equity = st.tabs(
    ["📊 Overview", "📍 Stations", "🕐 Temporal", "🗺️ Equity"]
)

# ============================================================================
# TAB 1 — Overview
# ============================================================================

with tab_overview:
    st.header("Pipeline Analytics Overview")

    # KPI row
    col1, col2, col3, col4 = st.columns(4)

    if "top" in data:
        total_trips = int(data["top"]["total_trips"].sum())
        total_stations = len(data["top"])
        col1.metric("Total Trips (sample)", f"{total_trips:,}")
        col2.metric("Stations Tracked", f"{total_stations:,}")

    if "rideable" in data:
        rd = data["rideable"]
        ebike_row = rd[rd["rideable_type"] == "electric_bike"]
        ebike_pct = float(ebike_row["pct"].values[0]) if not ebike_row.empty else 0.0
        col3.metric("E-bike Share", f"{ebike_pct:.1f}%")

    if "nbhd" in data:
        n_boroughs = data["nbhd"]["borough"].nunique()
        n_neighborhoods = len(data["nbhd"])
        col4.metric("Neighborhoods Covered", f"{n_neighborhoods:,}")

    st.divider()

    left, right = st.columns([3, 2])

    # Hourly heatmap preview
    with left:
        if "hourly" in data:
            st.subheader("Trip Volume Heatmap")
            pivot = (
                data["hourly"]
                .pivot_table(index="day_name", columns="hour_of_day", values="total_trips", aggfunc="sum")
            )
            day_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            pivot = pivot.reindex([d for d in day_order if d in pivot.index])

            fig = px.imshow(
                pivot,
                color_continuous_scale="YlOrRd",
                labels={"x": "Hour of Day", "y": "Day", "color": "Trips"},
                aspect="auto",
            )
            fig.update_layout(margin=dict(l=0, r=0, t=20, b=0), height=280)
            st.plotly_chart(fig, use_container_width=True)
        else:
            _missing_data_msg("Hourly")

    # Rideable donut
    with right:
        if "rideable" in data:
            st.subheader("Rideable Split")
            rd = data["rideable"].copy()
            rd["label"] = rd["rideable_type"].str.replace("_", " ").str.title()
            fig = go.Figure(
                go.Pie(
                    labels=rd["label"],
                    values=rd["total_trips"],
                    hole=0.52,
                    marker_colors=["#F4A736", "#4A90D9"],
                    textinfo="percent+label",
                    textfont_size=13,
                )
            )
            fig.update_layout(
                showlegend=False,
                margin=dict(l=10, r=10, t=10, b=10),
                height=280,
                annotations=[
                    dict(
                        text=f"{int(rd['total_trips'].sum()):,}<br>Trips",
                        x=0.5, y=0.5, showarrow=False,
                        font=dict(size=14, color="#2C3E50"),
                    )
                ],
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            _missing_data_msg("Rideable split")

# ============================================================================
# TAB 2 — Stations
# ============================================================================

with tab_stations:
    st.header("Station Analysis")

    if "top" not in data:
        _missing_data_msg("Top stations")
    else:
        top_df = data["top"].head(top_n).copy()

        col_a, col_b = st.columns([3, 2])

        with col_a:
            st.subheader(f"Top {top_n} Stations by Total Trips")
            top_df_sorted = top_df.sort_values("total_trips")
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=top_df_sorted["total_trips"] - top_df_sorted["ebike_trips"],
                    y=top_df_sorted["start_station_name"],
                    name="Classic Bike",
                    orientation="h",
                    marker_color="#4A90D9",
                )
            )
            fig.add_trace(
                go.Bar(
                    x=top_df_sorted["ebike_trips"],
                    y=top_df_sorted["start_station_name"],
                    name="E-bike",
                    orientation="h",
                    marker_color="#F4A736",
                )
            )
            fig.update_layout(
                barmode="stack",
                xaxis_title="Total Trips",
                margin=dict(l=0, r=20, t=10, b=0),
                height=420,
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            fig.update_xaxes(tickformat=",d")
            st.plotly_chart(fig, use_container_width=True)

        with col_b:
            st.subheader("Member vs Casual Share")
            fig2 = go.Figure()
            fig2.add_trace(
                go.Bar(
                    x=top_df_sorted["member_trips"],
                    y=top_df_sorted["start_station_name"],
                    name="Member",
                    orientation="h",
                    marker_color="#27AE60",
                )
            )
            fig2.add_trace(
                go.Bar(
                    x=top_df_sorted["total_trips"] - top_df_sorted["member_trips"],
                    y=top_df_sorted["start_station_name"],
                    name="Casual",
                    orientation="h",
                    marker_color="#E74C3C",
                )
            )
            fig2.update_layout(
                barmode="stack",
                xaxis_title="Total Trips",
                margin=dict(l=0, r=10, t=10, b=0),
                height=420,
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            fig2.update_xaxes(tickformat=",d")
            st.plotly_chart(fig2, use_container_width=True)

        st.divider()
        st.subheader("Station Volume vs E-bike Utilisation")

        full_top = data["top"].copy()
        fig3 = px.scatter(
            full_top,
            x="total_trips",
            y="ebike_share_pct",
            hover_name="start_station_name",
            color="ebike_share_pct",
            color_continuous_scale="RdYlGn",
            range_color=[0, 100],
            labels={"total_trips": "Total Trips", "ebike_share_pct": "E-bike Share (%)"},
            height=400,
        )
        fig3.add_hline(
            y=50, line_dash="dash",
            line_color="gray", annotation_text="50% threshold",
        )
        fig3.update_layout(margin=dict(l=0, r=0, t=10, b=0))
        fig3.update_xaxes(tickformat=",d")
        st.plotly_chart(fig3, use_container_width=True)

        st.divider()
        with st.expander("📋 Raw station data"):
            st.dataframe(
                data["top"].style.format(
                    {"total_trips": "{:,.0f}", "ebike_trips": "{:,.0f}",
                     "member_trips": "{:,.0f}", "ebike_share_pct": "{:.1f}%",
                     "member_pct": "{:.1f}%", "avg_duration_min": "{:.1f}"}
                ),
                use_container_width=True,
            )

# ============================================================================
# TAB 3 — Temporal
# ============================================================================

with tab_temporal:
    st.header("Temporal Usage Patterns")

    if "hourly" not in data:
        _missing_data_msg("Hourly")
    else:
        hourly = data["hourly"]

        st.subheader("Heatmap — Trips by Hour × Day of Week")
        pivot = hourly.pivot_table(
            index="day_name", columns="hour_of_day", values="total_trips", aggfunc="sum"
        )
        day_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        pivot = pivot.reindex([d for d in day_order if d in pivot.index])

        fig = px.imshow(
            pivot,
            color_continuous_scale="YlOrRd",
            labels={"x": "Hour of Day", "y": "Day", "color": "Trips"},
            aspect="auto",
            height=350,
        )
        fig.update_layout(margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)

        st.divider()
        col_l, col_r = st.columns(2)

        with col_l:
            st.subheader("Average Hourly Profile by Day Type")
            hourly2 = hourly.copy()
            # 1=Sun, 7=Sat are weekend; 2-6 are weekday
            hourly2["day_type"] = hourly2["day_of_week"].apply(
                lambda d: "Weekend" if d in (1, 7) else "Weekday"
            )
            profile = (
                hourly2.groupby(["day_type", "hour_of_day"])["total_trips"]
                .mean()
                .reset_index()
            )
            fig_l = px.line(
                profile,
                x="hour_of_day",
                y="total_trips",
                color="day_type",
                color_discrete_map={"Weekday": "#2C3E50", "Weekend": "#E74C3C"},
                labels={"hour_of_day": "Hour of Day", "total_trips": "Avg Trips"},
                markers=True,
                height=320,
            )
            fig_l.update_layout(margin=dict(l=0, r=0, t=10, b=0),
                                 legend_title_text="")
            st.plotly_chart(fig_l, use_container_width=True)

        with col_r:
            st.subheader("Total Trips by Day of Week")
            daily = hourly.groupby("day_name")["total_trips"].sum().reindex(day_order).reset_index()
            daily.columns = ["day_name", "total_trips"]
            fig_r = px.bar(
                daily,
                x="day_name",
                y="total_trips",
                color="total_trips",
                color_continuous_scale="Blues",
                labels={"day_name": "", "total_trips": "Total Trips"},
                height=320,
            )
            fig_r.update_layout(
                showlegend=False,
                coloraxis_showscale=False,
                margin=dict(l=0, r=0, t=10, b=0),
            )
            fig_r.update_yaxes(tickformat=",d")
            st.plotly_chart(fig_r, use_container_width=True)

        # Peak hours insight
        st.divider()
        peak = hourly.groupby("hour_of_day")["total_trips"].sum()
        peak_hour = int(peak.idxmax())
        off_hour = int(peak.idxmin())
        col1, col2, col3 = st.columns(3)
        col1.metric("Peak Hour", f"{peak_hour:02d}:00", f"{int(peak[peak_hour]):,} trips")
        col2.metric("Quietest Hour", f"{off_hour:02d}:00", f"{int(peak[off_hour]):,} trips")
        ratio = peak[peak_hour] / peak[off_hour]
        col3.metric("Peak:Off-Peak Ratio", f"{ratio:.1f}×")

# ============================================================================
# TAB 4 — Equity
# ============================================================================

with tab_equity:
    st.header("Neighborhood Equity & Coverage")

    if "nbhd" not in data:
        _missing_data_msg("Neighborhood accessibility")
    else:
        nbhd = data["nbhd"].copy()

        # Borough summary
        st.subheader("Station Infrastructure by Borough")
        borough_summary = (
            nbhd.groupby("borough")
            .agg(
                neighborhoods=("neighborhood_name", "count"),
                total_stations=("station_count", "sum"),
                total_capacity=("total_capacity", "sum"),
                avg_area_km2=("area_km2", "mean"),
            )
            .reset_index()
        )
        borough_summary["stations_per_km2"] = (
            borough_summary["total_stations"] / borough_summary["avg_area_km2"]
        ).round(2)

        col_a, col_b = st.columns(2)

        with col_a:
            fig_bc = px.bar(
                borough_summary.sort_values("total_capacity", ascending=False),
                x="borough",
                y="total_capacity",
                color="borough",
                labels={"total_capacity": "Total Dock Capacity", "borough": ""},
                height=320,
                title="Total Dock Capacity by Borough",
            )
            fig_bc.update_layout(showlegend=False, margin=dict(l=0, r=0, t=40, b=0))
            fig_bc.update_yaxes(tickformat=",d")
            st.plotly_chart(fig_bc, use_container_width=True)

        with col_b:
            fig_den = px.bar(
                borough_summary.sort_values("stations_per_km2", ascending=False),
                x="borough",
                y="stations_per_km2",
                color="borough",
                labels={"stations_per_km2": "Stations / km²", "borough": ""},
                height=320,
                title="Station Density by Borough (stations/km²)",
            )
            fig_den.update_layout(showlegend=False, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig_den, use_container_width=True)

        st.divider()

        # Top neighborhoods table
        st.subheader("Top 20 Neighborhoods by Dock Capacity")
        top_nbhd = nbhd.nlargest(20, "total_capacity")[
            ["neighborhood_name", "borough", "station_count", "total_capacity", "area_km2"]
        ].copy()
        top_nbhd["area_km2"] = top_nbhd["area_km2"].round(2)

        fig_nbhd = px.bar(
            top_nbhd.sort_values("total_capacity"),
            x="total_capacity",
            y="neighborhood_name",
            color="borough",
            orientation="h",
            labels={"total_capacity": "Total Dock Capacity", "neighborhood_name": ""},
            height=480,
        )
        fig_nbhd.update_layout(margin=dict(l=0, r=0, t=10, b=0))
        fig_nbhd.update_xaxes(tickformat=",d")
        st.plotly_chart(fig_nbhd, use_container_width=True)

        st.divider()
        st.subheader("📋 Full Neighborhood Table")
        st.dataframe(
            nbhd[["neighborhood_name", "borough", "station_count", "total_capacity", "area_km2"]]
            .sort_values("total_capacity", ascending=False)
            .style.format({"total_capacity": "{:,.0f}", "station_count": "{:,.0f}", "area_km2": "{:.2f}"}),
            use_container_width=True,
            height=400,
        )
