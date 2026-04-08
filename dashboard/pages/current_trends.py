"""
Streamlit page: Current Trends.
"""

from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pandas as pd
import streamlit as st

from dashboard.components.trend_chart import (
    plot_growth_rate_bar,
    plot_keyword_frequency_timeseries,
    plot_trend_distribution,
)
from dashboard.components.trend_table import (
    render_anomaly_alerts,
    render_metric_cards,
    render_trending_topics_table,
)
from dashboard.demo_data import load_demo_trends
from dashboard.theme import apply_theme, render_hero, render_info_card, render_navbar

_TRENDS_DIR = _PROJECT_ROOT / "data" / "hdfs" / "trends"

apply_theme()


@st.cache_data(ttl=60, show_spinner=False)
def _load_trends() -> pd.DataFrame | None:
    """Load trend parquet outputs from the local data lake."""
    if not _TRENDS_DIR.exists():
        return None

    parquet_files = list(_TRENDS_DIR.rglob("*.parquet"))
    if not parquet_files:
        return None

    frames: list[pd.DataFrame] = []
    for path in parquet_files:
        try:
            frames.append(pd.read_parquet(path))
        except Exception as exc:  # noqa: BLE001
            st.warning(f"Could not read {path.name}: {exc}")

    if not frames:
        return None

    df = pd.concat(frames, ignore_index=True)

    if "source" in df.columns:
        global_rows = df[df["source"] == "__all__"]
        if not global_rows.empty:
            df = global_rows

    if "timestamp" not in df.columns and "window_start" in df.columns:
        df["timestamp"] = df["window_start"]

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    if "growth_rate" not in df.columns and "growth_rate_24h" in df.columns:
        df["growth_rate"] = df["growth_rate_24h"]
    if "z_score" not in df.columns and "z_score_24h" in df.columns:
        df["z_score"] = df["z_score_24h"]
    if "trend_label" in df.columns:
        df["trend_label"] = df["trend_label"].astype(str).str.lower().str.strip()

    return df


@st.cache_data(show_spinner=False)
def _load_demo_trends() -> pd.DataFrame:
    return load_demo_trends()


def render_current_trends_page() -> None:
    """Render the full current trends experience."""
    render_navbar("current_trends")
    render_hero(
        kicker="Live Signal Board",
        title="Current Trending Topics",
        subtitle=(
            "Track which conversations are gaining traction right now through rolling keyword frequencies, "
            "growth-rate momentum, and anomaly-aware trend labeling."
        ),
        chips=["Current Signals", "Anomaly Detection", "Growth Momentum", "Multi-Window View"],
    )
    st.write("")

    with st.spinner("Loading trend data..."):
        df_raw = _load_trends()

    if df_raw is None:
        df_raw = _load_demo_trends()
        st.info(
            "Showing bundled demo trends because no generated parquet files were found in `data/hdfs/trends/`."
        )

    st.markdown("### Signal Filters")
    f1, f2, f3 = st.columns([1.2, 1, 1], gap="large")
    with f1:
        available_labels = (
            sorted(df_raw["trend_label"].dropna().unique().tolist())
            if "trend_label" in df_raw.columns
            else []
        )
        selected_labels = st.multiselect(
            "Trend Label",
            options=available_labels,
            default=available_labels,
        )
    with f2:
        max_freq = int(df_raw["frequency"].max()) if "frequency" in df_raw.columns else 100
        min_frequency = st.slider(
            "Minimum Frequency",
            min_value=1,
            max_value=max(max_freq, 1),
            value=min(5, max(max_freq, 1)),
            step=1,
        )
    with f3:
        lookback_options = {
            "Last 1 hour": 1,
            "Last 6 hours": 6,
            "Last 24 hours": 24,
            "Last 48 hours": 48,
            "Last 7 days": 168,
            "All time": None,
        }
        selected_range = st.selectbox(
            "Lookback Window",
            options=list(lookback_options.keys()),
            index=2,
        )
        lookback_hours = lookback_options[selected_range]

    st.write("")

    df = df_raw.copy()
    if selected_labels:
        df = df[df["trend_label"].isin(selected_labels)]
    if "frequency" in df.columns:
        df = df[df["frequency"] >= min_frequency]
    if lookback_hours and "timestamp" in df.columns:
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=lookback_hours)
        df = df[df["timestamp"] >= cutoff]

    if df.empty:
        st.warning("No trends match the selected filters.")
        return

    top_info, status_info = st.columns(2, gap="large")
    with top_info:
        render_info_card(
            title="How to interpret the board",
            body=(
                "Use frequency to understand current attention, growth rate to understand acceleration, "
                "and anomaly markers to spot non-routine spikes. Emerging topics usually sustain growth; "
                "viral topics often show short-window breakout behavior."
            ),
            label="Guide",
        )
    with status_info:
        latest_ts = df["timestamp"].max() if "timestamp" in df.columns else None
        latest_str = latest_ts.strftime("%Y-%m-%d %H:%M UTC") if pd.notna(latest_ts) else "Unknown"
        render_info_card(
            title="Active snapshot",
            body=(
                f"Results currently reflect data through {latest_str}. "
                "Filter the board to focus on immediate bursts or broader trend persistence."
            ),
            label="Status",
        )

    st.write("")
    render_metric_cards(df)
    st.write("")

    left, right = st.columns([1.45, 1], gap="large")
    with left:
        st.markdown("### Trend Ranking")
        render_trending_topics_table(
            df.sort_values(["frequency", "z_score"], ascending=[False, False]),
            top_n=20,
        )
    with right:
        st.markdown("### Label Mix")
        st.plotly_chart(plot_trend_distribution(df), use_container_width=True)

    st.write("")
    st.markdown("### Frequency Timelines")
    top_keywords = (
        df.groupby("keyword")["frequency"].max().sort_values(ascending=False).head(20).index.tolist()
        if "keyword" in df.columns
        else []
    )
    selected_keywords = st.multiselect(
        "Keywords to plot",
        options=top_keywords,
        default=top_keywords[:3],
    )
    show_anomalies = st.toggle("Highlight anomaly points", value=True)
    for keyword in selected_keywords:
        st.plotly_chart(
            plot_keyword_frequency_timeseries(df, keyword, show_anomalies=show_anomalies),
            use_container_width=True,
        )

    st.write("")
    st.markdown("### Momentum Leaderboard")
    top_n_bar = st.slider("Keywords in growth chart", min_value=5, max_value=40, value=18)
    st.plotly_chart(plot_growth_rate_bar(df, top_n=top_n_bar), use_container_width=True)

    st.write("")
    render_anomaly_alerts(df, max_alerts=6)


render_current_trends_page()
