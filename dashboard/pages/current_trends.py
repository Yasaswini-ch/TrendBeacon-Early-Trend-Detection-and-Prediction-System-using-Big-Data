"""
Streamlit page: Current Trends.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pandas as pd
import streamlit as st

from dashboard.bootstrap_data import ensure_demo_outputs
from dashboard.components.data_uploader import render_data_uploader
from dashboard.components.trend_chart import (
    plot_growth_rate_bar,
    plot_keyword_frequency_timeseries,
    plot_trend_distribution,
)
from dashboard.components.keyword_graph import plot_related_keywords_graph
from dashboard.components.trend_table import (
    render_anomaly_alerts,
    render_metric_cards,
    render_trending_topics_table,
)
from dashboard.components.user_guidance import (
    render_summary_banner,
    render_trend_explanation_card,
    render_all_metric_explanations,
    get_unique_trend_labels,
)
from dashboard.demo_data import load_demo_trends
from dashboard.theme import apply_theme, render_hero, render_info_card, render_navbar

_TRENDS_DIR = _PROJECT_ROOT / "data" / "hdfs" / "trends"

apply_theme()
ensure_demo_outputs()


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

    # Check for uploaded data from home page session
    if "uploaded_trend_data" in st.session_state:
        df_raw = st.session_state["uploaded_trend_data"]
        st.success("✅ Using your uploaded data from home page")
    else:
        # User data upload section
        uploaded_df = render_data_uploader()

        with st.spinner("Loading trend data..."):
            df_raw = _load_trends()

        if df_raw is None:
            df_raw = _load_demo_trends()
            st.info(
                "Showing bundled demo trends because no generated parquet files were found in `data/hdfs/trends/`."
            )

        # Use uploaded data if provided
        if uploaded_df is not None:
            df_raw = uploaded_df
            st.success("✅ Using your uploaded data for analysis")

    st.markdown("### Signal Filters")
    f1, f2, f3, f4, f5 = st.columns([1.2, 1, 1, 1, 1.2], gap="large")
    with f1:
        # Get unique trend labels without duplicates, with emoji badges
        available_labels = get_unique_trend_labels(df_raw)
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
        max_growth = float(df_raw["growth_rate"].max()) if "growth_rate" in df_raw.columns else 100.0
        min_growth = st.slider(
            "Min Growth Rate (%)",
            min_value=0.0,
            max_value=max(max_growth, 100.0),
            value=0.0,
            step=5.0,
        )
    with f4:
        lookback_options = {
            "All time": None,
            "Last 1 hour": 1,
            "Last 6 hours": 6,
            "Last 24 hours": 24,
            "Last 48 hours": 48,
            "Last 7 days": 168,
            "Historical Replay (Select Date)": "historical",
        }
        selected_range = st.selectbox(
            "Lookback Window",
            options=list(lookback_options.keys()),
            index=0,  # Default to All time for better first-run experience
        )
        lookback_hours = lookback_options[selected_range]

        if lookback_hours == "historical":
            replay_date = st.date_input(
                "Select historical date",
                value=datetime.now().date(),
            )
            # When historical is selected, we'll filter for that specific day
            lookback_hours = None  # We'll use custom filtering for the date
        else:
            replay_date = None
    with f5:
        available_sources = ["__all__", "twitter", "reddit", "news", "google_trends"]
        if "source" in df_raw.columns:
            # Filter out NaN and convert to string to avoid TypeError in sorted()
            actual_sources = [str(s) for s in df_raw["source"].dropna().unique()]
            available_sources = sorted(list(set(available_sources + actual_sources)))
        
        selected_sources = st.multiselect(
            "Data Sources",
            options=available_sources,
            default=["__all__"],
            help="Filter which sources feed the signal",
        )

    st.write("")

    # --- Discovery & Exploration: Search and Surprise Me ---
    st.markdown("### 🔍 Discovery & Exploration")
    d1, d2 = st.columns([3, 1])
    with d1:
        search_query = st.text_input(
            "Search keywords",
            placeholder="Type a keyword to filter the board...",
            help="Autocomplete search across all tracked keywords",
        )
    with d2:
        st.write("")  # Alignment
        st.write("")
        surprise_me = st.button("✨ Surprise me!", use_container_width=True)

    df = df_raw.copy()

    # Apply Historical Replay date filter
    if replay_date and "timestamp" in df.columns:
        # Ensure timestamp is comparable (naive for CSV data, aware for system data)
        if df["timestamp"].dt.tz is not None:
            start_ts = pd.Timestamp(replay_date, tz="UTC")
        else:
            start_ts = pd.Timestamp(replay_date)
            
        end_ts = start_ts + pd.Timedelta(days=1)
        df = df[(df["timestamp"] >= start_ts) & (df["timestamp"] < end_ts)]
    elif lookback_hours and "timestamp" in df.columns:
        if df["timestamp"].dt.tz is not None:
            cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=lookback_hours)
        else:
            cutoff = pd.Timestamp.now() - pd.Timedelta(hours=lookback_hours)
        df = df[df["timestamp"] >= cutoff]

    # Apply search filter
    if search_query:
        df = df[df["keyword"].str.contains(search_query, case=False, na=False)]

    # Apply Surprise Me filter (Move before other filters so thresholds don't kill the surprise)
    if surprise_me:
        import random
        # Filter for emerging/viral trends for better surprise
        potential_surprises = df_raw[df_raw["trend_label"].isin(["emerging", "viral"])]
        if potential_surprises.empty:
            potential_surprises = df_raw

        if not potential_surprises.empty:
            random_keyword = random.choice(potential_surprises["keyword"].unique())
            st.info(f"✨ Surprise! Looking at: **{random_keyword}**")
            # If surprise me is clicked, we override other filters for that keyword
            df = df_raw[df_raw["keyword"] == random_keyword]
            render_filtered_trends(df, df_raw) 
            return # Render and stop here

    if selected_labels:
        # Map emoji-decorated labels back to plain labels for filtering
        emoji_to_plain = {
            "🔥 Viral": "viral",
            "📈 Emerging": "emerging",
            "➡️ Stable": "stable",
            "📉 Declining": "declining",
            "⚡ Nan": "stable", # Map Nan to stable for filtering
        }
        selected_plain = [emoji_to_plain.get(lbl, lbl.lower()) for lbl in selected_labels]
        # Handle potential string 'nan' values by ensuring they are filtered as well
        df = df[df["trend_label"].fillna("stable").str.lower().isin(selected_plain)]
    if "frequency" in df.columns:
        df = df[df["frequency"] >= min_frequency]
    if "growth_rate" in df.columns:
        df = df[df["growth_rate"] >= min_growth]
    
    # Apply source filter
    if "source" in df.columns and selected_sources:
        df = df[df["source"].isin(selected_sources)]
    elif "source" in df.columns:
        # Default to global if nothing selected
        df = df[df["source"] == "__all__"]

    render_filtered_trends(df, df_raw)

def render_filtered_trends(df: pd.DataFrame, df_raw: pd.DataFrame) -> None:
    """Helper to render the trend dashboard once filtering is applied."""
    if df.empty:
        st.warning("No trends match the selected filters. Try adjusting the Frequency or Growth sliders, or set Lookback to 'All time'.")
        return

    # Plain-English summary banner for non-technical users
    render_summary_banner(df)

    # --- User Experience: Export & Share ---
    st.write("")
    e1, e2, e3, e4 = st.columns([1, 1, 1, 1.2])
    with e1:
        # CSV Export
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Export CSV",
            data=csv,
            file_name=f"trendbeacon_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            key="download_csv_btn",
            use_container_width=True,
        )
        # API Export (JSON)
        json_data = df.to_json(orient="records", date_format="iso").encode('utf-8')
        st.download_button(
            label="🔌 Export JSON (API)",
            data=json_data,
            file_name=f"trendbeacon_api_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            key="download_json_btn",
            use_container_width=True,
        )
    with e2:
        if st.button("🔗 Shareable URL", use_container_width=True, key="share_url_btn"):
            # In a real app, we'd use st.query_params to store state
            timestamp = datetime.now().strftime("%Y%m%d%H%M")
            share_url = f"https://trendbeacon.app/current_trends?snapshot={timestamp}"
            st.info(f"🔗 Snapshot URL (simulated): {share_url}")
        
        # Saved Searches
        if "saved_searches" not in st.session_state:
            st.session_state["saved_searches"] = []

        with st.expander("💾 Saved Searches", expanded=False):
            search_name = st.text_input("Name this search:", key="save_search_input")
            if st.button("Save Current View", key="save_search_btn"):
                # Save enough info to recreate the view
                search_config = {
                    "name": search_name or f"Search {datetime.now().strftime('%H:%M:%S')}",
                    "labels": selected_labels,
                    "min_freq": min_frequency,
                    "min_growth": min_growth,
                    "lookback": selected_range,
                    "sources": selected_sources,
                }
                st.session_state["saved_searches"].append(search_config)
                st.success("Search saved!")

            if st.session_state["saved_searches"]:
                st.write("Your Searches:")
                for i, s in enumerate(st.session_state["saved_searches"]):
                    st.write(f"{i+1}. {s['name']}")
                if st.button("Clear Saved Searches", key="clear_saved_searches_btn"):
                    st.session_state["saved_searches"] = []
                    st.rerun()
    with e3:
        # Theme toggle (Streamlit 1.29+ doesn't have a direct session-state theme setter, 
        # but we can simulate a preference toggle)
        current_theme = st.session_state.get("theme_preference", "Dark")
        if st.button(f"🌓 Theme: {current_theme}", use_container_width=True, key="theme_toggle_btn"):
            new_theme = "Light" if current_theme == "Dark" else "Dark"
            st.session_state["theme_preference"] = new_theme
            st.rerun()
    with e4:
        # Watchlist / Alert Subscription
        if "watchlist" not in st.session_state:
            st.session_state["watchlist"] = []
        
        # Streamlit 1.29.0 doesn't have st.popover, using expander instead
        with st.expander("🔔 Watchlist", expanded=False):
            new_keyword = st.text_input("Add keyword to alert on:", key="watchlist_input")
            if st.button("Add to Watchlist", key="add_watchlist_btn"):
                if new_keyword and new_keyword not in st.session_state["watchlist"]:
                    st.session_state["watchlist"].append(new_keyword)
                    st.success(f"Added {new_keyword} to watchlist!")
            
            if st.session_state["watchlist"]:
                st.write("Current Watchlist:")
                for kw in st.session_state["watchlist"]:
                    st.write(f"- {kw}")
                if st.button("Clear Watchlist", key="clear_watchlist_btn"):
                    st.session_state["watchlist"] = []
                    st.rerun()

    # Check for watchlist alerts
    if "watchlist" in st.session_state and st.session_state["watchlist"]:
        watched_trends = df[df["keyword"].isin(st.session_state["watchlist"])]
        # Alert if z_score > 3 (high impact)
        alerts = watched_trends[watched_trends["z_score"] > 3.0]
        for _, row in alerts.iterrows():
            st.warning(f"🚨 Watchlist Alert: **{row['keyword']}** z-score is {row['z_score']:.2f}!")

    st.write("")

    # --- Discovery & Exploration: Related Keywords Graph ---
    st.markdown("### 🕸️ Related Keywords Graph")
    with st.expander("Explore Keyword Clusters", expanded=False):
        graph_keyword = st.text_input("Enter keyword to explore its cluster:", value=df["keyword"].iloc[0] if not df.empty else "", key="graph_input")
        if graph_keyword:
            fig = plot_related_keywords_graph(graph_keyword, df_raw)
            st.plotly_chart(fig, use_container_width=True)

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

    # Metric explanations for non-technical users
    render_all_metric_explanations()
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
        default=top_keywords[:3] if len(top_keywords) >= 3 else top_keywords,
        key="plots_multiselect"
    )
    show_anomalies = st.toggle("Highlight anomaly points", value=True, key="anomaly_toggle")
    for keyword in selected_keywords:
        st.plotly_chart(
            plot_keyword_frequency_timeseries(df, keyword, show_anomalies=show_anomalies),
            use_container_width=True,
        )

    st.write("")
    st.markdown("### Momentum Leaderboard")
    top_n_bar = st.slider("Keywords in growth chart", min_value=5, max_value=40, value=18, key="growth_slider")
    st.plotly_chart(plot_growth_rate_bar(df, top_n=top_n_bar), use_container_width=True)

    st.write("")
    render_anomaly_alerts(df, max_alerts=6)


render_current_trends_page()
