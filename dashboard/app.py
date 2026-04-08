"""
Streamlit home page for the TrendBeacon dashboard.

The project uses Streamlit's native multipage behavior:
- `dashboard/app.py` is the home page
- `dashboard/pages/current_trends.py` is the current trends page
- `dashboard/pages/predicted_trends.py` is the prediction page
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import streamlit as st
from dashboard.demo_data import load_demo_features, load_demo_predictions, load_demo_trends
from dashboard.theme import apply_theme, render_hero, render_info_card, render_navbar

try:
    from config.config_loader import cfg
except Exception:  # noqa: BLE001
    cfg = {"dashboard": {"title": "TrendBeacon"}}

_TRENDS_DIR = _PROJECT_ROOT / "data" / "hdfs" / "trends"
_FEATURES_DIR = _PROJECT_ROOT / "data" / "hdfs" / "features"
_PREDICTIONS_DIR = _TRENDS_DIR / "predictions"

st.set_page_config(
    page_title="TrendBeacon",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)
apply_theme()


def _count_parquet_files(path: Path) -> int:
    return len(list(path.rglob("*.parquet"))) if path.exists() else 0


def main() -> None:
    title = cfg.get("dashboard", {}).get("title", "TrendBeacon")
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    render_navbar("home")

    render_hero(
        kicker="Big Data Intelligence",
        title=title,
        subtitle=(
            "A research-grade signal radar for catching emerging conversations before they turn fully viral. "
            "Built on multi-source ingestion, rolling frequency windows, growth-rate signals, anomaly scoring, "
            "and forecast-driven exploration."
        ),
        chips=[
            f"Updated {now_str}",
            "Growth Rate Detection",
            "Anomaly Scoring",
            "Forecast Dashboard",
        ],
    )

    st.write("")

    trends_count = _count_parquet_files(_TRENDS_DIR)
    features_count = _count_parquet_files(_FEATURES_DIR)
    preds_count = _count_parquet_files(_PREDICTIONS_DIR)
    demo_mode = trends_count == 0 and features_count == 0 and preds_count == 0
    if demo_mode:
        trends_count = len(load_demo_trends())
        features_count = len(load_demo_features())
        preds_count = len(load_demo_predictions())

    if demo_mode:
        st.info(
            "Showing bundled demo data because deployed environments do not include generated parquet outputs by default."
        )

    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("Trend Rows" if demo_mode else "Trend Files", trends_count)
    with m2:
        st.metric("Feature Rows" if demo_mode else "Feature Files", features_count)
    with m3:
        st.metric("Prediction Rows" if demo_mode else "Prediction Files", preds_count)

    st.write("")
    col1, col2 = st.columns(2, gap="large")
    with col1:
        render_info_card(
            title="How TrendBeacon Thinks",
            body=(
                "Emerging topics are identified through sustained positive growth in rolling windows, while viral "
                "topics are captured through sharp short-horizon spikes. The core early-warning signals are growth rate, "
                "z-score intensity, velocity, acceleration, and source diversity."
            ),
            label="Method",
        )
    with col2:
        render_info_card(
            title="Navigate the Radar",
            body=(
                "Use the top navigation to move between `Home`, `Current Trends`, and `Predicted Trends`. "
                "This landing page is your project overview and status board."
            ),
            label="Navigation",
        )


if __name__ == "__main__":
    main()
