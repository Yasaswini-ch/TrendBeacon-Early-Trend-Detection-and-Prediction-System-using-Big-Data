"""
Streamlit page: Predicted Trends.
"""

from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pandas as pd
import streamlit as st

from dashboard.components.trend_chart import plot_forecast, plot_top_trends_bubble
from dashboard.theme import apply_theme, render_hero, render_info_card, render_navbar

_PREDICTIONS_DIR = _PROJECT_ROOT / "data" / "hdfs" / "trends" / "predictions"
_FEATURES_DIR = _PROJECT_ROOT / "data" / "hdfs" / "features"

apply_theme()


@st.cache_data(ttl=60, show_spinner=False)
def _load_predictions() -> pd.DataFrame | None:
    """Load forecast parquet files."""
    if not _PREDICTIONS_DIR.exists():
        return None
    files = list(_PREDICTIONS_DIR.rglob("*.parquet"))
    if not files:
        return None

    frames: list[pd.DataFrame] = []
    for path in files:
        try:
            frames.append(pd.read_parquet(path))
        except Exception as exc:  # noqa: BLE001
            st.warning(f"Could not read {path.name}: {exc}")

    if not frames:
        return None

    df = pd.concat(frames, ignore_index=True)
    if "predicted_at" in df.columns:
        df["predicted_at"] = pd.to_datetime(df["predicted_at"], utc=True, errors="coerce")
    if "trend_class" in df.columns:
        df["trend_class"] = df["trend_class"].astype(str).str.lower().str.strip()
    return df


@st.cache_data(ttl=60, show_spinner=False)
def _load_features() -> pd.DataFrame | None:
    """Load feature parquet files."""
    if not _FEATURES_DIR.exists():
        return None
    files = list(_FEATURES_DIR.rglob("*.parquet"))
    if not files:
        return None

    frames: list[pd.DataFrame] = []
    for path in files:
        try:
            frames.append(pd.read_parquet(path))
        except Exception as exc:  # noqa: BLE001
            st.warning(f"Could not read {path.name}: {exc}")

    if not frames:
        return None

    df = pd.concat(frames, ignore_index=True)
    if "growth_rate" not in df.columns and "growth_rate_24h" in df.columns:
        df["growth_rate"] = df["growth_rate_24h"]
    if "frequency" not in df.columns and "freq_24h" in df.columns:
        df["frequency"] = df["freq_24h"]
    if "z_score" not in df.columns and "z_score_24h" in df.columns:
        df["z_score"] = df["z_score_24h"]
    return df


def _build_summary_table(pred_df: pd.DataFrame) -> pd.DataFrame:
    """Build a ranked per-keyword forecast summary."""
    if pred_df.empty:
        return pd.DataFrame()

    latest = (
        pred_df.sort_values("predicted_at", ascending=False)
        .groupby("keyword", as_index=False)
        .first()
    )

    summary = latest[["keyword"]].copy()
    summary["current_freq"] = pd.to_numeric(latest.get("lower_bound", pd.NA), errors="coerce")
    summary["predicted_freq"] = pd.to_numeric(latest.get("predicted_freq", pd.NA), errors="coerce")
    summary["trend_class"] = latest.get("trend_class", pd.NA)
    summary["confidence"] = pd.to_numeric(latest.get("confidence", pd.NA), errors="coerce")

    denom = summary["current_freq"].mask(summary["current_freq"] == 0)
    summary["change_numeric"] = ((summary["predicted_freq"] - summary["current_freq"]) / denom) * 100
    summary["change_numeric"] = pd.to_numeric(summary["change_numeric"], errors="coerce")

    summary["change_%"] = summary["change_numeric"].map(
        lambda v: f"{float(v):+.1f}%" if pd.notna(v) else "—"
    )
    summary["confidence"] = summary["confidence"].map(
        lambda v: f"{float(v):.0%}" if pd.notna(v) else "—"
    )

    return summary.sort_values("predicted_freq", ascending=False, na_position="last").reset_index(drop=True)


def render_predicted_trends_page() -> None:
    """Render the forecast-focused dashboard page."""
    render_navbar("predicted_trends")
    render_hero(
        kicker="Forecast Studio",
        title="Predicted Trends",
        subtitle=(
            "Explore the forward-looking layer of the trend system. This page focuses on topics likely "
            "to gain momentum next, combining modeled trajectory, confidence bands, and current signal strength."
        ),
        chips=["24h Forecast", "Future Momentum", "Confidence Bands", "Priority Topics"],
    )
    st.write("")

    with st.spinner("Loading forecast data..."):
        pred_df = _load_predictions()
        feat_df = _load_features()

    if pred_df is None:
        st.info("No prediction data was found in `data/hdfs/trends/predictions/`.")
        return

    st.markdown("### Forecast Controls")
    c1, c2, c3 = st.columns([1, 1.1, 1], gap="large")
    with c1:
        available_periods = (
            sorted(pred_df["forecast_period"].dropna().unique().tolist())
            if "forecast_period" in pred_df.columns
            else ["24h"]
        )
        selected_period = st.selectbox("Forecast Period", options=available_periods, index=0)

    period_df = pred_df[pred_df["forecast_period"] == selected_period].copy()
    keyword_list = sorted(period_df["keyword"].dropna().unique().tolist())
    with c2:
        selected_keyword = st.selectbox(
            "Focus Keyword",
            options=keyword_list if keyword_list else ["No keywords available"],
            index=0,
        )
    with c3:
        min_confidence = st.slider("Minimum Confidence", min_value=0.0, max_value=1.0, value=0.0, step=0.05)

    st.write("")

    display_df = period_df.copy()
    if "confidence" in display_df.columns:
        conf_numeric = pd.to_numeric(display_df["confidence"], errors="coerce")
        display_df = display_df[conf_numeric >= min_confidence]

    if display_df.empty:
        st.warning("No forecast rows match the current filter settings.")
        return

    cards = st.columns(2, gap="large")
    with cards[0]:
        render_info_card(
            title="How to use forecasts",
            body=(
                "The forecast view is most useful for comparing which topics already have strong current signal "
                "and which ones are projected to climb further. Confidence bands help interpret certainty, not guarantee outcome."
            ),
            label="Guide",
        )
    with cards[1]:
        render_info_card(
            title="Prediction lens",
            body=(
                "Pair this page with the current trends board. A topic that is both emerging now and projected upward "
                "is a strong candidate for early-trend reporting, monitoring, or intervention."
            ),
            label="Strategy",
        )

    st.write("")
    st.markdown("### Forecast Priority Table")
    summary = _build_summary_table(display_df)
    if not summary.empty:
        view = summary.rename(
            columns={
                "keyword": "Keyword",
                "current_freq": "Current Freq",
                "predicted_freq": "Predicted Freq",
                "trend_class": "Trend Class",
                "confidence": "Confidence",
                "change_%": "Change %",
            }
        )[
            ["Keyword", "Current Freq", "Predicted Freq", "Change %", "Trend Class", "Confidence"]
        ]
        st.dataframe(view, use_container_width=True, hide_index=True)
    else:
        st.info("No summary data available.")

    st.write("")
    st.markdown("### Growth vs Frequency Map")
    required_bubble_cols = {"keyword", "growth_rate", "frequency", "z_score", "trend_label"}
    if feat_df is not None and required_bubble_cols.issubset(feat_df.columns):
        top_n_bubble = st.slider("Keywords in feature map", min_value=10, max_value=100, value=30, step=5)
        st.plotly_chart(plot_top_trends_bubble(feat_df, top_n=top_n_bubble), use_container_width=True)
    else:
        st.info("Feature map requires fully populated feature columns.")

    st.write("")
    if selected_keyword and selected_keyword != "No keywords available":
        st.markdown(f"### Forecast Track: {selected_keyword}")
        st.plotly_chart(plot_forecast(display_df, selected_keyword), use_container_width=True)

        latest_row = (
            display_df[display_df["keyword"] == selected_keyword]
            .sort_values("predicted_at", ascending=False)
            .iloc[0]
        )
        conf = latest_row.get("confidence")
        conf_str = f"{float(conf):.0%}" if pd.notna(conf) else "N/A"
        trend_str = str(latest_row.get("trend_class", "unknown")).capitalize()
        st.caption(f"Trend class: {trend_str} | Confidence: {conf_str} | Window: {selected_period}")

    st.write("")
    st.markdown("### Classification Snapshot")
    class_df = (
        display_df[["keyword", "trend_class", "confidence", "predicted_freq"]]
        .sort_values("predicted_freq", ascending=False)
        .groupby("keyword", as_index=False)
        .first()
    )
    class_df["confidence"] = pd.to_numeric(class_df["confidence"], errors="coerce").map(
        lambda v: f"{float(v):.0%}" if pd.notna(v) else "—"
    )
    st.dataframe(
        class_df.rename(
            columns={
                "keyword": "Keyword",
                "trend_class": "Trend Class",
                "confidence": "Confidence",
                "predicted_freq": "Predicted Freq",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )


render_predicted_trends_page()
