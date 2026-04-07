"""
trend_table.py — Reusable Streamlit table and metric components for the
TrendBeacon dashboard.

All functions call Streamlit directly (unlike trend_chart.py which returns
figures).  Import and call inside a rendered Streamlit page.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st


# ---------------------------------------------------------------------------
# Emoji badges and highlight colours per trend label
# ---------------------------------------------------------------------------
_LABEL_EMOJI: dict[str, str] = {
    "viral":     "🔥 Viral",
    "emerging":  "📈 Emerging",
    "stable":    "➡️ Stable",
    "declining": "📉 Declining",
}

_LABEL_BG: dict[str, str] = {
    "viral":     "background-color: rgba(251,113,133,0.18); color: #fecdd3;",
    "emerging":  "background-color: rgba(245,158,11,0.18); color: #fde68a;",
    "stable":    "background-color: rgba(148,163,184,0.16); color: #e2e8f0;",
    "declining": "background-color: rgba(96,165,250,0.18); color: #dbeafe;",
}


def _format_label(raw: str) -> str:
    """Return the emoji-decorated label string for a trend_label value."""
    return _LABEL_EMOJI.get(str(raw).lower(), raw)


def _highlight_row(row: pd.Series) -> list[str]:
    """
    pandas Styler row-level callable.
    Returns a list of CSS strings — one per cell — based on trend_label.
    """
    label = str(row.get("trend_label_display", "")).lower()
    # Extract the plain label from the emoji string for lookup
    for key in _LABEL_BG:
        if key in label:
            style = _LABEL_BG[key]
            return [style] * len(row)
    return [""] * len(row)


# ---------------------------------------------------------------------------
# 1. Trending topics table
# ---------------------------------------------------------------------------

def render_trending_topics_table(df: pd.DataFrame, top_n: int = 20) -> None:
    """
    Render a styled Streamlit dataframe of the top trending keywords.

    Displayed columns (in order):
        keyword | trend_label (emoji) | frequency | growth_rate (%) |
        z_score | is_anomaly

    Colour coding:
        viral    → red tint
        emerging → orange tint
        stable   → grey tint
        declining → blue tint

    Args:
        df:     Trends DataFrame.  Expected columns:
                ['keyword', 'trend_label', 'frequency', 'growth_rate',
                 'z_score', 'is_anomaly'].
        top_n:  Maximum number of rows to display.
    """
    if df.empty:
        st.info("No trend data available to display.")
        return

    # --- Build display dataframe ---
    display_cols = {
        "keyword":      "Keyword",
        "trend_label":  "Trend",
        "frequency":    "Frequency",
        "growth_rate":  "Growth Rate (%)",
        "z_score":      "Z-Score",
        "is_anomaly":   "Anomaly",
    }

    available = [c for c in display_cols if c in df.columns]
    view = df[available].copy().head(top_n)

    # Add emoji badges to trend label
    if "trend_label" in view.columns:
        view["trend_label_display"] = view["trend_label"].apply(_format_label)
        view = view.drop(columns=["trend_label"])
        view = view.rename(columns={"trend_label_display": "trend_label"})

    # Rename columns to human-friendly headers
    rename_map = {c: display_cols[c] for c in available if c != "trend_label"}
    rename_map["trend_label"] = "Trend"
    view = view.rename(columns=rename_map)

    # Format numeric columns
    if "Growth Rate (%)" in view.columns:
        view["Growth Rate (%)"] = view["Growth Rate (%)"].map(lambda v: f"{v:+.1f}%" if pd.notna(v) else "—")
    if "Z-Score" in view.columns:
        view["Z-Score"] = view["Z-Score"].map(lambda v: f"{v:.2f}" if pd.notna(v) else "—")
    if "Anomaly" in view.columns:
        view["Anomaly"] = view["Anomaly"].map(lambda v: "⚠️ Yes" if v == 1 else "No")

    # --- Apply row styling via pandas Styler ---
    def _row_style(row: pd.Series) -> list[str]:
        label_val = str(row.get("Trend", "")).lower()
        for key, css in _LABEL_BG.items():
            if key in label_val:
                return [css] * len(row)
        return [""] * len(row)

    styled = view.style.apply(_row_style, axis=1)

    st.dataframe(styled, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# 2. Metric cards
# ---------------------------------------------------------------------------

def render_metric_cards(df: pd.DataFrame) -> None:
    """
    Render four st.metric cards summarising the current trends snapshot.

    Cards displayed:
        1. Total Keywords Tracked
        2. Viral Trends Detected
        3. Emerging Trends
        4. Anomalies Detected

    Args:
        df:  Trends DataFrame with 'trend_label' and 'is_anomaly' columns.
    """
    total_keywords = df["keyword"].nunique() if "keyword" in df.columns else len(df)

    viral_count = (
        int((df["trend_label"].str.lower() == "viral").sum())
        if "trend_label" in df.columns
        else 0
    )
    emerging_count = (
        int((df["trend_label"].str.lower() == "emerging").sum())
        if "trend_label" in df.columns
        else 0
    )
    anomaly_count = (
        int(df["is_anomaly"].sum())
        if "is_anomaly" in df.columns
        else 0
    )

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            label="Total Keywords Tracked",
            value=f"{total_keywords:,}",
        )
    with col2:
        st.metric(
            label="🔥 Viral Trends",
            value=f"{viral_count:,}",
            delta=None,
        )
    with col3:
        st.metric(
            label="📈 Emerging Trends",
            value=f"{emerging_count:,}",
        )
    with col4:
        st.metric(
            label="⚠️ Anomalies Detected",
            value=f"{anomaly_count:,}",
            delta_color="inverse",
        )


# ---------------------------------------------------------------------------
# 3. Anomaly alert boxes
# ---------------------------------------------------------------------------

def render_anomaly_alerts(df: pd.DataFrame, max_alerts: int = 5) -> None:
    """
    Render st.warning boxes for the top anomalous keywords, ranked by z_score.

    Each alert shows:
        - Keyword name
        - Z-score
        - Growth rate
        - Trend direction (trend_label)

    Args:
        df:          Trends DataFrame.  Should contain 'is_anomaly', 'keyword',
                     'z_score', 'growth_rate', 'trend_label'.
        max_alerts:  Maximum number of alert boxes to render.
    """
    if "is_anomaly" not in df.columns:
        st.info("No anomaly information available in the current dataset.")
        return

    anomalies = df[df["is_anomaly"] == 1].copy()
    if anomalies.empty:
        st.success("No anomalies detected in the current data window.")
        return

    # Rank by z_score descending (highest spike first)
    if "z_score" in anomalies.columns:
        anomalies = anomalies.sort_values("z_score", ascending=False)

    anomalies = anomalies.head(max_alerts)

    st.markdown("### ⚠️ Anomaly Alerts")
    for _, row in anomalies.iterrows():
        keyword      = row.get("keyword", "Unknown")
        z_score      = row.get("z_score", float("nan"))
        growth_rate  = row.get("growth_rate", float("nan"))
        trend_label  = str(row.get("trend_label", "unknown")).capitalize()
        trend_emoji  = {"viral": "🔥", "emerging": "📈", "stable": "➡️", "declining": "📉"}.get(
            str(row.get("trend_label", "")).lower(), "⚡"
        )

        z_str = f"{z_score:.2f}" if pd.notna(z_score) else "N/A"
        g_str = f"{growth_rate:+.1f}%" if pd.notna(growth_rate) else "N/A"

        st.warning(
            f"{trend_emoji} **{keyword}** — "
            f"Z-Score: `{z_str}` | "
            f"Growth: `{g_str}` | "
            f"Trend: **{trend_label}**"
        )
