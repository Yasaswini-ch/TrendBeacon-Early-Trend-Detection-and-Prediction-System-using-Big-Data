"""
user_guidance.py — Plain-English explanations for non-technical users.

This module provides human-readable explanations for trend classifications,
metrics, and alerts so anyone can understand what the data means.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st


# ---------------------------------------------------------------------------
# Trend Label Explanations
# ---------------------------------------------------------------------------

TREND_EXPLANATIONS: dict[str, dict[str, str]] = {
    "viral": {
        "title": "🔥 Viral",
        "short": "Exploding right now — sharp spike in mentions",
        "full": (
            "This topic is experiencing a sudden, explosive surge in attention. "
            "Think of it as a 'breaking news' moment — the conversation is spreading rapidly "
            "across multiple sources within a short time window. These trends often peak quickly "
            "but can also fade fast once the initial excitement passes."
        ),
        "action": "Monitor closely — these trends move fast and may indicate breaking news or major events.",
    },
    "emerging": {
        "title": "📈 Emerging",
        "short": "Gaining momentum — steady growth building up",
        "full": (
            "This topic is showing consistent, sustained growth over time. Unlike viral spikes, "
            "emerging trends build gradually and often indicate conversations that are gaining "
            "real traction. These are the topics to watch for early signals of what might "
            "become the next big thing."
        ),
        "action": "Good candidates for early intervention or content creation — they have staying power.",
    },
    "stable": {
        "title": "➡️ Stable",
        "short": "Consistent attention — steady baseline activity",
        "full": (
            "This topic maintains a relatively constant level of discussion. It's neither "
            "growing nor declining significantly. Stable trends often represent evergreen topics "
            "or ongoing conversations that have found their natural audience size."
        ),
        "action": "Reliable baseline topics — useful for comparison but unlikely to surprise.",
    },
    "declining": {
        "title": "📉 Declining",
        "short": "Losing steam — conversation is fading",
        "full": (
            "This topic is seeing less discussion over time. The peak interest has passed, "
            "and the conversation is winding down. Declining trends can still be relevant "
            "but are moving toward the end of their lifecycle."
        ),
        "action": "Consider wrapping up coverage or pivoting to fresher angles.",
    },
}


# ---------------------------------------------------------------------------
# Metric Explanations
# ---------------------------------------------------------------------------

METRIC_EXPLANATIONS: dict[str, dict[str, str]] = {
    "frequency": {
        "label": "Frequency",
        "what": "How many times this keyword appeared in the time window",
        "why": "Higher frequency = more people talking about it right now",
    },
    "growth_rate": {
        "label": "Growth Rate",
        "what": "Percentage change in mentions compared to the previous period",
        "why": "Positive growth = gaining momentum | Negative growth = losing interest",
    },
    "z_score": {
        "label": "Z-Score (Anomaly Score)",
        "what": "How unusual this spike is compared to normal activity",
        "why": "Z-score > 2 = statistically unusual spike | Z-score > 3 = rare event",
    },
    "confidence": {
        "label": "Confidence",
        "what": "How certain the model is about its prediction",
        "why": "Higher confidence = more reliable forecast (but never 100% guaranteed)",
    },
    "trend_label": {
        "label": "Trend Class",
        "what": "Classification of the trend's behavior pattern",
        "why": "Helps you quickly identify which topics are viral, emerging, stable, or declining",
    },
}


# ---------------------------------------------------------------------------
# Components
# ---------------------------------------------------------------------------

def render_trend_explanation_card(label: str) -> None:
    """
    Render an info card explaining a specific trend label.

    Args:
        label: The trend label to explain (viral, emerging, stable, declining)
    """
    label_lower = label.lower().strip()
    if label_lower not in TREND_EXPLANATIONS:
        return

    info = TREND_EXPLANATIONS[label_lower]
    st.info(
        f"**{info['title']}** — {info['short']}\n\n"
        f"{info['full']}\n\n"
        f"💡 **What to do:** {info['action']}"
    )


def render_metric_help_popover(metric_key: str) -> str:
    """
    Return a help tooltip string for a metric.

    Args:
        metric_key: Key into METRIC_EXPLANATIONS

    Returns:
        Formatted help text for use in tooltips
    """
    if metric_key not in METRIC_EXPLANATIONS:
        return ""

    info = METRIC_EXPLANATIONS[metric_key]
    return f"**{info['label']}**: {info['what']}. {info['why']}"


def render_all_metric_explanations() -> None:
    """Render a collapsible section with all metric explanations."""
    with st.expander("📖 What do these metrics mean?", expanded=False):
        cols = st.columns(2)
        for idx, (key, info) in enumerate(METRIC_EXPLANATIONS.items()):
            with cols[idx % 2]:
                st.markdown(f"**{info['label']}**")
                st.caption(f"{info['what']}")
                st.write(f"*{info['why']}*")


def render_trend_legend() -> None:
    """Render a visual legend of all trend labels."""
    with st.expander("🏷️ Trend Labels Explained", expanded=False):
        for label, info in TREND_EXPLANATIONS.items():
            st.markdown(f"### {info['title']}")
            st.write(info['short'])
            st.caption(info['full'])


def generate_plain_english_summary(df: pd.DataFrame, keyword: str | None = None) -> str:
    """
    Generate a plain-English summary of the trend data.

    Args:
        df: DataFrame with trend data
        keyword: Optional specific keyword to summarize

    Returns:
        Human-readable summary string
    """
    if df.empty:
        return "No data available to summarize."

    if keyword:
        df = df[df["keyword"] == keyword]
        if df.empty:
            return f"No data found for '{keyword}'."

    total_keywords = df["keyword"].nunique() if "keyword" in df.columns else len(df)

    counts = {}
    if "trend_label" in df.columns:
        counts = df["trend_label"].str.lower().value_counts().to_dict()

    viral = counts.get("viral", 0)
    emerging = counts.get("emerging", 0)
    stable = counts.get("stable", 0)
    declining = counts.get("declining", 0)

    anomaly_count = int(df["is_anomaly"].sum()) if "is_anomaly" in df.columns else 0

    parts = []

    if keyword:
        row = df.iloc[0]
        label = str(row.get("trend_label", "unknown")).lower()
        freq = row.get("frequency", 0)
        growth = row.get("growth_rate", 0)

        explanation = TREND_EXPLANATIONS.get(label, {}).get("short", "Unknown trend pattern")

        parts.append(f"**{keyword}** is currently **{TREND_EXPLANATIONS.get(label, {}).get('title', label)}**.")
        parts.append(f"{explanation}")
        parts.append(f"Current frequency: {freq} mentions | Growth: {growth:+.1f}%")

        if row.get("is_anomaly", 0) == 1:
            parts.append(f"⚠️ **Alert**: This is an unusual spike (Z-score: {row.get('z_score', 0):.2f})")
    else:
        parts.append(f"Tracking **{total_keywords} keywords** in this snapshot.")

        if viral > 0:
            parts.append(f"🔥 **{viral} viral** trend{'s' if viral > 1 else ''} — exploding right now")
        if emerging > 0:
            parts.append(f"📈 **{emerging} emerging** trend{'s' if emerging > 1 else ''} — gaining steady momentum")
        if stable > 0:
            parts.append(f"➡️ **{stable} stable** trend{'s' if stable > 1 else ''} — consistent activity")
        if declining > 0:
            parts.append(f"📉 **{declining} declining** trend{'s' if declining > 1 else ''} — losing steam")

        if anomaly_count > 0:
            parts.append(f"⚠️ **{anomaly_count} anomalies** detected — unusual spikes worth investigating")

    return " ".join(parts)


def render_summary_banner(df: pd.DataFrame, keyword: str | None = None) -> None:
    """
    Render a prominent banner with plain-English summary.

    Args:
        df: DataFrame with trend data
        keyword: Optional specific keyword to summarize
    """
    summary = generate_plain_english_summary(df, keyword)
    st.success(summary, icon="💡")


def get_unique_trend_labels(df: pd.DataFrame) -> list[str]:
    """
    Get unique trend labels without duplicates.

    Args:
        df: DataFrame with trend_label column

    Returns:
        Sorted list of unique trend labels with emoji badges
    """
    if "trend_label" not in df.columns:
        return []

    unique_labels = df["trend_label"].dropna().unique()
    unique_labels = [str(l).lower().strip() for l in unique_labels]
    unique_labels = list(dict.fromkeys(unique_labels))  # Preserve order, remove dupes

    result = []
    for label in unique_labels:
        emoji_map = {"viral": "🔥", "emerging": "📈", "stable": "➡️", "declining": "📉"}
        emoji = emoji_map.get(label, "⚡")
        result.append(f"{emoji} {label.capitalize()}")

    return result
