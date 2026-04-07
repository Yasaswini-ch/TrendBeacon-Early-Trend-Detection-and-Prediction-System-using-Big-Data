"""
trend_chart.py — Reusable Plotly chart components for the TrendBeacon dashboard.

Each function accepts a pandas DataFrame and returns a plotly Figure that can
be rendered via st.plotly_chart().  All functions are pure (no Streamlit calls)
so they can be unit-tested independently.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


# ---------------------------------------------------------------------------
# Colour palette — shared across charts for visual consistency
# ---------------------------------------------------------------------------
_TREND_COLOURS: dict[str, str] = {
    "viral":     "#fb7185",
    "emerging":  "#f59e0b",
    "stable":    "#94a3b8",
    "declining": "#60a5fa",
}

_BG_COLOUR = "#08111f"
_PAPER_BG = "rgba(11, 20, 35, 0.96)"
_GRID_COLOUR = "rgba(122, 146, 176, 0.15)"


def _apply_dark_theme(fig: go.Figure) -> go.Figure:
    """Apply a consistent dark theme to any plotly figure."""
    fig.update_layout(
        plot_bgcolor=_BG_COLOUR,
        paper_bgcolor=_PAPER_BG,
        font=dict(color="#edf4ff", family="Segoe UI, sans-serif"),
        xaxis=dict(gridcolor=_GRID_COLOUR, linecolor=_GRID_COLOUR, zerolinecolor=_GRID_COLOUR),
        yaxis=dict(gridcolor=_GRID_COLOUR, linecolor=_GRID_COLOUR, zerolinecolor=_GRID_COLOUR),
        legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor=_GRID_COLOUR, borderwidth=1),
        margin=dict(l=50, r=30, t=50, b=50),
    )
    return fig


# ---------------------------------------------------------------------------
# 1. Keyword frequency time-series
# ---------------------------------------------------------------------------

def plot_keyword_frequency_timeseries(
    df: pd.DataFrame,
    keyword: str,
    show_anomalies: bool = True,
) -> go.Figure:
    """
    Return a line chart of keyword mention frequency over time.

    Args:
        df:              Trends DataFrame with at minimum the columns
                         ['keyword', 'timestamp', 'frequency', 'is_anomaly'].
        keyword:         The specific keyword to plot.
        show_anomalies:  When True, overlay red markers on anomaly rows.

    Returns:
        plotly Figure
    """
    kdf = df[df["keyword"] == keyword].copy()
    if kdf.empty:
        fig = go.Figure()
        fig.add_annotation(
            text=f"No data found for keyword: <b>{keyword}</b>",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="#c9d1d9"),
        )
        return _apply_dark_theme(fig)

    kdf = kdf.sort_values("timestamp")

    fig = go.Figure()

    # --- Main frequency line ---
    fig.add_trace(
        go.Scatter(
            x=kdf["timestamp"],
            y=kdf["frequency"],
            mode="lines",
            name="Frequency",
            line=dict(color="#4fd1c5", width=3),
            hovertemplate="<b>%{x}</b><br>Frequency: %{y}<extra></extra>",
        )
    )

    # --- Anomaly overlay ---
    if show_anomalies:
        anomalies = kdf[kdf["is_anomaly"] == 1]
        if not anomalies.empty:
            fig.add_trace(
                go.Scatter(
                    x=anomalies["timestamp"],
                    y=anomalies["frequency"],
                    mode="markers",
                    name="Anomaly",
                    marker=dict(color="#fb7185", size=10, symbol="circle-open", line=dict(width=2)),
                    hovertemplate=(
                        "<b>Anomaly detected</b><br>"
                        "Time: %{x}<br>"
                        "Frequency: %{y}<extra></extra>"
                    ),
                )
            )

    fig.update_layout(
        title=dict(text=f"Frequency Over Time — <i>{keyword}</i>", font=dict(size=16)),
        xaxis_title="Time",
        yaxis_title="Mention Frequency",
        hovermode="x unified",
    )
    return _apply_dark_theme(fig)


# ---------------------------------------------------------------------------
# 2. Growth-rate bar chart (top N keywords)
# ---------------------------------------------------------------------------

def plot_growth_rate_bar(df: pd.DataFrame, top_n: int = 20) -> go.Figure:
    """
    Return a horizontal bar chart of the top N keywords ranked by growth_rate.

    Bars are coloured green for positive growth and red for negative growth.

    Args:
        df:     Trends DataFrame.  Must contain ['keyword', 'growth_rate'].
                Column name 'growth_rate_24h' is also accepted as a fallback.
        top_n:  Number of keywords to display.

    Returns:
        plotly Figure
    """
    # Accept both naming conventions
    rate_col = "growth_rate" if "growth_rate" in df.columns else "growth_rate_24h"
    if rate_col not in df.columns:
        fig = go.Figure()
        fig.add_annotation(
            text="Column 'growth_rate' not found in dataset.",
            xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False,
            font=dict(size=14, color="#c9d1d9"),
        )
        return _apply_dark_theme(fig)

    plot_df = (
        df[["keyword", rate_col]]
        .dropna()
        .sort_values(rate_col, ascending=False)
        .head(top_n)
        .sort_values(rate_col, ascending=True)   # ascending so highest is at top
    )

    colours = ["#4fd1c5" if v >= 0 else "#fb7185" for v in plot_df[rate_col]]

    fig = go.Figure(
        go.Bar(
            x=plot_df[rate_col],
            y=plot_df["keyword"],
            orientation="h",
            marker_color=colours,
            text=[f"{v:+.1f}%" for v in plot_df[rate_col]],
            textposition="outside",
            hovertemplate="<b>%{y}</b><br>Growth rate: %{x:.2f}%<extra></extra>",
        )
    )

    fig.update_layout(
        title=dict(text=f"Top {top_n} Keywords by Growth Rate", font=dict(size=16)),
        xaxis_title="Growth Rate (%)",
        yaxis_title="",
        showlegend=False,
        height=max(350, top_n * 28),
    )
    return _apply_dark_theme(fig)


# ---------------------------------------------------------------------------
# 3. Trend-label distribution (donut)
# ---------------------------------------------------------------------------

def plot_trend_distribution(df: pd.DataFrame) -> go.Figure:
    """
    Return a donut chart showing how many keywords fall into each trend_label.

    Args:
        df:  Trends DataFrame containing at least the 'trend_label' column.

    Returns:
        plotly Figure
    """
    if "trend_label" not in df.columns or df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No trend label data available.",
            xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False,
            font=dict(size=14, color="#c9d1d9"),
        )
        return _apply_dark_theme(fig)

    counts = df["trend_label"].value_counts().reset_index()
    counts.columns = ["trend_label", "count"]

    colours = [_TREND_COLOURS.get(label.lower(), "#95a5a6") for label in counts["trend_label"]]

    fig = go.Figure(
        go.Pie(
            labels=counts["trend_label"],
            values=counts["count"],
            hole=0.45,
            marker=dict(colors=colours, line=dict(color=_BG_COLOUR, width=2)),
            textinfo="label+percent",
            hovertemplate="<b>%{label}</b><br>Count: %{value}<br>Share: %{percent}<extra></extra>",
        )
    )

    fig.update_layout(
        title=dict(text="Trend Label Distribution", font=dict(size=16)),
        showlegend=True,
    )
    return _apply_dark_theme(fig)


# ---------------------------------------------------------------------------
# 4. Prophet forecast chart
# ---------------------------------------------------------------------------

def plot_forecast(forecast_df: pd.DataFrame, keyword: str) -> go.Figure:
    """
    Return a time-series chart combining historical observations and the Prophet
    forecast for a given keyword.

    Expected columns in forecast_df:
        - keyword          : str
        - predicted_at     : datetime  (timestamp the forecast was generated)
        - forecast_period  : str       (e.g. "24h", "7d")
        - predicted_freq   : float     (point forecast)
        - lower_bound      : float
        - upper_bound      : float
        - trend_class      : str
        - confidence       : float     (0–1)

    If a 'frequency' column is present it is treated as historical actuals.

    Args:
        forecast_df:  Predictions DataFrame (all keywords, will be filtered).
        keyword:      The keyword to visualise.

    Returns:
        plotly Figure
    """
    kdf = forecast_df[forecast_df["keyword"] == keyword].copy()
    if kdf.empty:
        fig = go.Figure()
        fig.add_annotation(
            text=f"No forecast data found for: <b>{keyword}</b>",
            xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="#c9d1d9"),
        )
        return _apply_dark_theme(fig)

    kdf = kdf.sort_values("predicted_at")

    fig = go.Figure()

    # --- Historical actuals (if present) ---
    if "frequency" in kdf.columns:
        hist = kdf.dropna(subset=["frequency"])
        if not hist.empty:
            fig.add_trace(
                go.Scatter(
                    x=hist["predicted_at"],
                    y=hist["frequency"],
                    mode="lines",
                    name="Actual",
                    line=dict(color="#58a6ff", width=2),
                    hovertemplate="<b>Actual</b><br>%{x}<br>Freq: %{y}<extra></extra>",
                )
            )

    # --- Confidence interval band ---
    x_band = list(kdf["predicted_at"]) + list(kdf["predicted_at"])[::-1]
    y_band = list(kdf["upper_bound"]) + list(kdf["lower_bound"])[::-1]
    fig.add_trace(
        go.Scatter(
            x=x_band,
            y=y_band,
            fill="toself",
            fillcolor="rgba(96,165,250,0.14)",
            line=dict(color="rgba(0,0,0,0)"),
            name="Confidence Interval",
            hoverinfo="skip",
        )
    )

    # --- Forecast line ---
    fig.add_trace(
        go.Scatter(
            x=kdf["predicted_at"],
            y=kdf["predicted_freq"],
            mode="lines",
            name="Forecast",
            line=dict(color="#f59e0b", width=3, dash="dash"),
            hovertemplate=(
                "<b>Forecast</b><br>%{x}<br>"
                "Predicted: %{y:.1f}<extra></extra>"
            ),
        )
    )

    # --- Anomaly markers (if column present) ---
    if "is_anomaly" in kdf.columns:
        anomalies = kdf[kdf["is_anomaly"] == 1]
        if not anomalies.empty:
            fig.add_trace(
                go.Scatter(
                    x=anomalies["predicted_at"],
                    y=anomalies["predicted_freq"],
                    mode="markers",
                    name="Anomaly",
                    marker=dict(color="#fb7185", size=10, symbol="x", line=dict(width=2)),
                    hovertemplate="<b>Anomaly</b><br>%{x}<extra></extra>",
                )
            )

    # Confidence annotation (use first row's value if available)
    conf_val = kdf["confidence"].iloc[0] if "confidence" in kdf.columns else None
    subtitle = f" — Confidence: {conf_val:.0%}" if conf_val is not None else ""

    fig.update_layout(
        title=dict(
            text=f"Prophet Forecast — <i>{keyword}</i>{subtitle}",
            font=dict(size=16),
        ),
        xaxis_title="Time",
        yaxis_title="Predicted Frequency",
        hovermode="x unified",
    )
    return _apply_dark_theme(fig)


# ---------------------------------------------------------------------------
# 5. Bubble chart — growth rate vs frequency
# ---------------------------------------------------------------------------

def plot_top_trends_bubble(df: pd.DataFrame, top_n: int = 30) -> go.Figure:
    """
    Return a bubble chart where:
        X-axis  = growth_rate
        Y-axis  = frequency
        Size    = z_score (absolute value, so negatives still render)
        Colour  = trend_label

    Args:
        df:     Trends DataFrame with columns:
                ['keyword', 'growth_rate', 'frequency', 'z_score', 'trend_label'].
        top_n:  Limit chart to top N keywords by frequency.

    Returns:
        plotly Figure
    """
    required = {"keyword", "growth_rate", "frequency", "z_score", "trend_label"}
    missing = required - set(df.columns)
    if missing:
        fig = go.Figure()
        fig.add_annotation(
            text=f"Missing columns for bubble chart: {', '.join(missing)}",
            xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False,
            font=dict(size=13, color="#c9d1d9"),
        )
        return _apply_dark_theme(fig)

    # Accept growth_rate_24h as alternative column name
    if "growth_rate" not in df.columns and "growth_rate_24h" in df.columns:
        df = df.rename(columns={"growth_rate_24h": "growth_rate"})

    plot_df = (
        df[list(required)]
        .dropna()
        .sort_values("frequency", ascending=False)
        .head(top_n)
        .copy()
    )
    plot_df["bubble_size"] = plot_df["z_score"].abs().clip(lower=0.5) * 5 + 5

    colour_map = {
        label: _TREND_COLOURS.get(label.lower(), "#95a5a6")
        for label in plot_df["trend_label"].unique()
    }

    fig = px.scatter(
        plot_df,
        x="growth_rate",
        y="frequency",
        size="bubble_size",
        color="trend_label",
        color_discrete_map=colour_map,
        hover_name="keyword",
        hover_data={"growth_rate": ":.2f", "frequency": True, "z_score": ":.2f", "bubble_size": False},
        labels={
            "growth_rate": "Growth Rate (%)",
            "frequency":   "Mention Frequency",
            "trend_label": "Trend Label",
        },
        title=f"Top {top_n} Trending Keywords — Growth vs Frequency",
        size_max=60,
    )

    fig.update_traces(
        marker=dict(line=dict(width=1, color=_BG_COLOUR)),
        selector=dict(mode="markers"),
    )
    fig.update_layout(title_font_size=16)
    return _apply_dark_theme(fig)
