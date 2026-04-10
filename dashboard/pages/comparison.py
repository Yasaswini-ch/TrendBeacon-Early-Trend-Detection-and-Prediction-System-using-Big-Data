"""
Streamlit page: Comparison View.
"""

from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from dashboard.bootstrap_data import ensure_demo_outputs
from dashboard.components.trend_chart import _apply_dark_theme
from dashboard.demo_data import load_demo_trends
from dashboard.theme import apply_theme, render_hero, render_navbar

_TRENDS_DIR = _PROJECT_ROOT / "data" / "hdfs" / "trends"

apply_theme()
ensure_demo_outputs()

@st.cache_data(ttl=60, show_spinner=False)
def _load_trends() -> pd.DataFrame:
    if not _TRENDS_DIR.exists():
        return load_demo_trends()
    parquet_files = list(_TRENDS_DIR.rglob("*.parquet"))
    if not parquet_files:
        return load_demo_trends()
    frames = [pd.read_parquet(p) for p in parquet_files]
    df = pd.concat(frames, ignore_index=True)
    if "timestamp" not in df.columns and "window_start" in df.columns:
        df["timestamp"] = df["window_start"]
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    return df

def plot_comparison(df: pd.DataFrame, kw1: str, kw2: str) -> go.Figure:
    fig = go.Figure()
    
    for kw, color in [(kw1, "#4fd1c5"), (kw2, "#fb7185")]:
        kdf = df[df["keyword"] == kw].sort_values("timestamp")
        if not kdf.empty:
            fig.add_trace(
                go.Scatter(
                    x=kdf["timestamp"],
                    y=kdf["frequency"],
                    mode="lines+markers",
                    name=kw,
                    line=dict(color=color, width=3),
                )
            )
            
    fig.update_layout(
        title=f"Comparison: {kw1} vs {kw2}",
        xaxis_title="Time",
        yaxis_title="Frequency",
        hovermode="x unified",
    )
    return _apply_dark_theme(fig)

def render_comparison_page() -> None:
    render_navbar("comparison")
    render_hero(
        kicker="Analysis Lab",
        title="Comparison View",
        subtitle="Pin two keywords side by side to compare growth curves and identify relative momentum.",
        chips=["Side-by-side Analysis", "Growth Comparison", "Relative Momentum"],
    )
    
    df = _load_trends()
    keywords = sorted(df["keyword"].unique().tolist())
    
    st.markdown("### Select Keywords to Compare")
    c1, c2 = st.columns(2)
    with c1:
        kw1 = st.selectbox("First Keyword", options=keywords, index=0)
    with c2:
        kw2 = st.selectbox("Second Keyword", options=keywords, index=min(1, len(keywords)-1))
        
    if kw1 and kw2:
        fig = plot_comparison(df, kw1, kw2)
        st.plotly_chart(fig, use_container_width=True)
        
        # Metric comparison
        m1, m2 = st.columns(2)
        for kw, col in [(kw1, m1), (kw2, m2)]:
            kdf = df[df["keyword"] == kw]
            latest_freq = kdf["frequency"].iloc[-1] if not kdf.empty else 0
            latest_growth = kdf["growth_rate"].iloc[-1] if "growth_rate" in kdf.columns and not kdf.empty else 0
            with col:
                st.metric(f"{kw} Latest Frequency", f"{latest_freq:,}", delta=f"{latest_growth:+.1f}%")

render_comparison_page()
