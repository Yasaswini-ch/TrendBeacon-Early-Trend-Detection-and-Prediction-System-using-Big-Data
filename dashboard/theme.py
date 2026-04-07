"""
Shared visual theme helpers for the TrendBeacon Streamlit dashboard.
"""

from __future__ import annotations

import streamlit as st


def apply_theme() -> None:
    """Inject shared CSS for a polished dashboard look."""
    st.markdown(
        """
        <style>
        :root {
            --bg: #08111f;
            --panel: rgba(10, 18, 33, 0.88);
            --panel-strong: rgba(16, 26, 44, 0.95);
            --border: rgba(120, 152, 198, 0.18);
            --text: #edf4ff;
            --muted: #91a1bd;
            --cyan: #4fd1c5;
            --teal: #0ea5a6;
            --amber: #f59e0b;
            --rose: #fb7185;
            --blue: #60a5fa;
        }

        .stApp {
            background:
                radial-gradient(circle at 12% 18%, rgba(79, 209, 197, 0.12), transparent 28%),
                radial-gradient(circle at 88% 10%, rgba(96, 165, 250, 0.14), transparent 24%),
                radial-gradient(circle at 50% 100%, rgba(245, 158, 11, 0.08), transparent 25%),
                linear-gradient(180deg, #07101d 0%, #091322 54%, #0c1627 100%);
            color: var(--text);
        }

        [data-testid="stHeader"] {
            background: rgba(8, 17, 31, 0.7);
        }

        section[data-testid="stSidebar"] {
            display: none !important;
        }

        [data-testid="collapsedControl"] {
            display: none !important;
        }

        div[data-testid="metric-container"] {
            background:
                linear-gradient(180deg, rgba(15, 25, 42, 0.98), rgba(9, 16, 29, 0.96));
            border: 1px solid var(--border);
            border-radius: 18px;
            padding: 1rem 1.05rem;
            box-shadow: 0 18px 40px rgba(0, 0, 0, 0.22);
        }

        div[data-testid="metric-container"] label {
            color: var(--muted) !important;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            font-size: 0.76rem;
        }

        div[data-testid="metric-container"] [data-testid="stMetricValue"] {
            color: var(--text) !important;
        }

        .et-hero {
            padding: 1.5rem 1.6rem;
            border-radius: 24px;
            border: 1px solid var(--border);
            background:
                radial-gradient(circle at top right, rgba(79, 209, 197, 0.2), transparent 30%),
                radial-gradient(circle at bottom left, rgba(96, 165, 250, 0.18), transparent 32%),
                linear-gradient(135deg, rgba(12, 24, 42, 0.96), rgba(7, 14, 27, 0.98));
            box-shadow: 0 24px 70px rgba(0, 0, 0, 0.28);
            overflow: hidden;
        }

        .et-kicker {
            display: inline-block;
            font-size: 0.73rem;
            letter-spacing: 0.18em;
            text-transform: uppercase;
            color: #d7fffa;
            background: rgba(79, 209, 197, 0.14);
            border: 1px solid rgba(79, 209, 197, 0.3);
            border-radius: 999px;
            padding: 0.3rem 0.7rem;
            margin-bottom: 0.8rem;
        }

        .et-title {
            font-size: 2.4rem;
            line-height: 1.05;
            margin: 0 0 0.4rem 0;
            color: var(--text);
            font-weight: 800;
        }

        .et-subtitle {
            font-size: 1rem;
            line-height: 1.6;
            color: var(--muted);
            margin-bottom: 0.9rem;
            max-width: 60rem;
        }

        .et-chip-row {
            display: flex;
            gap: 0.7rem;
            flex-wrap: wrap;
            margin-top: 1rem;
        }

        .et-chip {
            border-radius: 999px;
            border: 1px solid var(--border);
            background: rgba(255,255,255,0.04);
            color: var(--text);
            padding: 0.42rem 0.82rem;
            font-size: 0.85rem;
        }

        .et-card {
            padding: 1.15rem 1.2rem;
            border-radius: 20px;
            border: 1px solid var(--border);
            background:
                linear-gradient(180deg, rgba(17, 28, 48, 0.96), rgba(10, 17, 30, 0.98));
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.22);
        }

        .et-card h3 {
            margin-top: 0;
            color: var(--text);
        }

        .et-muted {
            color: var(--muted);
        }

        .et-section-label {
            color: #c9fff8;
            text-transform: uppercase;
            letter-spacing: 0.16em;
            font-size: 0.72rem;
            margin-bottom: 0.45rem;
        }

        .stDataFrame, [data-testid="stDataFrame"] {
            border-radius: 18px !important;
            overflow: hidden;
            border: 1px solid var(--border);
            background: rgba(10, 18, 33, 0.96);
        }

        div.stAlert {
            border-radius: 18px;
            border: 1px solid var(--border);
        }

        .js-plotly-plot {
            border-radius: 18px;
            overflow: hidden;
        }

        .block-container {
            padding-top: 1.2rem;
            padding-bottom: 2.5rem;
        }

        .et-nav {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 1rem;
            margin-bottom: 0.8rem;
            padding: 0.85rem 1rem;
            border-radius: 18px;
            border: 1px solid var(--border);
            background:
                linear-gradient(180deg, rgba(14, 24, 42, 0.95), rgba(8, 15, 27, 0.98));
            box-shadow: 0 20px 45px rgba(0, 0, 0, 0.18);
        }

        .et-nav-brand {
            font-size: 1rem;
            font-weight: 800;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--text);
        }

        .et-nav-note {
            color: var(--muted);
            font-size: 0.84rem;
        }

        .et-nav-links {
            display: flex;
            gap: 0.75rem;
            margin-bottom: 1.15rem;
            flex-wrap: wrap;
        }

        .et-nav-link {
            display: inline-block;
            text-decoration: none;
            color: var(--text) !important;
            padding: 0.6rem 0.95rem;
            border-radius: 999px;
            border: 1px solid var(--border);
            background: rgba(255,255,255,0.035);
            font-size: 0.9rem;
            font-weight: 600;
        }

        .et-nav-link.active {
            background: linear-gradient(90deg, rgba(79,209,197,0.18), rgba(96,165,250,0.16));
            border-color: rgba(79,209,197,0.28);
            box-shadow: 0 10px 24px rgba(0, 0, 0, 0.15);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_hero(
    kicker: str,
    title: str,
    subtitle: str,
    chips: list[str] | None = None,
) -> None:
    """Render a reusable hero banner."""
    chips = chips or []
    chip_html = "".join([f"<span class='et-chip'>{chip}</span>" for chip in chips])
    st.markdown(
        f"""
        <div class="et-hero">
            <div class="et-kicker">{kicker}</div>
            <div class="et-title">{title}</div>
            <div class="et-subtitle">{subtitle}</div>
            <div class="et-chip-row">{chip_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_info_card(title: str, body: str, label: str = "Overview") -> None:
    """Render a reusable content card."""
    st.markdown(
        f"""
        <div class="et-card">
            <div class="et-section-label">{label}</div>
            <h3>{title}</h3>
            <div class="et-muted">{body}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_navbar(current_page: str) -> None:
    """Render a top navigation bar using Streamlit page links."""
    st.markdown(
        """
        <div class="et-nav">
            <div>
                <div class="et-nav-brand">TrendBeacon</div>
                <div class="et-nav-note">Early trend detection and prediction system</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    nav_items = [
        ("/", "Home", "home"),
        ("/current_trends", "Current Trends", "current_trends"),
        ("/predicted_trends", "Predicted Trends", "predicted_trends"),
    ]
    links_html = "".join(
        [
            (
                f"<a class='et-nav-link{' active' if page_key == current_page else ''}' "
                f"href='{href}' target='_self'>{label}</a>"
            )
            for href, label, page_key in nav_items
        ]
    )
    st.markdown(f"<div class='et-nav-links'>{links_html}</div>", unsafe_allow_html=True)
