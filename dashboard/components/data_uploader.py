"""
data_uploader.py — User data upload components for TrendBeacon.

Allows non-technical users to upload their own CSV/Excel data
and see trend analysis without needing the full pipeline.
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# CSV Templates for users to download
# ---------------------------------------------------------------------------

TREND_DATA_TEMPLATE = """keyword,timestamp,frequency,growth_rate,z_score,is_anomaly,trend_label
example_topic,2026-04-09 10:00:00,50,15.5,2.1,1,viral
another_topic,2026-04-09 10:00:00,30,8.2,1.3,0,emerging
steady_topic,2026-04-09 10:00:00,25,1.0,0.5,0,stable
"""

REQUIRED_COLUMNS = {
    "keyword": str,
    "frequency": float,
}

OPTIONAL_COLUMNS = {
    "timestamp": str,
    "growth_rate": float,
    "z_score": float,
    "is_anomaly": int,
    "trend_label": str,
}


def render_data_uploader() -> pd.DataFrame | None:
    """
    Render a file uploader that accepts CSV/Excel files.

    Returns:
        DataFrame if valid data uploaded, None otherwise
    """
    st.markdown("### 📁 Upload Your Data")

    st.markdown("""
    **How to use:**
    1. Download the template CSV below
    2. Fill in your data (keyword, frequency are required)
    3. Upload your file to see analysis
    """)

    # Template download
    st.download_button(
        label="📋 Download CSV Template",
        data=TREND_DATA_TEMPLATE.strip(),
        file_name="trend_data_template.csv",
        mime="text/csv",
        help="Download a template with example data",
    )

    # File upload
    uploaded_file = st.file_uploader(
        "Choose a CSV or Excel file",
        type=["csv", "xlsx", "xls"],
        help="Upload your trend data file",
    )

    if uploaded_file is None:
        st.info("👆 Upload a file to analyze your own data")
        return None

    # Parse the file
    try:
        if uploaded_file.name.endswith(".csv"):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)

        # Normalize column names
        df.columns = df.columns.str.lower().str.strip()

        # Clean numeric columns (handle commas, percentages, etc.)
        numeric_cols = ["frequency", "growth_rate", "z_score"]
        for col in numeric_cols:
            if col in df.columns:
                # Convert to string first to handle mixed types
                df[col] = df[col].astype(str).str.replace(",", "").str.replace("%", "").str.replace("+", "")
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        # Clean boolean/int columns
        if "is_anomaly" in df.columns:
            df["is_anomaly"] = pd.to_numeric(df["is_anomaly"], errors="coerce").fillna(0).astype(int)

        # Validate required columns
        missing = set(REQUIRED_COLUMNS.keys()) - set(df.columns)
        if missing:
            st.error(f"Missing required columns: {missing}")
            st.info(f"Your file has columns: {list(df.columns)}")
            return None

        # Fill optional columns with defaults
        for col, dtype in OPTIONAL_COLUMNS.items():
            if col not in df.columns:
                if col == "timestamp":
                    df[col] = datetime.now(timezone.utc).isoformat()
                elif col == "growth_rate":
                    df[col] = 0.0
                elif col == "z_score":
                    df[col] = 0.0
                elif col == "is_anomaly":
                    df[col] = 0
                elif col == "trend_label":
                    df[col] = "stable"

        # Normalize trend_label values
        if "trend_label" in df.columns:
            # Ensure it's string type and fill NaN with 'stable'
            df["trend_label"] = df["trend_label"].fillna("stable").astype(str).str.lower().str.strip()
            # Map common variations
            label_map = {
                "trending": "viral",
                "hot": "viral",
                "growing": "emerging",
                "rising": "emerging",
                "normal": "stable",
                "flat": "stable",
                "dropping": "declining",
                "falling": "declining",
                "nan": "stable",
                "": "stable",
            }
            df["trend_label"] = df["trend_label"].replace(label_map)

        # Parse timestamp
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            df["timestamp"] = df["timestamp"].fillna(datetime.now(timezone.utc))

        st.success(f"✅ Loaded {len(df)} rows for {df['keyword'].nunique()} unique keywords")

        # Preview - using st.markdown instead of nested expander
        st.markdown("#### 👀 Data Preview (Top 10 rows)")
        st.dataframe(df.head(10), use_container_width=True)

        return df

    except Exception as e:
        import traceback
        st.error(f"Error reading file: {e}")
        st.code(traceback.format_exc())
        return None


def render_keyword_input() -> pd.DataFrame | None:
    """
    Render a simple text input for users to enter keywords manually.

    Returns:
        DataFrame with entered keywords, None if no input
    """
    st.markdown("### ✍️ Enter Keywords Manually")

    st.markdown("Enter keywords you want to track (one per line):")

    text_input = st.text_area(
        "Keywords",
        placeholder="climate change\nai technology\nrenewable energy\n...",
        height=150,
        help="Enter one keyword per line",
    )

    if not text_input.strip():
        return None

    keywords = [k.strip() for k in text_input.strip().split("\n") if k.strip()]

    if not keywords:
        return None

    # Create a minimal DataFrame
    df = pd.DataFrame({
        "keyword": keywords,
        "frequency": [1] * len(keywords),
        "growth_rate": [0.0] * len(keywords),
        "z_score": [0.0] * len(keywords),
        "is_anomaly": [0] * len(keywords),
        "trend_label": ["stable"] * len(keywords),
        "timestamp": [datetime.now(timezone.utc)] * len(keywords),
    })

    st.success(f"✅ Added {len(keywords)} keywords")
    return df


def render_prediction_uploader() -> pd.DataFrame | None:
    """
    Render a file uploader for prediction/forecast data.

    Returns:
        DataFrame if valid prediction data uploaded, None otherwise
    """
    st.markdown("### 📁 Upload Predictions")

    PREDICTION_TEMPLATE = """keyword,predicted_at,forecast_period,predicted_freq,lower_bound,upper_bound,trend_class,confidence
example_topic,2026-04-10 10:00:00,24h,75,60,90,viral,0.85
another_topic,2026-04-10 10:00:00,24h,45,35,55,emerging,0.72
"""

    st.download_button(
        label="📋 Download Prediction Template",
        data=PREDICTION_TEMPLATE.strip(),
        file_name="prediction_data_template.csv",
        mime="text/csv",
    )

    uploaded_file = st.file_uploader(
        "Choose a prediction CSV or Excel file",
        type=["csv", "xlsx", "xls"],
    )

    if uploaded_file is None:
        return None

    try:
        if uploaded_file.name.endswith(".csv"):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)

        required = {"keyword", "predicted_freq"}
        missing = required - set(df.columns)
        if missing:
            st.error(f"Missing required columns: {missing}")
            return None

        df.columns = df.columns.str.lower().str.strip()

        # Fill defaults
        if "predicted_at" not in df.columns:
            df["predicted_at"] = datetime.now(timezone.utc).isoformat()
        if "forecast_period" not in df.columns:
            df["forecast_period"] = "24h"
        if "trend_class" not in df.columns:
            df["trend_class"] = "stable"
        if "confidence" not in df.columns:
            df["confidence"] = 0.5
        if "lower_bound" not in df.columns:
            df["lower_bound"] = df["predicted_freq"] * 0.8
        if "upper_bound" not in df.columns:
            df["upper_bound"] = df["predicted_freq"] * 1.2

        st.success(f"✅ Loaded {len(df)} prediction rows")
        return df

    except Exception as e:
        import traceback
        st.error(f"Error reading file: {e}")
        st.code(traceback.format_exc())
        return None
