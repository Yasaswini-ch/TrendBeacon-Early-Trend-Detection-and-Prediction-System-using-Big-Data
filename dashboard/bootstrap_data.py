"""Create lightweight local parquet outputs for Streamlit deployments."""

from __future__ import annotations

from pathlib import Path

import streamlit as st

from dashboard.demo_data import load_demo_features, load_demo_predictions, load_demo_trends

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DATA_ROOT = _PROJECT_ROOT / "data" / "hdfs"
_TRENDS_DIR = _DATA_ROOT / "trends" / "dt=demo"
_FEATURES_DIR = _DATA_ROOT / "features" / "dt=demo"
_PREDICTIONS_DIR = _DATA_ROOT / "trends" / "predictions" / "dt=demo"


def _has_parquet(path: Path) -> bool:
    return path.exists() and any(path.rglob("*.parquet"))


@st.cache_resource(show_spinner=False)
def ensure_demo_outputs() -> bool:
    """Materialize demo parquet outputs when no generated data exists yet."""
    if _has_parquet(_DATA_ROOT / "trends") and _has_parquet(_DATA_ROOT / "features"):
        if _has_parquet(_DATA_ROOT / "trends" / "predictions"):
            return False

    _TRENDS_DIR.mkdir(parents=True, exist_ok=True)
    _FEATURES_DIR.mkdir(parents=True, exist_ok=True)
    _PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)

    load_demo_trends().to_parquet(_TRENDS_DIR / "part-00000.parquet", index=False)
    load_demo_features().to_parquet(_FEATURES_DIR / "part-00000.parquet", index=False)
    load_demo_predictions().to_parquet(_PREDICTIONS_DIR / "part-00000.parquet", index=False)
    return True
