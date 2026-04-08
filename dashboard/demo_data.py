"""Bundled demo datasets for Streamlit deployments without pipeline outputs."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd


def _base_timestamp() -> datetime:
    return datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)


def load_demo_trends() -> pd.DataFrame:
    """Return a deterministic-looking trends dataset for dashboard fallback."""
    end_ts = _base_timestamp()
    history = pd.date_range(end=end_ts, periods=18, freq="H", tz="UTC")

    series_map = {
        "quantum computing": {
            "freq": [8, 9, 11, 12, 13, 14, 16, 19, 22, 26, 29, 33, 38, 44, 51, 59, 67, 74],
            "trend_label": "emerging",
            "z_base": 1.6,
        },
        "bird flu": {
            "freq": [4, 5, 5, 6, 7, 8, 9, 12, 14, 18, 23, 29, 36, 45, 57, 68, 80, 96],
            "trend_label": "viral",
            "z_base": 2.4,
        },
        "ai breakthrough": {
            "freq": [12, 12, 13, 13, 14, 16, 18, 18, 19, 21, 22, 25, 28, 31, 37, 43, 50, 61],
            "trend_label": "emerging",
            "z_base": 1.9,
        },
        "climate": {
            "freq": [26, 25, 27, 26, 27, 28, 27, 29, 28, 27, 29, 28, 30, 29, 30, 31, 30, 31],
            "trend_label": "stable",
            "z_base": 0.8,
        },
        "crypto": {
            "freq": [41, 40, 39, 39, 38, 37, 36, 35, 35, 34, 33, 32, 31, 31, 30, 29, 28, 27],
            "trend_label": "declining",
            "z_base": 0.9,
        },
    }

    rows: list[dict[str, object]] = []
    for keyword, spec in series_map.items():
        freqs = spec["freq"]
        for idx, ts in enumerate(history):
            prev = freqs[idx - 1] if idx > 0 else max(freqs[idx] - 1, 1)
            growth_rate = ((freqs[idx] - prev) / prev) * 100 if prev else 0.0
            is_anomaly = int(keyword in {"bird flu", "ai breakthrough"} and idx >= len(history) - 3)
            z_score = spec["z_base"] + idx * 0.08 + (1.8 if is_anomaly else 0.0)
            rows.append(
                {
                    "keyword": keyword,
                    "timestamp": ts,
                    "frequency": freqs[idx],
                    "growth_rate": round(growth_rate, 2),
                    "z_score": round(z_score, 2),
                    "is_anomaly": is_anomaly,
                    "trend_label": spec["trend_label"],
                    "source": "__all__",
                }
            )

    return pd.DataFrame(rows)


def load_demo_features() -> pd.DataFrame:
    """Return a compact feature snapshot for the predicted trends page."""
    trends = load_demo_trends()
    latest = (
        trends.sort_values("timestamp")
        .groupby("keyword", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )
    return latest[["keyword", "growth_rate", "frequency", "z_score", "trend_label"]].copy()


def load_demo_predictions() -> pd.DataFrame:
    """Return a small forecast dataset for dashboard fallback."""
    start = _base_timestamp()
    horizons = pd.date_range(start=start + timedelta(hours=1), periods=12, freq="H", tz="UTC")

    base_specs = {
        "bird flu": {"start": 96, "step": 7, "trend_class": "viral", "confidence": 0.83},
        "quantum computing": {"start": 74, "step": 4, "trend_class": "emerging", "confidence": 0.79},
        "ai breakthrough": {"start": 61, "step": 5, "trend_class": "emerging", "confidence": 0.76},
        "climate": {"start": 31, "step": 1, "trend_class": "stable", "confidence": 0.72},
        "crypto": {"start": 27, "step": -1, "trend_class": "declining", "confidence": 0.68},
    }

    rows: list[dict[str, object]] = []
    for keyword, spec in base_specs.items():
        current_freq = float(spec["start"])
        for step, ts in enumerate(horizons, start=1):
            predicted = max(current_freq + spec["step"] * step, 1.0)
            band = max(predicted * 0.12, 1.5)
            rows.append(
                {
                    "keyword": keyword,
                    "predicted_at": ts,
                    "forecast_period": "24h",
                    "predicted_freq": round(predicted, 2),
                    "lower_bound": round(max(predicted - band, 0.0), 2),
                    "upper_bound": round(predicted + band, 2),
                    "trend_class": spec["trend_class"],
                    "confidence": spec["confidence"],
                    "frequency": current_freq if step == 1 else None,
                }
            )
    return pd.DataFrame(rows)
