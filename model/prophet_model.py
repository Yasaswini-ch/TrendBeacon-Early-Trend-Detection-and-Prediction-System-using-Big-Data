"""
Forecast future keyword frequencies for TrendBeacon.

Uses Prophet when available and falls back to a light-weight extrapolation
model for local demo environments.
"""

from __future__ import annotations

import io
import pickle
import time
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from config.config_loader import cfg
from config.logging_config import get_logger
from utils.hdfs_utils import HDFSUtils
from utils.spark_session import get_spark_session

logger = get_logger(__name__)

try:
    from prophet import Prophet  # type: ignore

    _PROPHET_AVAILABLE = True
except ImportError:
    Prophet = None  # type: ignore
    _PROPHET_AVAILABLE = False


class ProphetForecaster:
    """Forecast top keywords from the engineered feature table."""

    def __init__(self) -> None:
        prophet_cfg = cfg.get("models", {}).get("prophet", {})
        hdfs_cfg = cfg.get("hdfs", {})

        self.forecast_periods = int(prophet_cfg.get("forecast_periods", 24))
        self.forecast_freq = prophet_cfg.get("forecast_freq", "H")
        self.top_n_keywords = int(prophet_cfg.get("top_n_keywords", 20))
        self.yearly_seasonality = prophet_cfg.get("yearly_seasonality", False)
        self.weekly_seasonality = prophet_cfg.get("weekly_seasonality", True)
        self.daily_seasonality = prophet_cfg.get("daily_seasonality", True)
        self.changepoint_prior_scale = float(
            prophet_cfg.get("changepoint_prior_scale", 0.05)
        )

        base_uri = hdfs_cfg.get("base_uri", "file://")
        if base_uri == "file://":
            self.features_path = f"{base_uri}{hdfs_cfg['paths']['features']}"
            self.predictions_path = f"{base_uri}{hdfs_cfg['paths']['trends']}/predictions"
            self.models_path = f"{base_uri}{hdfs_cfg['paths']['models']}/prophet"
        else:
            self.features_path = f"{base_uri}/{hdfs_cfg['paths']['features']}"
            self.predictions_path = f"{base_uri}/{hdfs_cfg['paths']['trends']}/predictions"
            self.models_path = f"{base_uri}/{hdfs_cfg['paths']['models']}/prophet"

        self.hdfs = HDFSUtils()
        self.spark = get_spark_session()
        self.model_store: Dict[str, Any] = {}

    def run(self, feature_df=None) -> pd.DataFrame:
        """Generate forecasts and persist them for dashboard consumption."""
        start = time.time()
        if feature_df is None:
            feature_df = self.hdfs.read_parquet(self.features_path, spark=self.spark)

        pdf = feature_df.toPandas()
        if pdf.empty:
            logger.warning("No feature rows found for forecasting.")
            return pd.DataFrame()

        results = self.forecast_all_keywords(pdf, top_n=self.top_n_keywords)
        prediction_df = pd.DataFrame(results)
        if prediction_df.empty:
            logger.warning("No prediction rows generated.")
            return prediction_df

        output_path = f"{self.predictions_path}/dt={time.strftime('%Y-%m-%d')}"
        self.spark.createDataFrame(prediction_df).write.mode("overwrite").parquet(output_path)
        logger.info(
            "Forecasting complete | rows=%d | output=%s | elapsed=%.1fs",
            len(prediction_df),
            output_path,
            time.time() - start,
        )
        return prediction_df

    def forecast_all_keywords(self, feature_df: pd.DataFrame, top_n: int = 20) -> List[Dict[str, Any]]:
        """Forecast the top-N keywords by current 1h frequency."""
        ranked = (
            feature_df.groupby("keyword")["freq_1h"]
            .sum()
            .sort_values(ascending=False)
            .head(top_n)
            .index.tolist()
        )

        rows: List[Dict[str, Any]] = []
        for keyword in ranked:
            try:
                rows.extend(self._forecast_keyword(keyword, feature_df))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Forecast failed for %s: %s", keyword, exc)
        return rows

    def _forecast_keyword(self, keyword: str, feature_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Forecast one keyword and return dashboard-friendly rows."""
        series = (
            feature_df.loc[feature_df["keyword"] == keyword, ["timestamp", "freq_1h", "trend_label", "is_anomaly"]]
            .dropna(subset=["timestamp"])
            .copy()
        )
        if series.empty:
            return []

        series["timestamp"] = pd.to_datetime(series["timestamp"], utc=True)
        series = series.sort_values("timestamp")
        series = series.rename(columns={"timestamp": "ds", "freq_1h": "y"})

        if _PROPHET_AVAILABLE:
            forecast_df = self._run_prophet(keyword, series[["ds", "y"]])
        else:
            forecast_df = self._run_fallback(series[["ds", "y"]])

        latest_label = str(series["trend_label"].dropna().iloc[-1]) if series["trend_label"].notna().any() else "stable"
        confidence = self._estimate_confidence(forecast_df)
        rows: List[Dict[str, Any]] = []

        for _, row in forecast_df.iterrows():
            rows.append(
                {
                    "keyword": keyword,
                    "predicted_at": row["ds"],
                    "forecast_period": f"{self.forecast_periods}h",
                    "predicted_freq": float(max(row["yhat"], 0.0)),
                    "lower_bound": float(max(row["yhat_lower"], 0.0)),
                    "upper_bound": float(max(row["yhat_upper"], 0.0)),
                    "trend_class": latest_label,
                    "confidence": float(confidence),
                }
            )

        return rows

    def _run_prophet(self, keyword: str, series: pd.DataFrame) -> pd.DataFrame:
        """Fit Prophet and return only future forecast rows."""
        model = Prophet(
            yearly_seasonality=self.yearly_seasonality,
            weekly_seasonality=self.weekly_seasonality,
            daily_seasonality=self.daily_seasonality,
            changepoint_prior_scale=self.changepoint_prior_scale,
        )
        model.fit(series)

        future = model.make_future_dataframe(periods=self.forecast_periods, freq=self.forecast_freq)
        forecast = model.predict(future).tail(self.forecast_periods)[["ds", "yhat", "yhat_lower", "yhat_upper"]]
        self.model_store[keyword] = model
        self._save_model(keyword, model)
        return forecast

    def _run_fallback(self, series: pd.DataFrame) -> pd.DataFrame:
        """Simple extrapolation fallback when Prophet is unavailable."""
        history = series.copy()
        history["y"] = history["y"].astype(float)
        history = history.sort_values("ds")

        recent = history["y"].tail(min(12, len(history)))
        last_value = float(recent.iloc[-1])
        if len(recent) > 1:
            avg_delta = float(recent.diff().dropna().mean())
        else:
            avg_delta = 0.0

        future_dates = pd.date_range(
            start=history["ds"].iloc[-1] + pd.Timedelta(hours=1),
            periods=self.forecast_periods,
            freq=self.forecast_freq,
        )

        values = []
        for step, forecast_dt in enumerate(future_dates, start=1):
            yhat = max(last_value + avg_delta * step, 0.0)
            band = max(yhat * 0.15, 1.0)
            values.append(
                {
                    "ds": forecast_dt,
                    "yhat": yhat,
                    "yhat_lower": max(yhat - band, 0.0),
                    "yhat_upper": yhat + band,
                }
            )
        return pd.DataFrame(values)

    @staticmethod
    def _estimate_confidence(forecast_df: pd.DataFrame) -> float:
        """Map forecast interval width to a simple 0-1 confidence score."""
        widths = (forecast_df["yhat_upper"] - forecast_df["yhat_lower"]).clip(lower=0)
        centers = forecast_df["yhat"].abs().clip(lower=1.0)
        relative_width = float((widths / centers).mean())
        confidence = max(0.1, min(0.95, 1.0 - relative_width))
        return confidence

    def _save_model(self, keyword: str, model: Any) -> None:
        """Persist Prophet model bytes when local file storage is available."""
        safe_keyword = keyword.replace(" ", "_").replace("/", "-")
        model_path = f"{self.models_path}/prophet_{safe_keyword}.pkl"
        try:
            buffer = io.BytesIO()
            pickle.dump(model, buffer)
            self.hdfs.write_bytes(model_path, buffer.getvalue())
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not save model for %s: %s", keyword, exc)
