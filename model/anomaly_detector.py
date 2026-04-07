"""
model/anomaly_detector.py
-------------------------
Anomaly detection on keyword frequency using IsolationForest and statistical
methods (z-score, IQR).

Three detection strategies are supported, selectable via config:
    "isolation_forest" — sklearn IsolationForest (unsupervised, handles
                          multivariate features, robust to high dimensions)
    "zscore"           — classical z-score on a univariate series
    "iqr"              — interquartile range outlier detection

The ``run()`` method loads the feature DataFrame, runs the chosen strategy,
and appends ``is_anomaly`` (BOOLEAN) and ``anomaly_score`` (DOUBLE) columns
before writing the enriched features back to HDFS.
"""

from __future__ import annotations

import time
from typing import Optional

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType

from config.config_loader import cfg
from config.logging_config import get_logger
from utils.hdfs_utils import HDFSUtils
from utils.spark_session import get_spark_session

logger = get_logger(__name__)

# IsolationForest is an optional heavy dependency; guard gracefully
try:
    from sklearn.ensemble import IsolationForest  # type: ignore
    _SKLEARN_AVAILABLE = True
except ImportError:
    _SKLEARN_AVAILABLE = False
    logger.warning(
        "scikit-learn is not installed.  IsolationForest detection will be "
        "unavailable.  Install it with:  pip install scikit-learn"
    )

# Feature columns extracted for anomaly detection
_FEATURE_COLS = [
    "freq_1h",
    "freq_6h",
    "growth_rate_1h",
    "growth_rate_24h",
    "z_score_1h",
]


class AnomalyDetector:
    """
    Detects anomalous keyword frequency patterns using configurable strategies.

    Configuration keys read from ``models.anomaly_detector`` section:
        method                  STR   "isolation_forest" | "zscore" | "iqr"
        contamination           FLOAT fraction of outliers expected (default 0.05)
        n_estimators            INT   trees in IsolationForest (default 100)
        random_state            INT   reproducibility seed (default 42)
        zscore_threshold        FLOAT |z| threshold for zscore method (default 2.5)

    Attributes
    ----------
    method      : Active detection strategy.
    _model      : Fitted IsolationForest (or None for statistical methods).
    """

    def __init__(self) -> None:
        detector_cfg = cfg.get("models.anomaly_detector", {}) or {}

        self.method: str = detector_cfg.get("method", "isolation_forest")
        self.contamination: float = float(detector_cfg.get("contamination", 0.05))
        self.n_estimators: int = int(detector_cfg.get("n_estimators", 100))
        self.random_state: int = int(detector_cfg.get("random_state", 42))
        self.zscore_threshold: float = float(
            detector_cfg.get("zscore_threshold", 2.5)
        )

        self.features_path: str = cfg.get(
            "hdfs.paths.features", "/earlytrend/features"
        )

        self.hdfs: HDFSUtils = HDFSUtils()
        self.spark = get_spark_session()

        # Internal model state (populated by fit())
        self._model: Optional[IsolationForest] = None  # type: ignore[type-arg]

        logger.info(
            "AnomalyDetector initialised | method=%s | contamination=%.3f",
            self.method,
            self.contamination,
        )

    # ------------------------------------------------------------------
    # Feature extraction
    # ------------------------------------------------------------------

    def prepare_features(self, df: pd.DataFrame) -> np.ndarray:
        """
        Extract the numerical feature matrix used for anomaly detection.

        Expected input columns (from FEATURE_SCHEMA):
            freq_1h, freq_6h, growth_rate_1h, growth_rate_24h, z_score_1h

        Missing columns are filled with 0 so the pipeline degrades gracefully
        rather than crashing.  NaN / Inf values are replaced with column medians
        to avoid polluting IsolationForest's tree splits.

        Parameters
        ----------
        df : Pandas feature DataFrame.

        Returns
        -------
        np.ndarray
            Shape (n_samples, 5) float64 array.
        """
        available_cols = [c for c in _FEATURE_COLS if c in df.columns]
        missing_cols = [c for c in _FEATURE_COLS if c not in df.columns]

        if missing_cols:
            logger.warning(
                "prepare_features: columns missing from DataFrame, filling with 0: %s",
                missing_cols,
            )
            for col in missing_cols:
                df = df.copy()
                df[col] = 0.0

        X = df[_FEATURE_COLS].values.astype(np.float64)

        # Replace NaN / Inf with column median to keep the matrix valid
        for col_idx in range(X.shape[1]):
            col_data = X[:, col_idx]
            finite_mask = np.isfinite(col_data)
            if not finite_mask.all():
                median_val = (
                    np.median(col_data[finite_mask]) if finite_mask.any() else 0.0
                )
                X[~finite_mask, col_idx] = median_val

        return X

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def fit(self, X: np.ndarray) -> None:
        """
        Train the anomaly detection model on feature matrix X.

        For "isolation_forest": fits an IsolationForest.
        For "zscore" / "iqr":   computes and stores statistical bounds per
                                 feature column (mean, std, Q1, Q3, IQR).

        Parameters
        ----------
        X : Feature matrix, shape (n_samples, n_features).
        """
        if self.method == "isolation_forest":
            if not _SKLEARN_AVAILABLE:
                raise ImportError(
                    "scikit-learn must be installed to use IsolationForest.  "
                    "Run:  pip install scikit-learn"
                )
            logger.info(
                "Fitting IsolationForest | n_estimators=%d | contamination=%.3f",
                self.n_estimators,
                self.contamination,
            )
            self._model = IsolationForest(
                n_estimators=self.n_estimators,
                contamination=self.contamination,
                random_state=self.random_state,
                n_jobs=-1,  # Use all available cores
            )
            self._model.fit(X)
            logger.info("IsolationForest fitted.")

        elif self.method in ("zscore", "iqr"):
            # Statistical bounds do not require a separate fit step;
            # they are computed on-the-fly inside predict()
            logger.info(
                "Statistical method '%s' selected; no explicit fit required.",
                self.method,
            )

        else:
            raise ValueError(
                f"Unknown anomaly detection method: '{self.method}'.  "
                "Valid options are 'isolation_forest', 'zscore', 'iqr'."
            )

    # ------------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------------

    def predict(self, X: np.ndarray) -> np.ndarray:
        """
        Return anomaly labels for each row in X.

        The output is always a binary 0/1 integer array regardless of the
        underlying method (IsolationForest internally uses -1/+1).

        Parameters
        ----------
        X : Feature matrix, shape (n_samples, n_features).

        Returns
        -------
        np.ndarray
            Integer array of shape (n_samples,): 1 = anomaly, 0 = normal.
        """
        n = X.shape[0]

        if self.method == "isolation_forest":
            if self._model is None:
                raise RuntimeError(
                    "IsolationForest model has not been fitted yet.  Call fit() first."
                )
            # IsolationForest: -1 = anomaly, +1 = normal → convert to 0/1
            raw = self._model.predict(X)
            return (raw == -1).astype(int)

        elif self.method == "zscore":
            # Apply z-score detection column-wise, flag if ANY feature is anomalous
            labels = np.zeros(n, dtype=int)
            for col_idx in range(X.shape[1]):
                col_anomalies = self.detect_anomalies_zscore(
                    X[:, col_idx], threshold=self.zscore_threshold
                )
                labels = np.logical_or(labels, col_anomalies).astype(int)
            return labels

        elif self.method == "iqr":
            labels = np.zeros(n, dtype=int)
            for col_idx in range(X.shape[1]):
                col_anomalies = self.detect_anomalies_iqr(X[:, col_idx])
                labels = np.logical_or(labels, col_anomalies).astype(int)
            return labels

        else:
            raise ValueError(f"Unknown method: '{self.method}'")

    # ------------------------------------------------------------------
    # Static statistical detectors
    # ------------------------------------------------------------------

    @staticmethod
    def detect_anomalies_zscore(
        series: np.ndarray,
        threshold: float = 2.5,
    ) -> np.ndarray:
        """
        Flag outliers in a 1-D array using z-score thresholding.

        Formula:
            z_i = (x_i - mean) / std
            anomaly_i = |z_i| > threshold

        A standard deviation of 0 (all identical values) results in no
        anomalies being flagged (every z-score is 0).

        Parameters
        ----------
        series    : 1-D numpy array of float values.
        threshold : Absolute z-score cutoff (default 2.5).

        Returns
        -------
        np.ndarray
            Boolean array: True where the value is anomalous.
        """
        series = np.asarray(series, dtype=np.float64)
        mean = np.mean(series)
        std = np.std(series)

        if std == 0.0:
            # No variance — nothing is anomalous
            return np.zeros(len(series), dtype=bool)

        z_scores = (series - mean) / std
        return np.abs(z_scores) > threshold

    @staticmethod
    def detect_anomalies_iqr(series: np.ndarray) -> np.ndarray:
        """
        Flag outliers in a 1-D array using the interquartile range (IQR) method.

        Outlier bounds:
            lower = Q1 - 1.5 * IQR
            upper = Q3 + 1.5 * IQR

        The 1.5 multiplier is Tukey's standard fence coefficient, equivalent
        to roughly ±2.7 σ under a Gaussian distribution.

        Parameters
        ----------
        series : 1-D numpy array of float values.

        Returns
        -------
        np.ndarray
            Boolean array: True where the value is anomalous.
        """
        series = np.asarray(series, dtype=np.float64)
        q1 = np.percentile(series, 25)
        q3 = np.percentile(series, 75)
        iqr = q3 - q1

        lower_fence = q1 - 1.5 * iqr
        upper_fence = q3 + 1.5 * iqr

        return (series < lower_fence) | (series > upper_fence)

    # ------------------------------------------------------------------
    # Anomaly report
    # ------------------------------------------------------------------

    def get_anomaly_report(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Summarise anomalous keywords with their scores and labels.

        Filters to rows where ``is_anomaly`` is True and returns a concise
        DataFrame sorted by ``anomaly_score`` descending.

        Parameters
        ----------
        df : Pandas DataFrame with ``is_anomaly``, ``anomaly_score``,
             ``keyword``, and optionally ``trend_label`` columns.

        Returns
        -------
        pd.DataFrame
            Anomaly summary with columns:
            keyword, anomaly_score, trend_label, freq_1h, freq_6h.
        """
        if "is_anomaly" not in df.columns:
            logger.warning(
                "get_anomaly_report: 'is_anomaly' column not found; "
                "returning empty report."
            )
            return pd.DataFrame()

        anomalous = df[df["is_anomaly"]].copy()

        report_cols = [
            c for c in
            ["keyword", "anomaly_score", "trend_label", "freq_1h", "freq_6h",
             "growth_rate_1h", "z_score_1h"]
            if c in anomalous.columns
        ]
        report = anomalous[report_cols].sort_values(
            "anomaly_score", ascending=False
        ).reset_index(drop=True)

        logger.info(
            "get_anomaly_report | anomalous_keywords=%d", len(report)
        )
        return report

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------

    def run(self, feature_df: Optional[DataFrame] = None) -> DataFrame:
        """
        Execute the full anomaly detection pipeline.

        Steps:
          1. Load feature Spark DataFrame from HDFS (or use supplied one).
          2. Convert to Pandas for sklearn / numpy processing.
          3. Fit detector.
          4. Predict anomaly labels and scores.
          5. Merge results back and add ``is_anomaly`` / ``anomaly_score``.
          6. Overwrite features Parquet with enriched version.

        Parameters
        ----------
        feature_df : Optional Spark DataFrame; loaded from HDFS if None.

        Returns
        -------
        DataFrame
            Spark DataFrame enriched with anomaly columns.
        """
        pipeline_start = time.time()
        logger.info("AnomalyDetector.run() starting | method=%s …", self.method)

        # --- Load features ---
        if feature_df is None:
            logger.info("Reading features from %s", self.features_path)
            feature_df = self.spark.read.parquet(self.features_path)

        pandas_df: pd.DataFrame = feature_df.toPandas()
        logger.info("Loaded %d feature rows", len(pandas_df))

        # --- Extract feature matrix ---
        X = self.prepare_features(pandas_df)

        # --- Fit ---
        self.fit(X)

        # --- Predict binary labels ---
        anomaly_labels = self.predict(X)  # shape (n,)

        # --- Compute anomaly scores ---
        if self.method == "isolation_forest" and self._model is not None:
            # score_samples returns negative average depth; negate for "anomaly-ness"
            raw_scores = self._model.score_samples(X)
            # Normalise to [0, 1]: higher score = more anomalous
            min_s, max_s = raw_scores.min(), raw_scores.max()
            denom = (max_s - min_s) if (max_s - min_s) != 0 else 1.0
            anomaly_scores = 1.0 - (raw_scores - min_s) / denom
        else:
            # For statistical methods use the maximum absolute z-score across
            # feature columns as a proxy score
            z_scores_matrix = np.zeros_like(X)
            for col_idx in range(X.shape[1]):
                col = X[:, col_idx]
                std = np.std(col)
                if std > 0:
                    z_scores_matrix[:, col_idx] = np.abs((col - np.mean(col)) / std)
            anomaly_scores = z_scores_matrix.max(axis=1)

        # --- Attach results to Pandas DataFrame ---
        pandas_df["is_anomaly"] = anomaly_labels.astype(bool)
        pandas_df["anomaly_score"] = anomaly_scores.astype(float)

        # --- Convert back to Spark ---
        enriched_sdf = self.spark.createDataFrame(pandas_df)

        # Cast to correct types to match schema expectations
        enriched_sdf = (
            enriched_sdf
            .withColumn("is_anomaly",    F.col("is_anomaly").cast(BooleanType()))
            .withColumn("anomaly_score", F.col("anomaly_score").cast(DoubleType()))
        )

        # --- Persist enriched features ---
        output_path = (
            f"{self.features_path}/dt={time.strftime('%Y-%m-%d')}"
        )
        logger.info("Writing enriched features to %s", output_path)
        enriched_sdf.write.mode("overwrite").parquet(output_path)

        elapsed = time.time() - pipeline_start
        anomaly_count = int(anomaly_labels.sum())
        logger.info(
            "AnomalyDetector.run() complete in %.1f s | anomalies=%d / %d",
            elapsed,
            anomaly_count,
            len(pandas_df),
        )
        return enriched_sdf
