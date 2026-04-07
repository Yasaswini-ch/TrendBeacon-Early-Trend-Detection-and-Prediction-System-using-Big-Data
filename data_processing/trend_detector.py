"""
trend_detector.py
=================
Core early-trend detection module for the TrendBeacon big-data pipeline.

Scientific overview
-------------------
This module implements a multi-signal anomaly detection approach to identify
keywords that are *emerging as trends* before they reach mainstream awareness.
The detection pipeline consists of five sequential analytical stages:

  1. **Growth rate** — first derivative of keyword frequency over time.
     Measures how fast a keyword's popularity is increasing (or declining).

  2. **Z-score normalisation** — standardises each keyword's growth-rate
     series using its own historical mean and standard deviation, making
     keywords with different baseline frequencies directly comparable.

  3. **Anomaly flagging** — marks observations where abs(z_score) exceeds a
     configurable threshold as anomalies, and records whether the spike was
     upward (gaining) or downward (losing traction).

  4. **Trend classification** — assigns each (keyword, window) pair a
     human-readable trend label: viral / emerging / declining / stable.

  5. **Velocity & acceleration** — computes the 1st and 2nd discrete
     derivatives of frequency to capture not just whether a trend is growing
     but *how rapidly its growth rate is itself changing*.

All computations are implemented with Spark Window functions so the entire
pipeline is distributed and never requires collecting data to the driver.
"""

from __future__ import annotations

from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

from config.config_loader import cfg
from config.logging_config import get_logger
from utils.hdfs_utils import HDFSUtils
from utils.spark_session import get_spark_session

logger = get_logger(__name__)


class TrendDetector:
    """
    Distributed early-trend detection over rolling keyword frequency tables.

    Configuration keys used (from cfg):
      features.anomaly_z_threshold      – z-score threshold for anomaly flag
                                          (default 2.0)
      features.growth_rate_epsilon       – small constant to avoid division by
                                          zero in growth-rate formula (default 1)
      features.viral_z_threshold         – z-score for "viral" label (default 3.0)
      features.emerging_z_threshold      – z-score for "emerging" label (default 2.0)
      features.declining_growth_threshold– growth rate below which = "declining"
                                          (default -0.5)
      features.features_output_path      – HDFS root for feature outputs
    """

    def __init__(self) -> None:
        self.spark: SparkSession = get_spark_session()
        self.hdfs: HDFSUtils = HDFSUtils()

        feat_cfg = cfg.get("features", {})

        # Anomaly detection threshold: observations whose |z-score| exceeds
        # this value are flagged as anomalous.
        self.z_threshold: float = float(feat_cfg.get("anomaly_z_threshold", 2.0))

        # Epsilon prevents division by zero when freq_previous == 0.
        # A small positive value (e.g. 1) also smooths extreme growth rates
        # for keywords that jump from zero mentions to a positive count.
        self.epsilon: float = float(feat_cfg.get("growth_rate_epsilon", 1.0))

        # Per-label thresholds for trend classification
        trend_cfg = feat_cfg.get("trend_labels", {})
        self.viral_z: float = float(trend_cfg.get("viral_z", 3.0))
        self.emerging_z: float = float(trend_cfg.get("emerging_z", 2.0))
        self.declining_gr: float = float(feat_cfg.get("declining_growth_threshold", -0.5))
        hdfs_cfg = cfg.get("hdfs", {})
        base_uri = hdfs_cfg.get("base_uri", "file://")
        features_root = hdfs_cfg.get("paths", {}).get("features", "data/hdfs/features")
        trends_root = hdfs_cfg.get("paths", {}).get("trends", "data/hdfs/trends")
        default_features_path = (
            f"{base_uri}{features_root}" if base_uri == "file://" else f"{base_uri}/{features_root}"
        )
        default_trends_path = (
            f"{base_uri}{trends_root}" if base_uri == "file://" else f"{base_uri}/{trends_root}"
        )
        self.features_output_path = feat_cfg.get("features_output_path", default_features_path)
        self.freq_input_path = f"{self.features_output_path}/frequency"
        self.trends_output_path = default_trends_path

        logger.info(
            "TrendDetector initialised | z_threshold=%.2f | epsilon=%.4f | "
            "viral_z=%.2f | emerging_z=%.2f | declining_gr=%.2f",
            self.z_threshold,
            self.epsilon,
            self.viral_z,
            self.emerging_z,
            self.declining_gr,
        )

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self, freq_df: Optional[DataFrame] = None) -> DataFrame:
        """
        Execute the full trend detection pipeline.

        Parameters
        ----------
        freq_df:
            Pre-loaded frequency DataFrame (output of FrequencyAnalyzer).
            If not supplied, it is read from the HDFS features/frequency/ path.

        Returns
        -------
        DataFrame with full trend signals, written to HDFS trends/ layer.
        """
        if freq_df is None:
            logger.info("Loading frequency data from: %s", self.freq_input_path)
            freq_df = self.hdfs.read_parquet(self.freq_input_path)

        logger.info("Computing growth rates …")
        df = self.compute_growth_rate(freq_df)

        logger.info("Computing velocity and acceleration …")
        df = self.compute_velocity_acceleration(df)

        logger.info("Computing z-scores …")
        df = self.compute_zscore(df)

        logger.info("Flagging anomalies …")
        df = self.flag_anomalies(df)

        logger.info("Classifying trends …")
        df = self.classify_trend(df)

        output_path = self.trends_output_path
        logger.info("Writing trend signals to: %s", output_path)
        self.hdfs.write_parquet(df, output_path, mode="overwrite")

        # Log a quick summary to help with monitoring
        total = df.count()
        anomaly_count = df.filter(F.col("is_anomaly") == 1).count()
        viral_count = df.filter(F.col("trend_label") == "viral").count()
        logger.info(
            "Detection summary | total_rows=%d | anomalies=%d | viral=%d",
            total,
            anomaly_count,
            viral_count,
        )

        logger.info("TrendDetector.run() complete.")
        return df

    # ------------------------------------------------------------------
    # Stage 1 – Growth rate (1st derivative of frequency)
    # ------------------------------------------------------------------

    def compute_growth_rate(self, df: DataFrame) -> DataFrame:
        """
        Compute the relative growth rate of each keyword's frequency between
        consecutive time-window buckets within the same window size.

        Mathematical formula
        --------------------
        Let:
          f_t   = frequency of keyword k in window bucket t
          f_t-1 = frequency of keyword k in the immediately preceding bucket
          ε     = epsilon (smoothing constant, e.g. 1.0)

        Then:
          GR_t = (f_t - f_{t-1}) / (f_{t-1} + ε)

        Properties:
        * GR = 0   : no change between consecutive buckets.
        * GR > 0   : keyword is gaining mentions (positive growth).
        * GR < 0   : keyword is losing mentions (declining trend).
        * GR = 1.0 : frequency doubled ( (2x - x) / (x + ε) ≈ 1 for large x ).
        * ε prevents division by zero when f_{t-1} = 0 and also dampens
          extreme ratios for keywords that suddenly appear from nothing.

        Implementation notes
        --------------------
        We use a Spark Window partitioned by (keyword, window, source) and
        ordered by window_start.  ``F.lag("frequency", 1)`` retrieves the
        frequency of the previous time bucket for each partition.

        The ``window`` column (e.g. "1h", "6h") acts as a grouping key so
        that growth rates are only computed *within* the same window size —
        mixing 1-hour and 24-hour frequencies would produce nonsensical rates.

        Parameters
        ----------
        df:
            Frequency DataFrame with columns:
            keyword, window_start, window_end, frequency, window, source.

        Returns
        -------
        DataFrame with an additional ``growth_rate`` column (DoubleType).
        Rows where freq_previous is null (first observation per partition)
        will have growth_rate = null.
        """
        epsilon = self.epsilon

        # Window spec: for each unique (keyword, window-size, source) group,
        # order rows chronologically by the bucket start time.
        partition_window = (
            Window
            .partitionBy("keyword", "window", "source")
            .orderBy("window_start")
        )

        # lag(1) retrieves the value from the immediately preceding row in
        # the ordered partition — i.e. the previous time bucket's frequency.
        df = df.withColumn(
            "freq_previous",
            F.lag("frequency", 1).over(partition_window),
        )

        # GR = (f_current - f_previous) / (f_previous + epsilon)
        # When freq_previous is null (first row in partition), growth_rate
        # remains null to indicate insufficient history.
        df = df.withColumn(
            "growth_rate",
            F.when(
                F.col("freq_previous").isNotNull(),
                (F.col("frequency") - F.col("freq_previous"))
                / (F.col("freq_previous") + F.lit(epsilon)),
            ).otherwise(F.lit(None).cast("double")),
        )

        # Drop the intermediate column — it is embedded in the formula above
        df = df.drop("freq_previous")
        return df

    # ------------------------------------------------------------------
    # Stage 2 – Velocity and acceleration (1st & 2nd derivatives)
    # ------------------------------------------------------------------

    def compute_velocity_acceleration(self, df: DataFrame) -> DataFrame:
        """
        Compute velocity and acceleration of keyword frequency changes.

        Definitions
        -----------
        These are discrete approximations of calculus derivatives applied to
        the discrete time series of frequency counts.

        **Velocity** (1st derivative of frequency):
            v_t = GR_t
            The growth rate already encodes velocity — how fast the keyword
            count is changing at time t.  We alias it for semantic clarity.

        **Acceleration** (2nd derivative of frequency / 1st derivative of velocity):
            a_t = v_t - v_{t-1} = GR_t - GR_{t-1}

        A positive acceleration means the growth rate is *itself increasing*
        — a powerful early-trend signal because it indicates that a keyword
        is gaining momentum, not just persisting.

        A negative acceleration on a still-positive velocity means the trend
        is still growing but slowing down — useful for distinguishing peak
        from growth phase.

        Implementation notes
        --------------------
        * We compute ``velocity`` as a copy of ``growth_rate`` (no data
          movement) for semantic readability in downstream queries.
        * ``acceleration`` uses ``F.lag("growth_rate", 1)`` over the same
          window spec as Stage 1 to obtain the previous growth rate.
        * Rows with insufficient history (null growth_rate or null lag) will
          have null acceleration.

        Parameters
        ----------
        df:
            DataFrame produced by ``compute_growth_rate``, containing
            columns: keyword, window_start, window, source, growth_rate.

        Returns
        -------
        DataFrame with additional columns ``velocity`` and ``acceleration``.
        """
        partition_window = (
            Window
            .partitionBy("keyword", "window", "source")
            .orderBy("window_start")
        )

        # velocity = growth_rate (1st derivative of frequency)
        df = df.withColumn("velocity", F.col("growth_rate"))

        # acceleration = Δvelocity = v_t - v_{t-1}  (2nd derivative of freq)
        df = df.withColumn(
            "prev_growth_rate",
            F.lag("growth_rate", 1).over(partition_window),
        )

        df = df.withColumn(
            "acceleration",
            F.when(
                F.col("prev_growth_rate").isNotNull() & F.col("growth_rate").isNotNull(),
                F.col("growth_rate") - F.col("prev_growth_rate"),
            ).otherwise(F.lit(None).cast("double")),
        )

        df = df.drop("prev_growth_rate")
        return df

    # ------------------------------------------------------------------
    # Stage 3 – Z-score normalisation
    # ------------------------------------------------------------------

    def compute_zscore(self, df: DataFrame) -> DataFrame:
        """
        Normalise each keyword's growth-rate series to a z-score so that
        keywords with very different baseline frequencies are comparable.

        Mathematical formula
        --------------------
        For keyword k and window size w:
          μ_k,w  = mean(GR_t)  for all t in the history of (k, w)
          σ_k,w  = std(GR_t)   for all t in the history of (k, w)

          Z_t = (GR_t - μ_k,w) / σ_k,w

        Properties:
        * Z = 0   : growth rate equals the historical average.
        * Z > 2   : growth rate is more than 2 standard deviations above
                    average — statistically unusual (≈ top 2.3% if normal).
        * Z > 3   : extremely unusual — viral territory.
        * Z < -2  : keyword is declining unusually fast.

        Why z-score over raw growth rate?
        ----------------------------------
        A keyword that normally grows at 50% per hour and hits 60% is less
        interesting than one that normally grows at 1% and hits 20%.  The
        z-score captures *deviation from the keyword's own baseline*, not
        just the absolute growth magnitude.

        Implementation notes
        --------------------
        We use an *unbounded* window (no rowsBetween restriction) over the
        entire history of each (keyword, window, source) partition to compute
        the global mean and std.  This is equivalent to a full-partition
        aggregation with the result broadcast back to each row.

        When σ = 0 (all growth rates identical — no variance), we set Z = 0
        to avoid division by zero.

        Parameters
        ----------
        df:
            DataFrame with columns: keyword, window, source, window_start,
            growth_rate (from Stage 1).

        Returns
        -------
        DataFrame with an additional ``z_score`` column (DoubleType).
        """
        # Unbounded window: statistics computed over the entire partition history
        stats_window = (
            Window
            .partitionBy("keyword", "window", "source")
            # No orderBy / rowsBetween → aggregate over the full partition
        )

        # Compute per-keyword, per-window mean and std of growth_rate
        df = df.withColumn("_gr_mean", F.mean("growth_rate").over(stats_window))
        df = df.withColumn("_gr_std", F.stddev("growth_rate").over(stats_window))

        # Z = (GR - μ) / σ
        # Guard against σ = 0 (constant series) or null σ (single observation)
        df = df.withColumn(
            "z_score",
            F.when(
                F.col("_gr_std").isNull()
                | (F.col("_gr_std") == 0.0)
                | F.col("growth_rate").isNull(),
                F.lit(0.0),
            ).otherwise(
                (F.col("growth_rate") - F.col("_gr_mean")) / F.col("_gr_std")
            ),
        )

        # Drop intermediate columns used only for the z-score calculation
        df = df.drop("_gr_mean", "_gr_std")
        return df

    # ------------------------------------------------------------------
    # Stage 4 – Anomaly flagging
    # ------------------------------------------------------------------

    def flag_anomalies(self, df: DataFrame) -> DataFrame:
        """
        Flag rows where the z-score exceeds the configured threshold as
        statistical anomalies, and record the direction of the spike.

        Anomaly condition:
          is_anomaly = 1  iff  abs(Z_t) > z_threshold

        Spike direction:
          "up"   — Z_t > +z_threshold  (unusual positive growth)
          "down" — Z_t < -z_threshold  (unusual negative growth / decline)
          "none" — |Z_t| ≤ z_threshold (normal variation)

        The binary ``is_anomaly`` flag is useful for downstream ML classifiers
        as a feature, while ``spike_direction`` is useful for dashboards and
        alerting systems.

        Parameters
        ----------
        df:
            DataFrame with a ``z_score`` column (from Stage 3).

        Returns
        -------
        DataFrame with additional columns:
            is_anomaly      – IntegerType (0 or 1)
            spike_direction – StringType ("up" / "down" / "none")
        """
        threshold = self.z_threshold

        # is_anomaly: 1 if the absolute z-score exceeds the threshold
        df = df.withColumn(
            "is_anomaly",
            F.when(F.abs(F.col("z_score")) > F.lit(threshold), F.lit(1))
            .otherwise(F.lit(0))
            .cast("integer"),
        )

        # spike_direction: directional label for the anomaly
        df = df.withColumn(
            "spike_direction",
            F.when(F.col("z_score") > F.lit(threshold), F.lit("up"))
            .when(F.col("z_score") < F.lit(-threshold), F.lit("down"))
            .otherwise(F.lit("none")),
        )

        return df

    # ------------------------------------------------------------------
    # Stage 5 – Trend classification
    # ------------------------------------------------------------------

    def classify_trend(self, df: DataFrame) -> DataFrame:
        """
        Assign a human-readable trend label to each (keyword, window, bucket)
        observation based on the combined z-score, growth rate, and window size.

        Classification rules (evaluated in priority order):
        -------------------------------------------------------
        1. **viral**    — z_score > viral_z_threshold  AND window == "1h"
                          An extreme spike on the shortest time scale indicates
                          the content is spreading very rapidly right now.

        2. **emerging** — z_score > emerging_z_threshold  (any window)
                          Significant above-average growth; may graduate to
                          "viral" on shorter windows as the trend builds.

        3. **declining**— growth_rate < declining_growth_threshold
                          The keyword is losing mentions faster than its
                          baseline — the trend is past its peak.

        4. **stable**   — all other observations.
                          Normal variation; no notable trend signal.

        The "viral" label is deliberately restricted to the 1h window to
        reduce false positives: a keyword can look "viral" on a 7d window
        simply because it was briefly viral once — we want to capture
        *currently* viral, not *recently* viral.

        Parameters
        ----------
        df:
            DataFrame with columns: z_score, growth_rate, window (from
            Stages 1–4).

        Returns
        -------
        DataFrame with an additional ``trend_label`` column (StringType).
        """
        viral_z = self.viral_z
        emerging_z = self.emerging_z
        declining_gr = self.declining_gr

        df = df.withColumn(
            "trend_label",
            F.when(
                # Rule 1: viral — extreme z-score on the 1-hour window
                (F.col("z_score") > F.lit(viral_z)) & (F.col("window") == F.lit("1h")),
                F.lit("viral"),
            )
            .when(
                # Rule 2: emerging — significant z-score on any window
                F.col("z_score") > F.lit(emerging_z),
                F.lit("emerging"),
            )
            .when(
                # Rule 3: declining — growth rate below the decline threshold
                F.col("growth_rate") < F.lit(declining_gr),
                F.lit("declining"),
            )
            .otherwise(
                # Rule 4: stable — no notable signal
                F.lit("stable")
            ),
        )

        return df
