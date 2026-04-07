"""
Compute rolling keyword frequencies for TrendBeacon.
"""

from __future__ import annotations

import re
from typing import List, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, lit, window

from config.config_loader import cfg
from config.logging_config import get_logger
from utils.hdfs_utils import HDFSUtils
from utils.spark_session import get_spark_session

logger = get_logger(__name__)


class FrequencyAnalyzer:
    """Aggregate token mentions across multiple time windows."""

    _DEFAULT_WINDOWS: List[str] = ["1h", "6h", "24h", "7d"]

    def __init__(self) -> None:
        self.spark: SparkSession = get_spark_session()
        self.hdfs = HDFSUtils()

        feat_cfg = cfg.get("features", {})
        proc_cfg = cfg.get("processing", {})

        self.windows = feat_cfg.get("frequency_windows", self._DEFAULT_WINDOWS)
        self.slide_fraction = float(feat_cfg.get("frequency_slide_fraction", 1.0))
        self.features_output_path = feat_cfg.get("features_output_path", "")
        self.processed_input_path = proc_cfg.get("processed_output_path", "")

    def run(self, keywords_df: Optional[DataFrame] = None) -> DataFrame:
        """Compute global and per-source frequencies, then persist them."""
        if keywords_df is None:
            keywords_df = self._load_keyword_events()

        global_df = self.compute_all_windows(keywords_df)
        source_df = self.compute_per_source_frequency(keywords_df)
        combined = global_df.unionByName(source_df)

        self.hdfs.write_parquet(
            combined,
            f"{self.features_output_path}/frequency",
            mode="overwrite",
        )
        return combined

    def compute_window_frequency(self, df: DataFrame, window_label: str) -> DataFrame:
        """Compute keyword counts for a single configured time window."""
        spark_window = self._to_spark_window_duration(window_label)
        slide_duration = self._compute_slide_duration(window_label)

        return (
            df.groupBy(
                window(col("timestamp"), spark_window, slide_duration).alias("time_window"),
                col("keyword"),
            )
            .agg(count("*").alias("frequency"))
            .select(
                col("keyword"),
                col("time_window.start").alias("window_start"),
                col("time_window.end").alias("window_end"),
                col("frequency"),
                lit(window_label).alias("window"),
            )
        )

    def compute_all_windows(self, df: DataFrame) -> DataFrame:
        """Compute global frequencies across all configured windows."""
        frames = [
            self.compute_window_frequency(df, label).withColumn("source", lit("__all__"))
            for label in self.windows
        ]
        return self._union_all(frames)

    def compute_per_source_frequency(self, df: DataFrame) -> DataFrame:
        """Compute per-source keyword frequencies across all configured windows."""
        frames: List[DataFrame] = []
        for label in self.windows:
            spark_window = self._to_spark_window_duration(label)
            slide_duration = self._compute_slide_duration(label)
            frame = (
                df.groupBy(
                    window(col("timestamp"), spark_window, slide_duration).alias("time_window"),
                    col("keyword"),
                    col("source"),
                )
                .agg(count("*").alias("frequency"))
                .select(
                    col("keyword"),
                    col("time_window.start").alias("window_start"),
                    col("time_window.end").alias("window_end"),
                    col("frequency"),
                    lit(label).alias("window"),
                    col("source"),
                )
            )
            frames.append(frame)
        return self._union_all(frames)

    def _load_keyword_events(self) -> DataFrame:
        """Load processed records and explode tokens into keyword events."""
        raw_df = self.hdfs.read_parquet(self.processed_input_path, spark=self.spark)
        return (
            raw_df.select(
                F.explode(F.col("tokens")).alias("keyword"),
                F.col("timestamp"),
                F.col("source"),
            )
            .filter(F.col("keyword").isNotNull() & (F.col("keyword") != ""))
        )

    def _compute_slide_duration(self, window_label: str) -> str:
        """Compute slide duration in seconds from compact window labels."""
        unit_map = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        match = re.fullmatch(r"(\d+)([a-z]+)", window_label.strip().lower())
        if not match:
            raise ValueError(f"Cannot parse window string '{window_label}'.")
        value = int(match.group(1))
        unit = match.group(2)
        if unit not in unit_map:
            raise ValueError(f"Unsupported window unit '{unit}' in '{window_label}'.")
        total_seconds = value * unit_map[unit]
        slide_seconds = max(1, int(total_seconds * self.slide_fraction))
        return f"{slide_seconds} seconds"

    @staticmethod
    def _to_spark_window_duration(window_label: str) -> str:
        """Convert compact labels like `1h` to Spark interval strings."""
        unit_map = {"s": "second", "m": "minute", "h": "hour", "d": "day"}
        match = re.fullmatch(r"(\d+)([a-z]+)", window_label.strip().lower())
        if not match:
            raise ValueError(f"Cannot parse window string '{window_label}'.")
        value = int(match.group(1))
        unit = match.group(2)
        if unit not in unit_map:
            raise ValueError(f"Unsupported window unit '{unit}' in '{window_label}'.")
        noun = unit_map[unit]
        suffix = "" if value == 1 else "s"
        return f"{value} {noun}{suffix}"

    @staticmethod
    def _union_all(frames: List[DataFrame]) -> DataFrame:
        """Union a list of Spark DataFrames."""
        if not frames:
            raise RuntimeError("No DataFrames were generated for frequency analysis.")
        combined = frames[0]
        for frame in frames[1:]:
            combined = combined.unionByName(frame)
        return combined
