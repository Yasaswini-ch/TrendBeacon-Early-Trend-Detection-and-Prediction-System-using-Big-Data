"""
Feature engineering for the TrendBeacon pipeline.

Builds a wide feature table keyed by:
    keyword, timestamp

Inputs:
    - frequency parquet produced by FrequencyAnalyzer
    - trend parquet produced by TrendDetector
"""

from __future__ import annotations

import time
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType

from config.config_loader import cfg
from config.logging_config import get_logger
from utils.hdfs_utils import HDFSUtils
from utils.schema_definitions import FEATURE_SCHEMA
from utils.spark_session import get_spark_session

logger = get_logger(__name__)

WINDOWS = ["1h", "6h", "24h", "7d"]


class FeatureBuilder:
    """Assemble research-friendly keyword features from pipeline outputs."""

    def __init__(self) -> None:
        self.spark: SparkSession = get_spark_session()
        self.hdfs = HDFSUtils()

        features_root = cfg["hdfs"]["paths"]["features"]
        trends_root = cfg["hdfs"]["paths"]["trends"]

        base_uri = cfg["hdfs"]["base_uri"]
        if base_uri == "file://":
            self.frequency_path = f"{base_uri}{features_root}/frequency"
            self.trend_path = f"{base_uri}{trends_root}"
            self.output_path = f"{base_uri}{features_root}"
        else:
            self.frequency_path = f"{base_uri}/{features_root}/frequency"
            self.trend_path = f"{base_uri}/{trends_root}"
            self.output_path = f"{base_uri}/{features_root}"

    def run(
        self,
        freq_df: Optional[DataFrame] = None,
        trend_df: Optional[DataFrame] = None,
    ) -> DataFrame:
        """Build and persist the final feature table."""
        start = time.time()

        if freq_df is None:
            freq_df = self.hdfs.read_parquet(self.frequency_path)
        if trend_df is None:
            trend_df = self.hdfs.read_parquet(self.trend_path)

        feature_df = self.build_features(freq_df, trend_df)
        stamped_path = f"{self.output_path}/dt={time.strftime('%Y-%m-%d')}"
        self.hdfs.write_parquet(feature_df, stamped_path, mode="overwrite")

        logger.info(
            "FeatureBuilder complete | rows=%d | output=%s | elapsed=%.1fs",
            feature_df.count(),
            stamped_path,
            time.time() - start,
        )
        return feature_df

    def build_features(self, freq_df: DataFrame, trend_df: DataFrame) -> DataFrame:
        """Create a schema-aligned wide feature table."""
        global_freq = freq_df.filter(F.col("source") == "__all__")

        frequency_wide = (
            global_freq.groupBy("keyword", F.col("window_start").alias("timestamp"))
            .pivot("window", WINDOWS)
            .agg(F.first("frequency"))
            .fillna(0)
            .withColumnRenamed("1h", "freq_1h")
            .withColumnRenamed("6h", "freq_6h")
            .withColumnRenamed("24h", "freq_24h")
            .withColumnRenamed("7d", "freq_7d")
        )

        source_diversity = (
            freq_df.filter(F.col("source") != "__all__")
            .groupBy("keyword", F.col("window_start").alias("timestamp"))
            .agg(F.countDistinct("source").alias("source_diversity"))
        )

        trend_wide = self._pivot_trend_metrics(trend_df)

        features = (
            frequency_wide.join(trend_wide, on=["keyword", "timestamp"], how="left")
            .join(source_diversity, on=["keyword", "timestamp"], how="left")
            .withColumn("avg_sentiment", F.lit(0.0).cast("float"))
            .fillna(0, subset=["source_diversity", "is_anomaly"])
            .fillna("stable", subset=["trend_label"])
        )

        for freq_col in ["freq_1h", "freq_6h", "freq_24h", "freq_7d"]:
            if freq_col in features.columns:
                features = features.withColumn(freq_col, F.col(freq_col).cast("long"))

        select_exprs = []
        for field in FEATURE_SCHEMA:
            if field.name in features.columns:
                select_exprs.append(F.col(field.name).cast(field.dataType).alias(field.name))
            else:
                select_exprs.append(F.lit(None).cast(field.dataType).alias(field.name))

        return features.select(select_exprs)

    def _pivot_trend_metrics(self, trend_df: DataFrame) -> DataFrame:
        """Pivot per-window trend metrics into feature columns."""
        global_trends = trend_df.filter(F.col("source") == "__all__")

        aggregated = (
            global_trends.groupBy("keyword", F.col("window_start").alias("timestamp"))
            .agg(
                F.max(F.when(F.col("window") == "1h", F.col("growth_rate"))).alias("growth_rate_1h"),
                F.max(F.when(F.col("window") == "24h", F.col("growth_rate"))).alias("growth_rate_24h"),
                F.max(F.when(F.col("window") == "1h", F.col("z_score"))).alias("z_score_1h"),
                F.max(F.when(F.col("window") == "24h", F.col("z_score"))).alias("z_score_24h"),
                F.max(F.when(F.col("window") == "24h", F.col("velocity"))).alias("velocity"),
                F.max(F.when(F.col("window") == "24h", F.col("acceleration"))).alias("acceleration"),
                F.max(F.col("is_anomaly").cast(IntegerType())).alias("is_anomaly"),
                F.max(
                    F.when(F.col("trend_label") == "viral", F.lit(4))
                    .when(F.col("trend_label") == "emerging", F.lit(3))
                    .when(F.col("trend_label") == "declining", F.lit(2))
                    .otherwise(F.lit(1))
                ).alias("_label_rank"),
            )
        )

        label_col = (
            F.when(F.col("_label_rank") == 4, F.lit("viral"))
            .when(F.col("_label_rank") == 3, F.lit("emerging"))
            .when(F.col("_label_rank") == 2, F.lit("declining"))
            .otherwise(F.lit("stable"))
        )

        return (
            aggregated.withColumn("trend_label", label_col.cast(StringType()))
            .drop("_label_rank")
            .withColumn("growth_rate_1h", F.col("growth_rate_1h").cast(DoubleType()))
            .withColumn("growth_rate_24h", F.col("growth_rate_24h").cast(DoubleType()))
            .withColumn("z_score_1h", F.col("z_score_1h").cast(DoubleType()))
            .withColumn("z_score_24h", F.col("z_score_24h").cast(DoubleType()))
            .withColumn("velocity", F.col("velocity").cast(DoubleType()))
            .withColumn("acceleration", F.col("acceleration").cast(DoubleType()))
        )
