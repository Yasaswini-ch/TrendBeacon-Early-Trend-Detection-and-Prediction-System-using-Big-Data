"""
spark_session.py — SparkSession factory for the TrendBeacon pipeline.

Provides a single shared SparkSession configured from config.yaml.
All Spark modules should import `get_spark_session()` instead of
creating their own sessions — this prevents resource duplication.

Usage:
    from utils.spark_session import get_spark_session
    spark = get_spark_session()
    df = spark.read.parquet("data/hdfs/processed/")
"""

import os
import sys

from pyspark.sql import SparkSession

from config.config_loader import cfg
from config.logging_config import get_logger

logger = get_logger(__name__)

# Module-level cached session (singleton pattern)
_spark_session: SparkSession = None


def get_spark_session(app_name: str = None) -> SparkSession:
    """
    Return the shared SparkSession, creating it if it doesn't exist.

    Reads all settings from config.yaml under the `spark` key:
    - master:              Spark master URL (local[*] or yarn)
    - executor_memory:     Memory per executor (e.g. "2g")
    - driver_memory:       Driver memory (e.g. "2g")
    - shuffle_partitions:  Number of shuffle partitions
    - log_level:           Spark internal log verbosity

    Args:
        app_name: Optional override for the Spark application name.

    Returns:
        SparkSession: Active shared session.
    """
    global _spark_session

    if _spark_session is not None and not _spark_session.sparkContext._jsc.sc().isStopped():
        return _spark_session

    spark_cfg = cfg["spark"]
    name = app_name or spark_cfg["app_name"]
    master = spark_cfg["master"]
    if os.name == "nt" and master == "local[*]":
        master = "local[2]"
    shuffle_partitions = int(spark_cfg["shuffle_partitions"])
    if os.name == "nt":
        shuffle_partitions = min(shuffle_partitions, 4)

    logger.info(f"Creating SparkSession: app={name}, master={master}")

    python_executable = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_executable)

    _spark_session = (
        SparkSession.builder
        .appName(name)
        .master(master)
        # Memory settings
        .config("spark.executor.memory",           spark_cfg["executor_memory"])
        .config("spark.driver.memory",             spark_cfg["driver_memory"])
        # Shuffle partitions (use 8 locally, 200+ on cluster)
        .config("spark.sql.shuffle.partitions",    str(shuffle_partitions))
        # Optimizations
        .config("spark.sql.adaptive.enabled",      "true")   # Adaptive Query Execution
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Parquet / columnar optimizations
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.mergeSchema",       "false")
        # Timezone — keep consistent across pipeline
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    # Suppress noisy Spark logs — pipeline uses its own logger
    _spark_session.sparkContext.setLogLevel(spark_cfg.get("log_level", "WARN"))

    logger.info("SparkSession created successfully.")
    return _spark_session


def stop_spark_session() -> None:
    """Stop the active SparkSession. Call this at the end of a pipeline run."""
    global _spark_session
    if _spark_session is not None:
        logger.info("Stopping SparkSession.")
        _spark_session.stop()
        _spark_session = None
