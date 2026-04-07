"""
HDFS / local filesystem helpers for the TrendBeacon pipeline.

The project runs in two modes:
1. Local development via ``file://`` paths.
2. Hadoop-backed storage via ``hdfs://`` URIs.

Most of the current codebase passes fully qualified paths such as
``file://data/hdfs/features/frequency`` while some modules pass logical
storage layers such as ``features`` plus a subpath.  This helper supports
both styles so upstream modules can stay small and readable.
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any

from config.config_loader import cfg
from config.logging_config import get_logger

logger = get_logger(__name__)


class HDFSUtils:
    """Unified helper for parquet, JSONL, and raw-byte storage."""

    def __init__(self) -> None:
        hdfs_cfg = cfg["hdfs"]
        self.base_uri = self._normalize_base_uri(hdfs_cfg["base_uri"])
        self.paths = hdfs_cfg["paths"]
        logger.info("HDFSUtils initialized | base_uri=%s", self.base_uri)

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    def get_path(self, layer: str, subpath: str = "") -> str:
        """Build a fully qualified path from a logical storage layer."""
        if layer not in self.paths:
            raise ValueError(
                f"Unknown storage layer '{layer}'. Valid: {list(self.paths.keys())}"
            )

        if self.base_uri == "file://":
            base = f"{self.base_uri}{self.paths[layer]}"
        else:
            base = f"{self.base_uri}/{self.paths[layer]}"
        if subpath:
            return f"{base}/{subpath.lstrip('/')}"
        return base

    def resolve_path(self, path_or_layer: str, subpath: str = "") -> str:
        """
        Resolve either a logical layer name or a fully qualified path.
        """
        if path_or_layer in self.paths:
            resolved = self.get_path(path_or_layer, subpath)
        elif subpath:
            resolved = f"{path_or_layer.rstrip('/')}/{subpath.lstrip('/')}"
        else:
            resolved = path_or_layer
        return self._normalize_resolved_path(resolved)

    def dated_path(self, layer: str, source: str, dt: Optional[datetime] = None) -> str:
        """Return a date-partitioned path for a given layer and source."""
        if dt is None:
            dt = datetime.utcnow()
        subpath = (
            f"source={source}/"
            f"year={dt.year:04d}/"
            f"month={dt.month:02d}/"
            f"day={dt.day:02d}"
        )
        return self.get_path(layer, subpath)

    @staticmethod
    def _to_local_path(resolved_path: str) -> str:
        """Convert a file URI into an OS-native local path."""
        if resolved_path.startswith("file:///"):
            uri_path = resolved_path.replace("file:///", "", 1)
            return uri_path.replace("/", os.sep)
        return resolved_path.replace("file://", "")

    @staticmethod
    def _normalize_base_uri(base_uri: str) -> str:
        """Preserve `file://` while trimming extra trailing slashes elsewhere."""
        if base_uri == "file://":
            return base_uri
        return base_uri.rstrip("/")

    @staticmethod
    def _normalize_resolved_path(path: str) -> str:
        """
        Convert local file URIs to absolute Spark-safe file URIs.
        """
        if path.startswith("file://") and not path.startswith("file:///"):
            relative = path.replace("file://", "", 1)
            absolute = Path(relative).resolve().as_posix()
            return f"file:///{absolute}"
        return path

    # ------------------------------------------------------------------
    # Parquet helpers
    # ------------------------------------------------------------------

    def save_parquet(
        self,
        df: DataFrame,
        layer: str,
        subpath: str = "",
        partition_cols: Optional[List[str]] = None,
        mode: str = "append",
    ) -> str:
        """Write parquet using a logical storage layer."""
        return self.write_parquet(
            df=df,
            path_or_layer=self.get_path(layer, subpath),
            partition_cols=partition_cols,
            mode=mode,
        )

    def write_parquet(
        self,
        df: DataFrame,
        path_or_layer: str,
        mode: str = "append",
        partition_cols: Optional[List[str]] = None,
    ) -> str:
        """Write parquet using either a logical layer or a full path."""
        path = self.resolve_path(path_or_layer)
        logger.info(
            "Writing parquet | path=%s | mode=%s | partition_cols=%s",
            path,
            mode,
            partition_cols,
        )

        if path.startswith("file:///") and os.name == "nt":
            local_dir = self._to_local_path(path)
            if mode == "overwrite" and os.path.exists(local_dir):
                shutil.rmtree(local_dir, ignore_errors=True)
            os.makedirs(local_dir, exist_ok=True)
            output_file = os.path.join(local_dir, "part-00000.parquet")
            pdf = df.toPandas()
            pdf.to_parquet(output_file, index=False)
            return path

        writer = df.write.mode(mode).format("parquet")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(path)
        return path

    def load_parquet(
        self,
        layer: str,
        subpath: str = "",
        spark: Optional[SparkSession] = None,
    ) -> DataFrame:
        """Read parquet using a logical storage layer."""
        return self.read_parquet(self.get_path(layer, subpath), spark=spark)

    def read_parquet(
        self,
        path_or_layer: str,
        spark: Optional[SparkSession] = None,
    ) -> DataFrame:
        """Read parquet using either a logical layer or a full path."""
        if spark is None:
            from utils.spark_session import get_spark_session

            spark = get_spark_session()

        path = self.resolve_path(path_or_layer)
        logger.info("Reading parquet | path=%s", path)

        if path.startswith("file:///") and os.name == "nt":
            import pandas as pd

            local_dir = self._to_local_path(path)
            parquet_files = list(Path(local_dir).rglob("*.parquet"))
            if not parquet_files:
                raise FileNotFoundError(f"No parquet files found under {local_dir}")
            pdf = pd.concat([pd.read_parquet(file) for file in parquet_files], ignore_index=True)
            return spark.createDataFrame(pdf)

        return spark.read.parquet(path)

    # ------------------------------------------------------------------
    # JSON lines
    # ------------------------------------------------------------------

    def save_json_lines(
        self,
        records: List[dict],
        layer: str,
        filename: str,
        subpath: str = "",
    ) -> str:
        """Write newline-delimited JSON to local storage."""
        base_path = self.get_path(layer, subpath)
        local_path = self._to_local_path(base_path)
        os.makedirs(local_path, exist_ok=True)

        full_path = os.path.join(local_path, filename)
        with open(full_path, "w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")

        logger.info("Saved %d JSON records to %s", len(records), full_path)
        return full_path

    def load_json_lines(
        self,
        layer: str,
        filename: str,
        subpath: str = "",
    ) -> List[dict]:
        """Read newline-delimited JSON from local storage."""
        base_path = self.get_path(layer, subpath)
        local_path = self._to_local_path(base_path)
        full_path = os.path.join(local_path, filename)

        records: List[dict] = []
        with open(full_path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    records.append(json.loads(line))

        logger.info("Loaded %d JSON records from %s", len(records), full_path)
        return records

    # ------------------------------------------------------------------
    # CSV helpers
    # ------------------------------------------------------------------

    def save_csv(
        self,
        df: DataFrame,
        layer: str,
        subpath: str = "",
        mode: str = "overwrite",
    ) -> str:
        """Write a small Spark DataFrame to CSV."""
        path = self.get_path(layer, subpath)
        df.write.mode(mode).option("header", "true").csv(path)
        logger.info("CSV saved to %s", path)
        return path

    def load_csv(
        self,
        path: str,
        spark: Optional[SparkSession] = None,
        infer_schema: bool = True,
    ) -> DataFrame:
        """Load a CSV file into a Spark DataFrame."""
        if spark is None:
            from utils.spark_session import get_spark_session

            spark = get_spark_session()

        df = (
            spark.read.option("header", "true")
            .option("inferSchema", str(infer_schema))
            .csv(path)
        )
        logger.info("CSV loaded from %s | rows~%d", path, df.count())
        return df

    # ------------------------------------------------------------------
    # Misc helpers used by feature/model modules
    # ------------------------------------------------------------------

    def path_exists(self, path_or_layer: str) -> bool:
        """Check existence for local ``file://`` paths."""
        resolved = self.resolve_path(path_or_layer)
        if resolved.startswith("file://"):
            return os.path.exists(self._to_local_path(resolved))
        return False

    def write_bytes(self, path: str, payload: bytes) -> str:
        """Write binary data to a local ``file://`` path."""
        resolved = self.resolve_path(path)
        local_path = self._to_local_path(resolved)
        parent = os.path.dirname(local_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(local_path, "wb") as handle:
            handle.write(payload)
        logger.info("Binary payload written to %s", local_path)
        return resolved
