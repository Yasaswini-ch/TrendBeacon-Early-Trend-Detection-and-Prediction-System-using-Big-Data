"""
PySpark text normalization pipeline for TrendBeacon.

This module converts raw Twitter, Reddit, and news records into one canonical
processed schema:
    id, text, clean_text, tokens, timestamp, source, engagement, lang
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import List, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import TimestampType

from config.config_loader import cfg
from config.logging_config import get_logger
from utils.hdfs_utils import HDFSUtils
from utils.spark_session import get_spark_session

logger = get_logger(__name__)


class TextPreprocessor:
    """Normalize heterogeneous raw input into the project processed schema."""

    def __init__(self) -> None:
        self.spark: SparkSession = get_spark_session()
        self.hdfs = HDFSUtils()

        proc_cfg = cfg.get("processing", {})
        self.raw_input_paths: List[str] = proc_cfg.get("raw_input_paths", [])
        self.processed_output_path: str = proc_cfg.get("processed_output_path", "")
        self.min_token_length: int = int(proc_cfg.get("min_token_length", 3))
        self.extra_stopwords: List[str] = proc_cfg.get("extra_stopwords", [])
        self._stopwords = self._build_stopword_list()

        logger.info(
            "TextPreprocessor initialized | raw_paths=%s | output=%s",
            self.raw_input_paths,
            self.processed_output_path,
        )

    def run(
        self,
        input_path: Optional[str] = None,
        output_path: Optional[str] = None,
    ) -> DataFrame:
        """Execute the full preprocessing pipeline."""
        effective_input = [input_path] if input_path else self.raw_input_paths
        effective_output = output_path or self.processed_output_path

        df = self.load_raw_data(effective_input)
        df = self.clean_text(df)
        df = self.tokenize(df)
        df = self.normalize_timestamps(df)

        if df.limit(1).count() == 0:
            raise RuntimeError("Processed dataset is empty after preprocessing.")

        self.hdfs.write_parquet(df, effective_output, mode="overwrite")
        logger.info("Processed data written to %s", effective_output)
        return df

    def load_raw_data(self, paths: List[str]) -> DataFrame:
        """Read raw JSONL files from all ingestion sources and normalize columns."""
        records = []

        for path in paths:
            resolved_path = self.hdfs.resolve_path(path)
            if not self.hdfs.path_exists(path):
                logger.warning("Raw input path missing, skipping: %s", path)
                continue

            local_dir = Path(self.hdfs._to_local_path(resolved_path))
            jsonl_files = sorted(local_dir.glob("*.jsonl"))
            if not jsonl_files:
                logger.warning("No JSONL files found in %s", local_dir)
                continue

            raw_rows = []
            for jsonl_file in jsonl_files:
                with open(jsonl_file, "r", encoding="utf-8") as handle:
                    for line in handle:
                        line = line.strip()
                        if line:
                            raw_rows.append(json.loads(line))

            if not raw_rows:
                continue

            raw_df = self.spark.createDataFrame(raw_rows)
            source_name = self._infer_source_name(path)
            normalized = self._normalize_source_frame(raw_df, source_name)
            records.append(normalized)

        if not records:
            raise RuntimeError("No raw ingestion data found. Run ingestion first.")

        unified = records[0]
        for frame in records[1:]:
            unified = unified.unionByName(frame)

        return unified.filter(F.col("text").isNotNull() & (F.trim(F.col("text")) != ""))

    def _normalize_source_frame(self, raw_df: DataFrame, source_name: str) -> DataFrame:
        """Map source-specific raw columns into the canonical intermediate layout."""
        if source_name == "twitter":
            return raw_df.select(
                F.col("id").cast("string").alias("id"),
                F.col("text").cast("string").alias("text"),
                F.col("created_at").cast("string").alias("timestamp"),
                F.lit("twitter").alias("source"),
                (
                    F.coalesce(F.col("like_count"), F.lit(0))
                    + F.coalesce(F.col("retweet_count"), F.lit(0))
                    + F.coalesce(F.col("reply_count"), F.lit(0))
                ).cast("int").alias("engagement"),
                F.coalesce(F.col("lang"), F.lit("en")).alias("lang"),
            )

        if source_name == "reddit":
            return raw_df.select(
                F.col("id").cast("string").alias("id"),
                F.concat_ws(" ", F.col("title"), F.col("selftext")).alias("text"),
                F.col("created_utc").cast("string").alias("timestamp"),
                F.lit("reddit").alias("source"),
                (
                    F.coalesce(F.col("score"), F.lit(0))
                    + F.coalesce(F.col("num_comments"), F.lit(0))
                ).cast("int").alias("engagement"),
                F.lit("en").alias("lang"),
            )

        if source_name == "news":
            return raw_df.select(
                F.col("id").cast("string").alias("id"),
                F.concat_ws(" ", F.col("title"), F.col("summary")).alias("text"),
                F.col("published").cast("string").alias("timestamp"),
                F.lit("news").alias("source"),
                F.lit(0).cast("int").alias("engagement"),
                F.lit("en").alias("lang"),
            )

        raise ValueError(f"Unsupported source: {source_name}")

    @staticmethod
    def _infer_source_name(path: str) -> str:
        """Infer source name from a configured raw path."""
        lowered = path.lower()
        for source in ("twitter", "reddit", "news"):
            if source in lowered:
                return source
        raise ValueError(f"Could not infer source from path: {path}")

    def clean_text(self, df: DataFrame) -> DataFrame:
        """Remove URLs/noise and create a normalized `clean_text` column."""
        cleaned = (
            df.withColumn(
                "clean_text",
                F.regexp_replace(F.col("text"), r"https?://\S+|www\.\S+", " "),
            )
            .withColumn("clean_text", F.lower(F.col("clean_text")))
            .withColumn(
                "clean_text",
                F.regexp_replace(F.col("clean_text"), r"[^a-z0-9\s#@]", " "),
            )
            .withColumn(
                "clean_text",
                F.trim(F.regexp_replace(F.col("clean_text"), r"\s{2,}", " ")),
            )
        )
        return cleaned.filter(F.col("lang").isNull() | (F.col("lang") == "en"))

    def tokenize(self, df: DataFrame) -> DataFrame:
        """Tokenize `clean_text` with native Spark expressions."""
        min_length = self.min_token_length
        stopword_literals = F.array(*[F.lit(word) for word in self._stopwords])

        tokens = F.split(F.col("clean_text"), r"\s+")
        filtered = F.filter(
            tokens,
            lambda token: (
                (F.length(token) >= F.lit(min_length))
                & token.rlike("[a-z]")
                & (~token.rlike(r"^[\d\W]+$"))
                & (~F.array_contains(stopword_literals, token))
            ),
        )
        return df.withColumn("tokens", filtered)

    def normalize_timestamps(self, df: DataFrame) -> DataFrame:
        """Normalize ISO strings and unix-epoch strings into TimestampType."""
        ts_col = F.col("timestamp")
        parsed = F.coalesce(
            F.to_timestamp(ts_col, "yyyy-MM-dd'T'HH:mm:ssX"),
            F.to_timestamp(ts_col, "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(ts_col, "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(F.from_unixtime(ts_col.cast("long"))),
        )
        return df.withColumn("timestamp", parsed.cast(TimestampType()))

    def _build_stopword_list(self) -> List[str]:
        """Build stopwords from NLTK when available, otherwise use a fallback."""
        try:
            import nltk  # type: ignore
            from nltk.corpus import stopwords as nltk_stopwords  # type: ignore

            try:
                base = set(nltk_stopwords.words("english"))
            except LookupError:
                nltk.download("stopwords", quiet=True)
                base = set(nltk_stopwords.words("english"))
        except Exception:
            base = {
                "a", "an", "and", "are", "as", "at", "be", "but", "by", "for",
                "from", "has", "have", "if", "in", "is", "it", "its", "of",
                "on", "or", "that", "the", "their", "this", "to", "was",
                "were", "will", "with",
            }

        return sorted(base.union(set(self.extra_stopwords)))
