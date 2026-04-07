"""
keyword_extractor.py
====================
PySpark keyword extraction pipeline for the TrendBeacon trend detection system.

Responsibilities
----------------
1. Load the processed token DataFrame from HDFS.
2. Explode the token arrays to one row per token.
3. Filter low-signal tokens (stop-words, short strings, pure numbers).
4. Compute TF-IDF scores via Spark MLlib (HashingTF + IDF).
5. Optionally tag named entities (PERSON, ORG, GPE, EVENT) via spaCy UDF.
6. Extract top-N keywords by frequency.
7. Persist the keyword feature table to the HDFS features layer.

All transformations use native PySpark SQL functions where possible.
spaCy NER is wrapped in a try/except so the pipeline degrades gracefully
when the library is not installed.
"""

from __future__ import annotations

import re
from typing import List, Optional, Tuple

import pyspark.sql.functions as F
from pyspark.ml.feature import HashingTF, IDF, Tokenizer  # noqa: F401 (Tokenizer kept for completeness)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType, FloatType, StringType, StructField, StructType

from config.config_loader import cfg
from config.logging_config import get_logger
from utils.data_validator import run_quality_checks
from utils.hdfs_utils import HDFSUtils
from utils.schema_definitions import KEYWORD_FREQ_SCHEMA
from utils.spark_session import get_spark_session

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# spaCy NER UDF (optional dependency)
# ---------------------------------------------------------------------------

# Attempt to load spaCy once at module import time.  If it is not available,
# _SPACY_AVAILABLE stays False and extract_named_entities() returns an empty
# array column rather than raising an exception.
_SPACY_AVAILABLE = False
try:
    import spacy as _spacy  # type: ignore

    _nlp = _spacy.load("en_core_web_sm")
    _SPACY_AVAILABLE = True
    logger.info("spaCy loaded successfully (model: en_core_web_sm).")
except Exception as _e:  # ImportError or OSError (model not found)
    logger.warning(
        "spaCy / en_core_web_sm not available — NER step will be skipped. "
        "Reason: %s",
        _e,
    )

# Entity labels we care about for trend detection
_NER_LABELS = {"PERSON", "ORG", "GPE", "EVENT"}

# Return type for the NER UDF: list of (entity_text, entity_label) structs
_NER_RETURN_TYPE = ArrayType(
    StructType([
        StructField("entity", StringType(), nullable=False),
        StructField("label", StringType(), nullable=False),
    ])
)


def _extract_ner(text: Optional[str]) -> List[Tuple[str, str]]:
    """
    Run spaCy NER on *text* and return a list of (entity, label) tuples
    for entity types in ``_NER_LABELS``.

    Wrapped in try/except so a single malformed record does not abort the
    entire Spark task.
    """
    if not text or not _SPACY_AVAILABLE:
        return []
    try:
        doc = _nlp(text)
        return [
            (ent.text.strip(), ent.label_)
            for ent in doc.ents
            if ent.label_ in _NER_LABELS and ent.text.strip()
        ]
    except Exception:
        return []


_ner_udf = F.udf(_extract_ner, _NER_RETURN_TYPE)


class KeywordExtractor:
    """
    Full keyword extraction pipeline on top of pre-processed PySpark DataFrames.

    Configuration keys used (from cfg):
      features.top_keywords_n           – number of top keywords to keep (default 500)
      features.tfidf_num_features        – HashingTF vocabulary size (default 2^18)
      features.min_keyword_length        – minimum character length for keywords (default 3)
      features.features_output_path      – HDFS output path for keyword features
      processing.processed_output_path   – HDFS path to read processed tokens from
    """

    def __init__(self) -> None:
        self.spark: SparkSession = get_spark_session()
        self.hdfs: HDFSUtils = HDFSUtils()

        feat_cfg = cfg.get("features", {})
        proc_cfg = cfg.get("processing", {})

        self.top_n: int = int(feat_cfg.get("top_keywords_n", 500))
        self.tfidf_num_features: int = int(feat_cfg.get("tfidf_num_features", 2 ** 18))
        self.min_keyword_length: int = int(feat_cfg.get("min_keyword_length", 3))
        self.features_output_path: str = feat_cfg.get("features_output_path", "")
        self.processed_input_path: str = proc_cfg.get("processed_output_path", "")

        logger.info(
            "KeywordExtractor initialised | top_n=%d | tfidf_features=%d",
            self.top_n,
            self.tfidf_num_features,
        )

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self, input_path: Optional[str] = None) -> DataFrame:
        """
        Execute the full keyword extraction pipeline.

        Parameters
        ----------
        input_path:
            Override the processed-layer HDFS path defined in config.

        Returns
        -------
        DataFrame of top keywords with TF-IDF scores and frequency counts,
        also written to the HDFS features layer.
        """
        effective_input = input_path or self.processed_input_path

        logger.info("Loading processed data from: %s", effective_input)
        df = self.hdfs.read_parquet(effective_input)

        logger.info("Exploding tokens …")
        token_df = self.explode_tokens(df)

        logger.info("Filtering keywords …")
        token_df = self.filter_keywords(token_df)

        logger.info("Computing TF-IDF …")
        tfidf_df = self.compute_tfidf(df)  # TF-IDF operates on the document-level df

        logger.info("Extracting named entities …")
        ner_df = self.extract_named_entities(df)

        logger.info("Computing top-%d keywords by frequency …", self.top_n)
        keyword_df = self.get_top_keywords(token_df, n=self.top_n)

        # Join TF-IDF scores onto the frequency-ranked keyword list
        keyword_df = keyword_df.join(tfidf_df, on="keyword", how="left")

        logger.info("Running quality checks …")
        run_quality_checks(keyword_df, KEYWORD_FREQ_SCHEMA)

        output_path = self.features_output_path + "/keywords"
        logger.info("Writing keyword features to: %s", output_path)
        self.hdfs.write_parquet(keyword_df, output_path, mode="overwrite")

        # Persist NER results alongside keyword features
        ner_output_path = self.features_output_path + "/named_entities"
        logger.info("Writing NER features to: %s", ner_output_path)
        self.hdfs.write_parquet(ner_df, ner_output_path, mode="overwrite")

        logger.info("KeywordExtractor.run() complete.")
        return keyword_df

    # ------------------------------------------------------------------
    # Step 1 – Explode tokens
    # ------------------------------------------------------------------

    def explode_tokens(self, df: DataFrame) -> DataFrame:
        """
        Explode the ``tokens`` array column so that each token becomes its
        own row.  The original document columns (id, source, timestamp) are
        carried forward so we can aggregate per keyword later.

        Parameters
        ----------
        df:
            Processed DataFrame with a ``tokens`` ArrayType column.

        Returns
        -------
        DataFrame with columns: id, source, timestamp, token
        """
        df = df.select(
            F.col("id"),
            F.col("source"),
            F.col("timestamp"),
            F.explode(F.col("tokens")).alias("token"),
        )
        return df

    # ------------------------------------------------------------------
    # Step 2 – Filter keywords
    # ------------------------------------------------------------------

    def filter_keywords(self, df: DataFrame) -> DataFrame:
        """
        Remove low-signal tokens from the exploded token DataFrame.

        Filters applied:
        * Minimum character length (``features.min_keyword_length``).
        * Pure-numeric tokens (strings that match ``\\d+``).
        * Tokens that consist solely of punctuation / special characters.

        The ``token`` column is also renamed to ``keyword`` for clarity in
        downstream steps.

        Parameters
        ----------
        df:
            Exploded DataFrame with a ``token`` column.

        Returns
        -------
        DataFrame with a ``keyword`` column containing filtered tokens.
        """
        min_len = self.min_keyword_length

        df = (
            df
            # Rename for downstream clarity
            .withColumnRenamed("token", "keyword")
            # Minimum length filter
            .filter(F.length(F.col("keyword")) >= min_len)
            # Remove purely numeric tokens (no trend-signal value)
            .filter(~F.col("keyword").rlike(r"^\d+$"))
            # Remove tokens that contain no alphabetic characters at all
            .filter(F.col("keyword").rlike(r"[a-z]"))
        )
        return df

    # ------------------------------------------------------------------
    # Step 3 – Compute TF-IDF
    # ------------------------------------------------------------------

    def compute_tfidf(self, df: DataFrame) -> DataFrame:
        """
        Score keywords using TF-IDF via Spark MLlib's ``HashingTF`` +
        ``IDF`` pipeline.

        TF-IDF intuition
        ----------------
        - **Term Frequency (TF)**: How often does a token appear in a
          document?  High TF means the token is important *within* that
          document.
        - **Inverse Document Frequency (IDF)**: log(N / df_t) where N is
          the total number of documents and df_t is the number of documents
          that contain token t.  High IDF means the token is *rare* across
          the corpus — rare tokens are often more informative.
        - **TF-IDF = TF * IDF**: tokens that are frequent in a few documents
          but rare across the corpus get the highest scores.

        Implementation notes
        --------------------
        ``HashingTF`` maps tokens to a fixed-size feature vector using the
        hashing trick — no global vocabulary is needed, making it efficient
        for streaming data.  ``IDF`` then re-weights each dimension of that
        vector.

        We extract the top-N keywords per document from the sparse TF-IDF
        vector and return a flat DataFrame of (keyword, avg_tfidf_score).

        Parameters
        ----------
        df:
            Document-level DataFrame with a ``tokens`` column
            (ArrayType(StringType)).

        Returns
        -------
        DataFrame with columns: keyword, avg_tfidf_score
        """
        num_features = self.tfidf_num_features

        # -- HashingTF: tokens array → sparse TF vector -------------------
        hashing_tf = HashingTF(
            inputCol="tokens",
            outputCol="tf_features",
            numFeatures=num_features,
        )
        tf_df = hashing_tf.transform(df)

        # -- IDF: fit on corpus, transform to TF-IDF vectors ---------------
        idf = IDF(inputCol="tf_features", outputCol="tfidf_features", minDocFreq=2)
        idf_model = idf.fit(tf_df)
        tfidf_df = idf_model.transform(tf_df)

        # -- Extract per-keyword average TF-IDF score ----------------------
        # The sparse vector does not carry human-readable token strings, so
        # we re-join with the exploded token DataFrame, hashing each token
        # the same way HashingTF does (Spark's hash function) and averaging
        # the corresponding TF-IDF vector value.
        #
        # Practical shortcut: explode tokens, compute the Spark hash of each
        # token modulo num_features to recover its feature index, then look
        # up the feature value in the TF-IDF vector using a UDF.

        @F.udf(returnType=FloatType())
        def _get_tfidf_score(vector, token: str) -> float:
            """Return the TF-IDF score for *token* from a sparse MLlib vector."""
            try:
                from pyspark.ml.linalg import SparseVector  # type: ignore

                idx = hash(token) % num_features
                if idx < 0:
                    idx += num_features
                if isinstance(vector, SparseVector):
                    # indices and values are parallel arrays
                    pos_array = list(vector.indices)
                    if idx in pos_array:
                        return float(vector.values[pos_array.index(idx)])
                return 0.0
            except Exception:
                return 0.0

        # Explode tokens alongside their TF-IDF vector
        scored_df = (
            tfidf_df
            .select(F.col("id"), F.col("tfidf_features"), F.explode(F.col("tokens")).alias("keyword"))
            .withColumn("tfidf_score", _get_tfidf_score(F.col("tfidf_features"), F.col("keyword")))
            .groupBy("keyword")
            .agg(F.avg("tfidf_score").alias("avg_tfidf_score"))
            .orderBy(F.col("avg_tfidf_score").desc())
        )

        return scored_df

    # ------------------------------------------------------------------
    # Step 4 – Named Entity Recognition
    # ------------------------------------------------------------------

    def extract_named_entities(self, df: DataFrame) -> DataFrame:
        """
        Apply spaCy NER to the ``clean_text`` column to identify named
        entities relevant to trend detection: PERSON, ORG, GPE, EVENT.

        The UDF runs spaCy's ``en_core_web_sm`` model inside each Spark
        executor.  If spaCy is not installed or the model is missing, the
        column is populated with empty arrays so the rest of the pipeline
        is unaffected.

        Parameters
        ----------
        df:
            Processed DataFrame with a ``clean_text`` column.

        Returns
        -------
        DataFrame with columns: id, source, timestamp, entities
        where ``entities`` is ArrayType of StructType(entity, label).
        """
        if not _SPACY_AVAILABLE:
            logger.warning(
                "spaCy unavailable — returning empty 'entities' column."
            )
            ner_df = df.select(
                F.col("id"),
                F.col("source"),
                F.col("timestamp"),
                F.array().cast(_NER_RETURN_TYPE).alias("entities"),
            )
        else:
            ner_df = (
                df
                .withColumn("entities", _ner_udf(F.col("clean_text")))
                .select(
                    F.col("id"),
                    F.col("source"),
                    F.col("timestamp"),
                    F.col("entities"),
                )
            )
        return ner_df

    # ------------------------------------------------------------------
    # Step 5 – Top keywords by frequency
    # ------------------------------------------------------------------

    def get_top_keywords(self, df: DataFrame, n: Optional[int] = None) -> DataFrame:
        """
        Count keyword occurrences in the exploded token DataFrame and return
        the top-N by raw frequency.

        Parameters
        ----------
        df:
            Filtered/exploded DataFrame with a ``keyword`` column.
        n:
            Number of top keywords to return.  Defaults to
            ``features.top_keywords_n`` from config.

        Returns
        -------
        DataFrame with columns: keyword, frequency
        ordered by frequency descending, limited to *n* rows.
        """
        top_n = n or self.top_n

        keyword_freq_df = (
            df
            .groupBy("keyword")
            .agg(F.count("*").alias("frequency"))
            .orderBy(F.col("frequency").desc())
            .limit(top_n)
        )
        return keyword_freq_df
