"""
data_validator.py — Schema validation and data quality checks.

Used after ingestion and after each processing step to ensure
the pipeline doesn't silently propagate bad data.

Usage:
    from utils.data_validator import validate_schema, validate_not_empty

    validate_not_empty(df, "raw tweets")
    validate_schema(df, required_cols=["text", "created_at", "source"])
"""

from typing import Iterable, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from config.logging_config import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Schema / column checks
# ---------------------------------------------------------------------------

def _normalize_required_cols(required_cols) -> List[str]:
    """Accept either a list of names or a Spark StructType."""
    if isinstance(required_cols, StructType):
        return [field.name for field in required_cols]
    if isinstance(required_cols, Iterable) and not isinstance(required_cols, str):
        normalized: List[str] = []
        for item in required_cols:
            normalized.append(getattr(item, "name", str(item)))
        return normalized
    raise TypeError("required_cols must be a list of names or a StructType")


def validate_schema(df: DataFrame, required_cols: List[str]) -> bool:
    """
    Assert that all required columns are present in the DataFrame.

    Args:
        df:            Spark DataFrame to check.
        required_cols: List of column names that must exist.

    Returns:
        bool: True if all columns present.

    Raises:
        ValueError: If any required column is missing.
    """
    normalized_cols = _normalize_required_cols(required_cols)
    existing = set(df.columns)
    missing = [c for c in normalized_cols if c not in existing]

    if missing:
        msg = f"Schema validation FAILED — missing columns: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    logger.info(f"Schema validation PASSED | columns={normalized_cols}")
    return True


def validate_not_empty(df: DataFrame, context: str = "") -> bool:
    """
    Assert that the DataFrame is not empty.

    Args:
        df:      Spark DataFrame to check.
        context: Label for the log message (e.g. "raw tweets").

    Returns:
        bool: True if DataFrame has rows.

    Raises:
        ValueError: If DataFrame is empty.
    """
    count = df.count()
    label = f"[{context}] " if context else ""

    if count == 0:
        msg = f"{label}DataFrame is EMPTY — pipeline cannot continue."
        logger.error(msg)
        raise ValueError(msg)

    logger.info(f"{label}Row count check PASSED | rows={count:,}")
    return True


# ---------------------------------------------------------------------------
# Null / completeness checks
# ---------------------------------------------------------------------------

def validate_null_rate(
    df: DataFrame,
    col: str,
    max_null_fraction: float = 0.3,
    context: str = "",
) -> dict:
    """
    Check what fraction of a column's values are null.

    Args:
        df:                Spark DataFrame.
        col:               Column to check.
        max_null_fraction: Fail if null rate exceeds this (0.0 – 1.0).
        context:           Label for log messages.

    Returns:
        dict: {"column": col, "null_count": n, "null_fraction": f, "passed": bool}
    """
    total = df.count()
    null_count = df.filter(F.col(col).isNull()).count()
    null_fraction = null_count / total if total > 0 else 0.0

    passed = null_fraction <= max_null_fraction
    status = "PASSED" if passed else "FAILED"
    label = f"[{context}] " if context else ""

    logger.info(
        f"{label}Null check {status} | col={col} | "
        f"nulls={null_count:,}/{total:,} ({null_fraction:.1%}) | "
        f"threshold={max_null_fraction:.0%}"
    )

    if not passed:
        logger.warning(
            f"{label}Column '{col}' null rate {null_fraction:.1%} "
            f"exceeds threshold {max_null_fraction:.0%}."
        )

    return {
        "column": col,
        "null_count": null_count,
        "null_fraction": round(null_fraction, 4),
        "passed": passed,
    }


# ---------------------------------------------------------------------------
# Data profile summary
# ---------------------------------------------------------------------------

def profile_dataframe(df: DataFrame, context: str = "") -> None:
    """
    Log a quick data profile: row count, column count, and null rates.

    Args:
        df:      Spark DataFrame to profile.
        context: Label for log messages.
    """
    label = f"[{context}] " if context else ""
    row_count = df.count()
    col_count = len(df.columns)

    logger.info(f"{label}--- DataFrame Profile ---")
    logger.info(f"{label}Rows: {row_count:,} | Columns: {col_count}")
    logger.info(f"{label}Schema: {df.dtypes}")

    # Null counts for each column
    null_exprs = [
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]
    null_counts = df.select(null_exprs).collect()[0].asDict()
    null_summary = {
        k: f"{v:,} ({v/row_count:.1%})"
        for k, v in null_counts.items()
        if v > 0
    }
    if null_summary:
        logger.info(f"{label}Null counts: {null_summary}")
    else:
        logger.info(f"{label}No null values found.")


# ---------------------------------------------------------------------------
# Pipeline stage wrapper
# ---------------------------------------------------------------------------

def run_quality_checks(
    df: DataFrame,
    required_cols: List[str],
    context: str = "",
    null_checks: Optional[List[str]] = None,
    max_null_fraction: float = 0.3,
) -> DataFrame:
    """
    Run a standard battery of quality checks on a DataFrame.

    Combines: not-empty check, schema check, and null rate checks.
    Logs results and raises ValueError on critical failures.

    Args:
        df:                DataFrame to validate.
        required_cols:     Columns that must be present.
        context:           Label for log messages.
        null_checks:       Columns to check for null rate.
        max_null_fraction: Null rate threshold for warnings.

    Returns:
        DataFrame: The original DataFrame (unchanged — pass-through).
    """
    logger.info(f"[{context}] Running quality checks...")

    validate_not_empty(df, context)
    validate_schema(df, required_cols)

    if null_checks:
        for col in null_checks:
            validate_null_rate(df, col, max_null_fraction, context)

    profile_dataframe(df, context)
    logger.info(f"[{context}] All quality checks complete.")
    return df
