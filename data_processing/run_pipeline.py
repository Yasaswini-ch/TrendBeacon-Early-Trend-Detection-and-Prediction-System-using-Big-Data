"""
CLI orchestration for the TrendBeacon processing pipeline.

Stages:
1. Optional ingestion
2. Text preprocessing
3. Frequency analysis
4. Early trend detection
5. Feature engineering
"""

from __future__ import annotations

import argparse

from config.logging_config import get_logger
from data_ingestion.ingest_runner import run_ingestion
from data_processing.frequency_analyzer import FrequencyAnalyzer
from data_processing.text_preprocessor import TextPreprocessor
from data_processing.trend_detector import TrendDetector
from features.feature_builder import FeatureBuilder
from utils.spark_session import stop_spark_session

logger = get_logger(__name__)


def run_pipeline(run_ingestion_first: bool = False, simulate: bool = True) -> None:
    """Run the full batch pipeline end to end."""
    try:
        if run_ingestion_first:
            logger.info("Running ingestion before processing.")
            run_ingestion(simulate=simulate)

        processed_df = TextPreprocessor().run()
        freq_df = FrequencyAnalyzer().run(processed_df.selectExpr("explode(tokens) as keyword", "timestamp", "source"))
        trend_df = TrendDetector().run(freq_df)
        FeatureBuilder().run(freq_df=freq_df, trend_df=trend_df)
        logger.info("TrendBeacon processing pipeline finished successfully.")
    finally:
        stop_spark_session()


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the TrendBeacon processing pipeline.")
    parser.add_argument("--with-ingestion", action="store_true", help="Run ingestion before processing.")
    parser.add_argument("--real", action="store_true", help="Use real APIs where configured.")
    return parser


if __name__ == "__main__":
    args = _build_arg_parser().parse_args()
    run_pipeline(run_ingestion_first=args.with_ingestion, simulate=not args.real)
