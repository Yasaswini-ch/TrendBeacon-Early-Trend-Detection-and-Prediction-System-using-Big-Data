"""
CLI entrypoint for forecast generation.
"""

from __future__ import annotations

from config.logging_config import get_logger
from model.prophet_model import ProphetForecaster
from utils.spark_session import stop_spark_session

logger = get_logger(__name__)


def run_forecasting() -> None:
    """Generate prediction parquet files from the engineered feature table."""
    try:
        ProphetForecaster().run()
        logger.info("Forecast generation completed successfully.")
    finally:
        stop_spark_session()


if __name__ == "__main__":
    run_forecasting()
