"""
Ingestion orchestrator for Twitter/X, Reddit, and news.
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Dict

from config.config_loader import cfg
from config.logging_config import get_logger

logger = get_logger(__name__)


def run_ingestion(simulate: bool = True) -> Dict[str, int]:
    """
    Run all ingestion sources and return record counts.

    When `simulate=False`, each ingester is allowed to use real credentials if
    available. Missing credentials still cause that source to fall back to its
    synthetic mode.
    """
    original_simulate = cfg.get("ingestion", {}).get("simulate")
    cfg.setdefault("ingestion", {})["simulate"] = simulate

    start_time = time.perf_counter()
    logger.info(
        "run_ingestion() | starting | simulate=%s | sources=twitter,reddit,news",
        simulate,
    )

    counts: Dict[str, int] = {}

    try:
        from data_ingestion.twitter_ingest import TwitterIngester

        logger.info("run_ingestion() | [1/3] Starting Twitter ingestion")
        t0 = time.perf_counter()
        twitter_records = TwitterIngester().ingest(save_to_hdfs=True)
        counts["twitter"] = len(twitter_records)
        logger.info(
            "run_ingestion() | [1/3] Twitter complete | records=%s | elapsed=%.2fs",
            counts["twitter"],
            time.perf_counter() - t0,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("run_ingestion() | Twitter ingestion failed: %s", exc, exc_info=True)
        counts["twitter"] = 0

    try:
        from data_ingestion.reddit_ingest import RedditIngester

        logger.info("run_ingestion() | [2/3] Starting Reddit ingestion")
        t0 = time.perf_counter()
        reddit_records = RedditIngester().ingest(save_to_hdfs=True)
        counts["reddit"] = len(reddit_records)
        logger.info(
            "run_ingestion() | [2/3] Reddit complete | records=%s | elapsed=%.2fs",
            counts["reddit"],
            time.perf_counter() - t0,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("run_ingestion() | Reddit ingestion failed: %s", exc, exc_info=True)
        counts["reddit"] = 0

    try:
        from data_ingestion.news_ingest import NewsIngester

        logger.info("run_ingestion() | [3/3] Starting News ingestion")
        t0 = time.perf_counter()
        news_records = NewsIngester().ingest(save_to_hdfs=True)
        counts["news"] = len(news_records)
        logger.info(
            "run_ingestion() | [3/3] News complete | records=%s | elapsed=%.2fs",
            counts["news"],
            time.perf_counter() - t0,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("run_ingestion() | News ingestion failed: %s", exc, exc_info=True)
        counts["news"] = 0
    finally:
        if original_simulate is not None:
            cfg["ingestion"]["simulate"] = original_simulate

    total = sum(counts.values())
    wall = time.perf_counter() - start_time

    logger.info("=" * 60)
    logger.info("run_ingestion() | SUMMARY")
    logger.info("  Twitter  : %6s records", f"{counts.get('twitter', 0):,}")
    logger.info("  Reddit   : %6s records", f"{counts.get('reddit', 0):,}")
    logger.info("  News     : %6s records", f"{counts.get('news', 0):,}")
    logger.info("  TOTAL    : %6s records", f"{total:,}")
    logger.info("  Wall time: %.2fs", wall)
    logger.info("=" * 60)

    return counts


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ingest_runner",
        description="Run TrendBeacon data ingestion for Twitter, Reddit, and News.",
    )
    parser.add_argument(
        "--real",
        action="store_true",
        default=False,
        help="Attempt real API calls instead of synthetic data.",
    )
    return parser


if __name__ == "__main__":
    parser = _build_arg_parser()
    args = parser.parse_args()

    results = run_ingestion(simulate=not args.real)
    if all(v == 0 for v in results.values()):
        logger.error("All sources returned 0 records. Check configuration.")
        sys.exit(1)
    sys.exit(0)
