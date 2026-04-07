"""
news_ingest.py — News data ingestion for the TrendBeacon pipeline.

Supports two operating modes:

  Real mode   — Fetches articles from RSS feeds listed in config.yaml
                (ingestion.news.rss_feeds) using the `feedparser` library.
                No API key required for public RSS feeds.

  Simulated   — Falls back automatically when feedparser is unavailable, when
                no feeds are configured, or when ingestion.simulate=true in
                config.yaml.  Delegates to data_generator.generate_news_articles().

Output is normalised to the RAW_NEWS_SCHEMA field names and optionally
persisted to the HDFS raw layer as JSONL via HDFSUtils.

Usage:
    from data_ingestion.news_ingest import NewsIngester
    ingester = NewsIngester()
    records  = ingester.ingest(save_to_hdfs=True)
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional

from config.config_loader import cfg
from config.logging_config import get_logger
from utils.hdfs_utils import HDFSUtils

logger = get_logger(__name__)

# feedparser is an optional dependency — import lazily
try:
    import feedparser
    _FEEDPARSER_AVAILABLE = True
except ImportError:
    feedparser = None  # type: ignore[assignment]
    _FEEDPARSER_AVAILABLE = False


class NewsIngester:
    """
    Fetches news article data from RSS feeds or from the synthetic generator.

    Attributes:
        news_cfg (dict):   Section from config.yaml under ingestion.news.
        simulate (bool):   True when real RSS fetching is disabled.
        hdfs (HDFSUtils):  Storage helper for writing raw output.
    """

    def __init__(self) -> None:
        ingestion_cfg  = cfg.get("ingestion", {})
        self.news_cfg  = ingestion_cfg.get("news", {})
        self.hdfs      = HDFSUtils()

        feeds_configured = bool(self.news_cfg.get("rss_feeds"))

        self.simulate = (
            ingestion_cfg.get("simulate", True)
            or not feeds_configured
            or not _FEEDPARSER_AVAILABLE
        )

        if self.simulate:
            reason = "simulate=true" if ingestion_cfg.get("simulate", True) else (
                "feedparser not installed" if not _FEEDPARSER_AVAILABLE
                else "no rss_feeds configured"
            )
            logger.info(f"NewsIngester | mode=SIMULATED ({reason})")
            if not _FEEDPARSER_AVAILABLE and not ingestion_cfg.get("simulate", True):
                logger.warning(
                    "NewsIngester | feedparser not installed — falling back to SIMULATED mode. "
                    "Run: pip install feedparser"
                )
        else:
            logger.info(
                f"NewsIngester | mode=REAL | "
                f"{len(self.news_cfg['rss_feeds'])} RSS feed(s) configured"
            )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def ingest(self, save_to_hdfs: bool = True) -> List[Dict[str, Any]]:
        """
        Run a full ingestion cycle and return normalised news article records.

        Steps:
          1. Fetch raw records (RSS feeds or simulated).
          2. Normalise field names to match RAW_NEWS_SCHEMA.
          3. Optionally persist JSONL to HDFS raw layer.

        Args:
            save_to_hdfs: If True, write records to the raw HDFS layer.

        Returns:
            List[dict]: Normalised article records ready for Spark processing.
        """
        logger.info("NewsIngester.ingest() | starting")

        if self.simulate:
            raw = self._fetch_simulated()
        else:
            feeds = self.news_cfg.get("rss_feeds", [])
            raw   = self._fetch_real(feeds=feeds)

        records = self._normalize(raw)
        logger.info(f"NewsIngester.ingest() | normalised {len(records)} records")

        if save_to_hdfs and records:
            filename = (
                f"news_{datetime.now(tz=timezone.utc).strftime('%Y%m%d_%H%M%S')}.jsonl"
            )
            self.hdfs.save_json_lines(
                records=records,
                layer="raw",
                filename=filename,
                subpath="source=news",
            )

        return records

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch_real(self, feeds: List[str]) -> List[Dict[str, Any]]:
        """
        Parse one or more RSS feeds using feedparser and extract article entries.

        For each feed entry we capture:
          - title, summary, published date, source name, and canonical link.

        Duplicate articles (same link) across feeds are silently deduplicated.

        Args:
            feeds: List of RSS feed URLs.

        Returns:
            List[dict]: Raw article dicts (pre-normalisation).
        """
        logger.info(f"NewsIngester._fetch_real() | parsing {len(feeds)} RSS feed(s)")

        seen_links: set = set()
        raw: List[Dict[str, Any]] = []

        for feed_url in feeds:
            try:
                parsed = feedparser.parse(feed_url)
                feed_title = parsed.feed.get("title", feed_url)

                if parsed.bozo:
                    # feedparser sets .bozo when it encounters a malformed feed
                    logger.warning(
                        f"NewsIngester._fetch_real() | malformed feed: {feed_url} "
                        f"({parsed.bozo_exception})"
                    )

                for entry in parsed.entries:
                    link = entry.get("link", "")
                    if link in seen_links:
                        continue
                    seen_links.add(link)

                    raw.append({
                        "title":     entry.get("title", ""),
                        "summary":   entry.get("summary", ""),
                        "published": entry.get("published", ""),
                        "source":    feed_title,
                        "link":      link,
                    })

            except Exception as exc:
                logger.error(
                    f"NewsIngester._fetch_real() | failed to parse {feed_url}: {exc}"
                )
                continue

        logger.info(
            f"NewsIngester._fetch_real() | fetched {len(raw)} articles from RSS feeds"
        )
        return raw

    def _fetch_simulated(self) -> List[Dict[str, Any]]:
        """
        Generate synthetic news article records using the data_generator module.

        Returns:
            List[dict]: Synthetic article records already in normalised schema format.
        """
        from data_ingestion.data_generator import generate_news_articles

        n = 300
        logger.info(
            f"NewsIngester._fetch_simulated() | generating {n} synthetic articles"
        )
        return generate_news_articles(n=n)

    def _normalize(self, raw_articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Map raw article dicts (RSS or simulated) to RAW_NEWS_SCHEMA field names.

        Simulated records already carry all required fields and pass through
        unchanged.  RSS records need ID generation and date standardisation.

        ID generation: SHA-1 hash of the article link (deterministic, so
        re-ingesting the same feed produces the same IDs).

        Published date normalisation: feedparser delivers RFC 2822 strings
        (e.g. "Mon, 06 Jan 2025 12:00:00 GMT") which are converted to ISO 8601.
        If parsing fails the raw string is preserved.

        Args:
            raw_articles: List of raw article dicts.

        Returns:
            List[dict]: Records with fields matching RAW_NEWS_SCHEMA.
        """
        normalised: List[Dict[str, Any]] = []

        for raw in raw_articles:
            # Simulated records already carry data_source and a proper id
            if raw.get("data_source") == "news":
                normalised.append(raw)
                continue

            # --- RSS record mapping ---
            link       = raw.get("link", "")
            article_id = _link_to_id(link)
            published  = _parse_date(raw.get("published", ""))

            normalised.append({
                "id":          article_id,
                "title":       raw.get("title", ""),
                "summary":     _truncate(raw.get("summary", ""), max_chars=1000),
                "published":   published,
                "source":      raw.get("source", ""),
                "link":        link,
                "data_source": "news",
            })

        logger.debug(f"NewsIngester._normalize() | output {len(normalised)} records")
        return normalised


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _link_to_id(link: str) -> str:
    """
    Derive a short, deterministic record ID from an article URL.

    Uses the first 16 hex characters of a SHA-1 digest so the ID is stable
    across re-runs yet compact enough to store cheaply.
    """
    return "n_" + hashlib.sha1(link.encode("utf-8")).hexdigest()[:14]


def _parse_date(date_str: str) -> str:
    """
    Attempt to parse an RFC 2822 or ISO 8601 date string and return ISO 8601.

    Returns the original string unchanged if parsing fails.
    """
    if not date_str:
        return ""
    # Try RFC 2822 (standard RSS format)
    try:
        dt = parsedate_to_datetime(date_str)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        pass
    # Try ISO 8601 (Atom feeds)
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(date_str[:19], fmt[:len(date_str[:19])])
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
    # Return as-is if nothing matched
    return date_str


def _truncate(text: str, max_chars: int = 1000) -> str:
    """Truncate text to max_chars, appending '...' if truncated."""
    if len(text) <= max_chars:
        return text
    return text[:max_chars - 3] + "..."
