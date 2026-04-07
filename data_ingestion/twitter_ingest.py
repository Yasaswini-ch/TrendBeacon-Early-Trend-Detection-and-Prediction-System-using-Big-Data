"""
twitter_ingest.py — Twitter / X data ingestion for the TrendBeacon pipeline.

Supports two operating modes selected at runtime:

  Real mode   — Calls the Twitter API v2 via the `tweepy` library.
                Requires TWITTER_BEARER_TOKEN to be set as an environment
                variable (referenced in config.yaml as ${TWITTER_BEARER_TOKEN}).

  Simulated   — Falls back automatically when the bearer token is absent or
                when ingestion.simulate=true in config.yaml.
                Delegates to data_generator.generate_tweets() so the rest of
                the pipeline is exercised with realistic synthetic data.

Output is normalised to the RAW_TWEET_SCHEMA field names and optionally
persisted to the HDFS raw layer as a JSONL file via HDFSUtils.

Usage:
    from data_ingestion.twitter_ingest import TwitterIngester
    ingester = TwitterIngester()
    records  = ingester.ingest(save_to_hdfs=True)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from config.config_loader import cfg
from config.logging_config import get_logger
from utils.hdfs_utils import HDFSUtils

logger = get_logger(__name__)

# Tweepy is an optional dependency — import lazily so the module still loads
# in environments where only simulated mode is used.
try:
    import tweepy
    _TWEEPY_AVAILABLE = True
except ImportError:
    tweepy = None  # type: ignore[assignment]
    _TWEEPY_AVAILABLE = False


class TwitterIngester:
    """
    Fetches tweet data from the Twitter API v2 or from the synthetic generator.

    Attributes:
        twitter_cfg (dict): Section from config.yaml under ingestion.twitter.
        simulate (bool):    True if real API calls are disabled.
        client:             tweepy.Client instance, or None in simulated mode.
        hdfs (HDFSUtils):   Storage helper for writing raw output.
    """

    def __init__(self) -> None:
        ingestion_cfg      = cfg.get("ingestion", {})
        self.twitter_cfg   = ingestion_cfg.get("twitter", {})
        self.hdfs          = HDFSUtils()

        # Determine operating mode: simulate flag or missing/unresolved token
        bearer_token = self.twitter_cfg.get("bearer_token", "")
        token_missing = (
            not bearer_token
            or bearer_token.startswith("<")   # placeholder from config_loader
        )

        self.simulate = ingestion_cfg.get("simulate", True) or token_missing

        if self.simulate:
            logger.info("TwitterIngester | mode=SIMULATED (no bearer token / simulate=true)")
            self.client = None
        else:
            if not _TWEEPY_AVAILABLE:
                logger.warning(
                    "TwitterIngester | tweepy not installed — falling back to SIMULATED mode. "
                    "Run: pip install tweepy"
                )
                self.simulate = True
                self.client   = None
            else:
                self.client = tweepy.Client(
                    bearer_token=bearer_token,
                    wait_on_rate_limit=True,
                )
                logger.info("TwitterIngester | mode=REAL | tweepy client initialised")

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def ingest(self, save_to_hdfs: bool = True) -> List[Dict[str, Any]]:
        """
        Run a full ingestion cycle and return normalised tweet records.

        Steps:
          1. Fetch raw records (real API or simulated).
          2. Normalise field names to match RAW_TWEET_SCHEMA.
          3. Optionally persist JSONL to HDFS raw layer.

        Args:
            save_to_hdfs: If True, write records to the raw HDFS layer.

        Returns:
            List[dict]: Normalised tweet records ready for Spark processing.
        """
        logger.info("TwitterIngester.ingest() | starting")

        if self.simulate:
            raw = self._fetch_simulated()
        else:
            query       = self.twitter_cfg.get("search_query", "#AI OR trending")
            max_results = self.twitter_cfg.get("max_results", 100)
            raw         = self._fetch_real(query=query, max_results=max_results)

        records = self._normalize(raw)
        logger.info(f"TwitterIngester.ingest() | normalised {len(records)} records")

        if save_to_hdfs and records:
            filename = (
                f"tweets_{datetime.now(tz=timezone.utc).strftime('%Y%m%d_%H%M%S')}.jsonl"
            )
            self.hdfs.save_json_lines(
                records=records,
                layer="raw",
                filename=filename,
                subpath="source=twitter",
            )

        return records

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch_real(
        self,
        query: str,
        max_results: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Call the Twitter API v2 search/recent endpoint via tweepy.

        Requests the tweet fields needed to fill RAW_TWEET_SCHEMA:
          - public_metrics  (retweet/like/reply counts)
          - entities        (hashtags)
          - author_id, lang, source

        Args:
            query:       Twitter search query string.
            max_results: Maximum number of tweets to retrieve (10–100 per page).

        Returns:
            List[dict]: Raw API response objects serialised as plain dicts.
        """
        logger.info(f"TwitterIngester._fetch_real() | query='{query}' | max_results={max_results}")

        tweet_fields = [
            "id", "text", "created_at", "author_id", "lang",
            "source", "public_metrics", "entities",
        ]

        try:
            response = self.client.search_recent_tweets(
                query=query,
                max_results=min(max(max_results, 10), 100),  # API bounds: 10–100
                tweet_fields=tweet_fields,
            )
        except Exception as exc:
            logger.error(
                f"TwitterIngester._fetch_real() | API call failed: {exc} — "
                "falling back to simulated data"
            )
            return self._fetch_simulated()

        if not response.data:
            logger.warning("TwitterIngester._fetch_real() | API returned no tweets")
            return []

        # tweepy returns Data objects; serialise them to plain dicts for
        # uniform downstream handling.
        raw: List[Dict[str, Any]] = []
        for tweet in response.data:
            raw.append(dict(tweet.data))

        logger.info(f"TwitterIngester._fetch_real() | fetched {len(raw)} tweets from API")
        return raw

    def _fetch_simulated(self) -> List[Dict[str, Any]]:
        """
        Generate synthetic tweet records using the data_generator module.

        The count is taken from config.yaml ingestion.twitter.max_results
        (default 100), scaled up 5× to produce a more meaningful dataset.

        Returns:
            List[dict]: Synthetic tweet records already in normalised schema format.
        """
        # Import lazily to avoid circular dependencies at module load time
        from data_ingestion.data_generator import generate_tweets

        n = self.twitter_cfg.get("max_results", 100) * 10   # produce ~1000 records
        logger.info(f"TwitterIngester._fetch_simulated() | generating {n} synthetic tweets")
        return generate_tweets(n=n)

    def _normalize(self, raw_tweets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Map raw tweet dicts (API or simulated) to RAW_TWEET_SCHEMA field names.

        Simulated records are already in the target schema, so normalisation
        is a no-op for them.  Real API records need field remapping.

        Handles both dict-style and tweepy-object-style inputs gracefully.

        Args:
            raw_tweets: List of raw tweet dicts.

        Returns:
            List[dict]: Records with fields matching RAW_TWEET_SCHEMA.
        """
        normalised: List[Dict[str, Any]] = []

        for raw in raw_tweets:
            # If the record already carries 'data_source', it came from the
            # simulated generator and needs no further transformation.
            if raw.get("data_source") == "twitter":
                normalised.append(raw)
                continue

            # --- Real API response mapping ---
            metrics  = raw.get("public_metrics") or {}
            entities = raw.get("entities")       or {}

            # Extract hashtag strings from the entities block
            hashtag_list: List[str] = [
                tag.get("tag", "") for tag in entities.get("hashtags", [])
                if tag.get("tag")
            ]

            normalised.append({
                "id":            str(raw.get("id", "")),
                "text":          raw.get("text", ""),
                "created_at":    _coerce_str(raw.get("created_at")),
                "author_id":     str(raw.get("author_id", "")),
                "lang":          raw.get("lang", "en"),
                "source":        raw.get("source", "Twitter Web App"),
                "retweet_count": int(metrics.get("retweet_count", 0)),
                "like_count":    int(metrics.get("like_count", 0)),
                "reply_count":   int(metrics.get("reply_count", 0)),
                "hashtags":      ",".join(hashtag_list),
                "data_source":   "twitter",
            })

        logger.debug(f"TwitterIngester._normalize() | output {len(normalised)} records")
        return normalised


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _coerce_str(value: Any) -> Optional[str]:
    """Return str(value) if value is not None, else None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(value)
