"""
reddit_ingest.py — Reddit data ingestion for the TrendBeacon pipeline.

Supports two operating modes:

  Real mode   — Reads posts from configured subreddits via the `praw` library
                (Python Reddit API Wrapper).  Requires REDDIT_CLIENT_ID and
                REDDIT_CLIENT_SECRET to be set as environment variables.

  Simulated   — Falls back automatically when credentials are absent or when
                ingestion.simulate=true in config.yaml.  Delegates to
                data_generator.generate_reddit_posts() for synthetic data.

Output is normalised to the RAW_REDDIT_SCHEMA field names and optionally
persisted to the HDFS raw layer as JSONL via HDFSUtils.

Usage:
    from data_ingestion.reddit_ingest import RedditIngester
    ingester = RedditIngester()
    records  = ingester.ingest(save_to_hdfs=True)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from config.config_loader import cfg
from config.logging_config import get_logger
from utils.hdfs_utils import HDFSUtils

logger = get_logger(__name__)

# PRAW is an optional dependency — import lazily so the module still loads
# when only simulated mode is used.
try:
    import praw
    _PRAW_AVAILABLE = True
except ImportError:
    praw = None  # type: ignore[assignment]
    _PRAW_AVAILABLE = False


class RedditIngester:
    """
    Fetches Reddit post data via PRAW or from the synthetic generator.

    Attributes:
        reddit_cfg (dict): Section from config.yaml under ingestion.reddit.
        simulate (bool):   True when real API calls are disabled.
        reddit:            praw.Reddit instance, or None in simulated mode.
        hdfs (HDFSUtils):  Storage helper for writing raw output.
    """

    def __init__(self) -> None:
        ingestion_cfg    = cfg.get("ingestion", {})
        self.reddit_cfg  = ingestion_cfg.get("reddit", {})
        self.hdfs        = HDFSUtils()

        # Determine operating mode
        client_id     = self.reddit_cfg.get("client_id", "")
        client_secret = self.reddit_cfg.get("client_secret", "")
        credentials_missing = (
            not client_id     or client_id.startswith("<")
            or not client_secret or client_secret.startswith("<")
        )

        self.simulate = ingestion_cfg.get("simulate", True) or credentials_missing

        if self.simulate:
            logger.info("RedditIngester | mode=SIMULATED (no credentials / simulate=true)")
            self.reddit = None
        else:
            if not _PRAW_AVAILABLE:
                logger.warning(
                    "RedditIngester | praw not installed — falling back to SIMULATED mode. "
                    "Run: pip install praw"
                )
                self.simulate = True
                self.reddit   = None
            else:
                self.reddit = praw.Reddit(
                    client_id=client_id,
                    client_secret=client_secret,
                    user_agent=self.reddit_cfg.get("user_agent", "TrendBeacon/1.0"),
                    # Read-only mode — no username/password required for public data
                )
                logger.info("RedditIngester | mode=REAL | praw client initialised")

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def ingest(self, save_to_hdfs: bool = True) -> List[Dict[str, Any]]:
        """
        Run a full ingestion cycle and return normalised Reddit post records.

        Steps:
          1. Fetch raw records (real API or simulated).
          2. Normalise field names to match RAW_REDDIT_SCHEMA.
          3. Optionally persist JSONL to HDFS raw layer.

        Args:
            save_to_hdfs: If True, write records to the raw HDFS layer.

        Returns:
            List[dict]: Normalised post records ready for Spark processing.
        """
        logger.info("RedditIngester.ingest() | starting")

        if self.simulate:
            raw = self._fetch_simulated()
        else:
            subreddits = self.reddit_cfg.get(
                "subreddits", ["technology", "worldnews", "science", "politics"]
            )
            limit = self.reddit_cfg.get("post_limit", 200)
            raw   = self._fetch_real(subreddits=subreddits, limit=limit)

        records = self._normalize(raw)
        logger.info(f"RedditIngester.ingest() | normalised {len(records)} records")

        if save_to_hdfs and records:
            filename = (
                f"reddit_{datetime.now(tz=timezone.utc).strftime('%Y%m%d_%H%M%S')}.jsonl"
            )
            self.hdfs.save_json_lines(
                records=records,
                layer="raw",
                filename=filename,
                subpath="source=reddit",
            )

        return records

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch_real(
        self,
        subreddits: List[str],
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """
        Fetch the hottest posts from each configured subreddit via PRAW.

        The `limit` is applied per subreddit, so total records returned may be
        up to len(subreddits) × limit before deduplication.

        Args:
            subreddits: List of subreddit names (without the r/ prefix).
            limit:      Maximum posts to retrieve per subreddit.

        Returns:
            List[dict]: Raw PRAW submission attributes as plain dicts.
        """
        logger.info(
            f"RedditIngester._fetch_real() | subreddits={subreddits} | limit={limit}"
        )

        raw: List[Dict[str, Any]] = []

        for sub_name in subreddits:
            try:
                subreddit = self.reddit.subreddit(sub_name)
                for submission in subreddit.hot(limit=limit):
                    raw.append({
                        "id":           submission.id,
                        "title":        submission.title,
                        "selftext":     submission.selftext or "",
                        "subreddit":    sub_name,
                        "score":        submission.score,
                        "num_comments": submission.num_comments,
                        "created_utc":  int(submission.created_utc),
                        "url":          submission.url,
                        "data_source":  "reddit",
                    })
            except Exception as exc:
                logger.error(
                    f"RedditIngester._fetch_real() | failed on r/{sub_name}: {exc}"
                )
                continue

        logger.info(
            f"RedditIngester._fetch_real() | fetched {len(raw)} posts from PRAW"
        )
        return raw

    def _fetch_simulated(self) -> List[Dict[str, Any]]:
        """
        Generate synthetic Reddit post records using the data_generator module.

        The count is derived from config.yaml ingestion.reddit.post_limit
        (default 200); all configured subreddits are represented.

        Returns:
            List[dict]: Synthetic post records already in normalised schema format.
        """
        from data_ingestion.data_generator import generate_reddit_posts

        n = self.reddit_cfg.get("post_limit", 200) * 3   # produce ~600 records
        logger.info(
            f"RedditIngester._fetch_simulated() | generating {n} synthetic posts"
        )
        return generate_reddit_posts(n=n)

    def _normalize(self, raw_posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Map raw post dicts (API or simulated) to RAW_REDDIT_SCHEMA field names.

        Simulated records already carry all required fields; they pass through
        unchanged.  PRAW records are constructed in _fetch_real() with the
        correct field names, so this method serves primarily as a validation
        and type-coercion step.

        Args:
            raw_posts: List of raw post dicts.

        Returns:
            List[dict]: Records with fields matching RAW_REDDIT_SCHEMA.
        """
        normalised: List[Dict[str, Any]] = []

        for raw in raw_posts:
            # Records from both the generator and _fetch_real() share the same
            # field layout; we still validate and coerce types for safety.
            normalised.append({
                "id":           str(raw.get("id", "")),
                "title":        raw.get("title", ""),
                "selftext":     raw.get("selftext") or "",
                "subreddit":    raw.get("subreddit", ""),
                "score":        _safe_int(raw.get("score", 0)),
                "num_comments": _safe_int(raw.get("num_comments", 0)),
                "created_utc":  _safe_int(raw.get("created_utc", 0)),
                "url":          raw.get("url", ""),
                "data_source":  "reddit",
            })

        logger.debug(f"RedditIngester._normalize() | output {len(normalised)} records")
        return normalised


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _safe_int(value: Any, default: int = 0) -> int:
    """Coerce value to int, returning default on failure."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
