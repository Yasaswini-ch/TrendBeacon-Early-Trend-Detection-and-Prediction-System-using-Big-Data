"""
schema_definitions.py — Canonical Spark schemas for all pipeline stages.

Defining schemas explicitly (rather than inferring) makes the pipeline:
- Faster (no schema inference scan)
- Safer (catches malformed data early)
- Self-documenting (schemas serve as contracts between pipeline stages)

Usage:
    from utils.schema_definitions import RAW_TWEET_SCHEMA, PROCESSED_SCHEMA
    df = spark.read.schema(RAW_TWEET_SCHEMA).json(path)
"""

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Raw ingestion schemas (what we read from APIs / files)
# ---------------------------------------------------------------------------

RAW_TWEET_SCHEMA = StructType([
    StructField("id",           StringType(),    nullable=False),
    StructField("text",         StringType(),    nullable=False),
    StructField("created_at",   StringType(),    nullable=True),   # Parsed later
    StructField("author_id",    StringType(),    nullable=True),
    StructField("lang",         StringType(),    nullable=True),
    StructField("source",       StringType(),    nullable=True),
    StructField("retweet_count",IntegerType(),   nullable=True),
    StructField("like_count",   IntegerType(),   nullable=True),
    StructField("reply_count",  IntegerType(),   nullable=True),
    StructField("hashtags",     ArrayType(StringType()), nullable=True),
    StructField("data_source",  StringType(),    nullable=False),  # "twitter"
])

RAW_REDDIT_SCHEMA = StructType([
    StructField("id",           StringType(),    nullable=False),
    StructField("title",        StringType(),    nullable=False),
    StructField("selftext",     StringType(),    nullable=True),
    StructField("subreddit",    StringType(),    nullable=True),
    StructField("score",        IntegerType(),   nullable=True),
    StructField("num_comments", IntegerType(),   nullable=True),
    StructField("created_utc",  LongType(),      nullable=True),  # Unix timestamp
    StructField("url",          StringType(),    nullable=True),
    StructField("data_source",  StringType(),    nullable=False),  # "reddit"
])

RAW_NEWS_SCHEMA = StructType([
    StructField("id",           StringType(),    nullable=False),
    StructField("title",        StringType(),    nullable=False),
    StructField("summary",      StringType(),    nullable=True),
    StructField("published",    StringType(),    nullable=True),
    StructField("source",       StringType(),    nullable=True),
    StructField("link",         StringType(),    nullable=True),
    StructField("data_source",  StringType(),    nullable=False),  # "news"
])

# ---------------------------------------------------------------------------
# Unified schema — after merging all sources in preprocessing
# ---------------------------------------------------------------------------

PROCESSED_SCHEMA = StructType([
    StructField("id",           StringType(),    nullable=False),
    StructField("text",         StringType(),    nullable=False),   # Raw text
    StructField("clean_text",   StringType(),    nullable=True),    # After cleaning
    StructField("tokens",       ArrayType(StringType()), nullable=True),
    StructField("timestamp",    TimestampType(), nullable=False),   # Parsed datetime
    StructField("source",       StringType(),    nullable=False),   # twitter/reddit/news
    StructField("engagement",   IntegerType(),   nullable=True),    # likes+retweets etc.
    StructField("lang",         StringType(),    nullable=True),
])

# ---------------------------------------------------------------------------
# Keyword frequency schema — output of frequency_analyzer
# ---------------------------------------------------------------------------

KEYWORD_FREQ_SCHEMA = StructType([
    StructField("keyword",      StringType(),    nullable=False),
    StructField("window",       StringType(),    nullable=False),   # "1h", "6h", "24h", "7d"
    StructField("window_start", TimestampType(), nullable=False),
    StructField("window_end",   TimestampType(), nullable=False),
    StructField("frequency",    LongType(),      nullable=False),   # Count in window
    StructField("source",       StringType(),    nullable=True),    # Breakdown by source
])

# ---------------------------------------------------------------------------
# Feature vector schema — output of feature_builder
# ---------------------------------------------------------------------------

FEATURE_SCHEMA = StructType([
    StructField("keyword",         StringType(),  nullable=False),
    StructField("timestamp",       TimestampType(),nullable=False),
    StructField("freq_1h",         LongType(),    nullable=True),
    StructField("freq_6h",         LongType(),    nullable=True),
    StructField("freq_24h",        LongType(),    nullable=True),
    StructField("freq_7d",         LongType(),    nullable=True),
    StructField("growth_rate_1h",  DoubleType(),  nullable=True),
    StructField("growth_rate_24h", DoubleType(),  nullable=True),
    StructField("z_score_1h",      DoubleType(),  nullable=True),
    StructField("z_score_24h",     DoubleType(),  nullable=True),
    StructField("velocity",        DoubleType(),  nullable=True),   # 1st derivative
    StructField("acceleration",    DoubleType(),  nullable=True),   # 2nd derivative
    StructField("avg_sentiment",   FloatType(),   nullable=True),   # -1 to +1
    StructField("source_diversity",IntegerType(), nullable=True),   # Num unique sources
    StructField("is_anomaly",      IntegerType(), nullable=True),   # 0 or 1
    StructField("trend_label",     StringType(),  nullable=True),   # emerging/viral/stable
])

# ---------------------------------------------------------------------------
# Prediction output schema
# ---------------------------------------------------------------------------

PREDICTION_SCHEMA = StructType([
    StructField("keyword",         StringType(),  nullable=False),
    StructField("predicted_at",    TimestampType(),nullable=False),
    StructField("forecast_period", StringType(),  nullable=False),  # "24h", "7d"
    StructField("predicted_freq",  DoubleType(),  nullable=True),
    StructField("lower_bound",     DoubleType(),  nullable=True),
    StructField("upper_bound",     DoubleType(),  nullable=True),
    StructField("trend_class",     StringType(),  nullable=True),   # emerging/viral/declining
    StructField("confidence",      FloatType(),   nullable=True),   # 0.0 - 1.0
])
