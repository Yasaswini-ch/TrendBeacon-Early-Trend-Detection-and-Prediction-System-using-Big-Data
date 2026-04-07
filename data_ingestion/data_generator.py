"""
data_generator.py — Synthetic data generator for the TrendBeacon pipeline demo.

Produces realistic tweet-like, Reddit post, and news article records without
requiring any API keys.  The generator deliberately bakes in two types of
trend signals so downstream detection stages have something meaningful to find:

  - Emerging trends:  low early frequency that grows steadily over 7 days
                      (topics: "quantum_computing", "bird_flu")
  - Viral spikes:     low baseline with one massive burst in the most recent
                      12-hour window (topics: "asteroid", "ai_breakthrough")
  - Stable topics:    roughly constant throughout the week
                      (topics: "climate", "elections", "crypto", "space", "health")

Usage:
    from data_ingestion.data_generator import generate_all_sources
    generate_all_sources(output_dir="data/")

    # Or individually:
    from data_ingestion.data_generator import generate_tweets
    tweets = generate_tweets(n=1000)
"""

import csv
import json
import os
import random
import string
import uuid
from datetime import datetime, timedelta
from typing import List, Optional


# ---------------------------------------------------------------------------
# Topic corpus
# ---------------------------------------------------------------------------

# Maps topic key → (display label, list of text templates)
# Each template may contain a {detail} placeholder filled at generation time.
_TOPICS = {
    "ai": {
        "label": "AI",
        "hashtags": ["AI", "MachineLearning", "ChatGPT", "LLM", "GenerativeAI", "DeepLearning"],
        "subreddits": ["MachineLearning", "artificial", "singularity", "LocalLLaMA"],
        "news_sources": ["TechCrunch", "Wired", "MIT Technology Review", "VentureBeat"],
        "tweet_templates": [
            "The new {detail} model is absolutely mind-blowing 🤯 #AI #MachineLearning",
            "Just tried {detail} and I'm speechless. AI is moving too fast! #LLM",
            "Thread: Why {detail} will change everything we know about software 🧵 #AI",
            "Hot take: {detail} is the most important AI release since GPT-4 #GenerativeAI",
            "Breaking: {detail} just hit human-level performance on the benchmark 🚀 #AI",
        ],
        "details": [
            "GPT-5", "Gemini Ultra 2", "Claude 4", "Llama 4", "Mistral Large",
            "the new OpenAI reasoning model", "Meta's latest open-source LLM",
            "Google DeepMind's AlphaCode 3", "a new multimodal foundation model",
        ],
        "title_templates": [
            "{detail} Achieves State-of-the-Art on Every Major Benchmark",
            "Researchers Unveil {detail}: A New Era of Generative AI",
            "{detail} Can Now Reason Like a PhD Student, Study Shows",
            "Inside {detail}: How It Works and Why It Matters",
        ],
    },
    "climate": {
        "label": "Climate",
        "hashtags": ["ClimateChange", "GlobalWarming", "ClimateAction", "NetZero", "Sustainability"],
        "subreddits": ["climate", "environment", "sustainability", "collapse"],
        "news_sources": ["BBC News", "Guardian", "Reuters", "AP News"],
        "tweet_templates": [
            "New record: {detail} — scientists are alarmed #ClimateChange #GlobalWarming",
            "{detail} is accelerating faster than our worst-case models predicted #Climate",
            "Just read the new IPCC report on {detail}. We need to act NOW. #ClimateAction",
            "{detail} hits all-time high again this year. When will we wake up? #NetZero",
            "Climate win: {detail} reduces carbon emissions by 30% in pilot region #Sustainability",
        ],
        "details": [
            "global average temperature", "Arctic sea ice minimum", "CO2 levels",
            "ocean heat content", "glacier retreat rate", "extreme weather frequency",
            "permafrost thaw", "renewable energy adoption",
        ],
        "title_templates": [
            "Record {detail} Recorded for Third Consecutive Year",
            "UN Report: {detail} Worsening Faster Than Expected",
            "Scientists Warn {detail} Could Trigger Irreversible Tipping Point",
            "New Study Links {detail} to Increasing Extreme Weather Events",
        ],
    },
    "elections": {
        "label": "Elections",
        "hashtags": ["Elections2024", "Vote", "Democracy", "Politics", "Election"],
        "subreddits": ["politics", "news", "worldnews", "uspolitics"],
        "news_sources": ["CNN", "Fox News", "NYT", "Washington Post", "Politico"],
        "tweet_templates": [
            "BREAKING: {detail} just announced — this changes everything #Elections2024",
            "New poll shows {detail} leading by 8 points in swing states #Election",
            "I can't believe {detail}. Democracy is at stake. #Vote #Democracy",
            "Live: {detail} speech happening now — watch here #Politics",
            "{detail} just dropped out of the race. Major development 🚨 #Election",
        ],
        "details": [
            "the leading candidate", "new polling data", "a major endorsement",
            "the debate result", "voter turnout numbers", "the new campaign ad",
            "election integrity concerns", "the runoff results",
        ],
        "title_templates": [
            "Poll: {detail} Surges to Double-Digit Lead Ahead of Election Day",
            "{detail} Triggers Political Firestorm Across Both Parties",
            "Exclusive: {detail} Signals Major Policy Shift Before Crucial Vote",
            "Analysts: {detail} Could Decide the Outcome of the Race",
        ],
    },
    "crypto": {
        "label": "Crypto",
        "hashtags": ["Bitcoin", "Crypto", "Ethereum", "Web3", "Blockchain", "BTC"],
        "subreddits": ["Bitcoin", "CryptoCurrency", "ethereum", "CryptoMarkets"],
        "news_sources": ["CoinDesk", "CoinTelegraph", "Bloomberg Crypto", "The Block"],
        "tweet_templates": [
            "BTC just hit {detail}! Bullish or bearish? Drop your take 👇 #Bitcoin #Crypto",
            "{detail} is the most bullish signal I've seen in years. Accumulate. #BTC",
            "Thread: Why {detail} matters for the next crypto cycle 🧵 #Crypto #Web3",
            "JUST IN: {detail} — crypto markets react instantly #Blockchain",
            "Everyone sleeping on {detail}. This is the opportunity of the decade. #Crypto",
        ],
        "details": [
            "$75k", "$80k", "$100k", "a new ATH", "the ETF approval",
            "the halving event", "institutional buying", "Layer 2 adoption surge",
            "the SEC ruling", "on-chain volume records",
        ],
        "title_templates": [
            "Bitcoin Surges Past {detail} as Institutional Demand Grows",
            "Analysis: What {detail} Means for the Next Bull Market",
            "{detail} Sends Shockwaves Through Global Crypto Markets",
            "Exclusive: Major Hedge Funds Bet Big on {detail}",
        ],
    },
    "space": {
        "label": "Space",
        "hashtags": ["Space", "NASA", "SpaceX", "Mars", "Astronomy", "Science"],
        "subreddits": ["space", "nasa", "SpaceX", "Astronomy"],
        "news_sources": ["Space.com", "NASA", "Ars Technica", "New Scientist"],
        "tweet_templates": [
            "Mind-blowing: {detail} captured by James Webb Telescope 🌌 #Space #Science",
            "SpaceX just announced {detail}. Humanity is becoming multiplanetary! #SpaceX",
            "{detail} detected by astronomers — implications are huge #Astronomy #Space",
            "LIVE: Watch {detail} launch in T-minus 2 hours 🚀 #NASA #Space",
            "New research: {detail} suggests Mars was once habitable #Mars #Space",
        ],
        "details": [
            "a new exoplanet in the habitable zone", "gravitational waves from a black hole merger",
            "the Artemis IV crew", "a Starship orbital test", "methane on Enceladus",
            "a near-Earth asteroid", "the first image of a neutron star collision",
            "oxygen production on Mars", "the lunar gateway construction",
        ],
        "title_templates": [
            "Webb Telescope Reveals {detail}: Scientists Stunned",
            "SpaceX Successfully Launches {detail} on Historic Mission",
            "Astronomers Discover {detail} — Could Change Our Understanding of the Universe",
            "NASA Confirms {detail}: 'A Monumental Day for Science'",
        ],
    },
    "health": {
        "label": "Health",
        "hashtags": ["Health", "Medicine", "Healthcare", "MedTwitter", "PublicHealth"],
        "subreddits": ["medicine", "health", "Coronavirus", "science"],
        "news_sources": ["WebMD", "Healthline", "Medical News Today", "STAT News"],
        "tweet_templates": [
            "Major breakthrough: {detail} in clinical trial shows 90% efficacy #Medicine #Health",
            "{detail} could prevent millions of deaths per year, new study finds #PublicHealth",
            "Thread: What the new WHO report on {detail} actually means for you 🧵 #Health",
            "BREAKING: FDA approves {detail} — huge win for patients #Healthcare #Medicine",
            "{detail} is quietly becoming a public health crisis. Here's why 👇 #Health",
        ],
        "details": [
            "a new mRNA cancer vaccine", "the novel Alzheimer's drug",
            "a universal flu vaccine", "antibiotic-resistant bacteria",
            "the new obesity treatment", "long COVID biomarkers",
            "a CRISPR gene therapy", "the new dengue vaccine",
        ],
        "title_templates": [
            "Clinical Trial: {detail} Reduces Mortality by 40% in Phase 3 Study",
            "FDA Fast-Tracks {detail} Amid Growing Public Health Concerns",
            "Study: {detail} More Effective Than Current Standard of Care",
            "WHO Raises Alert as {detail} Spreads to New Regions",
        ],
    },
    # --- Emerging trend topics (low early, rising late) ---
    "quantum_computing": {
        "label": "Quantum Computing",
        "hashtags": ["QuantumComputing", "Quantum", "IBM", "Google", "FutureOfTech"],
        "subreddits": ["QuantumComputing", "Physics", "technology"],
        "news_sources": ["Nature", "Science", "IEEE Spectrum", "Quanta Magazine"],
        "tweet_templates": [
            "{detail} just demonstrated quantum advantage on a real-world problem! #QuantumComputing",
            "Quietly the most important tech story of the year: {detail} #Quantum #FutureOfTech",
            "Why {detail} will make current encryption obsolete within a decade 🧵 #Quantum",
            "{detail} — the quantum milestone nobody is talking about (but they should be)",
        ],
        "details": [
            "IBM's 1000-qubit processor", "Google's new quantum error correction",
            "the first quantum-classical hybrid algorithm", "Microsoft's topological qubit",
            "a 99.9% fidelity two-qubit gate",
        ],
        "title_templates": [
            "Researchers Achieve {detail}: A Quantum Computing Milestone",
            "{detail} Brings Practical Quantum Computing Closer to Reality",
        ],
    },
    "bird_flu": {
        "label": "Bird Flu",
        "hashtags": ["BirdFlu", "H5N1", "PublicHealth", "Pandemic", "CDC"],
        "subreddits": ["pandemic", "health", "news", "Coronavirus"],
        "news_sources": ["CDC", "WHO", "Reuters Health", "STAT News"],
        "tweet_templates": [
            "New H5N1 case confirmed in {detail}. CDC monitoring closely. #BirdFlu #PublicHealth",
            "Thread: Should we be worried about {detail}? Here's what virologists say. #H5N1",
            "{detail} — the bird flu development everyone needs to pay attention to #Pandemic",
        ],
        "details": [
            "a US dairy farm worker", "three new countries", "a major poultry facility",
            "the new strain with human transmissibility markers",
        ],
        "title_templates": [
            "Bird Flu Update: {detail} Raises Pandemic Preparedness Concerns",
            "H5N1 Detected in {detail} for First Time, Officials Say",
        ],
    },
    # --- Viral spike topics ---
    "asteroid": {
        "label": "Asteroid",
        "hashtags": ["Asteroid", "NASA", "Space", "PlanetaryDefense", "DART"],
        "subreddits": ["space", "worldnews", "astronomy"],
        "news_sources": ["NASA", "Space.com", "CNN", "BBC News"],
        "tweet_templates": [
            "NASA just confirmed {detail} — should we be worried? 🌍 #Asteroid #PlanetaryDefense",
            "BREAKING: {detail} — astronomers tracking closely #Asteroid #Space",
            "Real talk: {detail} is the story nobody prepared us for 🧵 #Asteroid",
        ],
        "details": [
            "a newly discovered near-Earth asteroid", "a 1-in-50 impact probability for 2032",
            "the largest asteroid flyby in a decade", "Apophis' updated trajectory",
        ],
        "title_templates": [
            "NASA Warns: {detail} on Course for Close Earth Approach",
            "Astronomers Track {detail}: 'No Immediate Danger' but Eyes Are Watching",
        ],
    },
    "ai_breakthrough": {
        "label": "AI Breakthrough",
        "hashtags": ["AI", "AGI", "AIBreakthrough", "OpenAI", "Superintelligence"],
        "subreddits": ["artificial", "MachineLearning", "singularity", "Futurology"],
        "news_sources": ["TechCrunch", "The Verge", "Wired", "Nature"],
        "tweet_templates": [
            "We may have just reached AGI. {detail} — I'm not being hyperbolic. #AI #AGI",
            "BREAKING: {detail} — the AI story that will define 2025 #AIBreakthrough",
            "{detail} solves a problem that stumped AI for 30 years. Read this. #AI #OpenAI",
        ],
        "details": [
            "the new reasoning model passes all Turing variants",
            "an AI agent completed a 6-month research project autonomously",
            "a model scored 99th percentile on every standardized test simultaneously",
        ],
        "title_templates": [
            "Landmark Study: {detail} — Experts Call It 'The Biggest AI Milestone Ever'",
            "OpenAI Unveils {detail}: Is This the AGI Moment We Were Warned About?",
        ],
    },
}

# Topic trend profiles: controls relative probability in each of 7 daily time windows
# (index 0 = oldest day, index 6 = today)
# Values are relative weights — they are normalised internally.
_TREND_PROFILES = {
    "ai":              [1.0, 1.1, 1.0, 1.2, 1.1, 1.3, 1.4],   # Stable with slight growth
    "climate":         [1.0, 0.9, 1.1, 1.0, 0.9, 1.0, 1.0],   # Flat / stable
    "elections":       [0.8, 1.0, 1.1, 1.2, 1.4, 1.6, 2.0],   # Steadily rising
    "crypto":          [1.2, 0.8, 1.0, 1.1, 0.9, 1.3, 1.5],   # Volatile
    "space":           [1.0, 1.0, 0.9, 1.0, 1.0, 1.1, 1.0],   # Flat
    "health":          [1.0, 1.0, 1.1, 1.0, 1.0, 0.9, 1.0],   # Flat
    "quantum_computing":[0.3, 0.4, 0.5, 0.7, 1.0, 1.5, 2.5],  # EMERGING — slow rise
    "bird_flu":        [0.2, 0.3, 0.4, 0.6, 1.0, 1.8, 3.0],   # EMERGING — faster rise
    "asteroid":        [0.2, 0.2, 0.2, 0.2, 0.3, 0.5, 8.0],   # VIRAL — sudden massive spike
    "ai_breakthrough": [0.3, 0.3, 0.3, 0.3, 0.4, 0.5, 7.0],   # VIRAL — sudden massive spike
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _random_id(prefix: str = "") -> str:
    """Return a short random alphanumeric ID."""
    chars = string.ascii_lowercase + string.digits
    suffix = "".join(random.choices(chars, k=12))
    return f"{prefix}{suffix}" if prefix else suffix


def _weighted_topic_for_window(window_index: int) -> str:
    """
    Sample a topic key according to the trend profile for a given time window.

    Args:
        window_index: 0 (oldest day) to 6 (today / most recent).

    Returns:
        str: A topic key from _TOPICS.
    """
    topics = list(_TREND_PROFILES.keys())
    weights = [_TREND_PROFILES[t][window_index] for t in topics]
    return random.choices(topics, weights=weights, k=1)[0]


def _timestamp_in_window(window_index: int, now: datetime) -> datetime:
    """
    Return a random timestamp within a specific 24-hour window.

    Window 6 is the most recent 24 hours; window 0 is 6-7 days ago.
    To create a realistic spike in the viral windows, the last 12 hours of
    window 6 are sampled at 3× the rate of the first 12 hours.

    Args:
        window_index: 0–6 (0 = oldest).
        now:          Reference "now" datetime.

    Returns:
        datetime: A UTC datetime within the window.
    """
    days_ago = 6 - window_index          # 6-days-ago for window 0, 0 for window 6
    window_start = now - timedelta(days=days_ago + 1)
    window_end   = now - timedelta(days=days_ago)

    if window_index == 6:
        # Weight towards the final 12 hours to amplify viral spikes
        mid = window_start + (window_end - window_start) / 2
        if random.random() < 0.75:
            start, end = mid, window_end
        else:
            start, end = window_start, mid
    else:
        start, end = window_start, window_end

    delta_seconds = int((end - start).total_seconds())
    offset = random.randint(0, max(delta_seconds - 1, 0))
    return start + timedelta(seconds=offset)


def _pick(lst: list):
    """Return a random element from a list."""
    return random.choice(lst)


def _engagement_score(topic_key: str, window_index: int) -> int:
    """
    Simulate an engagement score that correlates with trend intensity.
    Viral/emerging topics in recent windows get much higher scores.
    """
    base = random.randint(0, 200)
    profile = _TREND_PROFILES[topic_key]
    multiplier = profile[window_index] ** 2   # Square to amplify high-traffic windows
    return int(base * multiplier) + random.randint(0, 50)


def _make_tweet(window_index: int, now: datetime) -> dict:
    """Generate a single tweet-like record."""
    topic_key = _weighted_topic_for_window(window_index)
    topic = _TOPICS[topic_key]

    template = _pick(topic["tweet_templates"])
    detail   = _pick(topic["details"])
    text     = template.format(detail=detail)

    # Pick 1–3 hashtags relevant to the topic
    n_tags = random.randint(1, 3)
    hashtags = random.sample(topic["hashtags"], min(n_tags, len(topic["hashtags"])))

    ts = _timestamp_in_window(window_index, now)
    engagement = _engagement_score(topic_key, window_index)

    return {
        "id":           _random_id("tw_"),
        "text":         text,
        "created_at":   ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "author_id":    _random_id("u_"),
        "lang":         "en",
        "source":       "Twitter Web App",
        "retweet_count": random.randint(0, engagement // 2),
        "like_count":   engagement,
        "reply_count":  random.randint(0, engagement // 4),
        "hashtags":     ",".join(hashtags),
        "data_source":  "twitter",
    }


def _make_reddit_post(window_index: int, now: datetime) -> dict:
    """Generate a single Reddit post record."""
    topic_key = _weighted_topic_for_window(window_index)
    topic = _TOPICS[topic_key]

    detail    = _pick(topic["details"])
    title_tpl = _pick(topic["title_templates"])
    title     = title_tpl.format(detail=detail)

    subreddit = _pick(topic["subreddits"])
    ts        = _timestamp_in_window(window_index, now)
    engagement = _engagement_score(topic_key, window_index)

    # Selftext is optional — simulate ~40% of posts having body text
    selftext = ""
    if random.random() < 0.4:
        selftext = (
            f"This is a detailed discussion about {detail}. "
            f"Sharing because this seems highly relevant to r/{subreddit}. "
            f"Curious what the community thinks — drop your thoughts below."
        )

    post_id = _random_id("r_")

    return {
        "id":           post_id,
        "title":        title,
        "selftext":     selftext,
        "subreddit":    subreddit,
        "score":        engagement * random.randint(1, 10),
        "num_comments": random.randint(0, engagement // 2),
        "created_utc":  int(ts.timestamp()),
        "url":          f"https://reddit.com/r/{subreddit}/comments/{post_id}/",
        "data_source":  "reddit",
    }


def _make_news_article(window_index: int, now: datetime) -> dict:
    """Generate a single news article record."""
    topic_key = _weighted_topic_for_window(window_index)
    topic = _TOPICS[topic_key]

    detail    = _pick(topic["details"])
    title_tpl = _pick(topic["title_templates"])
    title     = title_tpl.format(detail=detail)
    source    = _pick(topic["news_sources"])

    ts = _timestamp_in_window(window_index, now)
    article_id = _random_id("n_")

    summary = (
        f"In a significant development, {detail} has drawn attention from experts and "
        f"the public alike. According to {source}, this is one of the most notable "
        f"stories relating to {topic['label']} in recent weeks. Full coverage available "
        f"in the linked article."
    )

    return {
        "id":        article_id,
        "title":     title,
        "summary":   summary,
        "published": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source":    source,
        "link":      f"https://{source.lower().replace(' ', '')}.com/article/{article_id}",
        "data_source": "news",
    }


def _distribute_across_windows(n: int) -> List[int]:
    """
    Given N records to generate, randomly assign each to one of 7 daily windows.
    The distribution is uniform — trend signals come from the per-window topic weights,
    not from the number of records per window.

    Returns:
        List[int]: A list of length N, each value in [0, 6].
    """
    return [random.randint(0, 6) for _ in range(n)]


# ---------------------------------------------------------------------------
# Public generation functions
# ---------------------------------------------------------------------------

def generate_tweets(n: int = 1000, output_path: Optional[str] = None) -> List[dict]:
    """
    Generate N synthetic tweet records with realistic trend patterns.

    The corpus spans the last 7 days.  Topics with "emerging" or "viral"
    profiles naturally produce higher volumes in recent windows.

    Args:
        n:           Number of tweet records to generate.
        output_path: Optional file path.  If given, records are also written
                     as newline-delimited JSON (JSONL) to this path.

    Returns:
        List[dict]: Tweet records matching the RAW_TWEET_SCHEMA field names.

    Example:
        tweets = generate_tweets(n=500)
        tweets = generate_tweets(n=1000, output_path="data/tweets.jsonl")
    """
    now = datetime.utcnow()
    windows = _distribute_across_windows(n)
    records = [_make_tweet(w, now) for w in windows]

    if output_path:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    return records


def generate_reddit_posts(n: int = 500, output_path: Optional[str] = None) -> List[dict]:
    """
    Generate N synthetic Reddit post records.

    Args:
        n:           Number of post records to generate.
        output_path: Optional path to write JSONL output.

    Returns:
        List[dict]: Post records matching the RAW_REDDIT_SCHEMA field names.
    """
    now = datetime.utcnow()
    windows = _distribute_across_windows(n)
    records = [_make_reddit_post(w, now) for w in windows]

    if output_path:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    return records


def generate_news_articles(n: int = 300, output_path: Optional[str] = None) -> List[dict]:
    """
    Generate N synthetic news article records.

    Args:
        n:           Number of article records to generate.
        output_path: Optional path to write JSONL output.

    Returns:
        List[dict]: Article records matching the RAW_NEWS_SCHEMA field names.
    """
    now = datetime.utcnow()
    windows = _distribute_across_windows(n)
    records = [_make_news_article(w, now) for w in windows]

    if output_path:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    return records


def generate_all_sources(output_dir: str = "data/") -> dict:
    """
    Generate data for all three sources and write CSV files to output_dir.

    Filenames written:
        <output_dir>/tweets.csv
        <output_dir>/reddit_posts.csv
        <output_dir>/news_articles.csv

    Args:
        output_dir: Directory to write CSV files into (created if absent).

    Returns:
        dict: {"twitter": n_tweets, "reddit": n_posts, "news": n_articles}

    Example:
        counts = generate_all_sources(output_dir="data/sample/")
        # {"twitter": 1000, "reddit": 500, "news": 300}
    """
    os.makedirs(output_dir, exist_ok=True)

    def _write_csv(records: List[dict], filepath: str) -> None:
        if not records:
            return
        fieldnames = list(records[0].keys())
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

    tweets  = generate_tweets(n=1000)
    posts   = generate_reddit_posts(n=500)
    articles = generate_news_articles(n=300)

    _write_csv(tweets,   os.path.join(output_dir, "tweets.csv"))
    _write_csv(posts,    os.path.join(output_dir, "reddit_posts.csv"))
    _write_csv(articles, os.path.join(output_dir, "news_articles.csv"))

    return {"twitter": len(tweets), "reddit": len(posts), "news": len(articles)}
