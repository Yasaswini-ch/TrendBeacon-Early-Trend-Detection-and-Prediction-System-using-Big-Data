"""
features/growth_rate.py
-----------------------
Pure Python functions for growth rate and anomaly math.

These functions are deliberately free of PySpark / pandas dependencies so they
can be used in three distinct contexts:
  1. Registered as PySpark UDFs (serialised to worker nodes).
  2. Called directly in the standalone model layer (prophet_model, anomaly_detector).
  3. Unit-tested in isolation without a Spark context.

All inputs and outputs are plain Python scalars or built-in lists so that
PySpark can serialise them without any extra type conversion.
"""

from __future__ import annotations

import math
import statistics
from typing import List


# ---------------------------------------------------------------------------
# Growth rate
# ---------------------------------------------------------------------------

def compute_growth_rate(
    freq_current: float,
    freq_previous: float,
    epsilon: float = 1.0,
) -> float:
    """
    Compute the relative growth rate between two consecutive frequency counts.

    Formula:
        GR = (freq_current - freq_previous) / (freq_previous + epsilon)

    The ``epsilon`` term (Laplace smoothing) prevents division by zero when a
    keyword has never been seen before (freq_previous == 0) and dampens extreme
    spikes caused by very small baseline counts.

    Parameters
    ----------
    freq_current  : Mention count in the current window.
    freq_previous : Mention count in the previous window.
    epsilon       : Smoothing constant (default 1.0).

    Returns
    -------
    float
        Growth rate.  0.0 means no change; 1.0 means 100 % growth.
        Negative values indicate a decline in mentions.
    """
    # Guard: both counts must be non-negative
    freq_current = max(0.0, float(freq_current))
    freq_previous = max(0.0, float(freq_previous))
    epsilon = float(epsilon)

    return (freq_current - freq_previous) / (freq_previous + epsilon)


# ---------------------------------------------------------------------------
# Z-score normalisation
# ---------------------------------------------------------------------------

def compute_zscore(values: List[float]) -> List[float]:
    """
    Z-score normalise a list of floats.

    Formula per element:
        z_i = (x_i - mean) / stdev

    A population standard deviation is used (rather than the Bessel-corrected
    sample stdev) because we are describing the *observed* distribution inside
    a fixed time window rather than estimating a population parameter.

    Parameters
    ----------
    values : List of numeric values (e.g. growth rates across keywords).

    Returns
    -------
    List[float]
        Z-scores in the same order as ``values``.
        Returns a list of 0.0 when fewer than 2 elements are provided or when
        the standard deviation is zero (all values identical).
    """
    if len(values) < 2:
        # Not enough data to compute a meaningful z-score
        return [0.0] * len(values)

    mean = statistics.mean(values)
    # pstdev = population standard deviation (denominator N, not N-1)
    stdev = statistics.pstdev(values)

    if stdev == 0.0:
        # All values are identical — every z-score is 0 by definition
        return [0.0] * len(values)

    return [(v - mean) / stdev for v in values]


# ---------------------------------------------------------------------------
# Velocity (1st derivative of growth rate)
# ---------------------------------------------------------------------------

def compute_velocity(growth_rates: List[float]) -> List[float]:
    """
    Return the velocity series from a list of pre-computed growth rates.

    Velocity is defined as the *first derivative* of the frequency signal.
    Because ``growth_rates`` already encodes how fast a metric is changing,
    velocity[i] == growth_rates[i] — this function exists as an explicit
    semantic layer so downstream callers can refer to "velocity" by name.

    Parameters
    ----------
    growth_rates : Ordered list of growth-rate values (oldest → newest).

    Returns
    -------
    List[float]
        Velocity values, same length as input.
    """
    # Velocity IS the growth rate (1st derivative already computed upstream)
    return list(growth_rates)


# ---------------------------------------------------------------------------
# Acceleration (2nd derivative of growth rate)
# ---------------------------------------------------------------------------

def compute_acceleration(growth_rates: List[float]) -> List[float]:
    """
    Compute the acceleration series from a list of growth rates.

    Acceleration is the *second derivative* of the frequency signal, i.e. how
    quickly the growth rate itself is changing:

        acceleration[i] = growth_rates[i] - growth_rates[i-1]

    A positive acceleration indicates the keyword is gaining momentum; negative
    acceleration indicates the growth is slowing down.

    The first element has no predecessor, so acceleration[0] is set to 0.0.

    Parameters
    ----------
    growth_rates : Ordered list of growth-rate values (oldest → newest).

    Returns
    -------
    List[float]
        Acceleration values, same length as input.
    """
    if not growth_rates:
        return []

    acceleration = [0.0]  # No previous value for the first observation
    for i in range(1, len(growth_rates)):
        acceleration.append(growth_rates[i] - growth_rates[i - 1])

    return acceleration


# ---------------------------------------------------------------------------
# Trend classification
# ---------------------------------------------------------------------------

def classify_trend(
    z_score: float,
    window: str,
    growth_rate: float,
    viral_z: float = 3.0,
    emerging_z: float = 2.0,
) -> str:
    """
    Classify a keyword's trend status based on its statistical signal strength.

    Decision rules (evaluated in priority order):

    1. **viral**    — z_score > viral_z   AND window == "1h"
                      A very large z-score within the shortest window means the
                      keyword exploded in the last hour — classic viral pattern.

    2. **emerging** — z_score > emerging_z AND window == "24h"
                      A sustained elevation over the day suggests organic growth
                      that hasn't yet peaked.

    3. **declining** — growth_rate < -0.5
                       The keyword lost more than half of its relative frequency
                       compared to the previous period.

    4. **stable**   — everything else (low signal, no strong directional move).

    Parameters
    ----------
    z_score     : Z-score of this keyword's growth rate versus its peers.
    window      : Time window label, e.g. "1h", "6h", "24h", "7d".
    growth_rate : Raw growth rate for the same keyword and window.
    viral_z     : Z-score threshold for viral classification (default 3.0).
    emerging_z  : Z-score threshold for emerging classification (default 2.0).

    Returns
    -------
    str
        One of "viral", "emerging", "declining", "stable".
    """
    # Rule 1: Viral — explosive short-term spike
    if z_score > viral_z and window == "1h":
        return "viral"

    # Rule 2: Emerging — sustained day-long elevation
    if z_score > emerging_z and window == "24h":
        return "emerging"

    # Rule 3: Declining — significant drop in mentions
    if growth_rate < -0.5:
        return "declining"

    # Rule 4: Default — no strong directional signal
    return "stable"


# ---------------------------------------------------------------------------
# Anomaly detection
# ---------------------------------------------------------------------------

def detect_anomaly(z_score: float, threshold: float = 2.5) -> bool:
    """
    Flag a single data point as anomalous based on its z-score magnitude.

    A two-tailed test is used: both unusually *high* and unusually *low* values
    are considered anomalies.

        anomaly = |z_score| > threshold

    Under a normal distribution, |z| > 2.5 corresponds to roughly the top/bottom
    0.6 % of observations — a conservative but practical default.

    Parameters
    ----------
    z_score   : Pre-computed z-score for this observation.
    threshold : Absolute z-score cutoff (default 2.5).

    Returns
    -------
    bool
        True if the observation is anomalous, False otherwise.
    """
    return abs(z_score) > threshold


# ---------------------------------------------------------------------------
# Source diversity
# ---------------------------------------------------------------------------

def compute_source_diversity(sources: List[str]) -> int:
    """
    Count the number of unique sources that mentioned a keyword.

    A higher diversity score indicates organic, multi-origin interest.
    A low diversity score (e.g. 1) combined with high frequency is a strong
    signal for coordinated amplification or bot activity.

    Parameters
    ----------
    sources : List of source identifiers (domain names, platform names, etc.).
              Duplicates are expected and will be collapsed.

    Returns
    -------
    int
        Number of distinct sources.  Returns 0 for an empty list.
    """
    if not sources:
        return 0

    # Use a set comprehension; strip whitespace to avoid "twitter " != "twitter"
    return len({s.strip() for s in sources if s})
