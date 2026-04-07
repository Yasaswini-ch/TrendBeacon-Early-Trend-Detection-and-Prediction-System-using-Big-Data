"""
config_loader.py — Loads config.yaml and resolves environment variable placeholders.

Usage:
    from config.config_loader import cfg
    spark_master = cfg["spark"]["master"]
    hdfs_raw     = cfg["hdfs"]["paths"]["raw"]
"""

import os
import re
import yaml
from dotenv import load_dotenv


def _resolve_env_vars(value: str) -> str:
    """
    Replace ${VAR_NAME} placeholders with actual environment variable values.
    Returns the original string if the env var is not set.
    """
    pattern = re.compile(r"\$\{(\w+)\}")
    matches = pattern.findall(value)
    for var in matches:
        env_val = os.environ.get(var, f"<{var}_NOT_SET>")
        value = value.replace(f"${{{var}}}", env_val)
    return value


def _walk_and_resolve(obj):
    """Recursively walk the config dict and resolve all env var strings."""
    if isinstance(obj, dict):
        return {k: _walk_and_resolve(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_walk_and_resolve(item) for item in obj]
    elif isinstance(obj, str):
        return _resolve_env_vars(obj)
    return obj


def load_config(path: str = None) -> dict:
    """
    Load and return the full configuration as a Python dict.

    Args:
        path: Absolute or relative path to config.yaml.
              Defaults to config/config.yaml relative to project root.

    Returns:
        dict: Fully resolved configuration dictionary.
    """
    if path is None:
        # Resolve path relative to this file's location
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(base_dir, "config", "config.yaml")

    # Load local environment variables from .env if present.
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(base_dir, ".env"), override=False)

    with open(path, "r") as f:
        raw_config = yaml.safe_load(f)

    return _walk_and_resolve(raw_config)


# Module-level singleton — import this directly in other modules
cfg = load_config()
