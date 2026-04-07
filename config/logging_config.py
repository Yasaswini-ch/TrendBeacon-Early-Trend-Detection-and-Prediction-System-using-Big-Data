"""
logging_config.py — Centralized logging setup for the TrendBeacon pipeline.

Usage:
    from config.logging_config import get_logger
    logger = get_logger(__name__)
    logger.info("Starting ingestion...")
"""

import logging
import os
from logging.handlers import RotatingFileHandler

from config.config_loader import cfg


def get_logger(name: str) -> logging.Logger:
    """
    Create and return a configured logger.

    Features:
    - Logs to console (always)
    - Logs to rotating file (if logging.log_to_file is true in config)
    - Format includes timestamp, level, module name

    Args:
        name: Usually pass __name__ from the calling module.

    Returns:
        logging.Logger: Configured logger instance.
    """
    log_cfg = cfg.get("logging", {})
    level_str = log_cfg.get("level", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)

    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if logger already configured
    if logger.handlers:
        return logger

    logger.setLevel(level)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # --- Console handler ---
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # --- Rotating file handler ---
    if log_cfg.get("log_to_file", False):
        log_dir = log_cfg.get("log_dir", "logs/")
        os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(log_dir, f"{name.replace('.', '_')}.log")
        file_handler = RotatingFileHandler(
            filename=log_file,
            maxBytes=log_cfg.get("max_bytes", 10 * 1024 * 1024),  # 10 MB
            backupCount=log_cfg.get("backup_count", 5),
            encoding="utf-8",
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
