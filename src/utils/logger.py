"""
Logging utilities using loguru.
"""
import sys
from pathlib import Path

from loguru import logger


def setup_logger(log_level: str = "INFO", log_file: str = "./logs/app.log") -> None:
    """
    Configure loguru logger with console and file sinks.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR).
        log_file: Path to the rotating log file.
    """
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    logger.remove()  # Remove default sink

    # Console sink
    logger.add(
        sys.stderr,
        level=log_level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        ),
        colorize=True,
    )

    # File sink with rotation
    logger.add(
        log_file,
        level=log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
        rotation="100 MB",
        retention="30 days",
        compression="zip",
        enqueue=True,
    )
