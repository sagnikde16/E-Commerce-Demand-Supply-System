"""
Utility helper functions shared across modules.
"""

from pathlib import Path
from typing import Optional

import yaml


def load_config(config_path: Optional[str] = None) -> dict:
    """
    Load YAML application configuration.

    Args:
        config_path: Path to config file. Defaults to config/app_config.yaml.

    Returns:
        Parsed configuration dictionary.
    """
    if config_path is None:
        config_path = Path(__file__).parent.parent.parent / "config" / "app_config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def ensure_dirs(*paths: str) -> None:
    """Create directories if they don't exist."""
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)


def format_number(n: float, prefix: str = "") -> str:
    """Format a large number with K/M suffixes."""
    if abs(n) >= 1_000_000:
        return f"{prefix}{n / 1_000_000:.1f}M"
    elif abs(n) >= 1_000:
        return f"{prefix}{n / 1_000:.1f}K"
    return f"{prefix}{n:.0f}"
