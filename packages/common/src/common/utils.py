"""Utility functions."""

import os
from typing import TypeVar, Optional
import structlog

logger = structlog.get_logger()

T = TypeVar("T")


def get_env(key: str, default: Optional[str] = None, required: bool = False) -> str:
    """
    Get environment variable with optional default and required flag.

    Args:
        key: Environment variable name
        default: Default value if not found
        required: If True, raise error if not found and no default

    Returns:
        Environment variable value

    Raises:
        ValueError: If required=True and variable not found
    """
    value = os.getenv(key, default)

    if required and value is None:
        raise ValueError(f"Required environment variable '{key}' not found")

    return value or ""
