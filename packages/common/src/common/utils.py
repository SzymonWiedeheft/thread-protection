"""Utility functions."""

import os
import time
import functools
from typing import TypeVar, Callable, Optional, Any
import structlog

logger = structlog.get_logger()

T = TypeVar("T")


def retry_with_backoff(
    retries: int = 3,
    backoff: float = 5.0,
    exceptions: tuple = (Exception,),
) -> Callable:
    """
    Decorator to retry a function with exponential backoff.

    Args:
        retries: Number of retry attempts
        backoff: Initial backoff time in seconds
        exceptions: Tuple of exceptions to catch

    Returns:
        Decorated function
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            attempt = 0
            current_backoff = backoff

            while attempt < retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    if attempt >= retries:
                        logger.error(
                            "Max retries reached",
                            function=func.__name__,
                            attempts=attempt,
                            error=str(e),
                        )
                        raise

                    logger.warning(
                        "Retry attempt",
                        function=func.__name__,
                        attempt=attempt,
                        max_retries=retries,
                        backoff_seconds=current_backoff,
                        error=str(e),
                    )

                    time.sleep(current_backoff)
                    current_backoff *= 2  # Exponential backoff

            return func(*args, **kwargs)

        return wrapper

    return decorator


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
