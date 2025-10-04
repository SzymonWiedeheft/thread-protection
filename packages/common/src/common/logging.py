"""Structured logging setup."""

import logging
import sys
from typing import Optional
import structlog


def setup_logging(
    level: str = "INFO",
    service_name: Optional[str] = None,
    json_format: bool = True,
) -> structlog.BoundLogger:
    """
    Setup structured logging with structlog.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        service_name: Name of the service (for log context)
        json_format: If True, output JSON logs; otherwise, console format

    Returns:
        Configured logger instance
    """
    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper()),
    )

    # Configure structlog processors
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    if service_name:
        processors.append(
            structlog.processors.CallsiteParameterAdder(
                {
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.LINENO,
                }
            )
        )

    if json_format:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper())
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logger = structlog.get_logger()

    if service_name:
        logger = logger.bind(service=service_name)

    return logger
