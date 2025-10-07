"""Custom exceptions for the pipeline."""

from typing import Optional, Dict, Any


class PipelineException(Exception):
    """Base exception for all pipeline errors.

    Attributes:
        message: Error message
        context: Additional context about the error
        original_error: Original exception if this wraps another error
    """

    def __init__(
        self,
        message: str,
        context: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
    ):
        """Initialize exception with context.

        Args:
            message: Error message
            context: Additional context (e.g., source_name, url)
            original_error: Original exception if wrapping
        """
        super().__init__(message)
        self.message = message
        self.context = context or {}
        self.original_error = original_error

    def __str__(self) -> str:
        """String representation with context."""
        base = self.message
        if self.context:
            ctx_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            base = f"{base} ({ctx_str})"
        if self.original_error:
            base = f"{base} [caused by: {type(self.original_error).__name__}: {self.original_error}]"
        return base


class FetchError(PipelineException):
    """Raised when fetching data from HTTP source fails.

    Common context fields:
        - source_name: Name of the source being fetched
        - url: URL that failed
        - status_code: HTTP status code (if applicable)
        - attempt: Retry attempt number
    """

    pass


class ParseError(PipelineException):
    """Raised when parsing data fails.

    Common context fields:
        - source_name: Name of the source
        - format: Format type (hosts, adblock)
        - line_number: Line number where parsing failed
    """

    pass


class ValidationError(PipelineException):
    """Raised when domain validation fails.

    Common context fields:
        - domain: The invalid domain
        - reason: Why validation failed
    """

    pass


class KafkaPublishError(PipelineException):
    """Raised when publishing to Kafka fails.

    Common context fields:
        - topic: Kafka topic
        - bootstrap_servers: Kafka servers
        - message_count: Number of messages attempted
    """

    pass


class ConfigurationError(PipelineException):
    """Raised when configuration is invalid.

    Common context fields:
        - config_path: Path to config file
        - field: Invalid field name
    """

    pass
