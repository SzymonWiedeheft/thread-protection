"""Custom exceptions for the pipeline."""


class PipelineException(Exception):
    """Base exception for all pipeline errors."""

    pass


class FetchError(PipelineException):
    """Raised when fetching data from HTTP source fails."""

    pass


class ParseError(PipelineException):
    """Raised when parsing data fails."""

    pass


class ValidationError(PipelineException):
    """Raised when domain validation fails."""

    pass


class KafkaPublishError(PipelineException):
    """Raised when publishing to Kafka fails."""

    pass
