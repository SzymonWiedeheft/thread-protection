"""Utilities module for Kafka and Delta Lake operations."""

from .kafka_utils import (
    get_kafka_options,
    get_kafka_write_options,
    validate_kafka_connectivity,
    get_kafka_metadata_columns,
    extract_kafka_metadata_expr,
)

from .delta_utils import (
    create_delta_table_if_not_exists,
    get_delta_table,
    optimize_delta_table,
    vacuum_delta_table,
    get_delta_table_version,
    get_streaming_write_options,
    write_stream_to_delta,
    get_table_statistics,
)

__all__ = [
    # Kafka utilities
    "get_kafka_options",
    "get_kafka_write_options",
    "validate_kafka_connectivity",
    "get_kafka_metadata_columns",
    "extract_kafka_metadata_expr",
    # Delta utilities
    "create_delta_table_if_not_exists",
    "get_delta_table",
    "optimize_delta_table",
    "vacuum_delta_table",
    "get_delta_table_version",
    "get_streaming_write_options",
    "write_stream_to_delta",
    "get_table_statistics",
]
