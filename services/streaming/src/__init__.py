"""Streaming service package.

This package provides Spark Structured Streaming pipelines for the
Medallion architecture (Bronze, Silver, Gold layers).
"""

from .streams.bronze_stream import BronzeStream, create_bronze_stream
from .config.spark_config import create_spark_session

__all__ = [
    "BronzeStream",
    "create_bronze_stream",
    "create_spark_session",
]
