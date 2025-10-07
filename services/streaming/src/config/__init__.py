"""Configuration module for Spark and streaming settings."""

from .spark_config import (
    create_spark_session,
    get_kafka_bootstrap_servers,
    get_delta_path,
    get_checkpoint_path,
    get_kafka_topics,
    get_trigger_interval,
    SPARK_CONF,
    KAFKA_CONF,
    DELTA_PATHS,
    CHECKPOINT_PATHS,
    KAFKA_TOPICS,
    TRIGGER_INTERVALS,
)

__all__ = [
    "create_spark_session",
    "get_kafka_bootstrap_servers",
    "get_delta_path",
    "get_checkpoint_path",
    "get_kafka_topics",
    "get_trigger_interval",
    "SPARK_CONF",
    "KAFKA_CONF",
    "DELTA_PATHS",
    "CHECKPOINT_PATHS",
    "KAFKA_TOPICS",
    "TRIGGER_INTERVALS",
]
