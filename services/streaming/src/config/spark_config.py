"""Spark configuration for Delta Lake and Kafka streaming."""

from typing import Dict, Optional
from pyspark.sql import SparkSession
import structlog

logger = structlog.get_logger()


# Spark configuration for Delta Lake and Kafka
SPARK_CONF = {
    # Delta Lake configuration
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    # Streaming configuration
    "spark.sql.streaming.schemaInference": "true",
    "spark.sql.streaming.checkpointFileManagerClass": "org.apache.spark.sql.execution.streaming.CheckpointFileManager",
    # Performance tuning
    "spark.sql.shuffle.partitions": "100",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    # Delta optimizations
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
    "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": "true",
    # Memory configuration
    "spark.driver.memory": "2g",
    "spark.executor.memory": "2g",
    "spark.driver.maxResultSize": "1g",
    # Serialization
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    # Logging
    "spark.eventLog.enabled": "false",  # Disable event logs for now
}


# Kafka-specific configuration
KAFKA_CONF = {
    "kafka.bootstrap.servers": "kafka:9092",
    "kafka.session.timeout.ms": "30000",
    "kafka.request.timeout.ms": "40000",
    "kafka.max.poll.interval.ms": "300000",
}


# Delta table paths (can be overridden via environment)
DELTA_PATHS = {
    "bronze": "/data/delta/bronze/domains",
    "silver": "/data/delta/silver/domains",
    "gold": "/data/delta/gold/domains",
}


# Checkpoint paths
CHECKPOINT_PATHS = {
    "bronze": "/data/checkpoints/bronze",
    "silver": "/data/checkpoints/silver",
    "gold": "/data/checkpoints/gold",
}


# Kafka topics
KAFKA_TOPICS = {
    "malware": "malware_domains",
    "ads_trackers": "ads_trackers_domains",
}


# Streaming trigger intervals
TRIGGER_INTERVALS = {
    "bronze": "5 seconds",
    "silver": "10 seconds",
    "gold": "30 seconds",
}


def create_spark_session(
    app_name: str = "DomainStreamingPipeline",
    master: Optional[str] = None,
    additional_conf: Optional[Dict[str, str]] = None,
) -> SparkSession:
    """
    Create and configure Spark session with Delta Lake support.

    Args:
        app_name: Application name
        master: Spark master URL (e.g., "local[*]", "spark://host:7077")
        additional_conf: Additional Spark configuration

    Returns:
        Configured SparkSession

    Examples:
        >>> spark = create_spark_session("BronzeStream")
        >>> spark.version
        '3.5.0'
    """
    logger.info("Creating Spark session", app_name=app_name, master=master)

    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    # Apply default configuration
    for key, value in SPARK_CONF.items():
        builder = builder.config(key, value)

    # Apply additional configuration
    if additional_conf:
        for key, value in additional_conf.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        "Spark session created",
        app_name=app_name,
        spark_version=spark.version,
        master=spark.sparkContext.master,
    )

    return spark


def get_kafka_bootstrap_servers() -> str:
    """
    Get Kafka bootstrap servers from environment or default.

    Returns:
        Kafka bootstrap servers string
    """
    import os

    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_CONF["kafka.bootstrap.servers"])


def get_delta_path(layer: str) -> str:
    """
    Get Delta table path for layer.

    Args:
        layer: Layer name (bronze, silver, gold)

    Returns:
        Delta table path

    Raises:
        ValueError: If layer is invalid
    """
    import os

    layer = layer.lower()
    if layer not in DELTA_PATHS:
        raise ValueError(
            f"Invalid layer '{layer}'. Must be one of {list(DELTA_PATHS.keys())}"
        )

    env_var = f"DELTA_{layer.upper()}_PATH"
    return os.getenv(env_var, DELTA_PATHS[layer])


def get_checkpoint_path(layer: str) -> str:
    """
    Get checkpoint path for layer.

    Args:
        layer: Layer name (bronze, silver, gold)

    Returns:
        Checkpoint path

    Raises:
        ValueError: If layer is invalid
    """
    import os

    layer = layer.lower()
    if layer not in CHECKPOINT_PATHS:
        raise ValueError(
            f"Invalid layer '{layer}'. Must be one of {list(CHECKPOINT_PATHS.keys())}"
        )

    env_var = f"CHECKPOINT_{layer.upper()}_PATH"
    return os.getenv(env_var, CHECKPOINT_PATHS[layer])


def get_kafka_topics() -> list:
    """
    Get list of Kafka topics to subscribe to.

    Returns:
        List of Kafka topic names
    """
    return list(KAFKA_TOPICS.values())


def get_trigger_interval(layer: str) -> str:
    """
    Get trigger interval for layer.

    Args:
        layer: Layer name (bronze, silver, gold)

    Returns:
        Trigger interval string (e.g., "5 seconds")

    Raises:
        ValueError: If layer is invalid
    """
    layer = layer.lower()
    if layer not in TRIGGER_INTERVALS:
        raise ValueError(
            f"Invalid layer '{layer}'. Must be one of {list(TRIGGER_INTERVALS.keys())}"
        )

    return TRIGGER_INTERVALS[layer]
