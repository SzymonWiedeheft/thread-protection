"""Spark configuration for Delta Lake and Kafka streaming."""

from typing import Dict, Optional
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import structlog

logger = structlog.get_logger()


# Spark configuration for Delta Lake and Kafka
SPARK_CONF = {
    # Delta Lake configuration
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    # Hive Metastore configuration (shared with Trino)
    "spark.sql.catalogImplementation": "hive",
    "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",
    "spark.sql.warehouse.dir": "/data/delta",
    # Streaming configuration
    "spark.sql.streaming.schemaInference": "true",
    # Performance tuning
    "spark.sql.shuffle.partitions": "16",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.default.parallelism": "2",
    # Delta optimizations
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
    "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": "true",
    # Memory configuration (defaults - can be overridden per stream)
    "spark.driver.memory": "512m",
    "spark.executor.memory": "768m",
    "spark.executor.cores": "1",
    "spark.cores.max": "1",  # Limit max cores per application
    "spark.driver.maxResultSize": "512m",
    # Dynamic resource allocation
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.shuffleTracking.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "0",
    "spark.dynamicAllocation.maxExecutors": "2",
    "spark.dynamicAllocation.initialExecutors": "1",
    "spark.dynamicAllocation.executorIdleTimeout": "60s",
    # Fair Scheduler for resource sharing
    "spark.scheduler.mode": "FAIR",
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


# Resource profiles per stream type
# Bronze handles raw ingestion (heaviest load)
# Silver does transformations (medium load)
# Gold does aggregations (lightest load)
STREAM_RESOURCE_PROFILES = {
    "bronze": {
        "spark.driver.memory": "512m",
        "spark.executor.memory": "1g",
        "spark.cores.max": "1",
    },
    "silver": {
        "spark.driver.memory": "512m",
        "spark.executor.memory": "768m",
        "spark.cores.max": "1",
    },
    "gold": {
        "spark.driver.memory": "512m",
        "spark.executor.memory": "512m",
        "spark.cores.max": "1",
    },
}


def create_spark_session(
    app_name: str = "DomainStreamingPipeline",
    master: Optional[str] = None,
    stream_type: Optional[str] = None,
    additional_conf: Optional[Dict[str, str]] = None,
) -> SparkSession:
    """
    Create and configure Spark session with Delta Lake support.

    Args:
        app_name: Application name
        master: Spark master URL (e.g., "local[*]", "spark://host:7077")
        stream_type: Stream type (bronze, silver, gold) for resource profile
        additional_conf: Additional Spark configuration

    Returns:
        Configured SparkSession

    Examples:
        >>> spark = create_spark_session("BronzeStream", stream_type="bronze")
        >>> spark.version
        '3.5.0'
    """
    logger.info(
        "Creating Spark session",
        app_name=app_name,
        master=master,
        stream_type=stream_type,
    )

    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    # Apply default configuration
    for key, value in SPARK_CONF.items():
        builder = builder.config(key, value)

    # Apply stream-specific resource profile if provided
    if stream_type and stream_type.lower() in STREAM_RESOURCE_PROFILES:
        logger.info(f"Applying {stream_type} resource profile")
        for key, value in STREAM_RESOURCE_PROFILES[stream_type.lower()].items():
            builder = builder.config(key, value)

    # Apply additional configuration (overrides everything)
    if additional_conf:
        for key, value in additional_conf.items():
            builder = builder.config(key, value)

    extra_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
        "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.6",
    ]

    try:
        spark = configure_spark_with_delta_pip(
            builder, extra_packages=extra_packages
        ).getOrCreate()
    except (TypeError, AttributeError) as exc:
        logger.warning(
            "Falling back to default Spark builder; Delta integration unavailable",
            error=str(exc),
        )
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
