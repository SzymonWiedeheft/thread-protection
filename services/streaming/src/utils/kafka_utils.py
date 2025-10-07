"""Kafka utilities for Spark Structured Streaming."""

from typing import Dict, List, Optional
import structlog

logger = structlog.get_logger()


def get_kafka_options(
    bootstrap_servers: str,
    topics: List[str],
    starting_offsets: str = "latest",
    max_offsets_per_trigger: Optional[int] = None,
    fail_on_data_loss: bool = False,
) -> Dict[str, str]:
    """
    Get Kafka readStream options for Spark.

    Args:
        bootstrap_servers: Kafka bootstrap servers (comma-separated)
        topics: List of Kafka topics to subscribe to
        starting_offsets: Starting offset strategy (earliest, latest, or JSON)
        max_offsets_per_trigger: Max offsets to process per trigger (throttling)
        fail_on_data_loss: Whether to fail on data loss

    Returns:
        Dictionary of Kafka options for Spark readStream

    Examples:
        >>> options = get_kafka_options(
        ...     "kafka:9092",
        ...     ["malware_domains", "ads_trackers_domains"],
        ...     starting_offsets="earliest"
        ... )
        >>> spark.readStream.format("kafka").options(**options).load()
    """
    options = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": ",".join(topics),
        "startingOffsets": starting_offsets,
        "failOnDataLoss": str(fail_on_data_loss).lower(),
        # Performance tuning
        "kafka.session.timeout.ms": "30000",
        "kafka.request.timeout.ms": "40000",
        "kafka.max.poll.interval.ms": "300000",
        # Consumer configuration
        "kafka.max.poll.records": "500",
        "kafka.fetch.max.wait.ms": "500",
        "kafka.fetch.min.bytes": "1024",
    }

    if max_offsets_per_trigger:
        options["maxOffsetsPerTrigger"] = str(max_offsets_per_trigger)

    logger.info(
        "Created Kafka options",
        bootstrap_servers=bootstrap_servers,
        topics=topics,
        starting_offsets=starting_offsets,
    )

    return options


def get_kafka_write_options(
    bootstrap_servers: str,
    topic: str,
    checkpoint_location: str,
) -> Dict[str, str]:
    """
    Get Kafka writeStream options for Spark.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        topic: Target Kafka topic
        checkpoint_location: Checkpoint directory

    Returns:
        Dictionary of Kafka write options
    """
    return {
        "kafka.bootstrap.servers": bootstrap_servers,
        "topic": topic,
        "checkpointLocation": checkpoint_location,
    }


def validate_kafka_connectivity(
    bootstrap_servers: str,
    timeout_ms: int = 10000,
) -> bool:
    """
    Validate Kafka connectivity (best effort).

    Note: This is a basic connectivity check. Full validation
    happens when Spark tries to read from Kafka.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        timeout_ms: Connection timeout in milliseconds

    Returns:
        True if connectivity check passes, False otherwise
    """
    try:
        from kafka import KafkaAdminClient
        from kafka.errors import KafkaError
    except ImportError:
        # kafka-python not available (only pyspark.sql.kafka available)
        logger.warning("kafka-python not available, skipping connectivity check")
        return True

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers.split(","),
            request_timeout_ms=timeout_ms,
        )
        # Try to list topics
        topics = admin_client.list_topics()
        admin_client.close()

        logger.info(
            "Kafka connectivity validated",
            bootstrap_servers=bootstrap_servers,
            topic_count=len(topics),
        )
        return True

    except KafkaError as e:
        logger.error(
            "Kafka connectivity check failed",
            bootstrap_servers=bootstrap_servers,
            error=str(e),
        )
        return False


def get_kafka_metadata_columns() -> List[str]:
    """
    Get list of Kafka metadata column names.

    Returns:
        List of Kafka metadata column names available in Spark Kafka source
    """
    return [
        "topic",  # Kafka topic name
        "partition",  # Kafka partition number
        "offset",  # Kafka message offset
        "timestamp",  # Kafka message timestamp
        "timestampType",  # Timestamp type (0=CreateTime, 1=LogAppendTime)
    ]


def extract_kafka_metadata_expr() -> Dict[str, str]:
    """
    Get Spark SQL expressions for extracting Kafka metadata.

    Returns:
        Dictionary mapping output column names to SQL expressions

    Examples:
        >>> metadata = extract_kafka_metadata_expr()
        >>> df.selectExpr(*[f"{expr} as {col}" for col, expr in metadata.items()])
    """
    return {
        "kafka_topic": "topic",
        "kafka_partition": "partition",
        "kafka_offset": "offset",
        "kafka_timestamp": "timestamp",
    }
