"""Streaming fixtures for integration testing with Spark and Delta Lake."""

import pytest
import json
import time
import tempfile
import shutil
from datetime import datetime, UTC, timedelta
from pathlib import Path
from typing import Generator, Dict, List
from pyspark.sql import SparkSession
import structlog

logger = structlog.get_logger()


@pytest.fixture(scope="function")
def temp_delta_paths() -> Generator[Dict[str, str], None, None]:
    """Create temporary directories for Delta Lake layers."""
    temp_dir = tempfile.mkdtemp(prefix="delta_test_")

    paths = {
        "bronze": f"{temp_dir}/bronze",
        "silver": f"{temp_dir}/silver",
        "gold": f"{temp_dir}/gold",
        "checkpoints_bronze": f"{temp_dir}/checkpoints/bronze",
        "checkpoints_silver": f"{temp_dir}/checkpoints/silver",
        "checkpoints_gold": f"{temp_dir}/checkpoints/gold",
    }

    # Create directories
    for path in paths.values():
        Path(path).mkdir(parents=True, exist_ok=True)

    logger.info("Created temporary Delta paths", paths=paths)

    yield paths

    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)
    logger.info("Cleaned up temporary Delta paths")


@pytest.fixture(scope="function")
def spark_session_with_delta(
    kafka_bootstrap_servers: str,
    temp_delta_paths: Dict[str, str],
) -> Generator[SparkSession, None, None]:
    """Create Spark session configured for Delta Lake and Kafka."""

    builder = (
        SparkSession.builder.appName("IntegrationTest-Streaming")
        .master("local[2]")  # Use 2 cores for parallel processing
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")  # Reduce for testing
        .config("spark.default.parallelism", "2")
        .config("spark.sql.streaming.schemaInference", "true")
        .config(
            "spark.sql.streaming.checkpointLocation",
            temp_delta_paths["checkpoints_bronze"],
        )
        # Kafka configurations
        .config("spark.kafka.bootstrap.servers", kafka_bootstrap_servers)
        # Delta Lake optimizations for testing
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # Packages: Delta Lake + Kafka connector
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.3.2,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
    )

    # Create Spark session (packages will be downloaded)
    spark = builder.getOrCreate()

    # Set log level to ERROR to reduce noise from Broken pipe warnings
    spark.sparkContext.setLogLevel("ERROR")

    logger.info(
        "Created Spark session with Delta Lake",
        app_name=spark.sparkContext.appName,
        master=spark.sparkContext.master,
    )

    yield spark

    # Cleanup with delay to allow Spark to finish internal cleanup
    time.sleep(0.5)  # Give Spark time to finish accumulator updates
    spark.stop()
    logger.info("Stopped Spark session")


@pytest.fixture
def sample_kafka_messages() -> List[Dict]:
    """Generate sample Kafka messages for Bronze ingestion testing."""
    base_time = datetime.now(UTC)

    messages = [
        # Malware domains
        {
            "domain": "malware-site.com",
            "category": "malware",
            "source": "stevenblack",
            "source_format": "hosts",
            "raw_entry": "0.0.0.0 malware-site.com",
            "ingestion_timestamp": base_time.isoformat(),
            "metadata": {
                "list_name": "StevenBlack Hosts",
                "threat_type": "malware",
            },
        },
        {
            "domain": "phishing.example.com",
            "category": "malware",
            "source": "urlhaus",
            "source_format": "hosts",
            "raw_entry": "0.0.0.0 phishing.example.com",
            "ingestion_timestamp": base_time.isoformat(),
            "metadata": {
                "threat_type": "phishing",
                "url": "http://phishing.example.com/fake",
            },
        },
        {
            "domain": "trojan.badsite.net",
            "category": "malware",
            "source": "malwaredomainlist",
            "source_format": "hosts",
            "raw_entry": "0.0.0.0 trojan.badsite.net",
            "ingestion_timestamp": base_time.isoformat(),
            "metadata": {"malware_family": "trojan"},
        },
        # Ads/Trackers domains
        {
            "domain": "tracker.ads.com",
            "category": "ads_trackers",
            "source": "easylist",
            "source_format": "adblock",
            "raw_entry": "||tracker.ads.com^",
            "ingestion_timestamp": base_time.isoformat(),
            "metadata": {"tracker_type": "analytics"},
        },
        {
            "domain": "analytics.example.net",
            "category": "ads_trackers",
            "source": "easyprivacy",
            "source_format": "adblock",
            "raw_entry": "||analytics.example.net^",
            "ingestion_timestamp": base_time.isoformat(),
            "metadata": {"tracker_type": "analytics"},
        },
        # Duplicate domain from different source (for Gold testing)
        {
            "domain": "malware-site.com",
            "category": "malware",
            "source": "urlhaus",
            "source_format": "hosts",
            "raw_entry": "0.0.0.0 malware-site.com",
            "ingestion_timestamp": (base_time + timedelta(hours=1)).isoformat(),
            "metadata": {"threat_type": "malware", "confidence": "high"},
        },
    ]

    return messages


@pytest.fixture
def sample_malformed_kafka_messages() -> List[Dict]:
    """Generate malformed Kafka messages for error handling tests."""
    return [
        {"invalid": "missing_required_fields"},
        {"domain": "test.com"},  # Missing category, source, etc.
        None,  # Null message
        {"domain": "", "category": "malware", "source": "test"},  # Empty domain
    ]


@pytest.fixture
def sample_bronze_data(spark_session_with_delta: SparkSession) -> List[Dict]:
    """Generate sample Bronze data for Silver stream testing."""
    base_time = datetime.now(UTC)
    bronze_time = base_time + timedelta(seconds=1)
    ingestion_date = base_time.date()

    return [
        # Valid domains with various formats
        {
            "domain": "Example.COM  ",  # Needs cleaning (uppercase, trailing space)
            "category": "malware",
            "source": "test_source",
            "source_format": "hosts",
            "raw_entry": "0.0.0.0 Example.COM",
            "ingestion_timestamp": base_time,
            "bronze_timestamp": bronze_time,
            "metadata": {"test": "data"},
            "kafka_topic": "test-topic",
            "kafka_offset": 0,
            "kafka_partition": 0,
            "kafka_timestamp": base_time,
            "ingestion_date": ingestion_date,
        },
        {
            "domain": "  valid-domain.net",  # Leading spaces
            "category": "ads_trackers",
            "source": "test_source",
            "source_format": "adblock",
            "raw_entry": "||valid-domain.net^",
            "ingestion_timestamp": base_time,
            "bronze_timestamp": bronze_time,
            "metadata": {},
            "kafka_topic": "test-topic",
            "kafka_offset": 1,
            "kafka_partition": 0,
            "kafka_timestamp": base_time,
            "ingestion_date": ingestion_date,
        },
        # Invalid domains
        {
            "domain": "-invalid-start.com",  # Starts with hyphen
            "category": "malware",
            "source": "test_source",
            "source_format": "hosts",
            "raw_entry": "0.0.0.0 -invalid-start.com",
            "ingestion_timestamp": base_time,
            "bronze_timestamp": bronze_time,
            "metadata": {},
            "kafka_topic": "test-topic",
            "kafka_offset": 2,
            "kafka_partition": 0,
            "kafka_timestamp": base_time,
            "ingestion_date": ingestion_date,
        },
        {
            "domain": "invalid..double-dot.com",  # Double dots
            "category": "malware",
            "source": "test_source",
            "source_format": "hosts",
            "raw_entry": "0.0.0.0 invalid..double-dot.com",
            "ingestion_timestamp": base_time,
            "bronze_timestamp": bronze_time,
            "metadata": {},
            "kafka_topic": "test-topic",
            "kafka_offset": 3,
            "kafka_partition": 0,
            "kafka_timestamp": base_time,
            "ingestion_date": ingestion_date,
        },
        {
            "domain": "good-domain.com",
            "category": "malware",
            "source": "test_source",
            "source_format": "hosts",
            "raw_entry": "0.0.0.0 good-domain.com",
            "ingestion_timestamp": base_time,
            "bronze_timestamp": bronze_time,
            "metadata": {"valid": "True"},
            "kafka_topic": "test-topic",
            "kafka_offset": 4,
            "kafka_partition": 0,
            "kafka_timestamp": base_time,
            "ingestion_date": ingestion_date,
        },
    ]


@pytest.fixture
def sample_silver_data(spark_session_with_delta: SparkSession) -> List[Dict]:
    """Generate sample Silver data for Gold stream testing."""
    base_time = datetime.now(UTC)
    bronze_time = base_time + timedelta(seconds=1)
    silver_time = bronze_time + timedelta(seconds=1)
    ingestion_date = base_time.date()

    return [
        # Same domain from multiple sources
        {
            "domain": "test-domain.com",
            "category": "malware",
            "source": "stevenblack",
            "source_format": "hosts",
            "raw_entry": "0.0.0.0 test-domain.com",
            "ingestion_timestamp": base_time,
            "bronze_timestamp": bronze_time,
            "silver_timestamp": silver_time,
            "metadata": {"source_priority": "high"},
            "is_valid": "true",
            "validation_errors": None,
            "ingestion_date": ingestion_date,
            "bronze_offset": 0,
        },
        {
            "domain": "test-domain.com",
            "category": "malware",
            "source": "urlhaus",
            "source_format": "hosts",
            "raw_entry": "0.0.0.0 test-domain.com",
            "ingestion_timestamp": base_time + timedelta(hours=1),
            "bronze_timestamp": bronze_time + timedelta(hours=1),
            "silver_timestamp": silver_time + timedelta(hours=1),
            "metadata": {"confidence": "high"},
            "is_valid": "true",
            "validation_errors": None,
            "ingestion_date": ingestion_date,
            "bronze_offset": 1,
        },
        # Domain in both categories (test category prioritization)
        {
            "domain": "mixed-category.com",
            "category": "ads_trackers",
            "source": "easylist",
            "source_format": "adblock",
            "raw_entry": "||mixed-category.com^",
            "ingestion_timestamp": base_time,
            "bronze_timestamp": bronze_time,
            "silver_timestamp": silver_time,
            "metadata": {},
            "is_valid": "true",
            "validation_errors": None,
            "ingestion_date": ingestion_date,
            "bronze_offset": 2,
        },
        {
            "domain": "mixed-category.com",
            "category": "malware",
            "source": "malwaredomainlist",
            "source_format": "hosts",
            "raw_entry": "0.0.0.0 mixed-category.com",
            "ingestion_timestamp": base_time + timedelta(minutes=30),
            "bronze_timestamp": bronze_time + timedelta(minutes=30),
            "silver_timestamp": silver_time + timedelta(minutes=30),
            "metadata": {},
            "is_valid": "true",
            "validation_errors": None,
            "ingestion_date": ingestion_date,
            "bronze_offset": 3,
        },
        # Old domain (for is_active testing)
        {
            "domain": "old-domain.net",
            "category": "malware",
            "source": "test",
            "source_format": "hosts",
            "raw_entry": "0.0.0.0 old-domain.net",
            "ingestion_timestamp": base_time - timedelta(days=10),
            "bronze_timestamp": bronze_time - timedelta(days=10),
            "silver_timestamp": silver_time - timedelta(days=10),
            "metadata": {},
            "is_valid": "true",
            "validation_errors": None,
            "ingestion_date": (base_time - timedelta(days=10)).date(),
            "bronze_offset": 4,
        },
        # Invalid domain (should be filtered by Gold)
        {
            "domain": "invalid-domain",
            "category": "malware",
            "source": "test",
            "source_format": "hosts",
            "raw_entry": "0.0.0.0 invalid-domain",
            "ingestion_timestamp": base_time,
            "bronze_timestamp": bronze_time,
            "silver_timestamp": silver_time,
            "metadata": {},
            "is_valid": "false",
            "validation_errors": "Invalid domain format",
            "ingestion_date": ingestion_date,
            "bronze_offset": 5,
        },
    ]


@pytest.fixture
def kafka_publish_helper(kafka_producer, kafka_topics):
    """Helper function to publish messages to Kafka for testing."""

    def publish(messages: List[Dict], topic_key: str = "malware"):
        """
        Publish messages to Kafka topic.

        Args:
            messages: List of message dictionaries
            topic_key: Key in kafka_topics dict ('malware' or 'ads_trackers')
        """
        topic = kafka_topics.get(topic_key, kafka_topics["malware"])

        for msg in messages:
            if msg is None:
                # Test null message - use empty byte string since Kafka requires key or value
                kafka_producer.send(topic, key=b"null_message", value=b"")
            else:
                # Serialize to JSON
                value = json.dumps(msg).encode("utf-8")

                # Use domain as key for partitioning (if present)
                key = (
                    msg.get("domain", "").encode("utf-8") if msg.get("domain") else None
                )

                kafka_producer.send(
                    topic,
                    key=key,
                    value=value,
                )

        # Flush to ensure messages are sent
        kafka_producer.flush()

        logger.info(
            "Published messages to Kafka",
            topic=topic,
            count=len(messages),
        )

    return publish
