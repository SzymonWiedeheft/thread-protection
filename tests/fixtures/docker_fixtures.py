"""Docker fixtures for integration testing using testcontainers."""

import pytest
import asyncio
import time
from typing import Generator
from testcontainers.kafka import KafkaContainer
from testcontainers.core.container import DockerContainer
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
import structlog
from .utils import is_docker_available

logger = structlog.get_logger()

# Skip all Docker fixtures if Docker is not available
pytestmark = pytest.mark.skipif(
    not is_docker_available(), reason="Docker is not available or not running"
)


@pytest.fixture(scope="session")
def docker_compose_file():
    """Path to docker-compose file for integration tests."""
    from pathlib import Path

    return Path(__file__).parent.parent.parent / "docker-compose.test.yml"


@pytest.fixture(scope="session")
def kafka_container() -> Generator[KafkaContainer, None, None]:
    """Kafka container for integration tests."""
    container = KafkaContainer(image="confluentinc/cp-kafka:7.5.0")
    container.start()

    logger.info(
        "Kafka container started",
        bootstrap_servers=container.get_bootstrap_server(),
    )

    yield container

    logger.info("Stopping Kafka container")
    container.stop()


@pytest.fixture(scope="session")
def kafka_bootstrap_servers(kafka_container: KafkaContainer) -> str:
    """Kafka bootstrap servers address."""
    return kafka_container.get_bootstrap_server()


@pytest.fixture(scope="session")
def kafka_admin_client(
    kafka_bootstrap_servers: str,
) -> Generator[KafkaAdminClient, None, None]:
    """Kafka admin client for managing topics."""
    admin = KafkaAdminClient(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id="test-admin",
    )

    yield admin

    admin.close()


@pytest.fixture(scope="function")
def kafka_topics(
    kafka_admin_client: KafkaAdminClient,
    kafka_bootstrap_servers: str,
) -> Generator[dict, None, None]:
    """Create Kafka topics for testing."""
    from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
    import time

    topics = [
        NewTopic(
            name="malware_domains",
            num_partitions=3,
            replication_factor=1,
        ),
        NewTopic(
            name="ads_trackers_domains",
            num_partitions=3,
            replication_factor=1,
        ),
    ]

    topic_names = [t.name for t in topics]

    # Wait for topics to be fully deleted if they exist
    max_retries = 10
    for attempt in range(max_retries):
        try:
            existing_topics = kafka_admin_client.list_topics()
            topics_to_delete = [name for name in topic_names if name in existing_topics]

            if topics_to_delete:
                logger.info(f"Found existing topics, deleting: {topics_to_delete}")
                try:
                    kafka_admin_client.delete_topics(topics_to_delete)
                except UnknownTopicOrPartitionError:
                    pass  # Topics already deleted
                time.sleep(2)  # Wait for deletion to complete
                continue

            # Try to create topics
            kafka_admin_client.create_topics(topics, validate_only=False)
            logger.info("Created Kafka topics", topics=topic_names)
            break
        except TopicAlreadyExistsError as e:
            if attempt < max_retries - 1:
                logger.warning(
                    f"Topics still exist (attempt {attempt + 1}/{max_retries}), waiting..."
                )
                time.sleep(2)
            else:
                raise RuntimeError(
                    f"Failed to create topics after {max_retries} attempts: {e}"
                )

    yield {
        "malware": "malware_domains",
        "ads_trackers": "ads_trackers_domains",
        "bootstrap_servers": kafka_bootstrap_servers,
    }

    # Cleanup topics
    try:
        kafka_admin_client.delete_topics(topic_names)
        logger.info("Deleted Kafka topics", topics=topic_names)
        # Wait a bit for deletion to start
        time.sleep(1)
    except UnknownTopicOrPartitionError:
        logger.info("Topics already deleted", topics=topic_names)


@pytest.fixture
def kafka_producer(
    kafka_bootstrap_servers: str,
) -> Generator[KafkaProducer, None, None]:
    """Kafka producer for integration tests."""
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        acks="all",
        retries=3,
        max_in_flight_requests_per_connection=1,
    )

    yield producer

    producer.flush()
    producer.close()


@pytest.fixture
def kafka_consumer(
    kafka_bootstrap_servers: str,
    kafka_topics: dict,
) -> Generator[KafkaConsumer, None, None]:
    """Kafka consumer for integration tests."""
    consumer = KafkaConsumer(
        kafka_topics["malware"],
        kafka_topics["ads_trackers"],
        bootstrap_servers=kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="test-consumer-group",
        consumer_timeout_ms=10000,
    )

    yield consumer

    consumer.close()


@pytest.fixture(scope="session")
def delta_lake_container() -> Generator[DockerContainer, None, None]:
    """MinIO container for Delta Lake storage (S3-compatible)."""
    container = (
        DockerContainer(image="minio/minio:latest")
        .with_command("server /data --console-address :9001")
        .with_exposed_ports(9000, 9001)
        .with_env("MINIO_ROOT_USER", "minioadmin")
        .with_env("MINIO_ROOT_PASSWORD", "minioadmin")
        .with_volume_mapping("/tmp/minio-data", "/data")
    )

    container.start()
    time.sleep(5)  # Wait for MinIO to start

    logger.info(
        "MinIO container started",
        api_port=container.get_exposed_port(9000),
        console_port=container.get_exposed_port(9001),
    )

    yield container

    logger.info("Stopping MinIO container")
    container.stop()


@pytest.fixture(scope="session")
def trino_container(
    kafka_container: KafkaContainer,
) -> Generator[DockerContainer, None, None]:
    """Trino container for querying Delta Lake and Kafka."""
    container = (
        DockerContainer(image="trinodb/trino:latest")
        .with_exposed_ports(8080)
        .with_env("CATALOG_MANAGEMENT", "dynamic")
    )

    container.start()
    time.sleep(10)  # Wait for Trino to start

    logger.info(
        "Trino container started",
        port=container.get_exposed_port(8080),
    )

    yield container

    logger.info("Stopping Trino container")
    container.stop()


@pytest.fixture(scope="session")
def spark_container() -> Generator[DockerContainer, None, None]:
    """Spark container for stream processing."""
    container = (
        DockerContainer(image="bitnami/spark:3.5")
        .with_exposed_ports(8080, 7077, 4040)
        .with_env("SPARK_MODE", "master")
        .with_env("SPARK_MASTER_HOST", "localhost")
        .with_env("SPARK_MASTER_PORT", "7077")
    )

    container.start()
    time.sleep(15)  # Wait for Spark to start

    logger.info(
        "Spark container started",
        master_port=container.get_exposed_port(7077),
        ui_port=container.get_exposed_port(8080),
    )

    yield container

    logger.info("Stopping Spark container")
    container.stop()


@pytest.fixture
def delta_lake_config(delta_lake_container: DockerContainer) -> dict:
    """Delta Lake configuration using MinIO."""
    port = delta_lake_container.get_exposed_port(9000)

    return {
        "endpoint": f"http://localhost:{port}",
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "bucket": "delta-lake",
        "bronze_path": "s3a://delta-lake/bronze",
        "silver_path": "s3a://delta-lake/silver",
        "gold_path": "s3a://delta-lake/gold",
    }


@pytest.fixture
def spark_session_config(
    kafka_bootstrap_servers: str,
    delta_lake_config: dict,
):
    """Spark session configuration for integration tests."""
    return {
        "app_name": "IntegrationTest",
        "master": "local[*]",
        "config": {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.hadoop.fs.s3a.endpoint": delta_lake_config["endpoint"],
            "spark.hadoop.fs.s3a.access.key": delta_lake_config["access_key"],
            "spark.hadoop.fs.s3a.secret.key": delta_lake_config["secret_key"],
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.kafka.bootstrap.servers": kafka_bootstrap_servers,
        },
    }


@pytest.fixture(scope="function")
async def wait_for_kafka_ready(kafka_bootstrap_servers: str):
    """Wait for Kafka to be fully ready."""
    from kafka.errors import NoBrokersAvailable

    max_retries = 30
    retry_delay = 1

    for attempt in range(max_retries):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=kafka_bootstrap_servers,
                client_id="readiness-check",
            )
            admin.close()
            logger.info("Kafka is ready")
            return
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                logger.debug(
                    "Kafka not ready, retrying",
                    attempt=attempt + 1,
                    max_retries=max_retries,
                )
                await asyncio.sleep(retry_delay)
            else:
                raise RuntimeError("Kafka failed to become ready in time")


@pytest.fixture(scope="function")
def clean_kafka_topics(kafka_admin_client: KafkaAdminClient):
    """Clean up Kafka topics before test."""

    def _clean():
        topics = kafka_admin_client.list_topics()
        if topics:
            kafka_admin_client.delete_topics(list(topics))
            logger.info("Cleaned Kafka topics", topics=list(topics))

    return _clean


@pytest.fixture
def ingestion_container_config(kafka_bootstrap_servers: str) -> dict:
    """Configuration for running ingestion service in Docker."""
    return {
        "image": "thread-protection-ingestion:test",
        "environment": {
            "KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap_servers,
            "LOG_LEVEL": "DEBUG",
            "FETCH_TIMEOUT": "30",
            "MAX_RETRIES": "3",
        },
        "volumes": {
            "./services/ingestion/src": {"bind": "/app/src", "mode": "ro"},
            "./services/ingestion/tests": {"bind": "/app/tests", "mode": "ro"},
        },
    }


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async fixtures."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
