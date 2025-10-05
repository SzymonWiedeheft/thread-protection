"""Tests for Kafka producer."""

import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from unittest.mock import MagicMock, patch, Mock
from datetime import datetime, UTC
from kafka.errors import KafkaError
from schemas import DomainModel
from producers import KafkaProducer
from common import KafkaPublishError


def create_domain_model(domain: str, category: str = "malware", source: str = "test"):
    """Helper to create DomainModel for testing."""
    return DomainModel(
        domain=domain,
        category=category,
        source=source,
        source_format="hosts",
        raw_entry=f"0.0.0.0 {domain}",
        ingestion_timestamp=datetime.now(UTC),
        metadata={"line_number": 1},
    )


# ==================== KafkaProducer Initialization Tests ====================


def test_kafka_producer_initialization():
    """Test Kafka producer initialization."""
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        batch_size=2000,
        linger_ms=200,
        max_retries=5,
    )

    assert producer.bootstrap_servers == "localhost:9092"
    assert producer.batch_size == 2000
    assert producer.linger_ms == 200
    assert producer.max_retries == 5
    assert producer.producer is None


def test_kafka_producer_default_values():
    """Test Kafka producer default values."""
    producer = KafkaProducer()

    assert producer.bootstrap_servers == "kafka:9092"
    assert producer.batch_size == 1000
    assert producer.linger_ms == 100
    assert producer.max_retries == 3


# ==================== Topic Mapping Tests ====================


def test_get_topic_malware():
    """Test getting topic for malware category."""
    producer = KafkaProducer()

    topic = producer.get_topic("malware")

    assert topic == "malware_domains"


def test_get_topic_ads_trackers():
    """Test getting topic for ads_trackers category."""
    producer = KafkaProducer()

    topic = producer.get_topic("ads_trackers")

    assert topic == "ads_trackers_domains"


def test_get_topic_invalid_category():
    """Test getting topic with invalid category."""
    producer = KafkaProducer()

    with pytest.raises(ValueError) as exc_info:
        producer.get_topic("invalid_category")

    assert "Invalid category" in str(exc_info.value)


# ==================== Connection Tests ====================


@patch("producers.kafka_producer.KafkaClient")
def test_kafka_producer_connect_success(mock_kafka_client):
    """Test successful Kafka connection."""
    producer = KafkaProducer(bootstrap_servers="localhost:9092")

    producer.connect()

    mock_kafka_client.assert_called_once()
    assert producer.producer is not None


@patch("producers.kafka_producer.KafkaClient")
def test_kafka_producer_connect_failure(mock_kafka_client):
    """Test Kafka connection failure."""
    mock_kafka_client.side_effect = Exception("Connection failed")

    producer = KafkaProducer()

    with pytest.raises(KafkaPublishError):
        producer.connect()


@patch("producers.kafka_producer.KafkaClient")
def test_kafka_producer_close(mock_kafka_client):
    """Test closing Kafka producer."""
    mock_producer = MagicMock()
    mock_kafka_client.return_value = mock_producer

    producer = KafkaProducer()
    producer.connect()
    producer.close()

    mock_producer.flush.assert_called_once()
    mock_producer.close.assert_called_once()


# ==================== Publishing Tests ====================


@patch("producers.kafka_producer.KafkaClient")
def test_publish_single_domain(mock_kafka_client):
    """Test publishing a single domain."""
    mock_producer = MagicMock()
    mock_kafka_client.return_value = mock_producer
    mock_producer.send.return_value = MagicMock()

    producer = KafkaProducer()
    producer.connect()

    domains = [create_domain_model("example.com", category="malware")]

    stats = producer.publish(domains)

    assert stats["malware"] == 1
    assert stats["ads_trackers"] == 0
    assert stats["total"] == 1

    mock_producer.send.assert_called_once()
    mock_producer.flush.assert_called_once()


@patch("producers.kafka_producer.KafkaClient")
def test_publish_multiple_domains(mock_kafka_client):
    """Test publishing multiple domains."""
    mock_producer = MagicMock()
    mock_kafka_client.return_value = mock_producer
    mock_producer.send.return_value = MagicMock()

    producer = KafkaProducer()
    producer.connect()

    domains = [
        create_domain_model("malware1.com", category="malware"),
        create_domain_model("malware2.com", category="malware"),
        create_domain_model("ads1.com", category="ads_trackers"),
        create_domain_model("ads2.com", category="ads_trackers"),
        create_domain_model("ads3.com", category="ads_trackers"),
    ]

    stats = producer.publish(domains)

    assert stats["malware"] == 2
    assert stats["ads_trackers"] == 3
    assert stats["total"] == 5

    assert mock_producer.send.call_count == 5
    mock_producer.flush.assert_called_once()


@patch("producers.kafka_producer.KafkaClient")
def test_publish_without_connection(mock_kafka_client):
    """Test publishing without connecting first."""
    producer = KafkaProducer()

    domains = [create_domain_model("example.com")]

    with pytest.raises(KafkaPublishError) as exc_info:
        producer.publish(domains)

    assert "not connected" in str(exc_info.value)


@patch("producers.kafka_producer.KafkaClient")
def test_publish_empty_list(mock_kafka_client):
    """Test publishing empty domain list."""
    mock_producer = MagicMock()
    mock_kafka_client.return_value = mock_producer

    producer = KafkaProducer()
    producer.connect()

    stats = producer.publish([])

    assert stats["malware"] == 0
    assert stats["ads_trackers"] == 0
    assert stats["total"] == 0

    mock_producer.send.assert_not_called()


@patch("producers.kafka_producer.KafkaClient")
def test_publish_with_kafka_error(mock_kafka_client):
    """Test publishing with Kafka error on send."""
    mock_producer = MagicMock()
    mock_kafka_client.return_value = mock_producer
    mock_producer.send.side_effect = KafkaError("Send failed")

    producer = KafkaProducer()
    producer.connect()

    domains = [
        create_domain_model("example1.com", category="malware"),
        create_domain_model("example2.com", category="malware"),
    ]

    stats = producer.publish(domains)

    # Should continue despite errors
    assert stats["total"] == 0  # No successful sends
    assert mock_producer.send.call_count == 2


@patch("producers.kafka_producer.KafkaClient")
def test_publish_correct_topics(mock_kafka_client):
    """Test that domains are published to correct topics."""
    mock_producer = MagicMock()
    mock_kafka_client.return_value = mock_producer
    mock_producer.send.return_value = MagicMock()

    producer = KafkaProducer()
    producer.connect()

    domains = [
        create_domain_model("malware.com", category="malware"),
        create_domain_model("tracker.com", category="ads_trackers"),
    ]

    producer.publish(domains)

    # Check that send was called with correct topics
    calls = mock_producer.send.call_args_list
    assert calls[0][0][0] == "malware_domains"  # First positional argument
    assert calls[1][0][0] == "ads_trackers_domains"  # First positional argument


@patch("producers.kafka_producer.KafkaClient")
def test_publish_with_domain_as_key(mock_kafka_client):
    """Test that domain is used as partition key."""
    mock_producer = MagicMock()
    mock_kafka_client.return_value = mock_producer
    mock_producer.send.return_value = MagicMock()

    producer = KafkaProducer()
    producer.connect()

    domains = [create_domain_model("example.com", category="malware")]

    producer.publish(domains)

    call_args = mock_producer.send.call_args
    assert call_args[1]["key"] == "example.com"


# ==================== Batch Publishing Tests ====================


@patch("producers.kafka_producer.KafkaClient")
def test_publish_batch(mock_kafka_client):
    """Test batch publishing from multiple sources."""
    mock_producer = MagicMock()
    mock_kafka_client.return_value = mock_producer
    mock_producer.send.return_value = MagicMock()

    producer = KafkaProducer()
    producer.connect()

    domains_by_source = {
        "source1": [
            create_domain_model("domain1.com", category="malware"),
            create_domain_model("domain2.com", category="malware"),
        ],
        "source2": [
            create_domain_model("domain3.com", category="ads_trackers"),
        ],
    }

    stats = producer.publish_batch(domains_by_source)

    assert stats["overall"]["malware"] == 2
    assert stats["overall"]["ads_trackers"] == 1
    assert stats["overall"]["total"] == 3

    assert "source1" in stats["by_source"]
    assert "source2" in stats["by_source"]

    assert stats["by_source"]["source1"]["malware"] == 2
    assert stats["by_source"]["source2"]["ads_trackers"] == 1


@patch("producers.kafka_producer.KafkaClient")
def test_publish_batch_empty(mock_kafka_client):
    """Test batch publishing with empty sources."""
    mock_producer = MagicMock()
    mock_kafka_client.return_value = mock_producer

    producer = KafkaProducer()
    producer.connect()

    stats = producer.publish_batch({})

    assert stats["overall"]["total"] == 0


# ==================== Context Manager Tests ====================


@patch("producers.kafka_producer.KafkaClient")
def test_context_manager(mock_kafka_client):
    """Test using KafkaProducer as context manager."""
    mock_producer = MagicMock()
    mock_kafka_client.return_value = mock_producer
    mock_producer.send.return_value = MagicMock()

    domains = [create_domain_model("example.com")]

    with KafkaProducer() as producer:
        stats = producer.publish(domains)

        assert stats["total"] == 1

    # Check that connect and close were called
    mock_kafka_client.assert_called_once()
    mock_producer.flush.assert_called()
    mock_producer.close.assert_called_once()


# ==================== Integration Tests ====================


@patch("producers.kafka_producer.KafkaClient")
def test_full_workflow(mock_kafka_client):
    """Test complete producer workflow."""
    mock_producer = MagicMock()
    mock_kafka_client.return_value = mock_producer
    mock_producer.send.return_value = MagicMock()

    # Create producer
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092", batch_size=500, linger_ms=50
    )

    # Connect
    producer.connect()
    assert producer.producer is not None

    # Publish domains
    domains = [
        create_domain_model("malware1.com", category="malware", source="urlhaus"),
        create_domain_model("malware2.com", category="malware", source="urlhaus"),
        create_domain_model("ads1.com", category="ads_trackers", source="stevenblack"),
    ]

    stats = producer.publish(domains)

    assert stats["malware"] == 2
    assert stats["ads_trackers"] == 1
    assert stats["total"] == 3

    # Close
    producer.close()
    mock_producer.close.assert_called_once()


@patch("producers.kafka_producer.KafkaClient")
def test_producer_configuration_passed_to_kafka(mock_kafka_client):
    """Test that producer configuration is passed to Kafka client."""
    producer = KafkaProducer(
        bootstrap_servers="host1:9092,host2:9092",
        batch_size=2000,
        linger_ms=200,
        max_retries=5,
    )

    producer.connect()

    call_args = mock_kafka_client.call_args
    assert call_args[1]["bootstrap_servers"] == ["host1:9092", "host2:9092"]
    assert call_args[1]["batch_size"] == 2000
    assert call_args[1]["linger_ms"] == 200
    assert call_args[1]["retries"] == 5
    assert call_args[1]["acks"] == "all"
    assert call_args[1]["compression_type"] == "gzip"
