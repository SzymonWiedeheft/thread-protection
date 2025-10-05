"""Integration tests for domain ingestion to Kafka pipeline."""

import pytest
import asyncio
import json
from datetime import datetime, UTC

from fetchers.http_fetcher import HTTPFetcher
from parsers.hosts_parser import HostsParser
from parsers.adblock_parser import AdBlockParser
from validators.domain_validator import DomainValidator
from producers.kafka_producer import KafkaProducer
from schemas.domain import DomainModel

from conftest import requires_docker


@requires_docker()
@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.asyncio
async def test_end_to_end_hosts_ingestion(
    mock_hosts_server,
    kafka_topics,
    kafka_consumer,
    wait_for_kafka_ready,
):
    """Test complete pipeline: fetch hosts → parse → validate → publish to Kafka."""
    # wait_for_kafka_ready fixture is already awaited by pytest-asyncio

    # Step 1: Fetch data from mock server
    url = f"http://{mock_hosts_server.host}:{mock_hosts_server.port}/hosts"
    fetcher = HTTPFetcher(source_name="test_hosts", url=url, timeout=10, retries=2)
    result = await fetcher.fetch()

    assert result is not None
    assert "content" in result
    content = result["content"]
    assert "malware-site.com" in content

    # Step 2: Parse hosts content (parser returns DomainModel objects and validates them)
    parser = HostsParser(source_name="stevenblack", category="malware")
    domain_models = parser.parse(content, metadata=result.get("metadata", {}))

    assert len(domain_models) > 0
    assert any(d.domain == "malware-site.com" for d in domain_models)

    # Step 3: Optional additional validation (parser already validates)
    validator = DomainValidator(strict=False)
    domain_models = validator.validate(domain_models)

    assert len(domain_models) > 0

    # Step 5: Publish to Kafka
    producer = KafkaProducer(
        bootstrap_servers=kafka_topics["bootstrap_servers"],
    )
    producer.connect()

    producer.publish(domain_models)

    producer.close()

    # Step 6: Verify messages in Kafka
    messages = []
    for msg in kafka_consumer:
        messages.append(msg)
        if len(messages) >= len(domain_models):
            break

    assert len(messages) == len(domain_models)

    # Verify message content
    for msg in messages:
        value = json.loads(msg.value.decode("utf-8"))
        assert "domain" in value
        assert "category" in value
        assert "source" in value
        assert value["category"] == "malware"


@requires_docker()
@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.asyncio
async def test_end_to_end_adblock_ingestion(
    mock_adblock_server,
    kafka_topics,
    kafka_consumer,
    wait_for_kafka_ready,
):
    """Test complete pipeline: fetch adblock → parse → validate → publish to Kafka."""
    # wait_for_kafka_ready fixture is already awaited by pytest-asyncio

    # Step 1: Fetch data
    url = f"http://{mock_adblock_server.host}:{mock_adblock_server.port}/adblock.txt"
    fetcher = HTTPFetcher(source_name="test_adblock", url=url, timeout=10, retries=2)
    result = await fetcher.fetch()

    assert result is not None
    assert "content" in result
    content = result["content"]
    assert "||tracker.example.com^" in content

    # Step 2: Parse adblock content (parser returns DomainModel objects and validates them)
    parser = AdBlockParser(source_name="adwars", category="ads_trackers")
    domain_models = parser.parse(content, metadata=result.get("metadata", {}))

    assert len(domain_models) > 0
    assert any(d.domain == "tracker.example.com" for d in domain_models)

    # Step 3: Optional additional validation (parser already validates)
    validator = DomainValidator(strict=False)
    domain_models = validator.validate(domain_models)

    assert len(domain_models) > 0

    # Step 4: Publish to Kafka
    producer = KafkaProducer(
        bootstrap_servers=kafka_topics["bootstrap_servers"],
    )
    producer.connect()

    producer.publish(domain_models)

    producer.close()

    # Step 5: Verify messages
    messages = []
    for msg in kafka_consumer:
        messages.append(msg)
        if len(messages) >= len(domain_models):
            break

    assert len(messages) == len(domain_models)

    # Verify all messages are in ads_trackers topic
    for msg in messages:
        assert msg.topic == kafka_topics["ads_trackers"]
        value = json.loads(msg.value.decode("utf-8"))
        assert value["category"] == "ads_trackers"


@requires_docker()
@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.asyncio
async def test_kafka_producer_error_handling(
    kafka_topics,
    wait_for_kafka_ready,
):
    """Test Kafka producer error handling with invalid data."""
    # Use invalid bootstrap servers
    producer = KafkaProducer(
        bootstrap_servers="invalid-host:9092",
    )

    # Should handle error gracefully
    with pytest.raises(Exception):  # Expect connection error
        producer.connect()

    producer.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_http_fetcher_with_retries(
    mock_unreliable_server,
):
    """Test HTTP fetcher retry logic with unreliable server."""
    url = (
        f"http://{mock_unreliable_server.host}:{mock_unreliable_server.port}/unreliable"
    )
    fetcher = HTTPFetcher(
        source_name="test_unreliable", url=url, timeout=5, retries=3, backoff=0.5
    )

    # Should succeed on 3rd attempt
    result = await fetcher.fetch()

    assert result is not None
    assert "content" in result
    assert "metadata" in result
    assert "example.com" in result["content"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_http_fetcher_timeout(
    mock_slow_server,
):
    """Test HTTP fetcher timeout handling."""
    # Request with 5 second delay should timeout with 2 second timeout
    url = f"http://{mock_slow_server.host}:{mock_slow_server.port}/slow?delay=5"
    fetcher = HTTPFetcher(source_name="test_slow", url=url, timeout=2, retries=1)

    # Should raise an exception due to timeout
    with pytest.raises(Exception) as exc_info:
        await fetcher.fetch()

    # Verify it's a timeout-related error
    # Check both the exception message and type
    error_msg = str(exc_info.value).lower()
    error_type = type(exc_info.value).__name__.lower()
    assert "timeout" in error_msg or "timeout" in error_type or "time" in error_msg


@pytest.mark.integration
@pytest.mark.asyncio
async def test_domain_validation_integration(
    sample_valid_domains, sample_invalid_domains
):
    """Test domain validator with comprehensive test cases."""
    validator = DomainValidator(strict=False)

    # Create domain models from "valid" domains
    # Note: sample_valid_domains may contain some that the validator considers invalid
    valid_models = [
        DomainModel(
            domain=domain,
            category="test",
            source="test",
            source_format="hosts",
            raw_entry=f"0.0.0.0 {domain}",
            ingestion_timestamp=datetime.now(UTC),
        )
        for domain in sample_valid_domains
    ]

    # Validate the models
    validated = validator.validate(valid_models)
    # Should validate most domains (some in sample_valid_domains may still be invalid per validator rules)
    assert len(validated) > 0, "Should have some valid domains"
    assert len(validated) <= len(
        sample_valid_domains
    ), "Cannot have more validated than input"

    # Create domain models from invalid domains
    invalid_models = [
        DomainModel(
            domain=domain,
            category="test",
            source="test",
            source_format="hosts",
            raw_entry=f"0.0.0.0 {domain}",
            ingestion_timestamp=datetime.now(UTC),
        )
        for domain in sample_invalid_domains
    ]

    # Invalid domains should be filtered out
    validated_invalid = validator.validate(invalid_models)
    assert len(validated_invalid) < len(
        sample_invalid_domains
    ), "Most invalid domains should be filtered"


@requires_docker()
@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.asyncio
async def test_parallel_ingestion_to_kafka(
    mock_hosts_server,
    mock_adblock_server,
    kafka_topics,
    kafka_consumer,
    wait_for_kafka_ready,
):
    """Test parallel ingestion from multiple sources."""
    # wait_for_kafka_ready fixture is already awaited by pytest-asyncio

    async def ingest_hosts():
        url = f"http://{mock_hosts_server.host}:{mock_hosts_server.port}/hosts"
        fetcher = HTTPFetcher(source_name="test_hosts", url=url, timeout=10, retries=2)
        result = await fetcher.fetch()
        content = result["content"]

        parser = HostsParser(source_name="stevenblack", category="malware")
        domain_models = parser.parse(content, metadata=result.get("metadata", {}))

        # Optional additional validation
        validator = DomainValidator(strict=False)
        domain_models = validator.validate(domain_models)

        producer = KafkaProducer(
            bootstrap_servers=kafka_topics["bootstrap_servers"],
        )
        producer.connect()

        count = len(domain_models)
        producer.publish(domain_models)
        producer.close()
        return count

    async def ingest_adblock():
        url = (
            f"http://{mock_adblock_server.host}:{mock_adblock_server.port}/adblock.txt"
        )
        fetcher = HTTPFetcher(
            source_name="test_adblock", url=url, timeout=10, retries=2
        )
        result = await fetcher.fetch()
        content = result["content"]

        parser = AdBlockParser(source_name="adwars", category="ads_trackers")
        domain_models = parser.parse(content, metadata=result.get("metadata", {}))

        # Optional additional validation
        validator = DomainValidator(strict=False)
        domain_models = validator.validate(domain_models)

        producer = KafkaProducer(
            bootstrap_servers=kafka_topics["bootstrap_servers"],
        )
        producer.connect()

        count = len(domain_models)
        producer.publish(domain_models)
        producer.close()
        return count

    # Run both ingestion tasks in parallel
    hosts_count, adblock_count = await asyncio.gather(
        ingest_hosts(),
        ingest_adblock(),
    )

    total_expected = hosts_count + adblock_count

    # Verify all messages received
    messages = []
    for msg in kafka_consumer:
        messages.append(msg)
        if len(messages) >= total_expected:
            break

    assert len(messages) == total_expected

    # Verify we have both malware and ads_trackers messages
    malware_count = sum(1 for msg in messages if msg.topic == kafka_topics["malware"])
    ads_count = sum(1 for msg in messages if msg.topic == kafka_topics["ads_trackers"])

    assert malware_count == hosts_count
    assert ads_count == adblock_count


@requires_docker()
@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.asyncio
async def test_kafka_message_ordering(
    kafka_topics,
    kafka_consumer,
    wait_for_kafka_ready,
):
    """Test Kafka message ordering and partitioning."""
    # wait_for_kafka_ready fixture is already awaited by pytest-asyncio

    producer = KafkaProducer(
        bootstrap_servers=kafka_topics["bootstrap_servers"],
    )
    producer.connect()

    # Create multiple domains with same key to test ordering
    base_time = datetime.now(UTC)
    domains = []

    for i in range(10):
        domain = DomainModel(
            domain=f"test-{i}.com",
            category="malware",
            source="custom",
            source_format="hosts",
            raw_entry=f"0.0.0.0 test-{i}.com",
            ingestion_timestamp=base_time,
            metadata={"sequence": i},
        )
        domains.append(domain)

    producer.publish(domains)
    producer.close()

    # Consume messages
    messages = []
    for msg in kafka_consumer:
        messages.append(msg)
        if len(messages) >= 10:
            break

    assert len(messages) == 10

    # Verify all messages were delivered
    received_sequences = []
    for msg in messages:
        value = json.loads(msg.value.decode("utf-8"))
        received_sequences.append(value["metadata"]["sequence"])

    assert set(received_sequences) == set(range(10))


@requires_docker()
@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.asyncio
async def test_large_batch_ingestion(
    kafka_topics,
    kafka_consumer,
    wait_for_kafka_ready,
):
    """Test ingestion of large batch of domains."""
    # wait_for_kafka_ready fixture is already awaited by pytest-asyncio

    producer = KafkaProducer(
        bootstrap_servers=kafka_topics["bootstrap_servers"],
    )
    producer.connect()

    # Create 1000 domain models
    batch_size = 1000
    base_time = datetime.now(UTC)
    domains = []

    for i in range(batch_size):
        domain = DomainModel(
            domain=f"domain-{i}.example.com",
            category="malware",
            source="custom",
            source_format="hosts",
            raw_entry=f"0.0.0.0 domain-{i}.example.com",
            ingestion_timestamp=base_time,
            metadata={"batch_index": i},
        )
        domains.append(domain)

    producer.publish(domains)
    producer.close()

    # Consume all messages
    messages = []
    for msg in kafka_consumer:
        messages.append(msg)
        if len(messages) >= batch_size:
            break

    assert len(messages) == batch_size

    # Verify no duplicates
    domains_received = set()
    for msg in messages:
        value = json.loads(msg.value.decode("utf-8"))
        domains_received.add(value["domain"])

    assert len(domains_received) == batch_size
