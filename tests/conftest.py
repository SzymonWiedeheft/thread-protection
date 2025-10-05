"""Pytest configuration and fixtures for integration tests."""

# Import utility functions
from fixtures.utils import is_docker_available, requires_docker

# Import all fixtures to make them available to tests
from fixtures.docker_fixtures import (
    docker_compose_file,
    kafka_container,
    kafka_bootstrap_servers,
    kafka_admin_client,
    kafka_topics,
    kafka_producer,
    kafka_consumer,
    wait_for_kafka_ready,
    clean_kafka_topics,
    delta_lake_container,
    trino_container,
    spark_container,
    delta_lake_config,
    spark_session_config,
    ingestion_container_config,
    event_loop,
)

from fixtures.mock_servers import (
    mock_hosts_server,
    mock_adblock_server,
    mock_unreliable_server,
    mock_slow_server,
    mock_redirect_server,
    mock_auth_server,
    mock_etag_server,
    mock_large_response_server,
    mock_error_responses_server,
    mock_malformed_server,
)

from fixtures.sample_data import (
    sample_hosts_content,
    sample_adblock_content,
    sample_malformed_data,
    sample_domains_list,
    sample_kafka_message_payloads,
    sample_valid_domains,
    sample_invalid_domains,
    sample_source_configs,
    sample_http_responses,
    sample_kafka_config,
    sample_validation_stats,
)

__all__ = [
    # Utilities
    "is_docker_available",
    "requires_docker",
    # Docker fixtures
    "docker_compose_file",
    "kafka_container",
    "kafka_bootstrap_servers",
    "kafka_admin_client",
    "kafka_topics",
    "kafka_producer",
    "kafka_consumer",
    "wait_for_kafka_ready",
    "clean_kafka_topics",
    "delta_lake_container",
    "trino_container",
    "spark_container",
    "delta_lake_config",
    "spark_session_config",
    "ingestion_container_config",
    "event_loop",
    # Mock server fixtures
    "mock_hosts_server",
    "mock_adblock_server",
    "mock_unreliable_server",
    "mock_slow_server",
    "mock_redirect_server",
    "mock_auth_server",
    "mock_etag_server",
    "mock_large_response_server",
    "mock_error_responses_server",
    "mock_malformed_server",
    # Sample data fixtures
    "sample_hosts_content",
    "sample_adblock_content",
    "sample_malformed_data",
    "sample_domains_list",
    "sample_kafka_message_payloads",
    "sample_valid_domains",
    "sample_invalid_domains",
    "sample_source_configs",
    "sample_http_responses",
    "sample_kafka_config",
    "sample_validation_stats",
]
