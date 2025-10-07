"""Sample data fixtures for testing."""

import pytest
from datetime import datetime, UTC
from schemas.domain import DomainModel


@pytest.fixture
def sample_hosts_content():
    """Sample hosts file content for testing parsers."""
    return """
# Sample hosts file
# Comments and empty lines should be ignored

127.0.0.1 localhost
0.0.0.0 malware-site.com
0.0.0.0 phishing.example.com

# More entries
0.0.0.0 tracker.ads.com
0.0.0.0 dangerous.site.net

# Invalid entries to test error handling
0.0.0.0
invalid_line_without_domain
0.0.0.0 -invalid-domain-.com
""".strip()


@pytest.fixture
def sample_adblock_content():
    """Sample adblock filter list content."""
    return """
[Adblock Plus 2.0]
! Title: Sample Blocklist
! Last modified: 2025-10-05
! Homepage: https://example.com

! Comments should be ignored
||tracker.example.com^
||ads.malicious.net^
||analytics.tracker.io^

! Domain-only rules
||bad-domain.com^
||another-tracker.net^

! Non-domain rules (should be skipped)
##.advertisement
###ad-banner
/ads/tracker.js

! More domains
||crypto-miner.xyz^
""".strip()


@pytest.fixture
def sample_malformed_data():
    """Malformed data for testing error handling."""
    return {
        "empty": "",
        "only_comments": "# Just comments\n! More comments\n\n",
        "invalid_hosts": "999.999.999.999 invalid-ip.com\n0.0.0.0 \n",
        "special_chars": "0.0.0.0 domain-with-émojis😀.com\n0.0.0.0 спам.com",
        "too_long": "0.0.0.0 " + "a" * 300 + ".com",
        "binary_content": b"\x00\x01\x02\x03\x04",
    }


@pytest.fixture
def sample_domains_list():
    """List of DomainModel objects for testing."""
    base_timestamp = datetime.now(UTC)

    return [
        DomainModel(
            domain="malware-site.com",
            category="malware",
            source="stevenblack",
            source_format="hosts",
            raw_entry="0.0.0.0 malware-site.com",
            ingestion_timestamp=base_timestamp,
            metadata={
                "list_name": "StevenBlack Hosts",
                "list_url": "https://raw.githubusercontent.com/StevenBlack/hosts/master/hosts",
            },
        ),
        DomainModel(
            domain="phishing.example.com",
            category="malware",
            source="urlhaus",
            source_format="hosts",
            raw_entry="0.0.0.0 phishing.example.com",
            ingestion_timestamp=base_timestamp,
            metadata={
                "threat_type": "phishing",
                "url": "http://phishing.example.com/fake-login",
            },
        ),
        DomainModel(
            domain="tracker.ads.com",
            category="ads_trackers",
            source="1hosts",
            source_format="adblock",
            raw_entry="||tracker.ads.com^",
            ingestion_timestamp=base_timestamp,
            metadata={
                "list_variant": "Lite",
            },
        ),
        DomainModel(
            domain="analytics.tracker.io",
            category="ads_trackers",
            source="adwars",
            source_format="adblock",
            raw_entry="||analytics.tracker.io^",
            ingestion_timestamp=base_timestamp,
            metadata={
                "adblock_rule": "||analytics.tracker.io^",
            },
        ),
        DomainModel(
            domain="crypto-miner.xyz",
            category="malware",
            source="custom",
            source_format="hosts",
            raw_entry="0.0.0.0 crypto-miner.xyz",
            ingestion_timestamp=base_timestamp,
            metadata={
                "manual_verification": True,
                "threat_type": "cryptomining",
            },
        ),
    ]


@pytest.fixture
def sample_kafka_message_payloads(sample_domains_list):
    """Pre-formatted Kafka message payloads for testing."""
    return [
        {
            "key": domain.domain.encode("utf-8"),
            "value": domain.model_dump_json().encode("utf-8"),
            "headers": [
                ("source", domain.source.encode("utf-8")),
                ("category", domain.category.encode("utf-8")),
                ("ingestion_version", b"1.0.0"),
            ],
        }
        for domain in sample_domains_list
    ]


@pytest.fixture
def sample_valid_domains():
    """List of valid domain strings for validator testing."""
    return [
        "example.com",
        "sub.example.com",
        "deep.sub.example.com",
        "example-with-dash.com",
        "123-numbers.com",
        "a.b.c.d.e.f.example.com",
        "xn--e1afmkfd.xn--p1ai",  # IDN: пример.рф
        "localhost",
        "example.co.uk",
        "test-123.example-site.org",
    ]


@pytest.fixture
def sample_invalid_domains():
    """List of invalid domain strings for validator testing."""
    return [
        "",
        " ",
        ".",
        "..",
        "-.com",
        "example-.com",
        "-example.com",
        "exam ple.com",
        "example..com",
        "example.com.",
        "example.com..",
        "192.168.1.1",  # IP address, not domain
        "example",  # Single label (might be valid in some contexts)
        "a" * 300 + ".com",  # Too long
        "example.com/path",  # Contains path
        "http://example.com",  # Contains scheme
        "example.com:8080",  # Contains port
        "user@example.com",  # Email address
        "*.example.com",  # Wildcard
        "!example.com",  # Special character prefix
    ]


@pytest.fixture
def sample_source_configs():
    """Sample source configurations for testing."""
    return {
        "stevenblack": {
            "name": "StevenBlack Hosts",
            "url": "https://raw.githubusercontent.com/StevenBlack/hosts/master/hosts",
            "parser": "hosts",
            "category": "malware",
            "enabled": True,
            "fetch_interval_hours": 24,
        },
        "urlhaus": {
            "name": "URLhaus",
            "url": "https://urlhaus.abuse.ch/downloads/hostfile/",
            "parser": "hosts",
            "category": "malware",
            "enabled": True,
            "fetch_interval_hours": 6,
        },
        "1hosts_lite": {
            "name": "1Hosts (Lite)",
            "url": "https://o0.pages.dev/Lite/adblock.txt",
            "parser": "adblock",
            "category": "ads_trackers",
            "enabled": True,
            "fetch_interval_hours": 12,
        },
    }


@pytest.fixture
def sample_http_responses():
    """Sample HTTP responses for testing fetchers."""
    return {
        "success": {
            "status": 200,
            "headers": {
                "Content-Type": "text/plain",
                "Last-Modified": "Sat, 05 Oct 2025 10:00:00 GMT",
            },
            "body": "0.0.0.0 example.com\n0.0.0.0 test.com",
        },
        "not_modified": {
            "status": 304,
            "headers": {
                "Last-Modified": "Sat, 05 Oct 2025 10:00:00 GMT",
            },
            "body": "",
        },
        "not_found": {
            "status": 404,
            "headers": {"Content-Type": "text/html"},
            "body": "<html><body>Not Found</body></html>",
        },
        "server_error": {
            "status": 500,
            "headers": {"Content-Type": "text/plain"},
            "body": "Internal Server Error",
        },
        "timeout": {
            "exception": "TimeoutError",
            "message": "Request timed out after 30 seconds",
        },
    }


@pytest.fixture
def sample_kafka_config():
    """Sample Kafka configuration for testing."""
    return {
        "bootstrap_servers": "localhost:9092",
        "topic_malware": "malware_domains",
        "topic_ads_trackers": "ads_trackers_domains",
        "producer_config": {
            "acks": "all",
            "retries": 3,
            "max_in_flight_requests_per_connection": 5,
            "compression_type": "gzip",
            "linger_ms": 100,
            "batch_size": 16384,
        },
    }


@pytest.fixture
def sample_validation_stats():
    """Sample validation statistics for testing."""
    return {
        "total_domains": 1000,
        "valid_domains": 950,
        "invalid_domains": 50,
        "duplicate_domains": 25,
        "unique_domains": 925,
        "invalid_reasons": {
            "too_long": 10,
            "invalid_chars": 15,
            "malformed": 20,
            "reserved": 5,
        },
    }
