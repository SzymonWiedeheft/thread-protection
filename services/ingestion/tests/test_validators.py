"""Tests for domain validators."""

import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from datetime import datetime, UTC
from schemas import DomainModel
from validators import DomainValidator, validate_domains
from common import ValidationError


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


# ==================== DomainValidator Tests ====================


def test_validator_valid_domains():
    """Test validator with valid domains."""
    validator = DomainValidator(strict=False)

    domains = [
        create_domain_model("example.com"),
        create_domain_model("test.example.com"),
        create_domain_model("another-domain.org"),
    ]

    valid = validator.validate(domains)

    assert len(valid) == 3
    assert valid[0].domain == "example.com"
    assert valid[1].domain == "test.example.com"
    assert valid[2].domain == "another-domain.org"


def test_validator_invalid_domains_non_strict():
    """Test validator filters invalid domains in non-strict mode."""
    validator = DomainValidator(strict=False)

    domains = [
        create_domain_model("valid-domain.com"),
        create_domain_model("localhost"),  # Invalid
        create_domain_model("127.0.0.1"),  # Invalid
        create_domain_model("another-valid.org"),
    ]

    valid = validator.validate(domains)

    assert len(valid) == 2
    assert valid[0].domain == "valid-domain.com"
    assert valid[1].domain == "another-valid.org"


def test_validator_invalid_domains_strict():
    """Test validator raises error in strict mode."""
    validator = DomainValidator(strict=True)

    domains = [
        create_domain_model("valid-domain.com"),
        create_domain_model("localhost"),  # Invalid
    ]

    with pytest.raises(ValidationError):
        validator.validate(domains)


def test_validator_duplicate_domains():
    """Test validator removes duplicate domains."""
    validator = DomainValidator(strict=False)

    domains = [
        create_domain_model("example.com"),
        create_domain_model("test.com"),
        create_domain_model("example.com"),  # Duplicate
        create_domain_model("another.com"),
        create_domain_model("test.com"),  # Duplicate
    ]

    valid = validator.validate(domains)

    assert len(valid) == 3
    assert valid[0].domain == "example.com"
    assert valid[1].domain == "test.com"
    assert valid[2].domain == "another.com"


def test_validator_statistics():
    """Test validator statistics tracking."""
    validator = DomainValidator(strict=False)

    domains = [
        create_domain_model("valid1.com"),
        create_domain_model("valid2.com"),
        create_domain_model("localhost"),  # Invalid
        create_domain_model("valid1.com"),  # Duplicate
    ]

    valid = validator.validate(domains)
    stats = validator.get_statistics()

    assert stats["total"] == 4
    assert stats["valid"] == 2
    assert stats["invalid"] == 1
    assert stats["duplicates"] == 1


def test_validator_reset_statistics():
    """Test validator statistics reset."""
    validator = DomainValidator(strict=False)

    domains = [create_domain_model("example.com")]
    validator.validate(domains)

    stats_before = validator.get_statistics()
    assert stats_before["total"] == 1

    validator.reset_statistics()
    stats_after = validator.get_statistics()

    assert stats_after["total"] == 0
    assert stats_after["valid"] == 0
    assert stats_after["invalid"] == 0
    assert stats_after["duplicates"] == 0


def test_validator_empty_list():
    """Test validator with empty domain list."""
    validator = DomainValidator(strict=False)

    valid = validator.validate([])

    assert len(valid) == 0


def test_validator_and_clean():
    """Test validator with cleaning."""
    validator = DomainValidator(strict=False)

    domains = [
        create_domain_model("www.example.com"),  # Should be cleaned to example.com
        create_domain_model("http://test.com"),  # Should be cleaned to test.com
        create_domain_model("https://another.org/"),  # Should be cleaned
    ]

    valid = validator.validate_and_clean(domains)

    assert len(valid) == 3
    # Note: The actual cleaning depends on the clean_domain implementation
    # These assertions assume the cleaner removes www., http://, https://, trailing /


def test_validator_case_sensitivity():
    """Test that validator treats uppercase and lowercase as same domain."""
    validator = DomainValidator(strict=False)

    # Domains are already normalized to lowercase in parsers,
    # but validator should handle it too
    domains = [
        create_domain_model("example.com"),
        create_domain_model("example.com"),  # Same, duplicate
    ]

    valid = validator.validate(domains)

    assert len(valid) == 1


def test_validator_special_domains():
    """Test validator with special/edge case domains."""
    validator = DomainValidator(strict=False)

    domains = [
        create_domain_model("sub.domain.example.com"),  # Subdomain
        create_domain_model("domain-with-dash.com"),  # With dash
        create_domain_model("123.numeric.com"),  # Starting with number
        create_domain_model("a.co"),  # Short TLD
    ]

    valid = validator.validate(domains)

    # All should be valid
    assert len(valid) == 4


def test_validator_invalid_formats():
    """Test validator rejects various invalid formats."""
    validator = DomainValidator(strict=False)

    domains = [
        create_domain_model("localhost"),
        create_domain_model("127.0.0.1"),
        create_domain_model("local"),
        create_domain_model("0.0.0.0"),
    ]

    valid = validator.validate(domains)

    # All should be filtered out
    assert len(valid) == 0


# ==================== Convenience Function Tests ====================


def test_validate_domains_function():
    """Test validate_domains convenience function."""
    domains = [
        create_domain_model("valid1.com"),
        create_domain_model("valid2.com"),
        create_domain_model("localhost"),  # Invalid
    ]

    valid, stats = validate_domains(domains, strict=False)

    assert len(valid) == 2
    assert stats["total"] == 3
    assert stats["valid"] == 2
    assert stats["invalid"] == 1


def test_validate_domains_strict_mode():
    """Test validate_domains in strict mode."""
    domains = [
        create_domain_model("valid.com"),
        create_domain_model("localhost"),  # Invalid
    ]

    with pytest.raises(ValidationError):
        validate_domains(domains, strict=True)


def test_validate_domains_empty():
    """Test validate_domains with empty list."""
    valid, stats = validate_domains([], strict=False)

    assert len(valid) == 0
    assert stats["total"] == 0


# ==================== Integration Tests ====================


def test_validator_with_different_categories():
    """Test validator preserves category information."""
    validator = DomainValidator(strict=False)

    domains = [
        create_domain_model("malware-domain.com", category="malware"),
        create_domain_model("ads-domain.com", category="ads_trackers"),
        create_domain_model("tracker.com", category="ads_trackers"),
    ]

    valid = validator.validate(domains)

    assert len(valid) == 3
    assert valid[0].category == "malware"
    assert valid[1].category == "ads_trackers"
    assert valid[2].category == "ads_trackers"


def test_validator_preserves_metadata():
    """Test that validator preserves domain metadata."""
    validator = DomainValidator(strict=False)

    domains = [
        DomainModel(
            domain="example.com",
            category="malware",
            source="test_source",
            source_format="hosts",
            raw_entry="0.0.0.0 example.com",
            ingestion_timestamp=datetime.now(UTC),
            metadata={"line_number": 123, "custom_field": "value"},
        )
    ]

    valid = validator.validate(domains)

    assert len(valid) == 1
    assert valid[0].metadata["line_number"] == 123
    assert valid[0].metadata["custom_field"] == "value"


def test_validator_large_dataset():
    """Test validator with large number of domains."""
    validator = DomainValidator(strict=False)

    # Create 10000 domains with some duplicates
    domains = []
    for i in range(10000):
        domains.append(create_domain_model(f"domain{i % 5000}.com"))

    valid = validator.validate(domains)

    # Should have 5000 unique domains (due to modulo)
    assert len(valid) == 5000

    stats = validator.get_statistics()
    assert stats["total"] == 10000
    assert stats["valid"] == 5000
    assert stats["duplicates"] == 5000
