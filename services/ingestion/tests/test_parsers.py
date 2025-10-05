"""Tests for data parsers."""

import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from datetime import datetime
from common import ParseError
from parsers import HostsParser, AdBlockParser


# ==================== HostsParser Tests ====================


def test_hosts_parser_valid_entries():
    """Test parsing valid hosts file entries."""
    parser = HostsParser("test_source", "malware")

    content = """
# Comment line
0.0.0.0 malicious1.com
127.0.0.1 malicious2.com
0.0.0.0 tracker.example.com

# Another comment
127.0.0.1 ads.example.net
    """

    domains = parser.parse(content)

    assert len(domains) == 4
    assert domains[0].domain == "malicious1.com"
    assert domains[1].domain == "malicious2.com"
    assert domains[2].domain == "tracker.example.com"
    assert domains[3].domain == "ads.example.net"

    # Check all have correct category and source
    for domain in domains:
        assert domain.category == "malware"
        assert domain.source == "test_source"
        assert domain.source_format == "hosts"


def test_hosts_parser_skips_comments():
    """Test that parser skips comment lines."""
    parser = HostsParser("test_source", "ads_trackers")

    content = """
# This is a comment
! This is also a comment
0.0.0.0 valid-domain.com
# 0.0.0.0 commented-out.com
    """

    domains = parser.parse(content)

    assert len(domains) == 1
    assert domains[0].domain == "valid-domain.com"


def test_hosts_parser_skips_empty_lines():
    """Test that parser skips empty lines."""
    parser = HostsParser("test_source", "malware")

    content = """
0.0.0.0 domain1.com


127.0.0.1 domain2.com

    """

    domains = parser.parse(content)

    assert len(domains) == 2


def test_hosts_parser_invalid_format():
    """Test that parser skips invalid format lines."""
    parser = HostsParser("test_source", "malware")

    content = """
0.0.0.0 valid-domain.com
192.168.1.1 invalid-ip.com
just-domain.com
0.0.0.0 another-valid.com
    """

    domains = parser.parse(content)

    # Should only parse lines with 0.0.0.0 or 127.0.0.1
    assert len(domains) == 2
    assert domains[0].domain == "valid-domain.com"
    assert domains[1].domain == "another-valid.com"


def test_hosts_parser_case_normalization():
    """Test that parser normalizes domain case."""
    parser = HostsParser("test_source", "malware")

    content = """
0.0.0.0 UPPERCASE.COM
127.0.0.1 MixedCase.Example.Com
    """

    domains = parser.parse(content)

    assert domains[0].domain == "uppercase.com"
    assert domains[1].domain == "mixedcase.example.com"


def test_hosts_parser_metadata():
    """Test that parser includes metadata."""
    parser = HostsParser("test_source", "malware")

    content = "0.0.0.0 test-domain.com"
    metadata = {"http_status": 200, "content_length": 1000}

    domains = parser.parse(content, metadata)

    assert len(domains) == 1
    assert domains[0].metadata["line_number"] == 1
    assert domains[0].metadata["http_status"] == 200
    assert domains[0].metadata["content_length"] == 1000


def test_hosts_parser_empty_content():
    """Test parser with empty content."""
    parser = HostsParser("test_source", "malware")

    with pytest.raises(ParseError):
        parser.parse("")


def test_hosts_parser_raw_entry():
    """Test that parser stores raw entry."""
    parser = HostsParser("test_source", "malware")

    content = "0.0.0.0 test-domain.com"

    domains = parser.parse(content)

    assert domains[0].raw_entry == "0.0.0.0 test-domain.com"


# ==================== AdBlockParser Tests ====================


def test_adblock_parser_valid_entries():
    """Test parsing valid AdBlock filter entries."""
    parser = AdBlockParser("test_source", "ads_trackers")

    content = """
! Comment line
||malicious1.com^
||tracker.example.com^$third-party
||ads.example.net^

# Another comment
||analytics.site.com^$important
    """

    domains = parser.parse(content)

    assert len(domains) == 4
    assert domains[0].domain == "malicious1.com"
    assert domains[1].domain == "tracker.example.com"
    assert domains[2].domain == "ads.example.net"
    assert domains[3].domain == "analytics.site.com"

    # Check all have correct category and source
    for domain in domains:
        assert domain.category == "ads_trackers"
        assert domain.source == "test_source"
        assert domain.source_format == "adblock"


def test_adblock_parser_with_modifiers():
    """Test parsing AdBlock entries with modifiers."""
    parser = AdBlockParser("test_source", "ads_trackers")

    content = """
||domain1.com^
||domain2.com^$third-party
||domain3.com^$script,important
||domain4.com^$domain=example.com
    """

    domains = parser.parse(content)

    assert len(domains) == 4
    assert all(d.domain.startswith("domain") for d in domains)


def test_adblock_parser_skips_comments():
    """Test that parser skips comment lines."""
    parser = AdBlockParser("test_source", "ads_trackers")

    content = """
! This is a comment
# This is also a comment
||valid-domain.com^
! ||commented-out.com^
[Adblock Plus 2.0]
    """

    domains = parser.parse(content)

    assert len(domains) == 1
    assert domains[0].domain == "valid-domain.com"


def test_adblock_parser_invalid_format():
    """Test that parser skips invalid format lines."""
    parser = AdBlockParser("test_source", "ads_trackers")

    content = """
||valid-domain.com^
invalid-line-without-pipes
domain.com
||another-valid.com^
    """

    domains = parser.parse(content)

    assert len(domains) == 2
    assert domains[0].domain == "valid-domain.com"
    assert domains[1].domain == "another-valid.com"


def test_adblock_parser_case_normalization():
    """Test that parser normalizes domain case."""
    parser = AdBlockParser("test_source", "ads_trackers")

    content = """
||UPPERCASE.COM^
||MixedCase.Example.Com^
    """

    domains = parser.parse(content)

    assert domains[0].domain == "uppercase.com"
    assert domains[1].domain == "mixedcase.example.com"


def test_adblock_parser_metadata():
    """Test that parser includes metadata."""
    parser = AdBlockParser("test_source", "ads_trackers")

    content = "||test-domain.com^"
    metadata = {"http_status": 200, "content_length": 1000}

    domains = parser.parse(content, metadata)

    assert len(domains) == 1
    assert domains[0].metadata["line_number"] == 1
    assert domains[0].metadata["http_status"] == 200
    assert domains[0].metadata["content_length"] == 1000


def test_adblock_parser_empty_content():
    """Test parser with empty content."""
    parser = AdBlockParser("test_source", "ads_trackers")

    with pytest.raises(ParseError):
        parser.parse("")


def test_adblock_parser_raw_entry():
    """Test that parser stores raw entry."""
    parser = AdBlockParser("test_source", "ads_trackers")

    content = "||test-domain.com^$third-party"

    domains = parser.parse(content)

    assert domains[0].raw_entry == "||test-domain.com^$third-party"


# ==================== Mixed Tests ====================


def test_parsers_timestamp():
    """Test that parsers add ingestion timestamp."""
    hosts_parser = HostsParser("test_source", "malware")
    adblock_parser = AdBlockParser("test_source", "ads_trackers")

    hosts_domains = hosts_parser.parse("0.0.0.0 domain.com")
    adblock_domains = adblock_parser.parse("||domain.com^")

    assert isinstance(hosts_domains[0].ingestion_timestamp, datetime)
    assert isinstance(adblock_domains[0].ingestion_timestamp, datetime)


def test_parsers_initialization():
    """Test parser initialization."""
    hosts_parser = HostsParser("source1", "malware")
    adblock_parser = AdBlockParser("source2", "ads_trackers")

    assert hosts_parser.source_name == "source1"
    assert hosts_parser.category == "malware"
    assert hosts_parser.source_format == "hosts"

    assert adblock_parser.source_name == "source2"
    assert adblock_parser.category == "ads_trackers"
    assert adblock_parser.source_format == "adblock"


def test_parsers_large_file():
    """Test parsers with large content."""
    hosts_parser = HostsParser("test_source", "malware")

    # Generate large content
    lines = [f"0.0.0.0 domain{i}.com" for i in range(10000)]
    content = "\n".join(lines)

    domains = hosts_parser.parse(content)

    assert len(domains) == 10000


def test_parsers_special_characters():
    """Test parsers handle special characters in domains."""
    hosts_parser = HostsParser("test_source", "malware")

    content = """
0.0.0.0 domain-with-dash.com
0.0.0.0 sub.domain.example.com
0.0.0.0 123-numeric.com
    """

    domains = hosts_parser.parse(content)

    assert len(domains) == 3
