"""Domain validation rules."""

import re
from typing import Optional
import ipaddress

# Domain validation regex
# Matches valid domain names (e.g., example.com, sub.example.com)
# - Labels must be 1-63 characters
# - Labels must start and end with alphanumeric
# - Labels can contain hyphens but not at start/end
# - TLD must be at least 2 characters and alphabetic
DOMAIN_PATTERN = re.compile(
    r"^(?!-)[A-Za-z0-9-]{1,63}(?<!-)(\.[A-Za-z0-9-]{1,63}(?<!-))*\.[A-Za-z]{2,}$"
)

# Localhost patterns to exclude
LOCALHOST_PATTERNS = [
    "localhost",
    "127.0.0.1",
    "0.0.0.0",
    "::1",
    "local",
    "localdomain",
]

# Reserved/private TLDs to exclude
RESERVED_TLDS = [
    "local",
    "localhost",
    "test",
    "example",
    "invalid",
    "onion",  # Tor addresses
    "i2p",  # I2P addresses
]


def is_valid_domain(domain: str) -> bool:
    """
    Check if domain is valid.

    Args:
        domain: Domain name to validate

    Returns:
        True if valid, False otherwise

    Examples:
        >>> is_valid_domain("example.com")
        True
        >>> is_valid_domain("sub.example.com")
        True
        >>> is_valid_domain("localhost")
        False
        >>> is_valid_domain("192.168.1.1")
        False
    """
    if not domain:
        return False

    # Remove whitespace and convert to lowercase
    domain = domain.strip().lower()

    # Check length constraints
    if len(domain) < 4 or len(domain) > 253:
        return False

    # Check for IP addresses (not valid domains)
    try:
        ipaddress.ip_address(domain)
        return False  # It's an IP address, not a domain
    except ValueError:
        pass  # Not an IP, continue validation

    # Check for localhost patterns
    if any(
        pattern == domain or domain.endswith(f".{pattern}")
        for pattern in LOCALHOST_PATTERNS
    ):
        return False

    # Check for reserved TLDs
    tld = domain.split(".")[-1]
    if tld in RESERVED_TLDS:
        return False

    # Check for invalid characters
    if any(
        char in domain
        for char in [
            " ",
            "_",
            ",",
            ";",
            "!",
            "@",
            "#",
            "$",
            "%",
            "^",
            "&",
            "*",
            "(",
            ")",
            "=",
            "+",
            "[",
            "]",
            "{",
            "}",
            "|",
            "\\",
            "<",
            ">",
            "?",
            "/",
            "`",
            "~",
        ]
    ):
        return False

    # Check against regex pattern
    return bool(DOMAIN_PATTERN.match(domain))


def clean_domain(domain: str) -> Optional[str]:
    """
    Clean and normalize domain.

    Removes common prefixes (www, http/https), port numbers,
    paths, and other artifacts from domain strings.

    Args:
        domain: Domain to clean

    Returns:
        Cleaned domain or None if invalid

    Examples:
        >>> clean_domain("  Example.COM  ")
        'example.com'
        >>> clean_domain("https://www.example.com/path")
        'example.com'
        >>> clean_domain("example.com:8080")
        'example.com'
    """
    if not domain:
        return None

    # Strip whitespace and convert to lowercase
    domain = domain.strip().lower()

    # Remove URL schemes
    for prefix in ["https://", "http://", "ftp://", "//"]:
        if domain.startswith(prefix):
            domain = domain[len(prefix) :]

    # Remove www prefix (multiple levels)
    while domain.startswith("www."):
        domain = domain[4:]

    # Remove path, query params, and fragments
    for delimiter in ["/", "?", "#"]:
        if delimiter in domain:
            domain = domain.split(delimiter)[0]

    # Remove port numbers
    if ":" in domain and not domain.count(":") > 1:  # Not IPv6
        domain = domain.split(":")[0]

    # Remove trailing dots
    domain = domain.rstrip(".")

    # Final cleanup
    domain = domain.strip()

    # Validate cleaned domain
    if is_valid_domain(domain):
        return domain

    return None
