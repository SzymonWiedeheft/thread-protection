"""Domain validation rules."""

import re
from typing import Optional

# Domain validation regex
# Matches valid domain names (e.g., example.com, sub.example.com)
DOMAIN_PATTERN = re.compile(
    r"^(?!-)[A-Za-z0-9-]{1,63}(?<!-)(\.[A-Za-z0-9-]{1,63})*\.[A-Za-z]{2,}$"
)

# Localhost patterns to exclude
LOCALHOST_PATTERNS = [
    "localhost",
    "127.0.0.1",
    "0.0.0.0",
    "::1",
    "local",
]


def is_valid_domain(domain: str) -> bool:
    """
    Check if domain is valid.

    Args:
        domain: Domain name to validate

    Returns:
        True if valid, False otherwise
    """
    if not domain:
        return False

    # Remove whitespace
    domain = domain.strip().lower()

    # Check length
    if len(domain) > 253:
        return False

    # Check for localhost patterns
    if any(pattern in domain for pattern in LOCALHOST_PATTERNS):
        return False

    # Check against regex pattern
    return bool(DOMAIN_PATTERN.match(domain))


def clean_domain(domain: str) -> Optional[str]:
    """
    Clean and normalize domain.

    Args:
        domain: Domain to clean

    Returns:
        Cleaned domain or None if invalid
    """
    if not domain:
        return None

    # Strip whitespace and lowercase
    domain = domain.strip().lower()

    # Remove common prefixes
    domain = domain.removeprefix("www.")
    domain = domain.removeprefix("http://")
    domain = domain.removeprefix("https://")

    # Remove trailing slashes
    domain = domain.rstrip("/")

    # Remove port numbers
    if ":" in domain:
        domain = domain.split(":")[0]

    # Validate
    if is_valid_domain(domain):
        return domain

    return None
