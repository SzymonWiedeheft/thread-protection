"""Hosts file format parser."""

import re
import structlog
from typing import List
from datetime import datetime
from schemas import DomainModel, is_valid_domain
from common import ParseError
from .base_parser import BaseParser

logger = structlog.get_logger()


class HostsParser(BaseParser):
    """Parser for hosts file format.

    Example format:
        0.0.0.0 malicious-site.com
        127.0.0.1 tracker.example.com
        # Comment line
    """

    def __init__(self, source_name: str, category: str):
        """
        Initialize hosts parser.

        Args:
            source_name: Name of the data source
            category: Category (malware or ads_trackers)
        """
        super().__init__(source_name, category, "hosts")
        # Regex to match hosts file entries
        # Format: IP_ADDRESS DOMAIN
        self.hosts_pattern = re.compile(
            r"^(?:0\.0\.0\.0|127\.0\.0\.1)\s+([a-zA-Z0-9\.-]+)$"
        )

    def parse(self, content: str, metadata: dict = None) -> List[DomainModel]:
        """
        Parse hosts file content.

        Args:
            content: Raw hosts file content
            metadata: Optional metadata from fetcher

        Returns:
            List of DomainModel objects

        Raises:
            ParseError: If parsing fails
        """
        if not content:
            raise ParseError(f"Empty content from source {self.source_name}")

        logger.info(
            "Parsing hosts file",
            source=self.source_name,
            content_length=len(content),
        )

        domains = []
        lines = content.splitlines()

        for line_number, line in enumerate(lines, start=1):
            # Strip whitespace
            line = line.strip()

            # Skip empty lines and comments
            if not line or line.startswith("#"):
                continue

            # Match hosts file pattern
            match = self.hosts_pattern.match(line)
            if match:
                domain = match.group(1).lower().strip()

                # Validate domain
                if is_valid_domain(domain):
                    domain_model = DomainModel(
                        domain=domain,
                        category=self.category,
                        source=self.source_name,
                        source_format=self.source_format,
                        raw_entry=line,
                        ingestion_timestamp=datetime.utcnow(),
                        metadata={
                            "line_number": line_number,
                            **(metadata or {}),
                        },
                    )
                    domains.append(domain_model)

        logger.info(
            "Hosts file parsing complete",
            source=self.source_name,
            total_lines=len(lines),
            domains_extracted=len(domains),
        )

        return domains
