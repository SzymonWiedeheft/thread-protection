"""Base parser abstract class."""

from abc import ABC, abstractmethod
from typing import List
from schemas import DomainModel


class BaseParser(ABC):
    """Abstract base class for data parsers."""

    def __init__(self, source_name: str, category: str, source_format: str):
        """
        Initialize parser.

        Args:
            source_name: Name of the data source
            category: Category (malware or ads_trackers)
            source_format: Format (hosts or adblock)
        """
        self.source_name = source_name
        self.category = category
        self.source_format = source_format

    @abstractmethod
    def parse(self, content: str, metadata: dict = None) -> List[DomainModel]:
        """
        Parse content and extract domains.

        Args:
            content: Raw content to parse
            metadata: Optional metadata from fetcher

        Returns:
            List of DomainModel objects

        Raises:
            ParseError: If parsing fails
        """
        pass
