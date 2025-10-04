"""Base fetcher abstract class."""

from abc import ABC, abstractmethod
from typing import Dict, Any


class BaseFetcher(ABC):
    """Abstract base class for data fetchers."""

    def __init__(self, source_name: str, url: str):
        """
        Initialize fetcher.

        Args:
            source_name: Name of the data source
            url: URL to fetch from
        """
        self.source_name = source_name
        self.url = url

    @abstractmethod
    async def fetch(self) -> Dict[str, Any]:
        """
        Fetch data from source.

        Returns:
            Dictionary containing:
                - content: The fetched content as string
                - metadata: Metadata about the fetch (status, size, etc.)

        Raises:
            FetchError: If fetching fails
        """
        pass
