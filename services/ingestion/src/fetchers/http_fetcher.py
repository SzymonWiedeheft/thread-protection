"""HTTP fetcher with retry logic."""

import asyncio
import aiohttp
import structlog
from typing import Dict, Any
from common import FetchError
from .base_fetcher import BaseFetcher

logger = structlog.get_logger()


class HTTPFetcher(BaseFetcher):
    """HTTP fetcher with retry logic."""

    def __init__(
        self,
        source_name: str,
        url: str,
        timeout: int = 30,
        retries: int = 3,
        backoff: float = 5.0,
    ):
        """
        Initialize HTTP fetcher.

        Args:
            source_name: Name of the data source
            url: URL to fetch from
            timeout: Request timeout in seconds
            retries: Number of retry attempts
            backoff: Initial backoff time for retries
        """
        super().__init__(source_name, url)
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff

    async def fetch(self) -> Dict[str, Any]:
        """
        Fetch data from HTTP source with retry logic.

        Returns:
            Dictionary containing:
                - content: The fetched content as string
                - metadata: Metadata (http_status, content_length, etc.)

        Raises:
            FetchError: If fetching fails after all retries
        """
        logger.info(
            "Starting HTTP fetch",
            source=self.source_name,
            url=self.url,
            timeout=self.timeout,
        )

        attempt = 0
        current_backoff = self.backoff

        while attempt < self.retries:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        self.url,
                        timeout=aiohttp.ClientTimeout(total=self.timeout),
                    ) as response:
                        response.raise_for_status()
                        content = await response.text()

                        metadata = {
                            "http_status": response.status,
                            "content_length": len(content),
                            "content_type": response.headers.get("Content-Type", ""),
                            "source_url": self.url,
                        }

                        logger.info(
                            "HTTP fetch successful",
                            source=self.source_name,
                            status=response.status,
                            content_length=len(content),
                        )

                        return {
                            "content": content,
                            "metadata": metadata,
                        }

            except (aiohttp.ClientError, aiohttp.ServerTimeoutError, Exception) as e:
                attempt += 1

                if attempt >= self.retries:
                    logger.error(
                        "HTTP fetch failed after all retries",
                        source=self.source_name,
                        url=self.url,
                        attempts=attempt,
                        error=str(e),
                    )
                    raise FetchError(
                        f"Failed to fetch from {self.url} after {attempt} attempts: {str(e)}"
                    ) from e

                logger.warning(
                    "HTTP fetch failed, retrying",
                    source=self.source_name,
                    url=self.url,
                    attempt=attempt,
                    max_retries=self.retries,
                    backoff_seconds=current_backoff,
                    error=str(e),
                )

                # Exponential backoff
                await asyncio.sleep(current_backoff)
                current_backoff *= 2

        # Shouldn't reach here, but for type safety
        raise FetchError(f"Failed to fetch from {self.url}")
