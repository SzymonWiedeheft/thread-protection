"""HTTP fetcher with retry logic."""

import aiohttp
import structlog
from typing import Dict, Any
from tenacity import (
    AsyncRetrying,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
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

        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(self.retries),
                wait=wait_exponential(multiplier=self.backoff, min=self.backoff),
                retry=retry_if_exception_type(
                    (aiohttp.ClientError, aiohttp.ServerTimeoutError)
                ),
                reraise=True,
            ):
                with attempt:
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
                                "content_type": response.headers.get(
                                    "Content-Type", ""
                                ),
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

        except (aiohttp.ClientError, aiohttp.ServerTimeoutError) as e:
            logger.error(
                "HTTP fetch failed after all retries",
                source=self.source_name,
                url=self.url,
                attempts=self.retries,
                error=str(e),
            )
            raise FetchError(
                message=f"Failed to fetch from {self.url} after {self.retries} attempts",
                context={
                    "source_name": self.source_name,
                    "url": self.url,
                    "attempts": self.retries,
                    "timeout": self.timeout,
                },
                original_error=e,
            )
