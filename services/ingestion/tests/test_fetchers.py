"""Tests for HTTP fetchers."""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from aiohttp import ClientError, ServerTimeoutError
from common import FetchError
from fetchers import HTTPFetcher


@pytest.mark.asyncio
async def test_http_fetcher_success():
    """Test successful HTTP fetch."""
    fetcher = HTTPFetcher(
        source_name="test_source",
        url="https://example.com/data.txt",
        timeout=30,
        retries=3,
    )

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.headers.get.return_value = "text/plain"
    mock_response.text = AsyncMock(return_value="domain1.com\ndomain2.com")
    mock_response.raise_for_status = MagicMock()

    with patch("aiohttp.ClientSession.get") as mock_get:
        mock_get.return_value.__aenter__.return_value = mock_response

        result = await fetcher.fetch()

        assert result["content"] == "domain1.com\ndomain2.com"
        assert result["metadata"]["http_status"] == 200
        assert result["metadata"]["content_type"] == "text/plain"
        assert result["metadata"]["content_length"] == 23
        assert result["metadata"]["source_url"] == "https://example.com/data.txt"


@pytest.mark.asyncio
async def test_http_fetcher_retry_on_failure():
    """Test HTTP fetcher retry logic on failure."""
    fetcher = HTTPFetcher(
        source_name="test_source",
        url="https://example.com/data.txt",
        timeout=30,
        retries=3,
        backoff=0.1,  # Small backoff for testing
    )

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.headers.get.return_value = "text/plain"
    mock_response.text = AsyncMock(return_value="success")
    mock_response.raise_for_status = MagicMock()

    with patch("aiohttp.ClientSession.get") as mock_get:
        mock_get.return_value.__aenter__.side_effect = [
            ClientError("Network error"),
            ClientError("Network error"),
            mock_response,
        ]

        result = await fetcher.fetch()

        assert result["content"] == "success"
        assert mock_get.call_count == 3


@pytest.mark.asyncio
async def test_http_fetcher_max_retries_exceeded():
    """Test HTTP fetcher when max retries exceeded."""
    fetcher = HTTPFetcher(
        source_name="test_source",
        url="https://example.com/data.txt",
        timeout=30,
        retries=3,
        backoff=0.1,
    )

    with patch("aiohttp.ClientSession.get") as mock_get:
        mock_get.return_value.__aenter__.side_effect = ClientError("Network error")

        with pytest.raises(FetchError) as exc_info:
            await fetcher.fetch()

        assert "Failed to fetch" in str(exc_info.value)
        assert mock_get.call_count == 3


@pytest.mark.asyncio
async def test_http_fetcher_timeout():
    """Test HTTP fetcher timeout handling."""
    fetcher = HTTPFetcher(
        source_name="test_source",
        url="https://example.com/data.txt",
        timeout=30,
        retries=2,
        backoff=0.1,
    )

    with patch("aiohttp.ClientSession.get") as mock_get:
        mock_get.return_value.__aenter__.side_effect = ServerTimeoutError("Timeout")

        with pytest.raises(FetchError):
            await fetcher.fetch()

        assert mock_get.call_count == 2


@pytest.mark.asyncio
async def test_http_fetcher_http_error():
    """Test HTTP fetcher handling HTTP errors."""
    fetcher = HTTPFetcher(
        source_name="test_source",
        url="https://example.com/data.txt",
        timeout=30,
        retries=3,
        backoff=0.1,
    )

    mock_response = MagicMock()
    mock_response.status = 404
    mock_response.raise_for_status.side_effect = ClientError("404 Not Found")

    with patch("aiohttp.ClientSession.get") as mock_get:
        mock_get.return_value.__aenter__.return_value = mock_response

        with pytest.raises(FetchError):
            await fetcher.fetch()


@pytest.mark.asyncio
async def test_http_fetcher_empty_content():
    """Test HTTP fetcher with empty content."""
    fetcher = HTTPFetcher(
        source_name="test_source",
        url="https://example.com/data.txt",
    )

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.headers.get.return_value = "text/plain"
    mock_response.text = AsyncMock(return_value="")
    mock_response.raise_for_status = MagicMock()

    with patch("aiohttp.ClientSession.get") as mock_get:
        mock_get.return_value.__aenter__.return_value = mock_response

        result = await fetcher.fetch()

        assert result["content"] == ""
        assert result["metadata"]["content_length"] == 0


@pytest.mark.asyncio
async def test_http_fetcher_large_content():
    """Test HTTP fetcher with large content."""
    fetcher = HTTPFetcher(
        source_name="test_source",
        url="https://example.com/data.txt",
    )

    large_content = "domain.com\n" * 10000

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.headers.get.return_value = "text/plain"
    mock_response.text = AsyncMock(return_value=large_content)
    mock_response.raise_for_status = MagicMock()

    with patch("aiohttp.ClientSession.get") as mock_get:
        mock_get.return_value.__aenter__.return_value = mock_response

        result = await fetcher.fetch()

        assert len(result["content"]) == len(large_content)
        assert result["metadata"]["content_length"] == len(large_content)


def test_http_fetcher_initialization():
    """Test HTTP fetcher initialization."""
    fetcher = HTTPFetcher(
        source_name="test",
        url="https://example.com",
        timeout=60,
        retries=5,
        backoff=10.0,
    )

    assert fetcher.source_name == "test"
    assert fetcher.url == "https://example.com"
    assert fetcher.timeout == 60
    assert fetcher.retries == 5
    assert fetcher.backoff == 10.0


def test_http_fetcher_default_values():
    """Test HTTP fetcher default values."""
    fetcher = HTTPFetcher(
        source_name="test",
        url="https://example.com",
    )

    assert fetcher.timeout == 30
    assert fetcher.retries == 3
    assert fetcher.backoff == 5.0
