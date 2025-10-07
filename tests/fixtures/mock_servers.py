"""Mock HTTP servers for testing fetchers."""

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
import asyncio


@pytest.fixture
async def mock_hosts_server(sample_hosts_content):
    """Mock HTTP server serving hosts file content."""

    async def handle_hosts(request):
        """Handle hosts file requests."""
        return web.Response(
            text=sample_hosts_content,
            headers={
                "Content-Type": "text/plain",
                "Last-Modified": "Sat, 05 Oct 2025 10:00:00 GMT",
            },
        )

    app = web.Application()
    app.router.add_get("/hosts", handle_hosts)

    server = TestServer(app)
    await server.start_server()

    yield server

    await server.close()


@pytest.fixture
async def mock_adblock_server(sample_adblock_content):
    """Mock HTTP server serving adblock filter list."""

    async def handle_adblock(request):
        """Handle adblock filter requests."""
        return web.Response(
            text=sample_adblock_content,
            headers={
                "Content-Type": "text/plain",
                "Last-Modified": "Sat, 05 Oct 2025 10:00:00 GMT",
            },
        )

    app = web.Application()
    app.router.add_get("/adblock.txt", handle_adblock)

    server = TestServer(app)
    await server.start_server()

    yield server

    await server.close()


@pytest.fixture
async def mock_unreliable_server():
    """Mock server that simulates unreliable responses."""

    request_count = {"count": 0}

    async def handle_unreliable(request):
        """Handle requests with intermittent failures."""
        request_count["count"] += 1

        # Fail first 2 requests, succeed on 3rd
        if request_count["count"] < 3:
            raise web.HTTPServiceUnavailable(text="Service temporarily unavailable")

        return web.Response(
            text="0.0.0.0 example.com",
            headers={"Content-Type": "text/plain"},
        )

    app = web.Application()
    app.router.add_get("/unreliable", handle_unreliable)

    server = TestServer(app)
    await server.start_server()

    yield server

    await server.close()


@pytest.fixture
async def mock_slow_server():
    """Mock server with slow responses to test timeouts."""

    async def handle_slow(request):
        """Handle requests with delay."""
        delay = float(request.query.get("delay", "5"))
        await asyncio.sleep(delay)

        return web.Response(
            text="0.0.0.0 example.com",
            headers={"Content-Type": "text/plain"},
        )

    app = web.Application()
    app.router.add_get("/slow", handle_slow)

    server = TestServer(app)
    await server.start_server()

    yield server

    await server.close()


@pytest.fixture
async def mock_redirect_server():
    """Mock server that handles redirects."""

    async def handle_redirect(request):
        """Redirect to final destination."""
        raise web.HTTPFound(location="/final")

    async def handle_final(request):
        """Final destination after redirect."""
        return web.Response(
            text="0.0.0.0 redirected.com",
            headers={"Content-Type": "text/plain"},
        )

    app = web.Application()
    app.router.add_get("/redirect", handle_redirect)
    app.router.add_get("/final", handle_final)

    server = TestServer(app)
    await server.start_server()

    yield server

    await server.close()


@pytest.fixture
async def mock_auth_server():
    """Mock server requiring authentication."""

    async def handle_protected(request):
        """Handle authenticated requests."""
        auth = request.headers.get("Authorization")

        if not auth or auth != "Bearer test-token":
            raise web.HTTPUnauthorized(
                text="Unauthorized",
                headers={"WWW-Authenticate": 'Bearer realm="test"'},
            )

        return web.Response(
            text="0.0.0.0 protected.com",
            headers={"Content-Type": "text/plain"},
        )

    app = web.Application()
    app.router.add_get("/protected", handle_protected)

    server = TestServer(app)
    await server.start_server()

    yield server

    await server.close()


@pytest.fixture
async def mock_etag_server():
    """Mock server supporting ETag/conditional requests."""

    etag = '"12345-test-etag"'
    content = "0.0.0.0 example.com"

    async def handle_etag(request):
        """Handle conditional requests with ETag."""
        if_none_match = request.headers.get("If-None-Match")

        if if_none_match == etag:
            raise web.HTTPNotModified(headers={"ETag": etag})

        return web.Response(
            text=content,
            headers={
                "Content-Type": "text/plain",
                "ETag": etag,
            },
        )

    app = web.Application()
    app.router.add_get("/etag", handle_etag)

    server = TestServer(app)
    await server.start_server()

    yield server

    await server.close()


@pytest.fixture
async def mock_large_response_server():
    """Mock server with large response to test streaming."""

    async def handle_large(request):
        """Handle large file responses."""
        # Generate 10MB of hosts entries
        lines = []
        for i in range(100000):
            lines.append(f"0.0.0.0 domain-{i}.example.com")

        content = "\n".join(lines)

        return web.Response(
            text=content,
            headers={
                "Content-Type": "text/plain",
                "Content-Length": str(len(content)),
            },
        )

    app = web.Application()
    app.router.add_get("/large", handle_large)

    server = TestServer(app)
    await server.start_server()

    yield server

    await server.close()


@pytest.fixture
async def mock_error_responses_server():
    """Mock server for testing various error responses."""

    async def handle_404(request):
        """404 Not Found."""
        raise web.HTTPNotFound(text="Resource not found")

    async def handle_500(request):
        """500 Internal Server Error."""
        raise web.HTTPInternalServerError(text="Internal server error")

    async def handle_503(request):
        """503 Service Unavailable."""
        raise web.HTTPServiceUnavailable(text="Service unavailable")

    async def handle_429(request):
        """429 Too Many Requests."""
        raise web.HTTPTooManyRequests(
            text="Rate limit exceeded",
            headers={"Retry-After": "60"},
        )

    app = web.Application()
    app.router.add_get("/404", handle_404)
    app.router.add_get("/500", handle_500)
    app.router.add_get("/503", handle_503)
    app.router.add_get("/429", handle_429)

    server = TestServer(app)
    await server.start_server()

    yield server

    await server.close()


@pytest.fixture
async def mock_malformed_server():
    """Mock server returning malformed content."""

    async def handle_binary(request):
        """Return binary content instead of text."""
        return web.Response(
            body=b"\x00\x01\x02\x03\x04",
            headers={"Content-Type": "application/octet-stream"},
        )

    async def handle_invalid_encoding(request):
        """Return content with invalid encoding."""
        # Latin-1 encoded text with special chars
        content = "Domínio com acentuação".encode("latin-1")
        return web.Response(
            body=content,
            headers={"Content-Type": "text/plain; charset=utf-8"},
        )

    async def handle_empty(request):
        """Return empty response."""
        return web.Response(text="")

    app = web.Application()
    app.router.add_get("/binary", handle_binary)
    app.router.add_get("/invalid-encoding", handle_invalid_encoding)
    app.router.add_get("/empty", handle_empty)

    server = TestServer(app)
    await server.start_server()

    yield server

    await server.close()
