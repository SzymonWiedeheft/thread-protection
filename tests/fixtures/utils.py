"""Utility functions for test fixtures."""

import docker
import pytest


def is_docker_available() -> bool:
    """Check if Docker is available and running."""
    try:
        client = docker.from_env()
        client.ping()
        return True
    except Exception:
        return False


def requires_docker():
    """Decorator to skip tests when Docker is not available."""
    return pytest.mark.skipif(
        not is_docker_available(), reason="Docker is not available or not running"
    )
