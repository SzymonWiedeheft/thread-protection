"""Fetchers package."""

from .base_fetcher import BaseFetcher
from .http_fetcher import HTTPFetcher

__all__ = ["BaseFetcher", "HTTPFetcher"]
