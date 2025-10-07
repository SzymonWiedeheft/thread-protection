"""Parsers package."""

from parsers.base_parser import BaseParser
from parsers.hosts_parser import HostsParser
from parsers.adblock_parser import AdBlockParser

__all__ = ["BaseParser", "HostsParser", "AdBlockParser"]
