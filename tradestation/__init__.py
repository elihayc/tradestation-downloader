"""
TradeStation Historical Data Downloader
=======================================

A Python library for downloading historical futures data from TradeStation API.

Quick Start:
    from tradestation import TradeStationDownloader, load_config

    config = load_config("config.yaml")
    downloader = TradeStationDownloader(config)
    downloader.download_all()
"""

from .auth import AuthenticationError, TradeStationAuth
from .config import ConfigurationError, load_config
from .downloader import DownloadStats, TradeStationDownloader
from .models import DEFAULT_SYMBOLS, Compression, DownloadConfig, StorageFormat, get_all_symbols
from .storage import StorageBackend, create_storage, detect_storage_format

__version__ = "1.0.2"

__all__ = [
    "TradeStationDownloader",
    "TradeStationAuth",
    "DownloadConfig",
    "DownloadStats",
    "StorageFormat",
    "Compression",
    "StorageBackend",
    "load_config",
    "create_storage",
    "detect_storage_format",
    "DEFAULT_SYMBOLS",
    "get_all_symbols",
    "AuthenticationError",
    "ConfigurationError",
]
