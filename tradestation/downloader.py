"""
TradeStation market data downloader.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any

import pandas as pd
import requests

from .auth import TradeStationAuth
from .models import DownloadConfig
from .storage import create_storage

logger = logging.getLogger(__name__)

# Column mapping from API response to output
_COLUMN_MAP = {
    "TimeStamp": "datetime",
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "TotalVolume": "volume",
}
_OUTPUT_COLUMNS = ["datetime", "open", "high", "low", "close", "volume"]


@dataclass
class DownloadStats:
    """Statistics for a download session."""

    symbols_processed: int = 0
    symbols_skipped: int = 0
    bars_downloaded: int = 0
    errors: int = 0
    failed_symbols: list[str] = field(default_factory=list)
    start_time: datetime | None = None
    end_time: datetime | None = None

    @property
    def elapsed(self) -> timedelta:
        if not self.start_time:
            return timedelta(0)
        return (self.end_time or datetime.now()) - self.start_time


class TradeStationDownloader:
    """
    Downloads historical market data from TradeStation API.

    Features:
    - Automatic token refresh
    - Rate limiting with exponential backoff
    - Incremental updates
    - Multiple storage formats
    """

    BASE_URL = "https://api.tradestation.com/v3"

    def __init__(self, config: DownloadConfig):
        self.config = config
        self._auth = TradeStationAuth(
            config.client_id,
            config.client_secret,
            config.refresh_token,
        )
        self._storage = create_storage(
            config.storage_format,
            Path(config.data_dir),
            compression=config.compression.value,
            datetime_index=config.datetime_index,
        )
        self._stats = DownloadStats()
        self._stats_lock = Lock()  # Thread-safe stats updates

    @property
    def stats(self) -> DownloadStats:
        return self._stats

    def download_all(
        self,
        symbols: list[str] | None = None,
        incremental: bool = True,
    ) -> dict[str, pd.DataFrame]:
        """Download data for all configured symbols (parallel or sequential)."""
        symbols = symbols or self.config.symbols
        if not symbols:
            logger.error("No symbols configured")
            return {}

        self._stats = DownloadStats(start_time=datetime.now())
        self._log_start(symbols, incremental)

        results = {}
        max_workers = self.config.max_workers

        if max_workers <= 1:
            # Sequential download (original behavior)
            results = self._download_sequential(symbols, incremental)
        else:
            # Parallel download
            results = self._download_parallel(symbols, incremental, max_workers)

        self._stats.end_time = datetime.now()
        self._log_summary()
        return results

    def _download_sequential(
        self,
        symbols: list[str],
        incremental: bool,
    ) -> dict[str, pd.DataFrame]:
        """Download symbols sequentially."""
        results = {}
        for i, symbol in enumerate(symbols, 1):
            logger.info("[%d/%d] Processing %s...", i, len(symbols), symbol)
            try:
                df = self.download_symbol(symbol, incremental)
                if df is not None:
                    results[symbol] = df
            except Exception as e:
                logger.error("Error processing %s: %s", symbol, e)
                with self._stats_lock:
                    self._stats.errors += 1
                    self._stats.failed_symbols.append(symbol)

            if i < len(symbols):
                time.sleep(0.2)

        return results

    def _download_parallel(
        self,
        symbols: list[str],
        incremental: bool,
        max_workers: int,
    ) -> dict[str, pd.DataFrame]:
        """Download symbols in parallel using ThreadPoolExecutor."""
        results = {}
        total = len(symbols)

        logger.info("Using %d parallel workers", max_workers)

        def download_one(symbol: str) -> tuple[str, pd.DataFrame | None]:
            try:
                df = self.download_symbol(symbol, incremental)
                return symbol, df
            except Exception as e:
                logger.error("Error processing %s: %s", symbol, e)
                with self._stats_lock:
                    self._stats.errors += 1
                    self._stats.failed_symbols.append(symbol)
                return symbol, None

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(download_one, sym): sym for sym in symbols}
            completed = 0

            for future in as_completed(futures):
                completed += 1
                symbol, df = future.result()
                logger.info("[%d/%d] Completed %s", completed, total, symbol)
                if df is not None:
                    results[symbol] = df

        return results

    def download_symbol(self, symbol: str, incremental: bool = True) -> pd.DataFrame | None:
        """Download data for a single symbol."""
        start_date = datetime.strptime(self.config.start_date, "%Y-%m-%d")
        existing_df = None

        if incremental:
            existing_df = self._storage.load(symbol)
            if existing_df is not None and len(existing_df) > 0:
                # Get last date from index (if datetime_index=True) or column
                if "datetime" in existing_df.columns:
                    last_date = existing_df["datetime"].max()
                else:
                    last_date = existing_df.index.max()
                logger.info("  Existing data up to %s", last_date)
                start_date = last_date + timedelta(minutes=1)

        logger.info("  Downloading from %s...", start_date.date())
        new_df = self._fetch_bars(symbol, start_date)

        if new_df.empty and existing_df is None:
            logger.warning("  No data retrieved")
            with self._stats_lock:
                self._stats.errors += 1
                self._stats.failed_symbols.append(symbol)
            return None

        df = self._merge_data(existing_df, new_df)
        self._storage.save(symbol, df)

        with self._stats_lock:
            self._stats.symbols_processed += 1
            self._stats.bars_downloaded += len(new_df)
        logger.info("  Saved %d bars", len(df))
        return df

    def _fetch_bars(self, symbol: str, start_date: datetime) -> pd.DataFrame:
        """Fetch all bars for a symbol from start_date to now."""
        all_bars = []
        current_end = datetime.now()
        batch_num = 0

        while current_end > start_date:
            data = self._api_request(symbol, current_end)
            if not data or "Bars" not in data or not data["Bars"]:
                break

            bars = data["Bars"]
            all_bars.extend(bars)
            batch_num += 1

            oldest = pd.to_datetime(bars[0]["TimeStamp"]).replace(tzinfo=None)
            newest = pd.to_datetime(bars[-1]["TimeStamp"]).replace(tzinfo=None)
            logger.info("  Batch %d: %d bars (%s to %s)", batch_num, len(bars), oldest.date(), newest.date())

            if oldest <= start_date:
                break

            current_end = oldest - timedelta(minutes=1)
            time.sleep(self.config.rate_limit_delay)

        df = self._bars_to_dataframe(all_bars, start_date)
        return df.iloc[:-1] if len(df) > 0 else df  # Drop last (incomplete) bar

    def _api_request(self, symbol: str, last_date: datetime, retry: int = 0) -> dict[str, Any] | None:
        """Make API request with retry logic."""
        url = f"{self.BASE_URL}/marketdata/barcharts/{symbol}"
        params = {
            "interval": self.config.interval,
            "unit": self.config.unit,
            "barsback": self.config.max_bars_per_request,
            "lastdate": last_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        headers = {
            "Authorization": f"Bearer {self._auth.get_access_token()}",
            "Content-Type": "application/json",
        }

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=60)

            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 60))
                logger.warning("Rate limited, waiting %ds...", wait)
                time.sleep(wait)
                return self._api_request(symbol, last_date, retry)

            if resp.status_code == 401:
                logger.info("Token expired, refreshing...")
                self._auth.invalidate()
                return self._api_request(symbol, last_date, retry)

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.RequestException as e:
            if retry < self.config.max_retries:
                wait = 2 ** retry
                logger.warning("Request failed: %s. Retrying in %ds...", e, wait)
                time.sleep(wait)
                return self._api_request(symbol, last_date, retry + 1)
            logger.error("Request failed after %d retries: %s", self.config.max_retries, e)
            return None

    @staticmethod
    def _bars_to_dataframe(bars: list[dict], start_date: datetime) -> pd.DataFrame:
        """Convert API bars to DataFrame."""
        if not bars:
            return pd.DataFrame(columns=_OUTPUT_COLUMNS)

        df = pd.DataFrame(bars)
        df["TimeStamp"] = pd.to_datetime(df["TimeStamp"])
        if df["TimeStamp"].dt.tz is not None:
            df["TimeStamp"] = df["TimeStamp"].dt.tz_convert(None)

        df = df.rename(columns=_COLUMN_MAP)
        df = df[[c for c in _OUTPUT_COLUMNS if c in df.columns]]

        # Convert OHLCV to numeric types
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")
        df = df[df["datetime"] >= start_date]
        return df.reset_index(drop=True)

    @staticmethod
    def _merge_data(existing: pd.DataFrame | None, new: pd.DataFrame) -> pd.DataFrame:
        """Merge existing and new data."""
        if existing is None or existing.empty:
            return new
        if new.empty:
            return existing

        # Ensure datetime is a column (not index) for both DataFrames before merging
        if "datetime" not in existing.columns and isinstance(existing.index, pd.DatetimeIndex):
            existing = existing.reset_index(names=["datetime"])
        if "datetime" not in new.columns and isinstance(new.index, pd.DatetimeIndex):
            new = new.reset_index(names=["datetime"])

        df = pd.concat([existing, new], ignore_index=True)
        return df.drop_duplicates(subset=["datetime"], keep="last").sort_values("datetime").reset_index(drop=True)

    def _log_start(self, symbols: list[str], incremental: bool) -> None:
        logger.info("")
        logger.info("#" * 60)
        logger.info("Starting download: %d symbols", len(symbols))
        logger.info("Data directory: %s", Path(self.config.data_dir).absolute())
        logger.info("Storage format: %s", self.config.storage_format.value)
        logger.info("Compression: %s", self.config.compression.value)
        logger.info("Incremental: %s", incremental)
        logger.info("Parallel workers: %d", self.config.max_workers)
        logger.info("#" * 60)

    def _log_summary(self) -> None:
        logger.info("")
        logger.info("=" * 60)
        logger.info("DOWNLOAD COMPLETE")
        logger.info("Processed: %d | Skipped: %d | Errors: %d",
                    self._stats.symbols_processed, self._stats.symbols_skipped, self._stats.errors)
        logger.info("Bars downloaded: %d | Time: %s", self._stats.bars_downloaded, self._stats.elapsed)
        if self._stats.failed_symbols:
            logger.info("Failed: %s", ", ".join(self._stats.failed_symbols))
        logger.info("=" * 60)
