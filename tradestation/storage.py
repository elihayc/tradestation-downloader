"""
Storage backends for market data persistence.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import pandas as pd

from .models import StorageFormat

logger = logging.getLogger(__name__)


def _clean_symbol(symbol: str) -> str:
    """Clean symbol name for filesystem compatibility."""
    return symbol.replace("@", "").replace("/", "_")


def _prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare DataFrame for storage (ensure datetime, sort, dedupe)."""
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"])
    if df["datetime"].dt.tz is not None:
        df["datetime"] = df["datetime"].dt.tz_convert(None)
    df = df.sort_values("datetime")
    df = df.drop_duplicates(subset=["datetime"], keep="last")
    return df.reset_index(drop=True)


class StorageBackend(ABC):
    """Abstract base class for data storage backends."""

    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def save(self, symbol: str, df: pd.DataFrame) -> None:
        """Save data for a symbol."""

    @abstractmethod
    def load(self, symbol: str) -> pd.DataFrame | None:
        """Load all data for a symbol."""

    @abstractmethod
    def list_symbols(self) -> list[str]:
        """List all symbols with stored data."""

    @abstractmethod
    def get_file_size(self, symbol: str) -> int:
        """Get total file size in bytes for a symbol."""


class SingleFileStorage(StorageBackend):
    """Store all data for each symbol in a single Parquet file."""

    def _get_filepath(self, symbol: str) -> Path:
        return self.data_dir / f"{_clean_symbol(symbol)}_1min.parquet"

    def save(self, symbol: str, df: pd.DataFrame) -> None:
        df = _prepare_dataframe(df)
        df.to_parquet(self._get_filepath(symbol), index=False, compression="snappy")

    def load(self, symbol: str) -> pd.DataFrame | None:
        filepath = self._get_filepath(symbol)
        if not filepath.exists():
            return None
        try:
            return _prepare_dataframe(pd.read_parquet(filepath))
        except Exception as e:
            logger.warning("Failed to load %s: %s", filepath, e)
            return None

    def list_symbols(self) -> list[str]:
        files = self.data_dir.glob("*_1min.parquet")
        return sorted(f.stem.replace("_1min", "") for f in files)

    def get_file_size(self, symbol: str) -> int:
        filepath = self._get_filepath(symbol)
        return filepath.stat().st_size if filepath.exists() else 0


class DailyPartitionedStorage(StorageBackend):
    """Store data partitioned by day (Hive-style: symbol/year=YYYY/month=MM/day=DD/)."""

    def _get_symbol_dir(self, symbol: str) -> Path:
        return self.data_dir / _clean_symbol(symbol)

    def _get_partition_path(self, symbol: str, dt: datetime) -> Path:
        return (
            self._get_symbol_dir(symbol)
            / f"year={dt.year}"
            / f"month={dt.month:02d}"
            / f"day={dt.day:02d}"
            / f"{_clean_symbol(symbol)}.parquet"
        )

    def _get_partition_files(self, symbol: str) -> list[Path]:
        symbol_dir = self._get_symbol_dir(symbol)
        if not symbol_dir.exists():
            return []
        return sorted(symbol_dir.glob("year=*/month=*/day=*/*.parquet"))

    def save(self, symbol: str, df: pd.DataFrame) -> None:
        df = _prepare_dataframe(df)
        for date, group in df.groupby(df["datetime"].dt.date):
            filepath = self._get_partition_path(symbol, datetime.combine(date, datetime.min.time()))
            filepath.parent.mkdir(parents=True, exist_ok=True)
            group.to_parquet(filepath, index=False, compression="snappy")

    def load(self, symbol: str) -> pd.DataFrame | None:
        files = self._get_partition_files(symbol)
        if not files:
            return None
        try:
            df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
            return _prepare_dataframe(df)
        except Exception as e:
            logger.warning("Failed to load partitions for %s: %s", symbol, e)
            return None

    def list_symbols(self) -> list[str]:
        symbols = []
        for item in self.data_dir.iterdir():
            if item.is_dir() and list(item.glob("year=*")):
                symbols.append(item.name)
        return sorted(symbols)

    def get_file_size(self, symbol: str) -> int:
        return sum(f.stat().st_size for f in self._get_partition_files(symbol))


class MonthlyPartitionedStorage(StorageBackend):
    """Store data partitioned by month (Hive-style: symbol/year_month=YYYY-MM/)."""

    def _get_symbol_dir(self, symbol: str) -> Path:
        return self.data_dir / _clean_symbol(symbol)

    def _get_partition_path(self, symbol: str, dt: datetime) -> Path:
        year_month = dt.strftime("%Y-%m")
        return (
            self._get_symbol_dir(symbol)
            / f"year_month={year_month}"
            / "data-0.parquet"
        )

    def _get_partition_files(self, symbol: str) -> list[Path]:
        symbol_dir = self._get_symbol_dir(symbol)
        if not symbol_dir.exists():
            return []
        return sorted(symbol_dir.glob("year_month=*/*.parquet"))

    def save(self, symbol: str, df: pd.DataFrame) -> None:
        df = _prepare_dataframe(df)
        for period, group in df.groupby(df["datetime"].dt.to_period("M")):
            filepath = self._get_partition_path(symbol, period.to_timestamp())
            filepath.parent.mkdir(parents=True, exist_ok=True)
            group.to_parquet(filepath, index=False, compression="snappy")

    def load(self, symbol: str) -> pd.DataFrame | None:
        files = self._get_partition_files(symbol)
        if not files:
            return None
        try:
            df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
            return _prepare_dataframe(df)
        except Exception as e:
            logger.warning("Failed to load partitions for %s: %s", symbol, e)
            return None

    def list_symbols(self) -> list[str]:
        symbols = []
        for item in self.data_dir.iterdir():
            if item.is_dir() and list(item.glob("year_month=*")):
                symbols.append(item.name)
        return sorted(symbols)

    def get_file_size(self, symbol: str) -> int:
        return sum(f.stat().st_size for f in self._get_partition_files(symbol))


_BACKENDS = {
    StorageFormat.SINGLE: SingleFileStorage,
    StorageFormat.DAILY: DailyPartitionedStorage,
    StorageFormat.MONTHLY: MonthlyPartitionedStorage,
}


def create_storage(storage_format: StorageFormat, data_dir: Path) -> StorageBackend:
    """Create the appropriate storage backend."""
    return _BACKENDS[storage_format](data_dir)


def detect_storage_format(data_dir: Path) -> StorageFormat:
    """Auto-detect storage format based on directory structure."""
    data_dir = Path(data_dir)
    if not data_dir.exists():
        return StorageFormat.SINGLE

    for item in data_dir.iterdir():
        if item.is_dir() and not item.name.endswith(".parquet"):
            if list(item.glob("year=*/month=*/day=*")):
                return StorageFormat.DAILY
            # Check for new year_month=YYYY-MM format first
            if list(item.glob("year_month=*")):
                return StorageFormat.MONTHLY
            # Also check legacy year=/month= format
            if list(item.glob("year=*/month=*")):
                return StorageFormat.MONTHLY

    return StorageFormat.SINGLE
