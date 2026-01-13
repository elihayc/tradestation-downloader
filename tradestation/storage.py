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


def _prepare_dataframe(df: pd.DataFrame, datetime_index: bool = True) -> pd.DataFrame:
    """Prepare DataFrame for storage (ensure datetime, sort, dedupe, optionally set index)."""
    df = df.copy()

    # Handle case where datetime is already the index (loaded from parquet with datetime_index=True)
    if "datetime" not in df.columns and isinstance(df.index, pd.DatetimeIndex):
        if df.index.tz is not None:
            df.index = df.index.tz_convert(None)
        df = df.sort_index()
        df = df[~df.index.duplicated(keep="last")]
        if not datetime_index:
            df = df.reset_index()
        return df

    # Normal case: datetime is a column
    df["datetime"] = pd.to_datetime(df["datetime"])
    if df["datetime"].dt.tz is not None:
        df["datetime"] = df["datetime"].dt.tz_convert(None)
    df = df.sort_values("datetime")
    df = df.drop_duplicates(subset=["datetime"], keep="last")
    df = df.set_index("datetime") if datetime_index else df.reset_index(drop=True)
    return df


class StorageBackend(ABC):
    """Abstract base class for data storage backends."""

    def __init__(self, data_dir: Path, compression: str = "zstd", datetime_index: bool = True):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        # Convert "none" to None for pandas (no compression)
        self.compression = None if compression == "none" else compression
        self.datetime_index = datetime_index

    def _get_symbol_folder(self, symbol: str) -> str:
        """Get folder name with optional _index_1 suffix."""
        return f"{symbol}_index_1" if self.datetime_index else symbol

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
        folder = self._get_symbol_folder(symbol)
        return self.data_dir / f"{folder}_1min.parquet"

    def save(self, symbol: str, df: pd.DataFrame) -> None:
        df = _prepare_dataframe(df, self.datetime_index)
        df.to_parquet(self._get_filepath(symbol), index=self.datetime_index, compression=self.compression)

    def load(self, symbol: str) -> pd.DataFrame | None:
        filepath = self._get_filepath(symbol)
        if not filepath.exists():
            return None
        try:
            return _prepare_dataframe(pd.read_parquet(filepath), self.datetime_index)
        except Exception as e:
            logger.warning("Failed to load %s: %s", filepath, e)
            return None

    def list_symbols(self) -> list[str]:
        files = self.data_dir.glob("*_1min.parquet")
        return sorted(f.stem.replace("_index_1_1min", "").replace("_1min", "") for f in files)

    def get_file_size(self, symbol: str) -> int:
        filepath = self._get_filepath(symbol)
        return filepath.stat().st_size if filepath.exists() else 0


class DailyPartitionedStorage(StorageBackend):
    """Store data partitioned by day (Hive-style: symbol/year=YYYY/month=MM/day=DD/)."""

    def _get_symbol_dir(self, symbol: str) -> Path:
        return self.data_dir / self._get_symbol_folder(symbol)

    def _get_partition_path(self, symbol: str, dt: datetime) -> Path:
        folder = self._get_symbol_folder(symbol)
        return (
            self._get_symbol_dir(symbol)
            / f"year={dt.year}"
            / f"month={dt.month:02d}"
            / f"day={dt.day:02d}"
            / f"{folder}.parquet"
        )

    def _get_partition_files(self, symbol: str) -> list[Path]:
        symbol_dir = self._get_symbol_dir(symbol)
        if not symbol_dir.exists():
            return []
        return sorted(symbol_dir.glob("year=*/month=*/day=*/*.parquet"))

    def save(self, symbol: str, df: pd.DataFrame) -> None:
        df = _prepare_dataframe(df, datetime_index=False)  # Keep datetime as column for groupby
        for date, group in df.groupby(df["datetime"].dt.date):
            filepath = self._get_partition_path(symbol, datetime.combine(date, datetime.min.time()))
            filepath.parent.mkdir(parents=True, exist_ok=True)
            if self.datetime_index:
                group = group.set_index("datetime")
            group.to_parquet(filepath, index=self.datetime_index, compression=self.compression)

    def load(self, symbol: str) -> pd.DataFrame | None:
        files = self._get_partition_files(symbol)
        if not files:
            return None
        try:
            df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=not self.datetime_index)
            return _prepare_dataframe(df, self.datetime_index)
        except Exception as e:
            logger.warning("Failed to load partitions for %s: %s", symbol, e)
            return None

    def list_symbols(self) -> list[str]:
        symbols = []
        for item in self.data_dir.iterdir():
            if item.is_dir() and list(item.glob("year=*")):
                name = item.name.replace("_index_1", "") if item.name.endswith("_index_1") else item.name
                symbols.append(name)
        return sorted(symbols)

    def get_file_size(self, symbol: str) -> int:
        return sum(f.stat().st_size for f in self._get_partition_files(symbol))


class MonthlyPartitionedStorage(StorageBackend):
    """Store data partitioned by month (Hive-style: symbol/year_month=YYYY-MM/)."""

    def _get_symbol_dir(self, symbol: str) -> Path:
        return self.data_dir / self._get_symbol_folder(symbol)

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
        df = _prepare_dataframe(df, datetime_index=False)  # Keep datetime as column for groupby
        for period, group in df.groupby(df["datetime"].dt.to_period("M")):
            filepath = self._get_partition_path(symbol, period.to_timestamp())
            filepath.parent.mkdir(parents=True, exist_ok=True)
            if self.datetime_index:
                group = group.set_index("datetime")
            group.to_parquet(filepath, index=self.datetime_index, compression=self.compression)

    def load(self, symbol: str) -> pd.DataFrame | None:
        files = self._get_partition_files(symbol)
        if not files:
            return None
        try:
            df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=not self.datetime_index)
            return _prepare_dataframe(df, self.datetime_index)
        except Exception as e:
            logger.warning("Failed to load partitions for %s: %s", symbol, e)
            return None

    def list_symbols(self) -> list[str]:
        symbols = []
        for item in self.data_dir.iterdir():
            if item.is_dir() and list(item.glob("year_month=*")):
                name = item.name.replace("_index_1", "") if item.name.endswith("_index_1") else item.name
                symbols.append(name)
        return sorted(symbols)

    def get_file_size(self, symbol: str) -> int:
        return sum(f.stat().st_size for f in self._get_partition_files(symbol))


_BACKENDS = {
    StorageFormat.SINGLE: SingleFileStorage,
    StorageFormat.DAILY: DailyPartitionedStorage,
    StorageFormat.MONTHLY: MonthlyPartitionedStorage,
}


def create_storage(
    storage_format: StorageFormat,
    data_dir: Path,
    compression: str = "zstd",
    datetime_index: bool = True
) -> StorageBackend:
    """Create the appropriate storage backend."""
    return _BACKENDS[storage_format](data_dir, compression=compression, datetime_index=datetime_index)


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
