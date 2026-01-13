"""
Data models and configuration for TradeStation downloader.
"""

from dataclasses import dataclass, field
from enum import Enum


class StorageFormat(Enum):
    """Storage format for downloaded market data."""

    SINGLE = "single"    # One file per symbol: ES_1min.parquet
    DAILY = "daily"      # Partitioned by day: ES/year=2024/month=01/day=15/ES.parquet
    MONTHLY = "monthly"  # Partitioned by month: ES/year_month=2024-01/data-0.parquet

    @classmethod
    def from_string(cls, value: str) -> "StorageFormat":
        """Create StorageFormat from string value."""
        try:
            return cls(value.lower())
        except ValueError:
            valid = ", ".join(f"'{f.value}'" for f in cls)
            raise ValueError(f"Invalid storage format: '{value}'. Must be one of: {valid}")


class Compression(Enum):
    """Parquet compression algorithm."""

    ZSTD = "zstd"        # Best compression ratio, good speed (recommended)
    SNAPPY = "snappy"    # Fast, moderate compression
    GZIP = "gzip"        # Good compression, slower
    LZ4 = "lz4"          # Fastest, lower compression
    NONE = "none"        # No compression

    @classmethod
    def from_string(cls, value: str) -> "Compression":
        """Create Compression from string value."""
        try:
            return cls(value.lower())
        except ValueError:
            valid = ", ".join(f"'{c.value}'" for c in cls)
            raise ValueError(f"Invalid compression: '{value}'. Must be one of: {valid}")


@dataclass
class DownloadConfig:
    """Configuration for the TradeStation data downloader."""

    client_id: str
    client_secret: str
    refresh_token: str
    data_dir: str = "./data"
    start_date: str = "2007-01-01"
    symbols: list[str] = field(default_factory=list)
    interval: int = 1
    unit: str = "Minute"
    max_bars_per_request: int = 57600  # ~40 days of 1-min bars
    rate_limit_delay: float = 0.5
    max_retries: int = 3
    storage_format: StorageFormat = StorageFormat.SINGLE
    compression: Compression = Compression.ZSTD
    datetime_index: bool = True  # Save with datetime as index (adds _index_1 suffix)

    def __post_init__(self):
        """Validate and convert fields after initialization."""
        if isinstance(self.storage_format, str):
            self.storage_format = StorageFormat.from_string(self.storage_format)
        if isinstance(self.compression, str):
            self.compression = Compression.from_string(self.compression)


# Default US Futures symbols organized by category
DEFAULT_SYMBOLS = {
    "index": [
        "@ES",    # E-Mini S&P 500
        "@NQ",    # E-Mini Nasdaq 100
        "@YM",    # E-Mini DJIA ($5)
        "@RTY",   # E-mini Russell 2000
        "@EMD",   # E-Mini S&P Mid Cap 400
        "@SMC",   # E-Mini S&P Small Cap 600
    ],
    "micro_index": [
        "@MES",   # Micro E-mini S&P 500
        "@MNQ",   # Micro E-mini Nasdaq-100
        "@MYM",   # Micro E-mini Dow
        "@M2K",   # Micro E-mini Russell 2000
    ],
    "energy": [
        "@CL",    # Light Sweet Crude Oil
        "@NG",    # Natural Gas
        "@RB",    # RBOB Gasoline
        "@HO",    # Heating Oil
        "@BRN",   # Brent Crude Oil
    ],
    "micro_energy": [
        "@MCL",   # Micro Crude Oil
        "@MNG",   # Micro Henry Hub Natural Gas
    ],
    "metals": [
        "@GC",    # Gold (COMEX)
        "@SI",    # Silver (COMEX)
        "@HG",    # Copper (COMEX)
        "@PL",    # Platinum
        "@PA",    # Palladium
    ],
    "micro_metals": [
        "@MGC",   # E-Micro Gold
        "@SIL",   # E-Micro Silver
        "@MHG",   # Micro Copper
    ],
    "treasuries": [
        "@US",    # 30 Year US Treasury Bond
        "@TY",    # 10 Year US Treasury Note
        "@FV",    # 5 Year US Treasury Note
        "@TU",    # 2 Year US Treasury Note
        "@UB",    # Ultra T-Bond
        "@TEN",   # Ultra 10-Year Treasury Note
        "@TWE",   # 20 Year US Treasury Bond
    ],
    "grains": [
        "@C",     # Corn
        "@S",     # Soybeans
        "@W",     # Wheat
        "@KW",    # KC Wheat (Hard Red Winter)
        "@BO",    # Soybean Oil
        "@SM",    # Soybean Meal
    ],
    "softs": [
        "@KC",    # Coffee "C"
        "@SB",    # Sugar No. 11
        "@CT",    # Cotton No. 2
        "@CC",    # Cocoa
        "@OJ",    # FCOJ-A (Orange Juice)
        "@LBR",   # Lumber
    ],
    "meats": [
        "@LC",    # Live Cattle
        "@LH",    # Lean Hogs
        "@FC",    # Feeder Cattle
    ],
    "currencies": [
        "@EC",    # Euro / US Dollar
        "@JY",    # Japanese Yen / US Dollar
        "@BP",    # British Pound / US Dollar
        "@AD",    # Australian Dollar / US Dollar
        "@CD",    # Canadian Dollar / US Dollar
        "@SF",    # Swiss Franc / US Dollar
        "@DX",    # U.S. Dollar Index
    ],
    "volatility": [
        "@VX",    # CBOE Volatility Index (VIX)
    ],
    "crypto": [
        "@BTC",   # CME Bitcoin Futures
        "@ETH",   # CME Ether Futures
        "@MBT",   # CME Micro Bitcoin Futures
        "@MET",   # CME Micro Ether Futures
    ],
}


def get_all_symbols() -> list[str]:
    """Get flat list of all default symbols."""
    return [symbol for symbols in DEFAULT_SYMBOLS.values() for symbol in symbols]


def get_symbols_by_category(category: str) -> list[str]:
    """Get symbols for a specific category."""
    if category not in DEFAULT_SYMBOLS:
        valid = ", ".join(DEFAULT_SYMBOLS.keys())
        raise ValueError(f"Unknown category: '{category}'. Valid categories: {valid}")
    return DEFAULT_SYMBOLS[category]
