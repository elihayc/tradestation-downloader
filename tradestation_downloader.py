#!/usr/bin/env python3
"""
TradeStation Historical Data Downloader
========================================
Automated download of 1-minute futures data from TradeStation API.
Supports incremental updates, rate limiting, and Parquet storage.

Author: Claude (Anthropic)
License: MIT
"""

import os
import sys
import time
import json
import yaml
import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import pyarrow as pa
import pyarrow.parquet as pq

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class DownloadConfig:
    """Configuration for the downloader."""
    client_id: str
    client_secret: str
    refresh_token: str
    data_dir: str = "./data"
    start_date: str = "2007-01-01"
    symbols: List[str] = None
    interval: int = 1
    unit: str = "Minute"
    max_bars_per_request: int = 57600  # ~40 days of 1-min bars
    rate_limit_delay: float = 0.5
    max_retries: int = 3
    parallel_downloads: int = 1  # Set to 1 to be safe with rate limits


class TradeStationAuth:
    """Handles TradeStation OAuth2 authentication."""
    
    TOKEN_URL = "https://signin.tradestation.com/oauth/token"
    
    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.access_token: Optional[str] = None
        self.token_expiry: Optional[datetime] = None
    
    def get_access_token(self) -> str:
        """Get a valid access token, refreshing if necessary."""
        if self._token_is_valid():
            return self.access_token
        
        self._refresh_access_token()
        return self.access_token
    
    def _token_is_valid(self) -> bool:
        """Check if the current token is still valid."""
        if not self.access_token or not self.token_expiry:
            return False
        # Refresh 5 minutes before expiry
        return datetime.now() < (self.token_expiry - timedelta(minutes=5))
    
    def _refresh_access_token(self) -> None:
        """Refresh the access token using the refresh token."""
        logger.info("Refreshing access token...")
        
        payload = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token
        }
        
        response = requests.post(self.TOKEN_URL, data=payload)
        response.raise_for_status()
        
        data = response.json()
        self.access_token = data["access_token"]
        # Tokens typically expire in 20 minutes
        expires_in = data.get("expires_in", 1200)
        self.token_expiry = datetime.now() + timedelta(seconds=expires_in)
        
        logger.info(f"✓ Token refreshed, expires in {expires_in}s")


class TradeStationDataDownloader:
    """
    Downloads historical market data from TradeStation API.
    
    Features:
    - Automatic token refresh
    - Rate limiting
    - Incremental updates (only download new data)
    - Parquet storage with partitioning
    - Resume capability
    """
    
    BASE_URL = "https://api.tradestation.com/v3"
    
    def __init__(self, config: DownloadConfig):
        self.config = config
        self.auth = TradeStationAuth(
            config.client_id,
            config.client_secret,
            config.refresh_token
        )
        self.data_dir = Path(config.data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Track download statistics
        self.stats = {
            "symbols_processed": 0,
            "bars_downloaded": 0,
            "errors": 0,
            "start_time": None
        }
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with current access token."""
        return {
            "Authorization": f"Bearer {self.auth.get_access_token()}",
            "Content-Type": "application/json"
        }
    
    def _make_request(self, url: str, params: Dict[str, Any], 
                      retry_count: int = 0) -> Optional[Dict]:
        """Make an API request with retry logic."""
        try:
            response = requests.get(
                url, 
                headers=self._get_headers(), 
                params=params,
                timeout=60
            )
            
            # Handle rate limiting
            if response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited. Waiting {wait_time}s...")
                time.sleep(wait_time)
                return self._make_request(url, params, retry_count)
            
            # Handle token expiration
            if response.status_code == 401:
                logger.info("Token expired, refreshing...")
                self.auth._refresh_access_token()
                return self._make_request(url, params, retry_count)
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if retry_count < self.config.max_retries:
                wait_time = 2 ** retry_count  # Exponential backoff
                logger.warning(f"Request failed: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
                return self._make_request(url, params, retry_count + 1)
            else:
                logger.error(f"Request failed after {self.config.max_retries} retries: {e}")
                return None
    
    def get_bars(self, symbol: str, start_date: datetime, 
                 end_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Download historical bars for a symbol.
        
        Args:
            symbol: The symbol to download (e.g., "@ES")
            start_date: Start date for the data
            end_date: End date (defaults to now)
        
        Returns:
            DataFrame with OHLCV data
        """
        if end_date is None:
            end_date = datetime.now()
        
        all_bars = []
        current_end = end_date
        request_count = 0
        
        while current_end > start_date:
            params = {
                "interval": self.config.interval,
                "unit": self.config.unit,
                "barsback": self.config.max_bars_per_request,
                "enddate": current_end.strftime("%Y-%m-%d"),
                "sessiontemplate": "USEQPreAndPost"
            }
            
            url = f"{self.BASE_URL}/marketdata/barcharts/{symbol}"
            data = self._make_request(url, params)
            
            if not data or "Bars" not in data or len(data["Bars"]) == 0:
                break
            
            bars = data["Bars"]
            all_bars.extend(bars)
            request_count += 1
            
            # Get the oldest bar timestamp (convert to naive datetime)
            oldest_bar_time = pd.to_datetime(bars[-1]["TimeStamp"])
            if oldest_bar_time.tzinfo is not None:
                oldest_bar_time = oldest_bar_time.replace(tzinfo=None)
            
            # Progress update
            if request_count % 5 == 0:
                logger.info(f"  {symbol}: Downloaded to {oldest_bar_time.date()} | "
                           f"Total bars: {len(all_bars):,}")
            
            # Check if we've gone past the start date
            if oldest_bar_time <= start_date:
                break
            
            # Move the window back
            current_end = oldest_bar_time - timedelta(minutes=1)
            
            # Rate limiting
            time.sleep(self.config.rate_limit_delay)
        
        if not all_bars:
            return pd.DataFrame()
        
        return self._bars_to_dataframe(all_bars, start_date)
    
    def _bars_to_dataframe(self, bars: List[Dict], 
                           start_date: datetime) -> pd.DataFrame:
        """Convert API response to a clean DataFrame."""
        df = pd.DataFrame(bars)
        
        # Parse timestamp (convert to naive datetime)
        df["TimeStamp"] = pd.to_datetime(df["TimeStamp"])
        if df["TimeStamp"].dt.tz is not None:
            df["TimeStamp"] = df["TimeStamp"].dt.tz_convert(None)
        
        # Rename columns
        df = df.rename(columns={
            "TimeStamp": "datetime",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "TotalVolume": "volume"
        })
        
        # Select and order columns
        columns = ["datetime", "open", "high", "low", "close", "volume"]
        df = df[[c for c in columns if c in df.columns]]
        
        # Sort and deduplicate
        df = df.sort_values("datetime")
        df = df.drop_duplicates(subset=["datetime"], keep="last")
        
        # Filter to start date
        df = df[df["datetime"] >= start_date]
        
        # Reset index
        df = df.reset_index(drop=True)
        
        return df
    
    def get_existing_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """Load existing data for a symbol if it exists."""
        filepath = self._get_filepath(symbol)
        
        if filepath.exists():
            try:
                df = pd.read_parquet(filepath)
                df["datetime"] = pd.to_datetime(df["datetime"])
                # Ensure timezone-naive
                if df["datetime"].dt.tz is not None:
                    df["datetime"] = df["datetime"].dt.tz_convert(None)
                return df
            except Exception as e:
                logger.warning(f"Could not read existing data for {symbol}: {e}")
        
        return None
    
    def _get_filepath(self, symbol: str) -> Path:
        """Get the file path for a symbol's data."""
        # Clean symbol name for filesystem
        clean_symbol = symbol.replace("@", "").replace("/", "_")
        return self.data_dir / f"{clean_symbol}_1min.parquet"
    
    def download_symbol(self, symbol: str, 
                        incremental: bool = True) -> Optional[pd.DataFrame]:
        """
        Download data for a single symbol.
        
        Args:
            symbol: Symbol to download
            incremental: If True, only download new data since last download
        
        Returns:
            Complete DataFrame for the symbol
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {symbol}")
        logger.info(f"{'='*60}")
        
        start_date = datetime.strptime(self.config.start_date, "%Y-%m-%d")
        existing_df = None
        
        # Check for existing data
        if incremental:
            existing_df = self.get_existing_data(symbol)
            if existing_df is not None and len(existing_df) > 0:
                last_date = existing_df["datetime"].max()
                logger.info(f"  Existing data found up to {last_date}")
                start_date = last_date + timedelta(minutes=1)
                
                # Check if we're already up to date
                if start_date >= datetime.now() - timedelta(hours=1):
                    logger.info(f"  ✓ {symbol} is already up to date")
                    return existing_df
        
        # Download new data
        logger.info(f"  Downloading from {start_date.date()} to now...")
        new_df = self.get_bars(symbol, start_date)
        
        if new_df.empty and existing_df is None:
            logger.warning(f"  ✗ No data retrieved for {symbol}")
            self.stats["errors"] += 1
            return None
        
        # Merge with existing data
        if existing_df is not None and not new_df.empty:
            df = pd.concat([existing_df, new_df], ignore_index=True)
            df = df.drop_duplicates(subset=["datetime"], keep="last")
            df = df.sort_values("datetime").reset_index(drop=True)
            logger.info(f"  Merged: {len(existing_df):,} existing + "
                       f"{len(new_df):,} new = {len(df):,} total bars")
        elif existing_df is not None:
            df = existing_df
            logger.info(f"  No new data, keeping {len(df):,} existing bars")
        else:
            df = new_df
            logger.info(f"  Downloaded {len(df):,} bars")
        
        # Save to Parquet
        filepath = self._get_filepath(symbol)
        df.to_parquet(filepath, index=False, compression="snappy")
        logger.info(f"  ✓ Saved to {filepath}")
        
        # Update stats
        self.stats["symbols_processed"] += 1
        self.stats["bars_downloaded"] += len(new_df) if not new_df.empty else 0
        
        return df
    
    def download_all(self, symbols: Optional[List[str]] = None,
                     incremental: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Download data for all configured symbols.
        
        Args:
            symbols: List of symbols (uses config if not provided)
            incremental: If True, only download new data
        
        Returns:
            Dictionary of symbol -> DataFrame
        """
        if symbols is None:
            symbols = self.config.symbols
        
        if not symbols:
            logger.error("No symbols configured!")
            return {}
        
        self.stats["start_time"] = datetime.now()
        logger.info(f"\n{'#'*60}")
        logger.info(f"Starting download of {len(symbols)} symbols")
        logger.info(f"Data directory: {self.data_dir.absolute()}")
        logger.info(f"Start date: {self.config.start_date}")
        logger.info(f"Incremental mode: {incremental}")
        logger.info(f"{'#'*60}\n")
        
        results = {}
        
        for i, symbol in enumerate(symbols, 1):
            logger.info(f"\n[{i}/{len(symbols)}] Processing {symbol}...")
            
            try:
                df = self.download_symbol(symbol, incremental=incremental)
                if df is not None:
                    results[symbol] = df
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                self.stats["errors"] += 1
            
            # Small delay between symbols
            if i < len(symbols):
                time.sleep(1)
        
        # Print summary
        self._print_summary()
        
        return results
    
    def _print_summary(self) -> None:
        """Print download summary statistics."""
        elapsed = datetime.now() - self.stats["start_time"]
        
        logger.info(f"\n{'='*60}")
        logger.info("DOWNLOAD SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Symbols processed: {self.stats['symbols_processed']}")
        logger.info(f"Total bars downloaded: {self.stats['bars_downloaded']:,}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"Time elapsed: {elapsed}")
        logger.info(f"Data saved to: {self.data_dir.absolute()}")
        logger.info(f"{'='*60}\n")


# Default US Futures symbols
DEFAULT_FUTURES_SYMBOLS = [
    # Equity Index Futures
    "@ES",    # E-mini S&P 500
    "@NQ",    # E-mini Nasdaq 100
    "@YM",    # E-mini Dow Jones
    "@RTY",   # E-mini Russell 2000
    "@EMD",   # E-mini S&P MidCap 400
    
    # Micro Equity Index
    "@MES",   # Micro E-mini S&P 500
    "@MNQ",   # Micro E-mini Nasdaq 100
    "@MYM",   # Micro E-mini Dow
    "@M2K",   # Micro E-mini Russell 2000
    
    # Energy Futures
    "@CL",    # Crude Oil (WTI)
    "@NG",    # Natural Gas
    "@RB",    # RBOB Gasoline
    "@HO",    # Heating Oil
    
    # Metals Futures
    "@GC",    # Gold
    "@SI",    # Silver
    "@HG",    # Copper
    "@PL",    # Platinum
    "@PA",    # Palladium
    
    # Treasury Futures
    "@ZB",    # 30-Year Treasury Bond
    "@ZN",    # 10-Year Treasury Note
    "@ZF",    # 5-Year Treasury Note
    "@ZT",    # 2-Year Treasury Note
    "@UB",    # Ultra Treasury Bond
    
    # Agriculture - Grains
    "@ZC",    # Corn
    "@ZS",    # Soybeans
    "@ZW",    # Wheat
    "@ZM",    # Soybean Meal
    "@ZL",    # Soybean Oil
    
    # Agriculture - Softs
    "@KC",    # Coffee
    "@SB",    # Sugar
    "@CT",    # Cotton
    "@CC",    # Cocoa
    
    # Agriculture - Meats
    "@LE",    # Live Cattle
    "@HE",    # Lean Hogs
    "@GF",    # Feeder Cattle
    
    # Currency Futures
    "@6E",    # Euro FX
    "@6J",    # Japanese Yen
    "@6B",    # British Pound
    "@6A",    # Australian Dollar
    "@6C",    # Canadian Dollar
    "@6S",    # Swiss Franc
    "@6N",    # New Zealand Dollar
    "@6M",    # Mexican Peso
    
    # Volatility
    "@VX",    # VIX Futures
]


def load_config(config_path: str = "config.yaml") -> DownloadConfig:
    """Load configuration from YAML file."""
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\n"
            f"Please create a config.yaml file with your TradeStation API credentials."
        )
    
    with open(config_file, "r") as f:
        config_data = yaml.safe_load(f)
    
    # Get symbols (use default if not specified)
    symbols = config_data.get("symbols", DEFAULT_FUTURES_SYMBOLS)
    
    return DownloadConfig(
        client_id=config_data["tradestation"]["client_id"],
        client_secret=config_data["tradestation"]["client_secret"],
        refresh_token=config_data["tradestation"]["refresh_token"],
        data_dir=config_data.get("data_dir", "./data"),
        start_date=config_data.get("start_date", "2007-01-01"),
        symbols=symbols,
        interval=config_data.get("interval", 1),
        unit=config_data.get("unit", "Minute"),
        rate_limit_delay=config_data.get("rate_limit_delay", 0.5),
        max_retries=config_data.get("max_retries", 3),
        parallel_downloads=config_data.get("parallel_downloads", 1)
    )


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Download historical futures data from TradeStation"
    )
    parser.add_argument(
        "-c", "--config",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    parser.add_argument(
        "-s", "--symbols",
        nargs="+",
        help="Specific symbols to download (overrides config)"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full download (ignore existing data)"
    )
    parser.add_argument(
        "--list-symbols",
        action="store_true",
        help="List all default symbols and exit"
    )
    
    args = parser.parse_args()
    
    # List symbols and exit
    if args.list_symbols:
        print("\nDefault US Futures Symbols:")
        print("-" * 40)
        for symbol in DEFAULT_FUTURES_SYMBOLS:
            print(f"  {symbol}")
        print(f"\nTotal: {len(DEFAULT_FUTURES_SYMBOLS)} symbols")
        return
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # Override symbols if provided
        if args.symbols:
            config.symbols = args.symbols
        
        # Create downloader and run
        downloader = TradeStationDataDownloader(config)
        downloader.download_all(incremental=not args.full)
        
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("\nDownload interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


if __name__ == "__main__":
    main()
