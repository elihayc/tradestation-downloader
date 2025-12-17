#!/usr/bin/env python3
"""
Data Analysis & Validation Utilities
=====================================
Tools to analyze, validate, and export downloaded TradeStation data.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


class DataAnalyzer:
    """Analyze and validate downloaded futures data."""
    
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = Path(data_dir)
    
    def list_symbols(self) -> List[str]:
        """List all downloaded symbols."""
        files = list(self.data_dir.glob("*_1min.parquet"))
        return [f.stem.replace("_1min", "") for f in files]
    
    def load_symbol(self, symbol: str) -> pd.DataFrame:
        """Load data for a single symbol."""
        filepath = self.data_dir / f"{symbol}_1min.parquet"
        if not filepath.exists():
            raise FileNotFoundError(f"No data found for {symbol}")
        
        df = pd.read_parquet(filepath)
        df["datetime"] = pd.to_datetime(df["datetime"])
        return df
    
    def get_summary(self) -> pd.DataFrame:
        """Get summary statistics for all symbols."""
        summaries = []
        
        for symbol in self.list_symbols():
            df = self.load_symbol(symbol)
            
            summaries.append({
                "symbol": symbol,
                "start_date": df["datetime"].min(),
                "end_date": df["datetime"].max(),
                "total_bars": len(df),
                "trading_days": df["datetime"].dt.date.nunique(),
                "avg_daily_bars": len(df) / df["datetime"].dt.date.nunique(),
                "file_size_mb": (self.data_dir / f"{symbol}_1min.parquet").stat().st_size / 1024 / 1024,
                "avg_volume": df["volume"].mean(),
                "total_volume": df["volume"].sum()
            })
        
        return pd.DataFrame(summaries).sort_values("symbol")
    
    def check_gaps(self, symbol: str, 
                   max_gap_minutes: int = 60) -> pd.DataFrame:
        """
        Find gaps in the data larger than max_gap_minutes.
        Note: Gaps during non-trading hours are normal.
        """
        df = self.load_symbol(symbol)
        
        # Calculate time differences
        df = df.sort_values("datetime")
        df["gap_minutes"] = df["datetime"].diff().dt.total_seconds() / 60
        
        # Find significant gaps (excluding first row)
        gaps = df[df["gap_minutes"] > max_gap_minutes].copy()
        gaps["gap_start"] = df["datetime"].shift(1)
        gaps["gap_end"] = df["datetime"]
        
        return gaps[["gap_start", "gap_end", "gap_minutes"]].reset_index(drop=True)
    
    def resample(self, symbol: str, 
                 timeframe: str = "5min") -> pd.DataFrame:
        """
        Resample 1-minute data to a different timeframe.
        
        Args:
            symbol: Symbol to resample
            timeframe: Target timeframe ('5min', '15min', '1H', '1D', etc.)
        
        Returns:
            Resampled OHLCV DataFrame
        """
        df = self.load_symbol(symbol)
        df = df.set_index("datetime")
        
        resampled = df.resample(timeframe).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum"
        }).dropna()
        
        return resampled.reset_index()
    
    def export_to_csv(self, symbol: str, 
                      output_dir: str = "./csv_export",
                      timeframe: Optional[str] = None) -> str:
        """Export symbol data to CSV format."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        if timeframe:
            df = self.resample(symbol, timeframe)
            filename = f"{symbol}_{timeframe}.csv"
        else:
            df = self.load_symbol(symbol)
            filename = f"{symbol}_1min.csv"
        
        filepath = output_path / filename
        df.to_csv(filepath, index=False)
        
        return str(filepath)
    
    def calculate_returns(self, symbol: str, 
                          period: int = 1) -> pd.DataFrame:
        """Calculate returns for a symbol."""
        df = self.load_symbol(symbol)
        df["returns"] = df["close"].pct_change(period)
        df["log_returns"] = np.log(df["close"] / df["close"].shift(period))
        return df
    
    def get_daily_stats(self, symbol: str) -> pd.DataFrame:
        """Get daily statistics for a symbol."""
        df = self.load_symbol(symbol)
        df["date"] = df["datetime"].dt.date
        
        daily = df.groupby("date").agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "datetime": "count"
        }).rename(columns={"datetime": "bar_count"})
        
        daily["range"] = daily["high"] - daily["low"]
        daily["range_pct"] = daily["range"] / daily["open"] * 100
        daily["return"] = daily["close"].pct_change() * 100
        
        return daily.reset_index()


def print_summary_report(data_dir: str = "./data"):
    """Print a formatted summary report of all data."""
    analyzer = DataAnalyzer(data_dir)
    
    symbols = analyzer.list_symbols()
    if not symbols:
        logger.info("No data found in %s", data_dir)
        return
    
    logger.info("\n" + "="*80)
    logger.info("DATA SUMMARY REPORT")
    logger.info("="*80)
    logger.info(f"Data directory: {Path(data_dir).absolute()}")
    logger.info(f"Total symbols: {len(symbols)}")
    logger.info("")
    
    summary = analyzer.get_summary()
    
    total_size = summary["file_size_mb"].sum()
    total_bars = summary["total_bars"].sum()
    
    logger.info(f"{'Symbol':<10} {'Start Date':<12} {'End Date':<12} "
               f"{'Bars':>12} {'Days':>8} {'Size (MB)':>10}")
    logger.info("-" * 80)
    
    for _, row in summary.iterrows():
        logger.info(f"{row['symbol']:<10} "
                   f"{row['start_date'].strftime('%Y-%m-%d'):<12} "
                   f"{row['end_date'].strftime('%Y-%m-%d'):<12} "
                   f"{row['total_bars']:>12,} "
                   f"{row['trading_days']:>8,} "
                   f"{row['file_size_mb']:>10.1f}")
    
    logger.info("-" * 80)
    logger.info(f"{'TOTAL':<10} {'':<12} {'':<12} "
               f"{total_bars:>12,} {'':<8} {total_size:>10.1f}")
    logger.info("="*80 + "\n")


def check_all_gaps(data_dir: str = "./data", max_gap_hours: int = 24):
    """Check for gaps in all symbols."""
    analyzer = DataAnalyzer(data_dir)
    
    logger.info("\n" + "="*60)
    logger.info("GAP ANALYSIS (gaps > %d hours)", max_gap_hours)
    logger.info("="*60 + "\n")
    
    for symbol in analyzer.list_symbols():
        gaps = analyzer.check_gaps(symbol, max_gap_minutes=max_gap_hours * 60)
        
        if len(gaps) > 0:
            logger.info(f"{symbol}: {len(gaps)} gaps found")
            for _, gap in gaps.head(5).iterrows():
                logger.info(f"  {gap['gap_start']} -> {gap['gap_end']} "
                           f"({gap['gap_minutes']/60:.1f} hours)")
            if len(gaps) > 5:
                logger.info(f"  ... and {len(gaps) - 5} more")
        else:
            logger.info(f"{symbol}: No significant gaps")


def export_all_to_csv(data_dir: str = "./data", 
                      output_dir: str = "./csv_export",
                      timeframe: Optional[str] = None):
    """Export all symbols to CSV."""
    analyzer = DataAnalyzer(data_dir)
    
    logger.info(f"\nExporting to CSV (timeframe: {timeframe or '1min'})...")
    
    for symbol in analyzer.list_symbols():
        filepath = analyzer.export_to_csv(symbol, output_dir, timeframe)
        logger.info(f"  Exported: {filepath}")
    
    logger.info("Done!")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Analyze downloaded data")
    parser.add_argument("command", choices=["summary", "gaps", "export"],
                       help="Command to run")
    parser.add_argument("-d", "--data-dir", default="./data",
                       help="Data directory")
    parser.add_argument("-o", "--output-dir", default="./csv_export",
                       help="Output directory for CSV export")
    parser.add_argument("-t", "--timeframe", default=None,
                       help="Timeframe for resampling (e.g., 5min, 1H, 1D)")
    
    args = parser.parse_args()
    
    if args.command == "summary":
        print_summary_report(args.data_dir)
    elif args.command == "gaps":
        check_all_gaps(args.data_dir)
    elif args.command == "export":
        export_all_to_csv(args.data_dir, args.output_dir, args.timeframe)
