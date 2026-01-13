"""
Command-line interface for TradeStation data downloader.
"""

import argparse
import logging
import sys

from .config import ConfigurationError, load_config
from .downloader import TradeStationDownloader
from .models import DEFAULT_SYMBOLS, Compression, StorageFormat

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def create_download_parser() -> argparse.ArgumentParser:
    """Create argument parser for download command."""
    parser = argparse.ArgumentParser(
        prog="tradestation-download",
        description="Download historical futures data from TradeStation API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          Download all configured symbols (incremental)
  %(prog)s -s @ES @NQ @CL           Download specific symbols
  %(prog)s --full                   Full download (ignore existing data)
  %(prog)s --storage-format daily   Use daily partitioned storage
  %(prog)s --list-symbols           List all default symbols
  %(prog)s --list-categories        List symbol categories
""",
    )

    parser.add_argument(
        "-c", "--config",
        default="config.yaml",
        metavar="FILE",
        help="Path to configuration file (default: config.yaml)",
    )
    parser.add_argument(
        "-s", "--symbols",
        nargs="+",
        metavar="SYMBOL",
        help="Specific symbols to download (overrides config)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full download (ignore existing data)",
    )
    parser.add_argument(
        "--storage-format",
        choices=["single", "daily", "monthly"],
        metavar="FORMAT",
        help="Storage format: single, daily, or monthly",
    )
    parser.add_argument(
        "--compression",
        choices=["zstd", "snappy", "gzip", "lz4", "none"],
        metavar="ALGO",
        help="Parquet compression: zstd (default), snappy, gzip, lz4, or none",
    )
    parser.add_argument(
        "--no-datetime-index",
        action="store_true",
        help="Save as @ES (raw) instead of @ES_index_1 (with datetime index)",
    )
    parser.add_argument(
        "--list-symbols",
        action="store_true",
        help="List all default symbols and exit",
    )
    parser.add_argument(
        "--list-categories",
        action="store_true",
        help="List symbol categories and exit",
    )
    parser.add_argument(
        "--category",
        choices=list(DEFAULT_SYMBOLS.keys()),
        metavar="CAT",
        help="Download only symbols from this category",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose (debug) logging",
    )

    return parser


def print_symbols() -> None:
    """Print all default symbols organized by category."""
    print("\nDefault US Futures Symbols")
    print("=" * 50)

    for category, symbols in DEFAULT_SYMBOLS.items():
        print(f"\n{category.upper().replace('_', ' ')}:")
        for symbol in symbols:
            print(f"  {symbol}")

    total = sum(len(s) for s in DEFAULT_SYMBOLS.values())
    print(f"\nTotal: {total} symbols")


def print_categories() -> None:
    """Print available symbol categories."""
    print("\nAvailable Symbol Categories")
    print("=" * 40)

    for category, symbols in DEFAULT_SYMBOLS.items():
        print(f"  {category:<15} ({len(symbols)} symbols)")

    print("\nUse --category <name> to download a specific category")


def run_download(args: argparse.Namespace) -> int:
    """Run the download command."""
    # Handle list commands
    if args.list_symbols:
        print_symbols()
        return 0

    if args.list_categories:
        print_categories()
        return 0

    # Load configuration
    try:
        config = load_config(args.config)
    except ConfigurationError as e:
        logger.error(str(e))
        return 1

    # Override symbols if provided
    if args.symbols:
        config.symbols = args.symbols
    elif args.category:
        config.symbols = DEFAULT_SYMBOLS[args.category]

    # Override storage format if provided
    if args.storage_format:
        config.storage_format = StorageFormat.from_string(args.storage_format)

    # Override compression if provided
    if args.compression:
        config.compression = Compression.from_string(args.compression)

    # Override datetime_index if provided
    if args.no_datetime_index:
        config.datetime_index = False

    # Run downloader
    try:
        downloader = TradeStationDownloader(config)
        downloader.download_all(incremental=not args.full)
        return 0 if downloader.stats.errors == 0 else 1
    except KeyboardInterrupt:
        logger.info("\nDownload interrupted by user")
        return 130
    except Exception as e:
        logger.error("Unexpected error: %s", e)
        if args.verbose:
            raise
        return 1


def main_download() -> None:
    """Entry point for download CLI."""
    parser = create_download_parser()
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    sys.exit(run_download(args))


def main_auth() -> None:
    """Entry point for auth setup CLI."""
    from .auth_setup import main

    main()
