# TradeStation Historical Data Downloader

Automated download of 1-minute futures data from TradeStation API with incremental updates, rate limiting, and Parquet storage.

## Features

- **OAuth2 Authentication** - Automatic token refresh
- **Incremental Updates** - Only downloads new bars since last run
- **Rate Limiting** - Respects API limits with configurable delays
- **Parquet Storage** - Fast, compressed columnar format
- **Resume Capability** - Interrupted downloads can be resumed
- **All US Futures** - Pre-configured list of ~50 continuous contracts

## Quick Start

### 1. Install

```bash
# From PyPI (recommended)
pip install tradestation-downloader

# Or from source using uv
pip install uv
uv sync

# Or standard pip from source
pip install -e .

# For development (includes pytest, ruff)
pip install -e ".[dev]"
```

### 2. Get TradeStation API Credentials

1. Go to [TradeStation Developer Portal](https://developer.tradestation.com/)
2. Create an application
3. Get your `client_id`, `client_secret`, and `refresh_token`

### 3. Configure

Option A - Interactive setup (recommended):
```bash
tradestation-auth
```

Option B - Manual setup:
```bash
cp config.yaml.template config.yaml
# Edit config.yaml with your credentials
```

### 4. Run

After install, CLI commands are available:

```bash
# Download all configured symbols (incremental)
tradestation-download

# Download specific symbols only
tradestation-download -s "@ES" "@NQ" "@CL"

# Full download (ignore existing data)
tradestation-download --full

# Use daily or monthly partitioned storage
tradestation-download --storage-format daily
tradestation-download --storage-format monthly

# Use different compression (default: zstd)
tradestation-download --compression snappy

# List all default symbols
tradestation-download --list-symbols

# List symbol categories
tradestation-download --list-categories

# Download specific category
tradestation-download --category index

# Save without datetime index (raw format)
tradestation-download -s @ES --no-datetime-index
```

> **Note:** On Windows, the `@` symbol has special meaning in CMD/PowerShell.
> Always quote symbols: `"@ES"` instead of `@ES`.

Or run directly with Python:

```bash
python tradestation_downloader.py
python tradestation_downloader.py -s "@ES" "@NQ" "@CL"
```

## Configuration

Edit `config.yaml`:

```yaml
tradestation:
  client_id: "YOUR_CLIENT_ID"
  client_secret: "YOUR_CLIENT_SECRET"
  refresh_token: "YOUR_REFRESH_TOKEN"

data_dir: "./data"
start_date: "2007-01-01"
interval: 1
unit: "Minute"
storage_format: "single"  # single, daily, or monthly
compression: "zstd"       # zstd, snappy, gzip, lz4, or none

symbols:
  - "@ES"    # E-mini S&P 500
  - "@NQ"    # E-mini Nasdaq 100
  # ... add more symbols
```

## Output Format

Data is saved as Parquet files in the `data_dir`. Structure depends on storage format:

**Single file (with datetime index):**
```
data/
├── @ES_index_1_1min.parquet
├── @NQ_index_1_1min.parquet
└── @CL_index_1_1min.parquet
```

**Monthly partitioned (default with datetime index):**
```
data/
├── @ES_index_1/
│   ├── year_month=2024-01/data-0.parquet
│   ├── year_month=2024-02/data-0.parquet
│   └── ...
└── @NQ_index_1/
    └── ...
```

Each file contains:

| Column   | Type     | Description              |
|----------|----------|--------------------------|
| datetime | datetime | Bar timestamp (UTC)      |
| open     | float    | Opening price            |
| high     | float    | High price               |
| low      | float    | Low price                |
| close    | float    | Closing price            |
| volume   | int      | Total volume             |

## DateTime Index Mode

By default, data is saved with `datetime` as the DataFrame index, enabling pandas time-series operations like `resample()` and time-based slicing. This adds an `_index_1` suffix to folder names.

**Default (recommended):**
```bash
tradestation-download -s @ES
```
→ Creates `@ES_index_1/` folder with datetime as index

**Raw format (datetime as column):**
```bash
tradestation-download -s @ES --no-datetime-index
```
→ Creates `@ES/` folder with datetime as regular column

**Why use datetime index?**
- `df.resample('5min')` works directly
- Time slicing: `df['2024-01':'2024-06']`
- Compatible with pandas time-series functions
- Matches common conventions for OHLCV data

**Folder structure with index (default):**
```
data/
├── @ES_index_1/
│   ├── year_month=2024-01/data-0.parquet
│   └── year_month=2024-02/data-0.parquet
└── @NQ_index_1/
    └── ...
```

## Loading Data

```python
import pandas as pd

# Load single symbol (monthly partitioned with datetime index)
df = pd.read_parquet("data/@ES_index_1")
print(df.head())

# Resample to 5-minute bars (works because datetime is the index)
df_5min = df.resample('5min').agg({
    'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
}).dropna()

# Time-based slicing
df_2024 = df['2024-01':'2024-12']

# Load single file format
df = pd.read_parquet("data/ES_1min.parquet")
```

## Python API

```bash
pip install tradestation-downloader
```

Use programmatically in your project:

```python
from tradestation import TradeStationDownloader, DownloadConfig, StorageFormat, Compression

config = DownloadConfig(
    client_id="your_client_id",
    client_secret="your_client_secret",
    refresh_token="your_refresh_token",
    symbols=["@ES"],
    data_dir="./data",
    start_date="2020-01-01",
    storage_format=StorageFormat.SINGLE,
    compression=Compression.ZSTD,  # or SNAPPY, GZIP, LZ4, NONE
)

downloader = TradeStationDownloader(config)
data = downloader.download_all()

# Access download statistics
stats = downloader.stats
print(f"Downloaded {stats.bars_downloaded} bars")
print(f"Processed {stats.symbols_processed} symbols")
print(f"Errors: {stats.errors}")

# Use the data
es_df = data["@ES"]
print(es_df.head())
```

Or load from config file:

```python
from tradestation import TradeStationDownloader, load_config

config = load_config("config.yaml")
config.symbols = ["@ES", "@NQ"]  # Override symbols

downloader = TradeStationDownloader(config)
downloader.download_all()
```

## Scheduling Daily Updates

### Linux/Mac (cron)

```bash
# Edit crontab
crontab -e

# Add line to run daily at 6 AM
0 6 * * * cd /path/to/tradestation_downloader && python tradestation_downloader.py >> logs/download.log 2>&1
```

### Windows (Task Scheduler)

1. Open Task Scheduler
2. Create Basic Task
3. Set trigger: Daily at 6:00 AM
4. Action: Start a program
5. Program: `python`
6. Arguments: `C:\path\to\tradestation_downloader.py`
7. Start in: `C:\path\to\tradestation_downloader`

## Data Validation

```python
import pandas as pd
from pathlib import Path

data_dir = Path("./data")

for f in data_dir.glob("*_1min.parquet"):
    df = pd.read_parquet(f)
    symbol = f.stem.replace("_1min", "")
    
    print(f"{symbol}:")
    print(f"  Date range: {df['datetime'].min()} to {df['datetime'].max()}")
    print(f"  Total bars: {len(df):,}")
    print(f"  Missing dates: {df['datetime'].diff().gt(pd.Timedelta(minutes=2)).sum()}")
    print()
```

## Troubleshooting

### "401 Unauthorized" Error

Your refresh token may have expired. Run `tradestation-auth` to get a new one.

### "429 Rate Limited" Error

The script handles this automatically, but if persistent:
- Increase `rate_limit_delay` in config
- Reduce number of symbols per run

### Missing Data

Some symbols may not have data going back to 2007. Check TradeStation's data availability for specific contracts.

### Large Download Size

1-minute data from 2007 is ~500MB-1GB per symbol. Total for all US futures: ~30-50GB.

## Default Symbols

Run `tradestation-download --list-symbols` to see all default symbols:

- **Equity Index**: @ES, @NQ, @YM, @RTY, @MES, @MNQ, etc.
- **Energy**: @CL, @NG, @RB, @HO, @BRN
- **Metals**: @GC, @SI, @HG, @PL, @PA
- **Treasury**: @US, @TY, @FV, @TU, @UB, @TEN, @TWE
- **Grains**: @C, @S, @W, @KW, @BO, @SM
- **Softs**: @KC, @SB, @CT, @CC, @OJ, @LBR
- **Meats**: @LC, @LH, @FC
- **Currency**: @EC, @JY, @BP, @AD, @CD, @SF, @DX
- **Volatility**: @VX
- **Crypto**: @BTC, @ETH, @MBT, @MET

## License

MIT License - Free to use and modify.

## Support

For TradeStation API issues: [TradeStation Developer Support](https://developer.tradestation.com/)
