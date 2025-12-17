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

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Get TradeStation API Credentials

1. Go to [TradeStation Developer Portal](https://developer.tradestation.com/)
2. Create an application
3. Get your `client_id`, `client_secret`, and `refresh_token`

### 3. Configure

```bash
cp config.yaml.template config.yaml
# Edit config.yaml with your credentials
```

### 4. Run

```bash
# Download all configured symbols (incremental)
python tradestation_downloader.py

# Download specific symbols only
python tradestation_downloader.py -s @ES @NQ @CL

# Full download (ignore existing data)
python tradestation_downloader.py --full

# List all default symbols
python tradestation_downloader.py --list-symbols
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

symbols:
  - "@ES"    # E-mini S&P 500
  - "@NQ"    # E-mini Nasdaq 100
  # ... add more symbols
```

## Output Format

Data is saved as Parquet files in the `data_dir`:

```
data/
├── ES_1min.parquet
├── NQ_1min.parquet
├── CL_1min.parquet
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

## Loading Data

```python
import pandas as pd

# Load single symbol
df = pd.read_parquet("data/ES_1min.parquet")
print(df.head())

# Load multiple symbols
symbols = ["ES", "NQ", "CL"]
data = {s: pd.read_parquet(f"data/{s}_1min.parquet") for s in symbols}
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

## Integration with Interactive Brokers

Once you have the data, you can use it for backtesting and then trade via IB:

```python
import pandas as pd
from ib_insync import *

# Load your backtested signals
signals = pd.read_parquet("signals.parquet")

# Connect to IB
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=1)

# Create contract
es = Future('ES', '202503', 'CME')
ib.qualifyContracts(es)

# Execute based on signals
for _, signal in signals.iterrows():
    if signal['action'] == 'BUY':
        order = MarketOrder('BUY', signal['quantity'])
        trade = ib.placeOrder(es, order)
```

## Troubleshooting

### "401 Unauthorized" Error

Your refresh token may have expired. Get a new one from the TradeStation developer portal.

### "429 Rate Limited" Error

The script handles this automatically, but if persistent:
- Increase `rate_limit_delay` in config
- Reduce number of symbols per run

### Missing Data

Some symbols may not have data going back to 2007. Check TradeStation's data availability for specific contracts.

### Large Download Size

1-minute data from 2007 is ~500MB-1GB per symbol. Total for all US futures: ~30-50GB.

## Default Symbols

Run `python tradestation_downloader.py --list-symbols` to see all default symbols:

- **Equity Index**: @ES, @NQ, @YM, @RTY, @MES, @MNQ, etc.
- **Energy**: @CL, @NG, @RB, @HO
- **Metals**: @GC, @SI, @HG, @PL, @PA
- **Treasury**: @ZB, @ZN, @ZF, @ZT, @UB
- **Agriculture**: @ZC, @ZS, @ZW, @KC, @SB, @CT, @LE, @HE
- **Currency**: @6E, @6J, @6B, @6A, @6C, @6S
- **Volatility**: @VX

## License

MIT License - Free to use and modify.

## Support

For TradeStation API issues: [TradeStation Developer Support](https://developer.tradestation.com/)
