# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A股缠论第三买点筛选脚本 - A Chinese A-share stock screening tool that identifies stocks meeting the "Third Buy Point" (第三买点) criteria from Chan Theory (缠论) across multiple timeframes.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run full market scan (slow, processes all stocks)
python main.py

# Test with limited stocks (recommended for development)
python main.py --limit 20 --delay 1

# Specify stock codes to check (comma-separated)
python main.py --codes 000001,600519,000858

# Use 2 months of daily data
python main.py --months 2

# Output to custom CSV file
python main.py -o result.csv

# Write detailed logs to log directory
python main.py --log-dir log

# Disable log file generation
python main.py --log-dir ""
```

## Architecture

The project consists of 4 main Python modules:

- **main.py**: Entry point, orchestrates stock list fetching, multi-level K-line retrieval, filtering, and CSV output. Handles CLI argument parsing.
- **data_fetcher.py**: Fetches K-line data from akshare API. Implements cache-first strategy - reads from local SQLite first, only fetches from API if data is stale. Computes MACD indicators.
- **kline_db.py**: SQLite database operations for local K-line storage. Uses WAL (Write-Ahead Logging) mode for performance. Tables: `kline` (symbol, level, dt, OHLCV, MACD). Stores data in `data/stock_kline.db`.
- **chan_logic.py**: Chan Theory implementation. Identifies "中枢" (central zone/ZS) from K-line patterns and checks third buy point conditions: price breaks above ZG, pulls back without breaking ZG, current close above ZG.

### Data Flow

1. Fetch stock list from akshare (with retry logic)
2. For each stock, fetch multi-level K-lines (daily, 60min, 30min, 15min, 5min)
3. K-line data: cache-first - reads from SQLite,增量 fetches if stale
4. Run third buy point analysis on each level
5. Output符合条件的 stocks to CSV

### Third Buy Point Logic (Simplified)

1. Identify 中枢 (central zone) from last 3 segments of K-lines - returns (ZD, ZG)
2. Check if price broke above ZG in recent bars
3. Check if pullback low is above ZG (didn't fall back into central zone)
4. Check if current close is above ZG

## Key Implementation Details

- **Database**: SQLite with WAL mode. Data stored in `data/stock_kline.db`. Uses checkpoint to merge WAL into main file after runs.
- **Data freshness**:
  - Daily: considered fresh if latest date >= today - 4 days
  - Minute: considered fresh if latest date is today
- **API Rate Limiting**: Built-in retry logic (2-3 retries with delays). Use `--delay` argument to add delay between stock requests.
- **Levels**: `LEVELS = ["daily", "60", "30", "15", "5"]`
- **Dependencies**: akshare>=1.14.0, pandas>=2.0.0, numpy>=1.24.0
