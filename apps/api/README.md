# stockanalyse-api

Custom Python backend scaffold for stockAnalyse.

## Local Refresh Providers

### `static_fixture`

Deterministic test provider backed by `apps/api/tests/fixtures/japan_equity_eod_fixture.json`.

### `local_csv_directory`

Local file-backed provider that reads Yahoo-style OHLCV CSV files from an archived seed directory such as
`data/archive/local_seed_csv`.

For full-universe refreshes it also requires a symbol allowlist file representing the approved
Tokyo Stock Exchange common-stock universe.

Example:

```bash
cd apps/api
PYTHONPATH=src python3 -m stockanalyse_api.jobs.refresh_market_data \
  --provider local_csv_directory \
  --all-supported \
  --csv-dir ../../data/archive/local_seed_csv \
  --symbols-file ../../data/tse_common_stock_symbols.txt \
  --commit-every 100
```

The repository includes a sample schema at `data/tse_common_stock_symbols.example.txt`.

Coverage note:

- The JPX manifest can contain more symbols than the local CSV directory covers.
- When the homepage shows values such as `已存日频行情涵盖 3757 只标的 / 普通股清单 3836 只`, that means 79 approved TSE common-stock symbols have no matching local CSV file yet, so they cannot be ingested from the current local source.
- The local seed dataset currently matches the manifest for 3757 symbols and misses mostly recent alphanumeric listings such as `130A.T`.

Incremental refresh behavior:

- `local_csv_directory` refreshes now pass each symbol's latest stored trade date into the provider.
- Only rows later than the stored date are emitted for ingestion, so the database no longer rewrites the full history on every run.
- Because the local source is still full-history CSV files, the provider must still scan each file to find the tail rows. A future remote provider can use the same `start_after_by_symbol` signal to request only missing date ranges upstream.

### `yahoo_finance_chart`

Remote fileless provider backed by Yahoo Finance chart JSON endpoints.

Example:

```bash
cd apps/api
PYTHONPATH=src python3 -m stockanalyse_api.jobs.refresh_market_data \
  --provider yahoo_finance_chart \
  --all-supported \
  --symbols-file ../../data/tse_common_stock_symbols.txt \
  --commit-every 100
```

Behavior:

- Uses the JPX manifest as the supported TSE common-stock universe.
- Pulls bars directly from Yahoo Finance, so symbols missing from the local CSV directory can still be ingested.
- Reuses the same incremental refresh contract and requests only dates later than each symbol's latest stored trade date.

### `yahoo_finance_chart_us`

Remote fileless provider for US daily bars through Yahoo Finance chart JSON. This is the default
research data source for the dashboard's 美股 / 更新数据 flow.

Example:

```bash
cd apps/api
PYTHONPATH=src python3 -m stockanalyse_api.jobs.refresh_market_data \
  --provider yahoo_finance_chart_us \
  --all-supported \
  --symbols-file ../../data/us_stock_symbols.txt \
  --universe-filter us_common_stock \
  --commit-every 100
```

Behavior:

- Uses `data/us_stock_symbols.txt` as the supported US common-stock universe.
- Requests Yahoo chart history from 2000-01-01 for cold symbols and only missing tail dates on incremental runs.
- Converts internal share-class symbols such as `BRK.B` to Yahoo request symbols such as `BRK-B`, while preserving `BRK.B` in the database.
- Sleeps between Yahoo requests using `STOCKANALYSE_YAHOO_REQUEST_INTERVAL_SECONDS` (default `0.2`) to reduce throttling risk.

### `alpha_vantage_daily_adjusted`

Remote provider for US adjusted daily bars. It requires the API key to stay on the backend:

```bash
export ALPHAVANTAGE_API_KEY=...
cd apps/api
PYTHONPATH=src python3 -m stockanalyse_api.jobs.refresh_market_data \
  --provider alpha_vantage_daily_adjusted \
  --all-supported \
  --symbols-file ../../data/us_stock_symbols.txt \
  --universe-filter us_common_stock \
  --commit-every 100
```

Alpha Vantage remains available as an explicit fallback through
`STOCKANALYSE_US_AUTO_REFRESH_PROVIDER=alpha_vantage_daily_adjusted`.

## US Universe Sync

To refresh the US common-stock symbol manifest from NASDAQ Trader symbol directories:

```bash
cd apps/api
PYTHONPATH=src python3 -m stockanalyse_api.jobs.sync_us_common_stock_universe
```

This job writes `data/us_stock_symbols.txt` by combining NASDAQ-listed common stocks with
NYSE common stocks from `otherlisted.txt`, excluding ETFs, ADR/ADS issues, units, warrants,
rights, preferred shares, notes, funds, and test issues. Pass `--other-exchange-code A` if you
also want to include NYSE American common stocks.

## US Fundamentals Refresh

US annual net-income rows come from SEC companyfacts:

```bash
cd apps/api
PYTHONPATH=src python3 -m stockanalyse_api.jobs.refresh_fundamentals \
  --provider sec_companyfacts \
  --exchange US
```

For SEC traffic, set a real contact string before large runs:

```bash
export STOCKANALYSE_SEC_USER_AGENT="stockAnalyse/0.1 contact=you@example.com"
```

The dashboard's 美股 / 刷新业绩 button uses the same provider by default through
`STOCKANALYSE_US_FUNDAMENTALS_PROVIDER=sec_companyfacts`.

## Universe Sync

To refresh the Tokyo Stock Exchange common-stock symbol manifest from the official JPX listed-issues page:

```bash
cd apps/api
PYTHONPATH=src python3 -m stockanalyse_api.jobs.sync_tse_common_stock_universe
```

This job:

- fetches the latest workbook link from the official JPX listed-issues page
- downloads the workbook into `data/reference/`
- derives a `data/tse_common_stock_symbols.txt` allowlist by keeping Prime/Standard/Growth common stocks
- excludes obvious non-common-stock products such as ETFs, ETNs, and REITs

## Scheduling

Repository templates are included for local scheduling:

- Cron example: `scripts/maintenance/sync_universe_and_refresh.cron.example`
- macOS `launchd` template: `scripts/maintenance/com.stockanalyse.universe-refresh.plist`

Both templates call `scripts/maintenance/sync_universe_and_refresh.sh`, which runs:

1. JPX universe sync
2. full-universe refresh with batched commits and incremental tail-only ingestion
3. derived-indicator materialization for screening/backtesting

By default the maintenance script now uses `REFRESH_PROVIDER=yahoo_finance_chart`, so nightly automation can fill JPX manifest symbols even when no local CSV exists for them.

## Screening Dependency

`/screen/runs` does not evaluate raw price rows directly. It reads persisted facts from
`derived_indicator_daily`.

That means a refresh pipeline must run both:

1. market-data refresh into `market_data_daily`
2. derived-fact materialization into `derived_indicator_daily`

If `derived_indicator_daily` is empty, the API correctly returns:
`No derived indicator facts are available for screening.`

Operational note:

- Derived-fact materialization now processes `market_data_daily` in trade-date order and commits in small trade-date batches.
- This keeps the local SQLite database more responsive while facts are being generated for large histories.

## Storage

In the current live runtime copy, refreshed market data is stored in:

- SQLite database: `/Users/adam/Documents/GitHub/stockAnalyse/data/stockanalyse.db`
- JPX universe manifest: `/Users/adam/Documents/GitHub/stockAnalyse/data/tse_common_stock_symbols.txt`
- Optional archived local seed CSVs: `/Users/adam/Documents/GitHub/stockAnalyse/data/archive/local_seed_csv`
