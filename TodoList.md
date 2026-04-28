    □ US price source: add ALPHAVANTAGE_API_KEY path, run small AAPL/MSFT ingest, confirm adjusted OHLCV rows and split handling.
    □ US universe: replace seed data/us_stock_symbols.txt with a real NYSE/NASDAQ common-stock universe and keep ETF/ADR/noise out.
    ✓ US fundamentals: run SEC companyfacts refresh for imported US instruments, validate ticker->CIK mapping and net_income annual rows. Completed 2026-04-28: 4,617/4,800 US instruments covered, 21,373 annual net-income rows after 20-F/40-F/IFRS retry.
    □ US materialization: materialize derived facts after US ingest, verify US RPS is ranked only against US instruments.
    □ US dashboard UX: show US data-source/key errors clearly, add fundamentals refresh trigger or documented workflow.
    □ US backtest: run cup-handle + RPS + fundamentals parameter sweeps on US history, record win rate, drawdown, stop-loss sensitivity.
    □ Provider fallback: evaluate non-Yahoo/non-Alpha alternatives if Alpha adjusted daily is paywalled/rate-limited for full universe.
