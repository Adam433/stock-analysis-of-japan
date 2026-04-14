# 故事 3.1: Serve Stock Detail and Chart Data from Stored Facts

状态: done

## 用户故事

作为用户，  
I want stock detail data assembled from the same stored dataset used by screening,  
以便chart review and qualification logic stay aligned。

## 验收标准

1. 假设a stock from a completed screen run，当the stock detail is requested，那么the backend returns candlestick data, relevant RPS values, 52-week-high proximity state, and rule breakdown from stored data。
2. 假设the stock detail payload，当it is compared with the originating screen run，那么the qualification values remain consistent with the run that produced the result。

## 任务 / 子任务

- [x] Add stock-detail assembly service. (AC: 1, 2)
  - [x] Assemble candlestick rows from persisted `market_data_daily`.
  - [x] Assemble latest indicator values from persisted `derived_indicator_daily`.
  - [x] Assemble rule breakdown from the persisted `screen_run_results` record tied to the originating run.
- [x] Expose a stock-detail API route. (AC: 1, 2)
  - [x] Add `GET /stocks/{instrument_id}/detail?screen_run_id=...`.
  - [x] Return 404 when the requested stock/run binding does not exist.
- [x] Validate consistency with originating runs. (AC: 2)
  - [x] Add regression tests for successful payload assembly and invalid run binding.
  - [x] Run backend tests and compile validation.

## 开发备注

- The detail payload must remain anchored to persisted screen-run results. This story should not loosen consistency by reading only “current latest indicators” when a specific screen run exists. [Source: _bmad-output/planning-artifacts/epics.md:388-394]
- Architecture requires stock-detail payloads that include chart data and condition breakdown from the same stored facts. [Source: _bmad-output/planning-artifacts/architecture.md:498,641]
- Frontend chart rendering belongs to Story 3.2. This story only delivers the backend payload contract that later UI work will consume. [Source: _bmad-output/planning-artifacts/epics.md:397-413]

## 开发代理记录

### 使用的代理模型

GPT-5.4

### 调试日志参考

- Added a chart-data service that assembles stock detail payloads from market data, derived facts, and persisted screen-run results.
- Added `GET /stocks/{instrument_id}/detail?screen_run_id=...`.
- Added regression tests covering payload consistency and invalid run binding.
- Verified with `PYTHONPATH=src python3 -m unittest tests.test_ingestion_review_regressions tests.test_market_data_health tests.test_strategy_configuration tests.test_factor_materialization tests.test_screening tests.test_chart_data`.
- Verified with `PYTHONPATH=src python3 -m compileall src`.

### 完成说明

- The backend can now return chart-ready candlestick data and rule-breakdown context for a stock from a completed screen run.
- Stock detail remains explicitly tied to the originating run instead of loosely using only current system state.
- The payload is ready for Story 3.2 to consume in a stock-detail page and chart view.

### 文件清单

- _bmad-output/implementation-artifacts/3-1-serve-stock-detail-and-chart-data-from-stored-facts.md
- apps/api/src/stockanalyse_api/api/routes/__init__.py
- apps/api/src/stockanalyse_api/api/routes/stocks.py
- apps/api/src/stockanalyse_api/main.py
- apps/api/src/stockanalyse_api/services/chart_data.py
- apps/api/tests/test_chart_data.py

### 变更日志

- 2026-04-14: Implemented stock-detail payload assembly and a stored-facts-backed stock detail API.
