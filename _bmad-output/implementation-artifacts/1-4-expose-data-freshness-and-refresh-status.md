# 故事 1.4: Expose Data Freshness and Refresh Status

状态: done

## 用户故事

作为用户，  
我希望see whether stored market data is fresh and usable，  
以便I can trust or question screening and backtest outputs appropriately。

## 验收标准

1. 假设market-data refresh jobs have run，当the user requests data health information，那么the system returns refresh status, freshness state, and failure or incompleteness indicators。
2. 假设a refresh fails or only partially completes，当the user views the product status，那么the failed or partial state is visible and not masked as normal success。

## 任务 / 子任务

- [x] Add operational persistence for refresh runs. (AC: 1, 2)
  - [x] Create a `market_data_refresh_runs` table with explicit run status and outcome counters.
  - [x] Record successful, partial, and failed refresh outcomes from the ingestion entrypoint.
- [x] Add a market-data health aggregation service and API route. (AC: 1, 2)
  - [x] Summarize freshness, last refresh outcome, and incomplete row counts from stored data.
  - [x] Expose the snapshot from `/health/market-data`.
- [x] Surface health status in the web shell. (AC: 2)
  - [x] Replace the static landing page content with freshness and refresh-state cards.
  - [x] Show stale, partial, failed, and unavailable states explicitly.
- [x] Validate with regression tests and frontend lint. (AC: 1, 2)
  - [x] Add backend tests for partial, failed, and stale health states.
  - [x] Run API-side unit tests and web lint successfully.

## 开发备注

- Story 1.4 builds on Story 1.3's normalized daily bar store and explicit `data_status` classification. It should not invent a parallel health store for bar completeness. [Source: _bmad-output/implementation-artifacts/1-3-ingest-and-normalize-japan-equity-end-of-day-data.md]
- The architecture explicitly reserves a `health` service boundary for data freshness and operational status visibility, and the UI should render backend-provided trust signals instead of inferring them client-side. [Source: _bmad-output/planning-artifacts/architecture.md:643,622-627]
- Freshness visibility is operational trust functionality, not generic uptime monitoring. The status payload should expose both recency and whether the last refresh failed or partially completed. [Source: _bmad-output/planning-artifacts/prd.md:173,240]

## 开发代理记录

### 使用的代理模型

GPT-5.4

### 调试日志参考

- Added refresh-run tracking plus a market-data health aggregation service.
- Added `GET /health/market-data` through the FastAPI app shell.
- Reworked the Next.js homepage to display freshness, coverage, and refresh outcome signals from the backend payload.
- Verified with `PYTHONPATH=src python3 -m unittest tests.test_ingestion_review_regressions tests.test_market_data_health`.
- Verified with `npm run lint` in `apps/web`.
- Follow-up review fixes ensured coverage status reflects persisted incomplete rows, refresh-run counters follow final row state after duplicate updates, and UI distinguishes API connectivity issues from actual data-health failures.

### 完成说明

- Refresh runs are now persisted with `running`, `succeeded`, `partial`, and `failed` outcomes.
- Health snapshots report freshness (`fresh`, `stale`, `missing`) and coverage (`complete`, `partial`, `failed`, `missing`) using stored market data and the latest refresh run.
- The web shell now surfaces data trust signals directly instead of only showing static roadmap content.

### 文件清单

- _bmad-output/implementation-artifacts/1-4-expose-data-freshness-and-refresh-status.md
- apps/api/migrations/versions/20260413_0003_add_market_data_refresh_runs.py
- apps/api/src/stockanalyse_api/api/routes/__init__.py
- apps/api/src/stockanalyse_api/api/routes/health.py
- apps/api/src/stockanalyse_api/domain/operations/__init__.py
- apps/api/src/stockanalyse_api/domain/operations/models.py
- apps/api/src/stockanalyse_api/main.py
- apps/api/src/stockanalyse_api/services/health.py
- apps/api/src/stockanalyse_api/services/ingestion/refresh_service.py
- apps/api/src/stockanalyse_api/jobs/refresh_market_data.py
- apps/api/migrations/env.py
- apps/api/pyproject.toml
- apps/api/tests/test_market_data_health.py
- apps/api/tests/test_ingestion_review_regressions.py
- apps/web/src/app/page.tsx
- apps/web/src/app/globals.css

### 变更日志

- 2026-04-13: Implemented operational market-data health visibility across persistence, API, and web shell.
- 2026-04-13: Fixed 1.4 review findings around aggregate coverage semantics, duplicate-row run counters, and API connectivity messaging.
