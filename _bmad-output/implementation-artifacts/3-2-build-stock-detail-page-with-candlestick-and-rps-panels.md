# 故事 3.2: Build Stock Detail Page with Candlestick and RPS Panels

状态: done

## 用户故事

作为用户，  
I want a stock detail view with price and RPS visualization,  
以便I can inspect the setup visually in one workflow。

## 验收标准

1. 假设a selected qualified stock，当the stock detail page loads，那么the page displays a candlestick chart for the stock。
2. 假设a selected qualified stock，当the stock detail page loads，那么the page displays an RPS panel below the main price chart。
3. 假设the RPS panel is shown，当the active threshold is applied，那么RPS conditions meeting the threshold are visually distinguishable without relying on color alone。

## 任务 / 子任务

- [x] Add stock-detail routing to the web app. (AC: 1, 2)
  - [x] Add a stock detail page under `/stocks/[instrumentId]`.
  - [x] Require `screen_run_id` so the detail view stays aligned with the originating result set.
- [x] Render price and RPS visuals from stored-facts payloads. (AC: 1, 2, 3)
  - [x] Build a candlestick-style SVG chart from the returned daily bars.
  - [x] Build an RPS panel from the returned indicator snapshot and threshold context.
  - [x] Use line style and textual summaries so threshold state is not communicated by color alone.
- [x] Connect result-list drill-down to stock detail. (AC: 1, 2)
  - [x] Link qualified results from `/screen` into the detail workflow.
- [x] Validate the implementation. (AC: 1, 2, 3)
  - [x] Run frontend lint and frontend build.
  - [x] Re-run backend tests to confirm the stock-detail API contract remains valid.

## 开发备注

- Story 3.2 must consume the stock-detail payload from Story 3.1 instead of introducing a separate frontend data model for charts. [Source: _bmad-output/implementation-artifacts/3-1-serve-stock-detail-and-chart-data-from-stored-facts.md]
- The PRD expects the chart and RPS panel to operate as one visual verification workflow, not as disconnected widgets. [Source: _bmad-output/planning-artifacts/prd.md:149,183-184,311]
- Accessibility requirement NFR17 means threshold-state distinction cannot rely on color alone; line style and visible text are required. [Source: _bmad-output/planning-artifacts/prd.md:485-487]

## 开发代理记录

### 使用的代理模型

GPT-5.4

### 调试日志参考

- Added `/stocks/[instrumentId]` and a stock-detail visualization component.
- Connected the screen result list to the stock detail route using `screen_run_id`.
- Added SVG-based candlestick and RPS panels using the stored-facts payload from the backend.
- Verified with `npm run lint` and `npm run build`.
- Re-verified backend compatibility with `PYTHONPATH=src python3 -m unittest tests.test_ingestion_review_regressions tests.test_market_data_health tests.test_strategy_configuration tests.test_factor_materialization tests.test_screening tests.test_chart_data`.

### 完成说明

- Qualified stocks in the result list now drill into a stock detail page.
- The detail page renders both price action and RPS context from the same backend payload.
- Threshold state in the RPS panel is distinguished with line style and text, not color alone.
- The page is ready for Story 3.3 to layer explicit rule-breakdown explanation on top.

### 文件清单

- _bmad-output/implementation-artifacts/3-2-build-stock-detail-page-with-candlestick-and-rps-panels.md
- apps/web/src/app/screen/page.tsx
- apps/web/src/app/stocks/[instrumentId]/page.tsx
- apps/web/src/components/screen/StrategyConfigPanel.tsx
- apps/web/src/components/stocks/StockDetailView.tsx
- apps/web/src/app/globals.css

### 变更日志

- 2026-04-14: Implemented the stock detail page with candlestick and RPS panel visuals wired to stored backend facts.
