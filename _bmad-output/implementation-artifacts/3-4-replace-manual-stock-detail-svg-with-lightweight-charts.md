# 故事 3.4: Replace Manual Stock Detail SVG Charts with Lightweight Charts

状态: done

## 用户故事

作为用户，
I want the stock detail page to use mature charting components for price and RPS history,
以便the visual analysis workflow feels trustworthy and does not rely on fabricated chart geometry。

## 验收标准

1. 假设a stock detail page loads，当the price panel renders，那么candlesticks are drawn by a mature charting library instead of custom SVG geometry。
2. 假设a stock detail page loads，当the RPS panel renders，那么it uses true historical derived-indicator values from the backend rather than a frontend-generated decay curve。
3. 假设the active threshold is shown in the RPS panel，当users inspect the chart，那么the threshold is represented directly in the chart and remains visually distinguishable。
4. 假设the revised stock detail payload and chart UI are in place，当frontend and backend tests run，那么the stock detail workflow remains valid。

## 实施说明

- The existing candlestick panel in `StockDetailView.tsx` uses hand-built SVG geometry and should be retired.
- The existing RPS panel is visually misleading because it extrapolates a fake time series from the latest snapshot rather than plotting persisted history.
- `lightweight-charts` is the preferred replacement because it is widely adopted for candlestick rendering and can also render line-series overlays or companion charts for RPS history.
- The backend stock-detail payload must expose historical RPS values aligned with the displayed candle window.

## 完成说明

- Replaced the custom SVG candlestick implementation with `lightweight-charts`.
- Added true historical RPS series to the stock-detail API payload.
- Removed the frontend-generated faux RPS curve and plotted stored derived facts instead.
- Closed follow-up review findings by restoring `indicator_history` as a required API contract instead of silently defaulting it away in the client.
- Fixed the stock-detail chart interaction regression so the price pane and RPS pane keep their visible ranges synchronized in both directions.
- Adjusted frontend dev/runtime resolution so the workspace can resolve `lightweight-charts` consistently during local verification.

## 文件清单

- _bmad-output/implementation-artifacts/3-4-replace-manual-stock-detail-svg-with-lightweight-charts.md
- apps/api/src/stockanalyse_api/services/chart_data.py
- apps/api/tests/test_chart_data.py
- apps/web/next.config.ts
- apps/web/package.json
- apps/web/src/components/stocks/StockDetailCharts.tsx
- apps/web/src/components/stocks/StockDetailView.tsx
- apps/web/src/app/globals.css
