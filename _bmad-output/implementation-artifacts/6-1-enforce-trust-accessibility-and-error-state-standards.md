# 故事 6.1: Enforce Trust, Accessibility, and Error-State Standards

状态: done

## 用户故事

作为用户，  
I want the product to surface trust and usability signals consistently,  
以便I can use the tool confidently during daily research。

## 验收标准

1. 假设a screen, stock detail, watchlist, or backtest workflow，当stale data, partial data, invalid input, or a failed run occurs，那么the UI presents a clear explicit state instead of a silent or misleading success state。
2. 假设primary workflows in the web app，当the user navigates them with keyboard-only interaction，那么the main parameter, result, stock detail, and watchlist flows remain operable and important pass or fail states are not communicated by color alone。

## 任务 / 子任务

- [x] Add shared trust-state presentation across primary workflows. (AC: 1)
  - [x] Load market-data health in screen, stock-detail, watchlist, and backtest routes.
  - [x] Add a shared workflow-level trust banner that distinguishes trusted, stale/partial, and unavailable/error states with explicit text.
- [x] Improve form and action accessibility. (AC: 2)
  - [x] Add visible focus styles for links, buttons, inputs, and textareas.
  - [x] Mark invalid inputs and status/error messages with accessibility-friendly attributes.
- [x] Validate the hardening changes. (AC: 1, 2)
  - [x] Run frontend lint.
  - [x] Run frontend build.

## 开发备注

- Story 6.1 is a cross-cutting hardening story, not a new domain. The safest implementation path is to reuse existing health signals and thread them consistently through existing workflows. [Source: _bmad-output/planning-artifacts/architecture.md:59,79,436,464,472,497]
- Trust-state messaging must distinguish stale data, partial coverage, invalid input, and system unavailability rather than collapsing them into generic failure language. [Source: _bmad-output/planning-artifacts/prd.md:171,173,204,446-451,470,494]
- Accessibility requirements emphasize keyboard reachability and non-color-only state communication across core workflows, so focus visibility and explicit text/status roles matter as much as visuals. [Source: _bmad-output/planning-artifacts/prd.md:273,485-488]

## 开发代理记录

### 使用的代理模型

GPT-5.4

### 调试日志参考

- Added a shared workflow trust banner and threaded market-data health into the major workflow routes.
- Added focus-visible styles and accessibility attributes for form validation and status/error messaging.
- Verified with `npm run lint` and `npm run build`.

### 完成说明

- Screen, stock-detail, watchlist, and backtest workflows now expose explicit trust-state context instead of relying on users to infer data health indirectly.
- Keyboard-only interaction is more usable because primary interactive elements now expose visible focus treatment.
- Error and validation states are more explicit through ARIA-friendly status and invalid-input signaling.

### 文件清单

- _bmad-output/implementation-artifacts/6-1-enforce-trust-accessibility-and-error-state-standards.md
- apps/web/src/app/backtests/page.tsx
- apps/web/src/app/globals.css
- apps/web/src/app/screen/page.tsx
- apps/web/src/app/stocks/[instrumentId]/page.tsx
- apps/web/src/app/watchlist/page.tsx
- apps/web/src/components/backtests/BacktestLaunchPanel.tsx
- apps/web/src/components/screen/StrategyConfigPanel.tsx
- apps/web/src/components/shared/WorkflowTrustBanner.tsx
- apps/web/src/components/watchlist/WatchlistToggleButton.tsx
- apps/web/src/lib/marketDataHealth.ts

### 变更日志

- 2026-04-14: Added shared trust-state banners and keyboard/error-state accessibility hardening across primary workflows.
