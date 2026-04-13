# Story 3.3: Show Rule Breakdown and Exact Qualifying Values

Status: done

## Story

As a user,  
I want the stock detail page to show the exact rule breakdown and qualifying values,  
so that I can understand why a stock qualified without leaving the analysis flow.

## Acceptance Criteria

1. Given a stock detail page is loaded, when the explainability section is shown, then the page displays the exact rule breakdown for the stock.
2. Given rule breakdown is displayed, when the user reviews each rule, then the underlying values used to determine pass or fail are visible.
3. Given the user is analyzing a stock, when they inspect qualification details, then they can determine why the stock qualified without leaving the stock analysis flow.

## Tasks / Subtasks

- [x] Extend the stock detail page with a dedicated explainability section. (AC: 1, 3)
  - [x] Add a narrative qualification summary bound to the originating screen run.
  - [x] Add separate cards for RPS strength and 52-week-high proximity conditions.
- [x] Show exact qualifying inputs for each rule. (AC: 1, 2)
  - [x] Render threshold, underlying values, best-RPS selection, and explicit pass/fail decision for the RPS rule.
  - [x] Render allowed drawdown, observed drawdown, proximity ratio, related price values, and explicit pass/fail decision for the 52-week-high rule.
- [x] Validate the frontend implementation. (AC: 1, 2, 3)
  - [x] Run frontend lint.
  - [x] Run frontend build.

## Dev Notes

- Story 3.3 should build directly on the stock-detail payload from Story 3.1 and the page shell introduced in Story 3.2; it does not need a new API contract for explainability. [Source: _bmad-output/implementation-artifacts/3-1-serve-stock-detail-and-chart-data-from-stored-facts.md, _bmad-output/implementation-artifacts/3-2-build-stock-detail-page-with-candlestick-and-rps-panels.md]
- The epic requires exact rule breakdown and qualifying values to remain within the same stock analysis flow rather than forcing the user back to the result list or configuration page. [Source: _bmad-output/planning-artifacts/epics.md:414-429]
- Existing trust and accessibility requirements still apply, so pass/fail state must remain readable and not be communicated by color alone. [Source: _bmad-output/planning-artifacts/prd.md:485-487]

## Dev Agent Record

### Agent Model Used

GPT-5.4

### Debug Log References

- Extended the stock detail page with a dedicated explainability section driven by the existing stored-facts payload.
- Added exact-value breakdown cards for the RPS rule and the 52-week-high proximity rule.
- Verified with `npm run lint` and `npm run build`.

### Completion Notes List

- The stock detail page now explains qualification with exact stored values and explicit per-rule decisions.
- Users can see why a stock qualified without leaving the detail workflow.
- The implementation stayed inside the existing Story 3.1 payload contract and Story 3.2 page structure.

### File List

- _bmad-output/implementation-artifacts/3-3-show-rule-breakdown-and-exact-qualifying-values.md
- apps/web/src/components/stocks/StockDetailView.tsx
- apps/web/src/app/globals.css

### Change Log

- 2026-04-14: Added a rule-breakdown explainability section with exact qualifying values to the stock detail page.
