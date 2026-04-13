# Story 6.1: Enforce Trust, Accessibility, and Error-State Standards

Status: done

## Story

As a user,  
I want the product to surface trust and usability signals consistently,  
so that I can use the tool confidently during daily research.

## Acceptance Criteria

1. Given a screen, stock detail, watchlist, or backtest workflow, when stale data, partial data, invalid input, or a failed run occurs, then the UI presents a clear explicit state instead of a silent or misleading success state.
2. Given primary workflows in the web app, when the user navigates them with keyboard-only interaction, then the main parameter, result, stock detail, and watchlist flows remain operable and important pass or fail states are not communicated by color alone.

## Tasks / Subtasks

- [x] Add shared trust-state presentation across primary workflows. (AC: 1)
  - [x] Load market-data health in screen, stock-detail, watchlist, and backtest routes.
  - [x] Add a shared workflow-level trust banner that distinguishes trusted, stale/partial, and unavailable/error states with explicit text.
- [x] Improve form and action accessibility. (AC: 2)
  - [x] Add visible focus styles for links, buttons, inputs, and textareas.
  - [x] Mark invalid inputs and status/error messages with accessibility-friendly attributes.
- [x] Validate the hardening changes. (AC: 1, 2)
  - [x] Run frontend lint.
  - [x] Run frontend build.

## Dev Notes

- Story 6.1 is a cross-cutting hardening story, not a new domain. The safest implementation path is to reuse existing health signals and thread them consistently through existing workflows. [Source: _bmad-output/planning-artifacts/architecture.md:59,79,436,464,472,497]
- Trust-state messaging must distinguish stale data, partial coverage, invalid input, and system unavailability rather than collapsing them into generic failure language. [Source: _bmad-output/planning-artifacts/prd.md:171,173,204,446-451,470,494]
- Accessibility requirements emphasize keyboard reachability and non-color-only state communication across core workflows, so focus visibility and explicit text/status roles matter as much as visuals. [Source: _bmad-output/planning-artifacts/prd.md:273,485-488]

## Dev Agent Record

### Agent Model Used

GPT-5.4

### Debug Log References

- Added a shared workflow trust banner and threaded market-data health into the major workflow routes.
- Added focus-visible styles and accessibility attributes for form validation and status/error messaging.
- Verified with `npm run lint` and `npm run build`.

### Completion Notes List

- Screen, stock-detail, watchlist, and backtest workflows now expose explicit trust-state context instead of relying on users to infer data health indirectly.
- Keyboard-only interaction is more usable because primary interactive elements now expose visible focus treatment.
- Error and validation states are more explicit through ARIA-friendly status and invalid-input signaling.

### File List

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

### Change Log

- 2026-04-14: Added shared trust-state banners and keyboard/error-state accessibility hardening across primary workflows.
