# UX Supplement: TodoList Follow-Ups

**Date:** 2026-04-15
**Scope:** Data health, stock detail charts, screening parameter controls, watchlist navigation

## Purpose

This document supplements the existing planning artifacts for the approved TodoList follow-up changes. It defines interaction and presentation decisions that were not previously captured in a dedicated UX artifact.

## 1. Stock Detail Chart UX

### 1.1 Default History Range

- The candlestick chart should open with enough historical context for routine pattern review rather than a narrowly cropped recent slice.
- Preferred behavior:
  - Load a materially longer default history window on first render.
  - Only introduce explicit incremental loading if the backend payload becomes too large for a fast detail view.
- The user should not need a manual setting change just to see a normal multi-month trend.

### 1.2 History Expansion Behavior

- If incremental loading is introduced later, it should follow standard chart expectations:
  - loading older history when the user pans toward the left boundary
  - preserving the current visible range without snapping the chart unexpectedly
- Any loading state should be lightweight and non-blocking.

### 1.3 RPS Label Placement

- Fixed labels pinned near the most recent plotted points must not cover the latest visible RPS curves.
- Preferred behavior:
  - place labels in a reserved legend area above or beside the RPS panel, or
  - offset end labels so they avoid overlapping recent lines and key intersections
- The latest RPS area is a high-value inspection zone and must remain readable.

### 1.4 Chart Date Formatting

- Dates shown in the stock detail workflow should use a localized date-only format.
- Preferred format:
  - `2026年03月04日`
- Time-of-day should not be shown in chart-adjacent date displays for end-of-day workflows.

## 2. Shared Navigation UX

### 2.1 Top Navigation Consistency

- The top navigation bar must render consistently across data health, strategy configuration, watchlist, and backtest pages.
- Navigation labels must not collapse into or visually merge with descriptive page copy.
- The page-level descriptive sentence should remain separate from the primary navigation row.

## 3. Data Health Page UX

### 3.1 Information Hierarchy

- The data health page should prioritize trust signals over implementation details.
- Primary visible items:
  - market data coverage
  - approved common-stock universe size
  - universe manifest last-updated timestamp
  - refresh execution state
- Local filesystem paths should not be shown as a primary trust signal in the main UI.

### 3.2 Universe Manifest Presentation

- Replace raw source path emphasis with a simpler timestamp-oriented presentation.
- Preferred summary pattern:
  - `普通股清单更新于 2026年04月15日 09:57`
- If deeper provenance is needed later, place it behind secondary detail disclosure rather than in the default summary row.

## 4. Screening Parameter UX

### 4.1 Configurable RPS Windows

- The screening configuration should no longer imply that `50 / 120 / 250` are the only valid windows.
- The interface should let the user define the active RPS windows explicitly.
- The control should communicate that only approved windows are accepted by the backend.

### 4.2 Minimum Satisfied-Line Count

- The screening form should expose how many selected RPS lines must pass the threshold.
- This control should be framed as a strategy rule, not as an implementation detail.
- Validation should prevent impossible combinations, such as requiring more satisfied lines than the number of selected windows.

## 5. Watchlist UX

### 5.1 Direct Detail Access

- Each watchlist row should provide a clear path into the corresponding stock detail page.
- The stock symbol itself may serve as the primary link if styling and affordance make the navigation obvious.
- The user should not need to copy a symbol manually or return to another page to continue research.

## 6. Accessibility Notes

- Date formatting, labels, and links must remain readable on desktop and narrower layouts.
- Signal meaning must not rely only on line color or chart position.
- Navigation targets from the watchlist must be keyboard reachable.
- Data health summaries should remain understandable without exposing internal file paths.
