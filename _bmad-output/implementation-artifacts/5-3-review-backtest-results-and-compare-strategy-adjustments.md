# Story 5.3: Review Backtest Results and Compare Strategy Adjustments

Status: done

## Story

As a user,  
I want to inspect completed backtest results and compare runs,  
so that I can iterate on my strategy with evidence rather than intuition alone.

## Acceptance Criteria

1. Given one or more completed backtest runs, when the user opens a backtest result, then the system displays the completed backtest output linked to the run record.
2. Given multiple backtest runs exist, when the user reviews them, then the system provides enough run context to compare parameter adjustments across runs.

## Tasks / Subtasks

- [x] Add backtest run listing support. (AC: 1, 2)
  - [x] Add backend retrieval for all persisted backtest runs.
  - [x] Keep run summaries bound to persisted run records and parameter sets.
- [x] Extend the `/backtests` workflow into a review surface. (AC: 1, 2)
  - [x] Show completed backtest runs with persisted summary output.
  - [x] Present enough context to compare parameter version, range, checksum, and summary differences across runs.
- [x] Validate review and comparison behavior. (AC: 1, 2)
  - [x] Extend backend tests for run listing order and retrieval.
  - [x] Run backend unit tests, compile validation, frontend lint, and frontend build.

## Dev Notes

- Story 5.3 should consume the persisted run summaries created in Stories 5.1 and 5.2 rather than inventing a separate comparison store. [Source: _bmad-output/implementation-artifacts/5-1-launch-and-persist-backtest-runs.md, _bmad-output/implementation-artifacts/5-2-execute-reproducible-backtests-from-stored-inputs.md]
- The goal is evidence-oriented review, not advanced performance analytics. Comparison should stay grounded in persisted run context, parameter adjustments, and output summaries. [Source: _bmad-output/planning-artifacts/prd.md:165,315,326,440-442]
- Epic 5 closes when launch, execution, and run-linked review all exist within the same backtests workflow area. [Source: _bmad-output/planning-artifacts/epics.md:500-540]

## Dev Agent Record

### Agent Model Used

GPT-5.4

### Debug Log References

- Added backtest run listing support on the backend.
- Extended the `/backtests` page to review completed runs and compare strategy-adjustment context across multiple persisted runs.
- Verified with `PYTHONPATH=src python3 -m unittest tests.test_backtesting tests.test_watchlist tests.test_screening tests.test_chart_data`.
- Verified with `PYTHONPATH=src python3 -m compileall src`.
- Verified with `npm run lint` and `npm run build`.

### Completion Notes List

- Completed backtest runs can now be reviewed from the `/backtests` workflow with run-linked persisted outputs.
- Multiple runs can be compared using parameter version, date range, checksum, and summary deltas.
- Epic 5 now has an end-to-end backtest workflow from launch through execution to review.

### File List

- _bmad-output/implementation-artifacts/5-3-review-backtest-results-and-compare-strategy-adjustments.md
- apps/api/src/stockanalyse_api/api/routes/backtests.py
- apps/api/src/stockanalyse_api/services/backtesting.py
- apps/api/tests/test_backtesting.py
- apps/web/src/app/backtests/page.tsx
- apps/web/src/components/backtests/BacktestLaunchPanel.tsx

### Change Log

- 2026-04-14: Added backtest result review and multi-run comparison workflow.
