# Story 2.4: Display Screen Result List with Qualification Summary

Status: done

## Story

As a user,  
I want a result list of qualified stocks with immediate qualification context,  
so that I can choose which candidates to inspect further.

## Acceptance Criteria

1. Given a completed screen run with qualifying securities, when the user views the result list, then the system displays the qualified stocks from that run.
2. Given a completed screen run with qualifying securities, when the user views the result list, then the result list is associated with the run date and parameter set that produced it.
3. Given a stock appears in the result list, when the user views its summary, then the summary indicates that the stock passed and provides enough context to open the detailed explanation flow.

## Tasks / Subtasks

- [x] Extend screening APIs for result-list retrieval. (AC: 1, 2)
  - [x] Add latest-run retrieval to support the screen workflow landing directly on the most recent result set.
- [x] Upgrade the `/screen` workflow to display result lists. (AC: 1, 2, 3)
  - [x] Load the latest screen run alongside the active strategy configuration.
  - [x] Add a `Run Screen` action to execute the current workflow end-to-end.
  - [x] Render the qualified stock list with run metadata and parameter-set context.
- [x] Add immediate qualification summaries. (AC: 3)
  - [x] Show best RPS value vs threshold.
  - [x] Show drawdown from the 52-week high vs allowed threshold.
  - [x] Show condition-pass summaries so the next detail story can deepen, not reinvent, the explanation.
- [x] Validate the implementation. (AC: 1, 2, 3)
  - [x] Run backend regression tests for latest-run retrieval.
  - [x] Run frontend lint and frontend build.

## Dev Notes

- Story 2.4 should consume the persisted screen-run data from Story 2.3 rather than inventing a new result calculation path in the UI. [Source: _bmad-output/implementation-artifacts/2-3-execute-screen-runs-and-persist-results.md]
- The result list is still part of the `screen` workflow area in the architecture and should stay closely coupled to the saved parameter set and run context. [Source: _bmad-output/planning-artifacts/architecture.md:657-660]
- Stock-detail explainability belongs to Epic 3. This story only needs enough summary context to support candidate selection and trust in the list itself. [Source: _bmad-output/planning-artifacts/epics.md:365-372]

## Dev Agent Record

### Agent Model Used

GPT-5.4

### Debug Log References

- Added latest-run retrieval to the screening service and route set.
- Upgraded `/screen` so the user can save parameters, launch a run, and inspect the resulting qualified list in one workflow.
- Added frontend result-list rendering with run metadata and qualification summaries.
- Verified with `PYTHONPATH=src python3 -m unittest tests.test_ingestion_review_regressions tests.test_market_data_health tests.test_strategy_configuration tests.test_factor_materialization tests.test_screening`.
- Verified with `npm run lint` and `npm run build`.

### Completion Notes List

- `/screen` now displays the most recent persisted screen run on load.
- The workflow can launch a new run and immediately render the qualified stock list.
- Each listed stock shows enough qualification summary to justify opening a later detail workflow.
- Run date, parameter-set version, candidate count, and qualified count are visible in the result-list surface.

### File List

- _bmad-output/implementation-artifacts/2-4-display-screen-result-list-with-qualification-summary.md
- apps/api/src/stockanalyse_api/api/routes/screening.py
- apps/api/src/stockanalyse_api/services/screening.py
- apps/api/tests/test_screening.py
- apps/web/src/app/screen/page.tsx
- apps/web/src/components/screen/StrategyConfigPanel.tsx
- apps/web/src/app/globals.css

### Change Log

- 2026-04-14: Implemented the screen result-list workflow with run metadata and qualification summaries.
