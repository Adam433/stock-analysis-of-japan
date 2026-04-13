# Story 5.1: Launch and Persist Backtest Runs

Status: done

## Story

As a user,  
I want to launch a historical backtest using my strategy parameters,  
so that I can evaluate the strategy with historical evidence.

## Acceptance Criteria

1. Given a valid strategy parameter set, when the user selects a historical range and starts a backtest, then the system creates and persists a backtest run record with the parameter set and historical range.
2. Given a backtest run is started, when execution takes longer than an immediate request cycle, then the system exposes an explicit in-progress state for that run.

## Tasks / Subtasks

- [x] Add backtest-run persistence and API routes. (AC: 1, 2)
  - [x] Create a `backtest_runs` table with parameter-set linkage, historical range, and lifecycle status.
  - [x] Add launch, latest-run, and single-run retrieval endpoints.
  - [x] Keep launch validation minimal and explicit, including invalid date-range rejection.
- [x] Add backtest launch workflow to the web app. (AC: 1, 2)
  - [x] Add a `/backtests` route with historical range inputs.
  - [x] Persist a launched run through the API and show the resulting run identifier and status.
  - [x] Display explicit `running` state instead of implying immediate completion.
- [x] Validate persistence and route integration. (AC: 1, 2)
  - [x] Add backend tests for launch, invalid ranges, and latest-run retrieval.
  - [x] Run backend unit tests, compile validation, Alembic upgrade, frontend lint, and frontend build.

## Dev Notes

- Story 5.1 is about launch and persistence, not execution results. The run record and explicit status should exist before 5.2 adds reproducible simulation logic. [Source: _bmad-output/planning-artifacts/epics.md:500-525]
- Architecture expects backtests to be first-class persisted runs, parallel to screen runs, and to use local job-style execution semantics for longer tasks. [Source: _bmad-output/planning-artifacts/architecture.md:225,310,345,450,649]
- Existing strategy configuration versioning should be reused directly so every backtest run remains tied to the exact active parameter set at launch time. [Source: _bmad-output/planning-artifacts/prd.md:385,441,471]

## Dev Agent Record

### Agent Model Used

GPT-5.4

### Debug Log References

- Added a backtests domain, migration, service layer, API routes, and CLI launch job.
- Added a `/backtests` page with historical range selection and explicit persisted-run status display.
- Verified with `PYTHONPATH=src python3 -m unittest tests.test_backtesting tests.test_watchlist tests.test_screening tests.test_chart_data`.
- Verified with `PYTHONPATH=src python3 -m compileall src`.
- Verified with `PYTHONPATH=src python3 -m alembic -c alembic.ini upgrade head`.
- Verified with `npm run lint` and `npm run build`.

### Completion Notes List

- Backtest runs are now persisted records linked to strategy configurations and date ranges.
- Launching a backtest surfaces an explicit `running` state immediately instead of pretending the result is already finished.
- The implementation creates the persistence and UI foundation for Story 5.2 to execute reproducible backtests against stored facts.

### File List

- _bmad-output/implementation-artifacts/5-1-launch-and-persist-backtest-runs.md
- apps/api/migrations/env.py
- apps/api/migrations/versions/20260414_0009_add_backtest_runs.py
- apps/api/src/stockanalyse_api/api/routes/__init__.py
- apps/api/src/stockanalyse_api/api/routes/backtests.py
- apps/api/src/stockanalyse_api/domain/backtests/__init__.py
- apps/api/src/stockanalyse_api/domain/backtests/models.py
- apps/api/src/stockanalyse_api/jobs/run_backtest.py
- apps/api/src/stockanalyse_api/main.py
- apps/api/src/stockanalyse_api/services/backtesting.py
- apps/api/tests/test_backtesting.py
- apps/web/src/app/backtests/page.tsx
- apps/web/src/app/page.tsx
- apps/web/src/app/screen/page.tsx
- apps/web/src/app/stocks/[instrumentId]/page.tsx
- apps/web/src/app/watchlist/page.tsx
- apps/web/src/components/backtests/BacktestLaunchPanel.tsx

### Change Log

- 2026-04-14: Added persisted backtest run launch workflow with explicit in-progress state.
