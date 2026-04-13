# Story 5.2: Execute Reproducible Backtests from Stored Inputs

Status: done

## Story

As a user,  
I want backtests to run from the same stored facts used by screening,  
so that the results are reproducible and trustworthy.

## Acceptance Criteria

1. Given a backtest run with a historical range and parameter set, when the system executes the backtest, then it uses the same normalized dataset and parameterized conditions as the screen logic.
2. Given the same historical range, parameter set, and stored dataset, when the backtest is run again, then the system returns the same result.

## Tasks / Subtasks

- [x] Reuse screening-aligned condition evaluation for backtesting. (AC: 1, 2)
  - [x] Extract shared stored-facts rule evaluation logic from screening.
  - [x] Execute backtests over persisted derived facts inside the requested historical range.
- [x] Persist reproducible backtest output summaries. (AC: 1, 2)
  - [x] Extend `backtest_runs` with persisted execution summary fields and checksum.
  - [x] Mark runs `completed` or `failed` explicitly after execution.
- [x] Expose execution through API and local job paths. (AC: 1)
  - [x] Add run execution endpoint for persisted backtest runs.
  - [x] Update the local backtest job to launch and execute against stored inputs.
- [x] Extend the backtests UI to execute and display persisted summaries. (AC: 1, 2)
  - [x] Add execute action for the latest persisted run.
  - [x] Show persisted execution summary fields and checksum after completion.
- [x] Validate reproducibility and integration. (AC: 1, 2)
  - [x] Add backend tests covering completed execution and deterministic re-run results.
  - [x] Run backend unit tests, compile validation, Alembic upgrade, frontend lint, and frontend build.

## Dev Notes

- Story 5.2 must use the same parameterized condition logic as screening rather than a parallel approximation, otherwise trust and reproducibility guarantees weaken immediately. [Source: _bmad-output/planning-artifacts/prd.md:202,206,438-439,492]
- Architecture expects backtests to run against stored inputs and remain first-class persisted records with explicit status transitions. [Source: _bmad-output/planning-artifacts/architecture.md:58,60,380,450,470,642,649,807]
- Story 5.3 will consume the persisted outputs added here for result review and comparison. This story should focus on execution correctness and durable summary persistence, not comparative presentation. [Source: _bmad-output/planning-artifacts/epics.md:526-540]

## Dev Agent Record

### Agent Model Used

GPT-5.4

### Debug Log References

- Extracted shared screening-aligned indicator evaluation logic and reused it during backtest execution.
- Extended `backtest_runs` with persisted execution summary fields and checksum.
- Added execution API support and updated the `/backtests` workflow to execute persisted runs and show summary output.
- Verified with `PYTHONPATH=src python3 -m unittest tests.test_backtesting tests.test_watchlist tests.test_screening tests.test_chart_data`.
- Verified with `PYTHONPATH=src python3 -m compileall src`.
- Verified with `PYTHONPATH=src python3 -m alembic -c alembic.ini upgrade head`.
- Verified with `npm run lint` and `npm run build`.

### Completion Notes List

- Backtests now execute from stored derived facts using the same parameterized rule evaluation as screening.
- Completed runs persist deterministic summary output and checksum so identical inputs can be verified as identical results.
- The `/backtests` workflow now supports both launch and execute, and is ready for Story 5.3 to focus on review and comparison.

### File List

- _bmad-output/implementation-artifacts/5-2-execute-reproducible-backtests-from-stored-inputs.md
- apps/api/migrations/versions/20260414_0010_add_backtest_run_summary_fields.py
- apps/api/src/stockanalyse_api/api/routes/backtests.py
- apps/api/src/stockanalyse_api/domain/backtests/models.py
- apps/api/src/stockanalyse_api/jobs/run_backtest.py
- apps/api/src/stockanalyse_api/services/backtesting.py
- apps/api/src/stockanalyse_api/services/screening.py
- apps/api/tests/test_backtesting.py
- apps/web/src/components/backtests/BacktestLaunchPanel.tsx

### Change Log

- 2026-04-14: Added stored-facts backtest execution with deterministic summary persistence.
