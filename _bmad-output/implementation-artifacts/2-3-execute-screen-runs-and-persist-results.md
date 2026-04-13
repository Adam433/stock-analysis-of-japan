# Story 2.3: Execute Screen Runs and Persist Results

Status: done

## Story

As a user,  
I want to run the MVP screen against the Japan equity universe,  
so that I can retrieve the stocks that satisfy the active strategy.

## Acceptance Criteria

1. Given a valid parameter set and available derived facts, when the user launches a screen run, then the system evaluates the configured strategy across the supported Japan equity universe.
2. Given a valid parameter set and available derived facts, when the user launches a screen run, then the system persists a screen run record with the parameter set and run context.
3. Given a completed screen run, when results are returned, then each qualified stock is linked to the stored values that caused it to pass.

## Tasks / Subtasks

- [x] Extend the screening domain with run persistence. (AC: 2, 3)
  - [x] Add `screen_runs` for run-level context and parameter-set linkage.
  - [x] Add `screen_run_results` for per-instrument pass/fail facts and traceable values.
- [x] Implement screen execution. (AC: 1, 2, 3)
  - [x] Evaluate the latest derived facts against the active strategy configuration.
  - [x] Persist all candidate evaluations while returning the qualified subset.
  - [x] Preserve the values and threshold checks that explain why a stock passed.
- [x] Expose minimal execution and retrieval APIs. (AC: 1, 2, 3)
  - [x] Add `POST /screen/runs` to launch a run.
  - [x] Add `GET /screen/runs/{screen_run_id}` to retrieve a persisted run summary.
- [x] Validate the implementation. (AC: 1, 2, 3)
  - [x] Add regression tests for run persistence, qualified result traceability, and missing-derived-fact failure handling.
  - [x] Run backend tests, compile validation, and migration upgrade validation.

## Dev Notes

- This story consumes the active parameter set from Story 2.1 and the persisted derived facts from Story 2.2. It should not recalculate authoritative factors during screen execution. [Source: _bmad-output/implementation-artifacts/2-1-create-strategy-configuration-workflow.md; _bmad-output/implementation-artifacts/2-2-materialize-rps-and-52-week-high-derived-facts.md]
- Architecture requires persisted screen runs and traceability from each qualifying stock back to thresholds and stored values. [Source: _bmad-output/planning-artifacts/architecture.md:246,480,649,865]
- Result-list rendering belongs to Story 2.4. This story only needs the execution and persisted-result backbone that the list can query next. [Source: _bmad-output/planning-artifacts/epics.md:346-372]

## Dev Agent Record

### Agent Model Used

GPT-5.4

### Debug Log References

- Added `screen_runs` and `screen_run_results` persistence through migration `20260414_0006`.
- Implemented a screening service that evaluates persisted derived facts against the active strategy configuration.
- Added `POST /screen/runs` and `GET /screen/runs/{screen_run_id}`.
- Added a `run_screen.py` job entrypoint.
- Verified with `PYTHONPATH=src python3 -m unittest tests.test_ingestion_review_regressions tests.test_market_data_health tests.test_strategy_configuration tests.test_factor_materialization tests.test_screening`.
- Verified with `PYTHONPATH=src python3 -m compileall src` and `PYTHONPATH=src python3 -m alembic -c alembic.ini upgrade head`.

### Completion Notes List

- Screen execution now uses the active strategy configuration plus the latest derived indicator facts.
- Each run persists both run-level context and per-instrument evaluation output.
- Qualified results include traceable threshold/value fields needed for later explainability and result-list summaries.
- The API now exposes enough persisted run data for Story 2.4 to render the result list without changing the execution model.

### File List

- _bmad-output/implementation-artifacts/2-3-execute-screen-runs-and-persist-results.md
- apps/api/migrations/versions/20260414_0006_add_screen_runs_and_results.py
- apps/api/src/stockanalyse_api/api/routes/__init__.py
- apps/api/src/stockanalyse_api/api/routes/screening.py
- apps/api/src/stockanalyse_api/domain/screens/models.py
- apps/api/src/stockanalyse_api/main.py
- apps/api/src/stockanalyse_api/services/screening.py
- apps/api/src/stockanalyse_api/jobs/run_screen.py
- apps/api/tests/test_screening.py

### Change Log

- 2026-04-14: Implemented persisted screen runs, per-stock result records, and minimal screening execution APIs.
