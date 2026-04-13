# Story 2.2: Materialize RPS and 52-Week-High Derived Facts

Status: done

## Story

As a system,  
I want to compute and persist derived screening facts from normalized market data,  
so that screening, charting, and backtesting all use the same authoritative values.

## Acceptance Criteria

1. Given normalized daily market data exists, when derived-facts materialization runs, then the system computes and stores 50-day, 120-day, and 250-day RPS-related values for supported securities.
2. Given normalized daily market data exists, when derived-facts materialization runs, then the system computes and stores 52-week-high proximity values for supported securities.
3. Given derived facts have been computed, when later services query them, then the same stored values are available for screening, chart detail, and backtesting.

## Tasks / Subtasks

- [x] Add derived-indicator persistence. (AC: 1, 2, 3)
  - [x] Create a `derived_indicator_daily` table for per-instrument, per-date screening facts.
  - [x] Store `rps_50`, `rps_120`, `rps_250`, `fifty_two_week_high`, and `high_proximity_ratio`.
- [x] Implement factor materialization. (AC: 1, 2, 3)
  - [x] Materialize RPS values as stored cross-sectional percentile facts derived from normalized market data.
  - [x] Materialize 52-week-high proximity from the rolling 252-trading-day window.
  - [x] Upsert materialized rows so repeat execution is idempotent.
- [x] Add an execution entrypoint. (AC: 1, 2)
  - [x] Add a backend job for derived-fact materialization.
- [x] Validate the implementation. (AC: 1, 2, 3)
  - [x] Add regression tests for RPS persistence, high-proximity persistence, and idempotent reruns.
  - [x] Run backend tests, compile validation, and migration upgrade validation.

## Dev Notes

- This story must persist derived facts rather than leave them as ad hoc runtime calculations. Later screening, chart explainability, and backtesting all depend on one shared stored source of truth. [Source: _bmad-output/planning-artifacts/architecture.md:217-218,377-378,807]
- The architecture explicitly calls for derived-facts tables covering RPS values and 52-week-high proximity. [Source: _bmad-output/planning-artifacts/architecture.md:247]
- This story stops at materialization. It does not yet execute a screen run or persist qualified result sets; that belongs to Story 2.3. [Source: _bmad-output/planning-artifacts/epics.md:327-353]

## Dev Agent Record

### Agent Model Used

GPT-5.4

### Debug Log References

- Added `derived_indicator_daily` plus migration `20260414_0005`.
- Implemented `materialize_derived_indicator_facts` to persist RPS 50/120/250 and 52-week-high proximity facts from normalized price history.
- Added a materialization job entrypoint under `apps/api/src/stockanalyse_api/jobs/materialize_derived_facts.py`.
- Verified with `PYTHONPATH=src python3 -m unittest tests.test_ingestion_review_regressions tests.test_market_data_health tests.test_strategy_configuration tests.test_factor_materialization`.
- Verified with `PYTHONPATH=src python3 -m compileall src` and `PYTHONPATH=src python3 -m alembic -c alembic.ini upgrade head`.

### Completion Notes List

- Derived screening facts are now persisted separately from raw market data.
- RPS values are materialized as stored cross-sectional percentiles for 50-day, 120-day, and 250-day lookbacks.
- 52-week-high proximity is persisted using a rolling 252-trading-day high and a reusable `high_proximity_ratio`.
- Repeat materialization runs update existing derived-fact rows without multiplying records.

### File List

- _bmad-output/implementation-artifacts/2-2-materialize-rps-and-52-week-high-derived-facts.md
- apps/api/migrations/env.py
- apps/api/migrations/versions/20260414_0005_add_derived_indicator_daily.py
- apps/api/src/stockanalyse_api/domain/indicators/__init__.py
- apps/api/src/stockanalyse_api/domain/indicators/models.py
- apps/api/src/stockanalyse_api/services/factor_materialization.py
- apps/api/src/stockanalyse_api/jobs/materialize_derived_facts.py
- apps/api/tests/test_factor_materialization.py

### Change Log

- 2026-04-14: Implemented persisted derived indicator facts for RPS 50/120/250 and 52-week-high proximity.
