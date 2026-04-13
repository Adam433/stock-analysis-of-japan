# Story 1.3: Ingest and Normalize Japan Equity End-of-Day Data

Status: done

<!-- Note: Validation is optional. Run validate-create-story for quality check before dev-story. -->

## Story

As a user,  
I want Japan equity end-of-day market data ingested and normalized,  
so that screening and backtesting run on a trustworthy dataset.

## Acceptance Criteria

1. Given a configured market-data provider, when a market-data refresh is executed, then the system stores normalized Japan equity instrument records and daily market data records.
2. Given a configured market-data provider, when a market-data refresh is executed, then raw-price and adjusted-price concepts remain distinct in storage.
3. Given provider data contains incomplete or unavailable values, when normalization runs, then the system marks incomplete or unavailable states explicitly instead of silently treating them as complete data.

## Tasks / Subtasks

- [x] Add the persistence fields needed for normalized daily market data states. (AC: 1, 2, 3)
  - [x] Extend `market_data_daily` with explicit source/state metadata through a new migration.
  - [x] Keep compatibility with the SQLite baseline established in Story 1.2.
- [x] Implement provider-facing ingestion contracts. (AC: 1, 3)
  - [x] Add typed provider models for instruments and daily bars.
  - [x] Add a provider interface plus at least one concrete provider path for Japan equity EOD data.
- [x] Implement normalization and upsert logic. (AC: 1, 2, 3)
  - [x] Normalize provider payloads into canonical instrument and daily bar records.
  - [x] Preserve separate `close` and `adj_close` values.
  - [x] Mark rows as `complete`, `partial`, or `unavailable` based on normalized payload quality.
- [x] Implement a refresh entrypoint. (AC: 1, 3)
  - [x] Add a refresh job/CLI path under the backend app boundary.
  - [x] Make the refresh path configurable by provider and symbol list.
- [x] Validate the ingestion flow end-to-end. (AC: 1, 2, 3)
  - [x] Run the refresh logic against a deterministic provider input.
  - [x] Verify normalized instruments and daily rows are written to SQLite.
  - [x] Verify incomplete payloads are not silently stored as complete rows.

## Dev Notes

- This story builds directly on Story 1.2. Reuse the SQLite database, Alembic setup, SQLAlchemy base/session, and `instruments` / `market_data_daily` tables rather than creating parallel persistence paths.
- The product requires a provider model where historical data is ingested, stored, and normalized locally before screening or backtesting. [Source: _bmad-output/planning-artifacts/prd.md:208-214]
- Keep provider integrations behind backend service boundaries. Do not let frontend code or browser-side logic participate in ingestion. [Source: _bmad-output/planning-artifacts/architecture.md:622-627,679-682]
- Preserve separate raw and adjusted prices, and use explicit data-quality states instead of silent null handling. [Source: _bmad-output/planning-artifacts/epics.md:120,268-277]

### References

- Epic story definition: [epics.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/epics.md:262)
- Previous story: [1-2-establish-database-schema-and-migration-workflow.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/implementation-artifacts/1-2-establish-database-schema-and-migration-workflow.md)

## Dev Agent Record

### Agent Model Used

GPT-5.4

### Debug Log References

- Story created from the approved Epic 1.3 definition and previous Story 1.2 database baseline.
- `../../.venv/bin/python -m alembic -c alembic.ini upgrade head` applied the `20260413_0002` migration.
- `PYTHONPATH=src ../../.venv/bin/python -m stockanalyse_api.jobs.refresh_market_data --provider static_fixture --symbols 7203.T 6758.T` inserted fixture data successfully.
- Re-running the same refresh command returned `{'inserted': 0, 'updated': 3}`, confirming idempotent upsert behavior.
- SQLite inspection confirmed `complete`, `partial`, and `unavailable` rows were stored explicitly.

### Completion Notes List

- Extended `market_data_daily` with `data_source` and `data_status` fields through a new migration.
- Added provider dataclasses, provider protocol, and a deterministic static fixture provider.
- Added normalization logic that classifies rows as `complete`, `partial`, or `unavailable`.
- Added a refresh service plus backend CLI entrypoint for market-data ingestion.
- Verified end-to-end ingestion into SQLite and confirmed upsert behavior on repeat refresh.

### File List

- _bmad-output/implementation-artifacts/1-3-ingest-and-normalize-japan-equity-end-of-day-data.md
- apps/api/migrations/versions/20260413_0002_add_market_data_status_columns.py
- apps/api/src/stockanalyse_api/domain/market_data/models.py
- apps/api/src/stockanalyse_api/services/ingestion/__init__.py
- apps/api/src/stockanalyse_api/services/ingestion/provider_models.py
- apps/api/src/stockanalyse_api/services/ingestion/providers/__init__.py
- apps/api/src/stockanalyse_api/services/ingestion/providers/base.py
- apps/api/src/stockanalyse_api/services/ingestion/providers/static_provider.py
- apps/api/src/stockanalyse_api/services/ingestion/refresh_service.py
- apps/api/src/stockanalyse_api/services/normalization/__init__.py
- apps/api/src/stockanalyse_api/services/normalization/eod_normalizer.py
- apps/api/src/stockanalyse_api/jobs/refresh_market_data.py
- apps/api/tests/fixtures/japan_equity_eod_fixture.json

### Change Log

- 2026-04-13: Implemented Story 1.3 ingestion/normalization baseline with explicit data states, refresh CLI, and verified SQLite writes.
