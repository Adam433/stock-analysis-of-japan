# Story 1.2: Establish Database Schema and Migration Workflow

Status: review

<!-- Note: Validation is optional. Run validate-create-story for quality check before dev-story. -->

## Story

As a developer,  
I want the initial database schema baseline and migration workflow in place,  
so that market data foundations can be stored consistently and later stories can evolve the schema safely.

## Acceptance Criteria

1. Given the backend application, when the persistence layer is implemented, then the SQLite-backed schema supports instruments and daily market data records needed by the historical data backbone.
2. Given the backend application, when the persistence layer is implemented, then schema migration tooling is configured and usable in local development.
3. Given a new local environment, when migrations are applied, then the database can be created from migrations without manual schema editing.

## Tasks / Subtasks

- [x] Add backend database dependencies and runtime configuration. (AC: 1, 2)
  - [x] Add `SQLAlchemy` and `Alembic` to `apps/api/pyproject.toml`.
  - [x] Add backend settings that resolve the SQLite database path in a way that works from the packaged `apps/api/src/stockanalyse_api` layout.
- [x] Establish the SQLAlchemy base and session foundations. (AC: 1, 2)
  - [x] Create a shared declarative base with naming conventions compatible with future migrations.
  - [x] Create a session/engine module that targets the local SQLite database.
- [x] Implement only the schema objects needed for the historical data backbone. (AC: 1)
  - [x] Add `instruments` domain models for the Japan equity universe baseline.
  - [x] Add `market_data_daily` domain models with distinct raw and adjusted price fields.
  - [x] Avoid creating screen, backtest, watchlist, or derived-facts tables in this story.
- [x] Configure Alembic in the backend app boundary. (AC: 2, 3)
  - [x] Add `apps/api/alembic.ini`, migration environment files, and version directory.
  - [x] Wire Alembic metadata loading to the packaged backend source tree.
  - [x] Keep the migration layout aligned with the architecture's `apps/api/migrations` structure.
- [x] Create and validate the initial migration baseline. (AC: 1, 2, 3)
  - [x] Add an initial migration that creates only `instruments` and `market_data_daily`.
  - [x] Apply the migration locally with Alembic.
  - [x] Verify the SQLite database contains the expected baseline tables after migration.

## Dev Notes

- This story owns the first real persistence layer. It should stay limited to the minimum schema needed by the historical data backbone. It is not the place to pre-create future tables just because they are known to be coming later. [Source: _bmad-output/planning-artifacts/epics.md:243-260]
- The MVP system-of-record database starts with SQLite, but the schema and migration path must remain compatible with later PostgreSQL migration. [Source: _bmad-output/planning-artifacts/architecture.md:215-258]
- Database and API naming must stay `snake_case`. [Source: _bmad-output/planning-artifacts/architecture.md:393-405]

### Technical Requirements

- Use SQLite as the initial system-of-record database. [Source: _bmad-output/planning-artifacts/architecture.md:215-242]
- Support schema migration tooling from the start, even in SQLite-backed MVP development. [Source: _bmad-output/planning-artifacts/architecture.md:257-258]
- Scope the initial schema to instruments and daily market data only. Do not create future tables early. [Source: _bmad-output/planning-artifacts/epics.md:249-260]
- Preserve separate raw and adjusted price concepts in storage. [Source: _bmad-output/planning-artifacts/epics.md:120]

### Architecture Compliance

- Keep migration files under `apps/api/migrations` and DB lifecycle code under `apps/api/src/stockanalyse_api/db`. [Source: _bmad-output/planning-artifacts/architecture.md:565-669]
- Keep the backend organized by domain modules, so the first concrete tables land under `domain/instruments` and `domain/market_data`. [Source: _bmad-output/planning-artifacts/architecture.md:578-594,657-668]
- Keep broker, watchlist, screen, and backtest persistence out of this story. [Source: _bmad-output/planning-artifacts/architecture.md:233-235; _bmad-output/planning-artifacts/epics.md:243-260]

### Library / Framework Requirements

- Use `SQLAlchemy 2.0.48` for ORM and metadata management. Current SQLAlchemy docs identify `2.0.48` as the current 2.0 release. [Source: https://docs.sqlalchemy.org/20/]
- Use `Alembic 1.18.4` for migrations. Current Alembic docs identify `1.18.4` as the current release. [Source: https://alembic.sqlalchemy.org/en/latest/]
- For SQLite migrations, Alembic supports batch operations and SQLite-specific migration handling; keeping `render_as_batch` enabled in the migration environment is the safe baseline for future schema evolution. [Source: https://alembic.sqlalchemy.org/en/latest/]

### File Structure Requirements

- Files added or updated by this story should stay within:
  - `apps/api/pyproject.toml`
  - `apps/api/alembic.ini`
  - `apps/api/migrations/**`
  - `apps/api/src/stockanalyse_api/config/**`
  - `apps/api/src/stockanalyse_api/db/**`
  - `apps/api/src/stockanalyse_api/domain/instruments/**`
  - `apps/api/src/stockanalyse_api/domain/market_data/**`
- Local SQLite data is allowed under the repo `data/` directory and should remain ignored by git.

### Testing Requirements

- Validate migrations by actually applying them, not just by writing files.
- Minimum verification for this story:
  - Alembic can run `upgrade head`
  - SQLite DB is created
  - `instruments` and `market_data_daily` tables exist
- Full application tests are not required yet; this story only needs migration-level proof.

### Previous Story Intelligence

- Story 1.1 established the monorepo shell and confirmed `apps/web` and `apps/api` are the only foundations to build on.
- Root package management was normalized to npm workspaces on this machine; do not reintroduce assumptions that `pnpm` is available locally.
- Legacy top-level `src/` and `tests/` prototype files were removed after Story 1.1 review, so new persistence work should target only the new backend tree.

### Latest Tech Information

- SQLAlchemy 2.0 docs currently list `2.0.48` as the current release. [Source: https://docs.sqlalchemy.org/20/]
- Alembic docs currently list `1.18.4` as the current release. [Source: https://alembic.sqlalchemy.org/en/latest/]

### References

- Epic story definition: [epics.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/epics.md:243)
- Architecture database decisions: [architecture.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/architecture.md:215)
- Architecture project structure: [architecture.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/architecture.md:565)
- Previous story: [1-1-initialize-monorepo-application-shells.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/implementation-artifacts/1-1-initialize-monorepo-application-shells.md)

## Dev Agent Record

### Agent Model Used

GPT-5.4

### Debug Log References

- Installed `SQLAlchemy==2.0.48` and `alembic==1.18.4` into the local `.venv`.
- `../../.venv/bin/python -m alembic -c alembic.ini upgrade head` succeeded from `apps/api`.
- Verified resulting SQLite schema with `sqlite3` inspection through Python using `PYTHONPATH=src`.

### Completion Notes List

- Added the backend migration toolchain and pinned persistence dependencies.
- Created SQLAlchemy base/session foundations and backend settings for the local SQLite path.
- Added `Instrument` and `MarketDataDaily` models only; deferred all other tables.
- Added Alembic environment files and an initial baseline migration.
- Applied the migration successfully and verified the expected tables in `data/stockanalyse.db`.

### File List

- _bmad-output/implementation-artifacts/1-2-establish-database-schema-and-migration-workflow.md
- apps/api/pyproject.toml
- apps/api/alembic.ini
- apps/api/migrations/README
- apps/api/migrations/env.py
- apps/api/migrations/script.py.mako
- apps/api/migrations/versions/20260413_0001_initial_market_data_baseline.py
- apps/api/src/stockanalyse_api/config/settings.py
- apps/api/src/stockanalyse_api/db/base.py
- apps/api/src/stockanalyse_api/db/session.py
- apps/api/src/stockanalyse_api/domain/instruments/__init__.py
- apps/api/src/stockanalyse_api/domain/instruments/models.py
- apps/api/src/stockanalyse_api/domain/market_data/__init__.py
- apps/api/src/stockanalyse_api/domain/market_data/models.py

### Change Log

- 2026-04-13: Implemented Story 1.2 database baseline with SQLite, SQLAlchemy, Alembic, and verified migrations locally.
