# Story 4.1: Add and Remove Qualified Stocks from the Watchlist

Status: done

## Story

As a user,  
I want to add or remove qualified stocks from a watchlist,  
so that I can maintain a focused list of candidates worth monitoring.

## Acceptance Criteria

1. Given a screened or reviewed stock, when the user adds it to the watchlist, then the stock is stored as a watchlist entry linked to the canonical instrument identity.
2. Given a stock is already in the watchlist, when the user removes it, then the watchlist no longer includes that entry.

## Tasks / Subtasks

- [x] Add watchlist persistence and API endpoints. (AC: 1, 2)
  - [x] Create a `watchlist_entries` table linked to canonical `instrument_id`.
  - [x] Add idempotent add/remove services and REST endpoints.
  - [x] Expose a list endpoint so UI surfaces can resolve watchlist state.
- [x] Add watchlist actions to screening and review workflows. (AC: 1, 2)
  - [x] Add watchlist toggle actions to qualified result cards on `/screen`.
  - [x] Add watchlist toggle action to the stock detail page.
- [x] Validate persistence and frontend integration. (AC: 1, 2)
  - [x] Add backend tests for add, remove, and idempotent re-add behavior.
  - [x] Run backend unit tests, compile validation, Alembic upgrade, frontend lint, and frontend build.

## Dev Notes

- Epic 4 depends on the canonical instrument identity and the existing screen/detail workflows from Epics 2 and 3. Watchlist persistence should remain a separate domain instead of being bolted into screen results or stock detail records. [Source: _bmad-output/planning-artifacts/architecture.md:379,420,650-661]
- Story 4.1 is intentionally limited to add/remove behavior. Notes, observation reasons, and explicit added-date display belong to Stories 4.2 and 4.3. [Source: _bmad-output/planning-artifacts/epics.md:445-476]
- The PRD expects watchlist actions to behave like core research workflow actions, so both the result-list path and the reviewed-stock path should support add/remove without leaving the current flow. [Source: _bmad-output/planning-artifacts/prd.md:151,236,261-282]

## Dev Agent Record

### Agent Model Used

GPT-5.4

### Debug Log References

- Added a watchlist domain, migration, service layer, and `/watchlist` API routes.
- Connected watchlist add/remove actions to the qualified result list and stock detail page.
- Verified with `PYTHONPATH=src python3 -m unittest tests.test_watchlist tests.test_screening tests.test_chart_data`.
- Verified with `PYTHONPATH=src python3 -m compileall src`.
- Verified with `PYTHONPATH=src python3 -m alembic -c alembic.ini upgrade head`.
- Verified with `npm run lint` and `npm run build`.

### Completion Notes List

- Qualified stocks can now be added to or removed from a persisted watchlist from both screening and reviewed-stock workflows.
- Watchlist entries are stored against canonical instruments and add behavior is idempotent.
- The implementation lays the persistence and UI foundation for Story 4.2 to add notes, observation reasons, and explicit added-date handling.

### File List

- _bmad-output/implementation-artifacts/4-1-add-and-remove-qualified-stocks-from-the-watchlist.md
- apps/api/migrations/env.py
- apps/api/migrations/versions/20260414_0007_add_watchlist_entries.py
- apps/api/src/stockanalyse_api/api/routes/__init__.py
- apps/api/src/stockanalyse_api/api/routes/watchlist.py
- apps/api/src/stockanalyse_api/domain/watchlists/__init__.py
- apps/api/src/stockanalyse_api/domain/watchlists/models.py
- apps/api/src/stockanalyse_api/main.py
- apps/api/src/stockanalyse_api/services/watchlist.py
- apps/api/tests/test_watchlist.py
- apps/web/src/app/globals.css
- apps/web/src/app/stocks/[instrumentId]/page.tsx
- apps/web/src/components/screen/StrategyConfigPanel.tsx
- apps/web/src/components/stocks/StockDetailView.tsx
- apps/web/src/components/watchlist/WatchlistToggleButton.tsx

### Change Log

- 2026-04-14: Added persisted watchlist add/remove workflow across the result list and stock detail page.
