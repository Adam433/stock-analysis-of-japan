# Story 4.2: Persist Watchlist Notes, Observation Reasons, and Added Dates

Status: done

## Story

As a user,  
I want each watchlist entry to include my research context,  
so that I can remember why the stock matters later.

## Acceptance Criteria

1. Given a watchlist entry, when the user saves a note and observation reason, then the system persists those fields with the watchlist entry.
2. Given a watchlist entry is created, when it is stored, then the system preserves the date the stock was added to the watchlist.

## Tasks / Subtasks

- [x] Extend watchlist persistence with research-context fields. (AC: 1, 2)
  - [x] Add `note`, `observation_reason`, and `added_date` to `watchlist_entries`.
  - [x] Preserve `added_date` at creation time and keep it stable through later context edits.
- [x] Extend the watchlist API for context save/update. (AC: 1, 2)
  - [x] Accept note and observation reason during watchlist creation.
  - [x] Add update semantics for existing watchlist entries without removing and recreating them.
  - [x] Return context fields from the watchlist list/read surfaces so the UI can prefill them.
- [x] Add UI to save and review watchlist context inside the existing workflow. (AC: 1, 2)
  - [x] Upgrade the watchlist control used in screen results and stock detail to edit note and observation reason.
  - [x] Show the preserved added date within the inline watchlist context editor.
- [x] Validate persistence and frontend integration. (AC: 1, 2)
  - [x] Extend backend tests for note/reason persistence and stable entry updates.
  - [x] Run backend unit tests, compile validation, Alembic upgrade, frontend lint, and frontend build.

## Dev Notes

- Story 4.2 should build directly on the watchlist entry model introduced in Story 4.1 instead of introducing a separate research-notes record. Watchlist context remains part of the entry itself in the MVP. [Source: _bmad-output/planning-artifacts/architecture.md:650-661]
- The PRD explicitly frames the watchlist as a lightweight research notebook, so notes and observation reason need to be editable within the same analysis workflow, not only from a future dedicated watchlist page. [Source: _bmad-output/planning-artifacts/prd.md:151,276,282]
- Story 4.3 will add the dedicated review surface. This story should focus on persistence and in-flow editing, not a standalone watchlist view. [Source: _bmad-output/planning-artifacts/epics.md:463-490]

## Dev Agent Record

### Agent Model Used

GPT-5.4

### Debug Log References

- Extended `watchlist_entries` with note, observation reason, and preserved added date.
- Added watchlist update API semantics and returned richer watchlist entry payloads.
- Upgraded the inline watchlist control to edit and save research context from both the result list and stock detail workflow.
- Verified with `PYTHONPATH=src python3 -m unittest tests.test_watchlist tests.test_screening tests.test_chart_data`.
- Verified with `PYTHONPATH=src python3 -m compileall src`.
- Verified with `PYTHONPATH=src python3 -m alembic -c alembic.ini upgrade head`.
- Verified with `npm run lint` and `npm run build`.

### Completion Notes List

- Watchlist entries now persist note, observation reason, and an explicit added date.
- Research context can be edited inline from the existing screen and stock-detail workflow.
- The implementation leaves Story 4.3 with a clear next step: building the standalone watchlist review view on top of already persisted context.

### File List

- _bmad-output/implementation-artifacts/4-2-persist-watchlist-notes-observation-reasons-and-added-dates.md
- apps/api/migrations/versions/20260414_0008_add_watchlist_context_fields.py
- apps/api/src/stockanalyse_api/api/routes/watchlist.py
- apps/api/src/stockanalyse_api/domain/watchlists/models.py
- apps/api/src/stockanalyse_api/services/watchlist.py
- apps/api/tests/test_watchlist.py
- apps/web/src/app/globals.css
- apps/web/src/components/watchlist/WatchlistToggleButton.tsx

### Change Log

- 2026-04-14: Added watchlist note, observation reason, and added-date persistence with inline editing.
