# Story 2.1: Create Strategy Configuration Workflow

Status: done

## Story

As a user,  
I want to define and edit MVP strategy parameters,  
so that I can control the screen logic without changing code.

## Acceptance Criteria

1. Given the screen configuration view, when the user enters an RPS threshold and a 52-week-high proximity threshold, then the system accepts and validates the parameter values for a screen run.
2. Given a saved or current parameter set, when the user updates the values, then the new values are preserved for the next screen run.

## Tasks / Subtasks

- [x] Add persisted strategy-configuration storage. (AC: 2)
  - [x] Create a `strategy_configurations` table under the screening domain.
  - [x] Preserve parameter-set history by versioning new active saves instead of mutating the old row in place.
- [x] Add backend strategy-configuration services and routes. (AC: 1, 2)
  - [x] Implement a default active parameter set when none exists yet.
  - [x] Add `GET /screen/configuration` and `PUT /screen/configuration`.
  - [x] Validate RPS and 52-week-high threshold values at the API boundary.
- [x] Build the screen configuration workflow in the web app. (AC: 1, 2)
  - [x] Add a dedicated `/screen` page for the configuration workflow.
  - [x] Display the active parameter-set version and editable threshold fields.
  - [x] Surface save success and validation errors in the UI.
- [x] Validate the implementation. (AC: 1, 2)
  - [x] Add backend regression tests for default config creation, versioned saves, and invalid values.
  - [x] Run backend tests, frontend lint, frontend build, and migration upgrade validation.

## Dev Notes

- Epic 2 begins the screening domain, but this story should stop at parameter configuration and persistence. It must not implement derived facts or universe-wide screen execution yet. [Source: _bmad-output/planning-artifacts/epics.md:301-353]
- Architecture maps strategy configuration to `apps/web/src/app/screen` and `apps/api/src/stockanalyse_api/domain/screens`. Keep that boundary clean for later stories. [Source: _bmad-output/planning-artifacts/architecture.md:643-669]
- Later screen and backtest runs need parameter traceability, so versioned persistence is a safer baseline than a singleton mutable settings record. [Source: _bmad-output/planning-artifacts/prd.md:163-165,382-384,440-441]

## Dev Agent Record

### Agent Model Used

GPT-5.4

### Debug Log References

- Added `strategy_configurations` persistence and migration `20260414_0004`.
- Added strategy configuration services plus `GET/PUT /screen/configuration`.
- Added CORS for local web-to-API interaction in development.
- Added `/screen` in the Next.js app with editable thresholds and version visibility.
- Verified with `PYTHONPATH=src python3 -m unittest tests.test_ingestion_review_regressions tests.test_market_data_health tests.test_strategy_configuration`.
- Verified with `PYTHONPATH=src python3 -m compileall src`, `PYTHONPATH=src python3 -m alembic -c alembic.ini upgrade head`, `npm run lint`, and `npm run build`.
- Follow-up implementation review tightened the `/screen` error-state semantics so API connectivity issues are not shown as an existing active configuration.

### Completion Notes List

- Strategy parameters are now persisted server-side with version history and an active configuration concept.
- The web app exposes a dedicated screen configuration workflow instead of relying on static placeholders.
- Validation now exists both client-side and server-side for the two MVP thresholds.
- The active parameter set is ready to be consumed by later screen-run and backtest stories.

### File List

- _bmad-output/implementation-artifacts/2-1-create-strategy-configuration-workflow.md
- apps/api/migrations/env.py
- apps/api/migrations/versions/20260414_0004_add_strategy_configurations.py
- apps/api/src/stockanalyse_api/api/routes/__init__.py
- apps/api/src/stockanalyse_api/api/routes/strategy_config.py
- apps/api/src/stockanalyse_api/domain/screens/__init__.py
- apps/api/src/stockanalyse_api/domain/screens/models.py
- apps/api/src/stockanalyse_api/main.py
- apps/api/src/stockanalyse_api/services/strategy_config.py
- apps/api/tests/test_strategy_configuration.py
- apps/web/src/app/page.tsx
- apps/web/src/app/screen/page.tsx
- apps/web/src/app/globals.css
- apps/web/src/components/screen/StrategyConfigPanel.tsx

### Change Log

- 2026-04-14: Implemented versioned strategy configuration persistence, backend API routes, and the initial `/screen` workflow.
- 2026-04-14: Closed the implementation with a quick review pass and corrected the screen configuration error-state presentation.
