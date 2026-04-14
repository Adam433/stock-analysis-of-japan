# 故事 4.2: Persist Watchlist Notes, Observation Reasons, and Added Dates

状态: done

## 用户故事

作为用户，  
I want each watchlist entry to include my research context,  
以便I can remember why the stock matters later。

## 验收标准

1. 假设a watchlist entry，当the user saves a note and observation reason，那么the system persists those fields with the watchlist entry。
2. 假设a watchlist entry is created，当it is stored，那么the system preserves the date the stock was added to the watchlist。

## 任务 / 子任务

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

## 开发备注

- Story 4.2 should build directly on the watchlist entry model introduced in Story 4.1 instead of introducing a separate research-notes record. Watchlist context remains part of the entry itself in the MVP. [Source: _bmad-output/planning-artifacts/architecture.md:650-661]
- The PRD explicitly frames the watchlist as a lightweight research notebook, so notes and observation reason need to be editable within the same analysis workflow, not only from a future dedicated watchlist page. [Source: _bmad-output/planning-artifacts/prd.md:151,276,282]
- Story 4.3 will add the dedicated review surface. This story should focus on persistence and in-flow editing, not a standalone watchlist view. [Source: _bmad-output/planning-artifacts/epics.md:463-490]

## 开发代理记录

### 使用的代理模型

GPT-5.4

### 调试日志参考

- Extended `watchlist_entries` with note, observation reason, and preserved added date.
- Added watchlist update API semantics and returned richer watchlist entry payloads.
- Upgraded the inline watchlist control to edit and save research context from both the result list and stock detail workflow.
- Verified with `PYTHONPATH=src python3 -m unittest tests.test_watchlist tests.test_screening tests.test_chart_data`.
- Verified with `PYTHONPATH=src python3 -m compileall src`.
- Verified with `PYTHONPATH=src python3 -m alembic -c alembic.ini upgrade head`.
- Verified with `npm run lint` and `npm run build`.

### 完成说明

- Watchlist entries now persist note, observation reason, and an explicit added date.
- Research context can be edited inline from the existing screen and stock-detail workflow.
- The implementation leaves Story 4.3 with a clear next step: building the standalone watchlist review view on top of already persisted context.

### 文件清单

- _bmad-output/implementation-artifacts/4-2-persist-watchlist-notes-observation-reasons-and-added-dates.md
- apps/api/migrations/versions/20260414_0008_add_watchlist_context_fields.py
- apps/api/src/stockanalyse_api/api/routes/watchlist.py
- apps/api/src/stockanalyse_api/domain/watchlists/models.py
- apps/api/src/stockanalyse_api/services/watchlist.py
- apps/api/tests/test_watchlist.py
- apps/web/src/app/globals.css
- apps/web/src/components/watchlist/WatchlistToggleButton.tsx

### 变更日志

- 2026-04-14: Added watchlist note, observation reason, and added-date persistence with inline editing.
