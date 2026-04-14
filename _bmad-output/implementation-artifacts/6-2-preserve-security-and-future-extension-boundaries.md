# 故事 6.2: Preserve Security and Future Extension Boundaries

状态: done

## 用户故事

As a developer,  
I want MVP research boundaries and extension boundaries enforced,  
以便future broker or market expansion work does not compromise current correctness。

## 验收标准

1. 假设the MVP implementation，当provider credentials are configured，那么they are handled only on the backend and never exposed to the browser。
2. 假设future post-MVP concerns such as broker integration or non-Japan market expansion，当the MVP modules are reviewed，那么research workflows remain structurally separate from those deferred concerns and the MVP architecture still supports future addition of new strategy conditions and markets without invalidating the core workflow structure。

## 任务 / 子任务

- [x] Tighten browser/backend boundary for configuration. (AC: 1)
  - [x] Remove `NEXT_PUBLIC_*` fallback usage from workflow routes so API base configuration remains server-side.
- [x] Make provider boundary rules explicit in backend code. (AC: 1, 2)
  - [x] Add explicit provider metadata for market scope and credential boundary.
  - [x] Add a provider registry that rejects providers outside the MVP backend-only/Japan-only boundary.
  - [x] Route ingestion job provider construction through that registry.
- [x] Record the architectural boundary for future extension work. (AC: 2)
  - [x] Add an ADR documenting MVP research boundaries, backend-only secret handling, and future broker/multi-market separation.
- [x] Validate boundary rules. (AC: 1, 2)
  - [x] Add backend tests covering provider boundary metadata enforcement.
  - [x] Run backend unit tests, compile validation, frontend lint, and frontend build.

## 开发备注

- Provider and future broker credentials must stay server-side only; the browser should never be treated as a credential-bearing integration boundary. [Source: _bmad-output/planning-artifacts/prd.md:477-480; _bmad-output/planning-artifacts/architecture.md:62,278-280]
- MVP scope remains Japan-equity end-of-day research. Future broker and multi-market support must be additive extensions behind separate boundaries, not quiet broadening of current provider contracts. [Source: _bmad-output/planning-artifacts/prd.md:212,347,453-455,496; _bmad-output/planning-artifacts/architecture.md:36,73,86,219,482,804,806,867]
- The implementation should preserve current research reproducibility and domain clarity while making the future extension path explicit enough to avoid accidental coupling. [Source: _bmad-output/planning-artifacts/architecture.md:54,420,627,681-682,850]

## 开发代理记录

### 使用的代理模型

GPT-5.4

### 调试日志参考

- Removed public-env fallback usage from workflow routes so API base configuration remains server-side.
- Added explicit backend-only/Japan-only provider metadata and registry enforcement.
- Added a lightweight ADR documenting MVP research boundaries and future extension constraints.
- Verified with `PYTHONPATH=src python3 -m unittest tests.test_security_boundaries tests.test_backtesting tests.test_watchlist tests.test_screening tests.test_chart_data`.
- Verified with `PYTHONPATH=src python3 -m compileall src`.
- Verified with `npm run lint` and `npm run build`.

### 完成说明

- Provider integration boundaries are now encoded in code rather than only implied by architecture docs.
- Workflow routes no longer depend on `NEXT_PUBLIC_*` API base configuration, reducing the chance of bleeding privileged integration assumptions into the browser layer.
- The MVP now has an explicit ADR for future broker and multi-market isolation, which gives later expansion work a concrete boundary to preserve.

### 文件清单

- _bmad-output/implementation-artifacts/6-2-preserve-security-and-future-extension-boundaries.md
- apps/api/src/stockanalyse_api/jobs/refresh_market_data.py
- apps/api/src/stockanalyse_api/services/ingestion/providers/__init__.py
- apps/api/src/stockanalyse_api/services/ingestion/providers/base.py
- apps/api/src/stockanalyse_api/services/ingestion/providers/registry.py
- apps/api/src/stockanalyse_api/services/ingestion/providers/static_provider.py
- apps/api/tests/test_security_boundaries.py
- apps/web/src/app/backtests/page.tsx
- apps/web/src/app/screen/page.tsx
- apps/web/src/app/stocks/[instrumentId]/page.tsx
- apps/web/src/app/watchlist/page.tsx
- docs/adr/0001-mvp-research-boundaries.md

### 变更日志

- 2026-04-14: Added explicit provider/security boundaries and documented MVP extension constraints.
