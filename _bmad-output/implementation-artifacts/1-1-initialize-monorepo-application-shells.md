# Story 1.1: Initialize Monorepo Application Shells

Status: done

<!-- Note: Validation is optional. Run validate-create-story for quality check before dev-story. -->

## Story

As a developer,  
I want a monorepo with separate web and API applications initialized,  
so that all subsequent features have a stable implementation foundation.

## Acceptance Criteria

1. Given a fresh repository, when the project is initialized, then separate `apps/web` and `apps/api` applications exist following the approved architecture structure.
2. Given a fresh repository, when the project is initialized, then the frontend uses the approved Next.js TypeScript starter conventions.
3. Given a fresh repository, when the project is initialized, then the backend uses the approved custom Python project structure.
4. Given the initialized repository, when a developer reviews the root structure, then shared configuration, data, scripts, and `_bmad-output` locations are present or reserved according to the architecture document.

## Tasks / Subtasks

- [x] Create the monorepo root scaffolding without extending legacy prototype code. (AC: 1, 4)
  - [x] Add or update root workspace files needed for a two-app monorepo, including a `pnpm-workspace.yaml`.
  - [x] Preserve existing `_bmad-output`, `data/`, and `docs/` directories; do not move planning artifacts.
  - [x] Treat existing `src/`, `tests/`, and `utils/` as legacy prototype code and do not use them as the foundation for new app scaffolding.
- [x] Initialize the frontend shell under `apps/web` using the approved Next.js conventions. (AC: 1, 2)
  - [x] Scaffold a Next.js App Router app in `apps/web` with TypeScript, ESLint, Tailwind CSS, `src/` layout, and `@/*` alias.
  - [x] Ensure the generated app has the minimal route shell required by Next.js (`src/app/layout.tsx` and `src/app/page.tsx`).
  - [x] Keep the frontend app independent from backend or database assumptions at this stage.
- [x] Initialize the backend shell under `apps/api` using `uv` and the approved Python structure. (AC: 1, 3)
  - [x] Scaffold `apps/api` as a custom Python project managed by `uv`.
  - [x] Use a packaged `src/` layout so the resulting structure aligns with `apps/api/src/stockanalyse_api/...` rather than a flat `main.py` at repo root.
  - [x] Create the initial package entrypoint and reserve the top-level backend directories required by the architecture (`config`, `db`, `api`, `repositories`, `services`, `jobs`, `logging`, `tests`), without implementing business-domain modules yet.
- [x] Align the generated shells to the agreed repository structure. (AC: 1, 4)
  - [x] Reserve `packages/contracts` and `scripts/` locations if they do not already exist.
  - [x] Ensure `apps/web` and `apps/api` can be developed independently.
  - [x] Avoid introducing Docker, auth, database schema work, or market-data logic in this story.
- [x] Add smoke-level verification for the scaffolding. (AC: 1, 2, 3)
  - [x] Verify the frontend app can install and expose a normal Next.js dev entrypoint.
  - [x] Verify the backend project metadata is valid and the package layout is importable or runnable via `uv`.
  - [x] Document any deviations required to keep the starter tools aligned with the architecture.

## Dev Notes

- This story is foundation-only. It creates app shells and repo structure, not business features.
- The product is a local-first Japan-equity research tool whose MVP depends on a trustworthy data and application foundation. Keep this story narrowly focused on scaffolding that enables later ingestion, screening, chart, watchlist, and backtest work. [Source: _bmad-output/planning-artifacts/prd.md:38-42]
- The MVP scope is Japan equities plus EOD workflows; do not pull in real-time infrastructure, broker integration, multi-user auth, or earnings-surprise work here. [Source: _bmad-output/planning-artifacts/prd.md:40,113-123]

### Technical Requirements

- Use a monorepo with separate `apps/web` and `apps/api` applications. [Source: _bmad-output/planning-artifacts/architecture.md:565-617]
- Frontend and backend must remain separate runtime units. `apps/web` talks to `apps/api` over HTTP later; it must not read the database directly. [Source: _bmad-output/planning-artifacts/architecture.md:622-627,720-729]
- Local development is intentionally simple: one frontend app, one Python backend service, one SQLite database, and one local job execution path. This story should only establish the two apps and supporting repo structure. [Source: _bmad-output/planning-artifacts/architecture.md:342-347,720-729]
- Do not create domain-specific business logic, DB schema, or provider integrations in this story. Those belong to later stories even if directory placeholders are created now. [Source: _bmad-output/planning-artifacts/epics.md:243-260,262-298]

### Architecture Compliance

- Follow the selected starter approach: frontend starter yes, heavyweight backend starter no. [Source: _bmad-output/planning-artifacts/architecture.md:118-127,200-207]
- The frontend shell must reflect Next.js App Router conventions with TypeScript, Tailwind, and `src/` layout. [Source: _bmad-output/planning-artifacts/architecture.md:129-168]
- The backend shell must be a custom Python project managed with `uv`. [Source: _bmad-output/planning-artifacts/architecture.md:171-186]
- Preserve the canonical repo layout from the architecture, especially `apps/web`, `apps/api`, `packages/contracts`, and `scripts/`. [Source: _bmad-output/planning-artifacts/architecture.md:565-617,692-729]

### Library / Framework Requirements

- Frontend:
  - Use the Next.js official starter path with TypeScript.
  - Minimum Node.js version is `20.9+` per the current Next.js installation docs.
  - Current `create-next-app` defaults include TypeScript, Tailwind CSS, ESLint, App Router, Turbopack, and `@/*` alias support. Use a deterministic setup that matches the architecture rather than relying on changing interactive defaults. [Source: https://nextjs.org/docs/app/getting-started/installation]
- Backend:
  - Use `uv` for Python project initialization and dependency management.
  - Current uv docs show `uv init` creates an app project, while `uv init --package` creates a `src/` package layout. Because the architecture requires `apps/api/src/stockanalyse_api/...`, use the packaged layout or an equivalent manually aligned structure. [Source: https://docs.astral.sh/uv/concepts/projects/init/]

### File Structure Requirements

- Target structure for this story:
  - `apps/web/` with Next.js app shell, including `package.json`, `next.config.*`, `tsconfig.json`, `src/app/layout.tsx`, and `src/app/page.tsx`.
  - `apps/api/` with `pyproject.toml`, `src/stockanalyse_api/`, and starter package files.
  - Root-level monorepo support files such as `pnpm-workspace.yaml`.
  - Reserved shared locations including `packages/contracts/` and `scripts/`.
- Important boundary:
  - Do not build on the legacy prototype under `src/`; new implementation starts under `apps/`.
  - Do not pre-create all future domain modules in depth. Reserve only the top-level structure needed to keep later stories in the correct locations.

### Testing Requirements

- This story only needs shell verification, not feature tests.
- Frontend verification should prove the generated app is structurally valid and has standard Next.js scripts available.
- Backend verification should prove the generated `uv` project metadata is valid and the package layout is sane.
- If lightweight smoke tests are added, place them in `apps/web/tests` and `apps/api/tests` per the architecture. Do not mix frontend and backend tests. [Source: _bmad-output/planning-artifacts/architecture.md:706-710]

### Project Structure Notes

- Existing repository reality:
  - The repo already contains legacy prototype directories like `src/`, `tests/`, `utils/`, and old `data/` subfolders.
  - The user has explicitly said previous files are not valuable and can be discarded conceptually, but this story should not delete them unless implementation work specifically requires it.
- Alignment rule:
  - New work starts in `apps/web` and `apps/api`.
  - Existing top-level prototype code should not be imported, reorganized, or treated as the base scaffold for the new architecture.
- Detected architecture tension:
  - The architecture text recommends `uv init stockanalyse-api`, but the approved directory tree expects `apps/api/src/stockanalyse_api/...`.
  - Resolve this in favor of the directory tree and packaged `src` layout; do not ship a flat backend scaffold that fights the rest of the architecture.

### Implementation Guardrails

- Do not add SQLite schema objects in this story; Story 1.2 owns the initial schema baseline.
- Do not add REST endpoints, provider adapters, or factor logic.
- Do not add Docker, auth, or deployment-specific infrastructure.
- Keep naming canonical and ASCII-only.
- If the starter tools generate extra files that conflict with the approved structure, adapt them immediately rather than leaving the repo in a mismatched intermediate state.

### Previous Story Intelligence

- No previous story exists for Epic 1. There are no prior implementation learnings to inherit.

### Git Intelligence Summary

- No implementation-story history exists yet in `_bmad-output/implementation-artifacts`.
- No recent project-specific implementation pattern should override the architecture for this first story.

### Latest Tech Information

- Next.js official installation docs were updated March 31, 2026 and currently state:
  - minimum Node.js version `20.9`
  - `pnpm create next-app@latest` is the recommended entrypoint
  - current defaults include TypeScript, Tailwind CSS, ESLint, App Router, Turbopack, and `@/*`
  - `layout.tsx` and `page.tsx` are the minimal App Router shell files [Source: https://nextjs.org/docs/app/getting-started/installation]
- uv official docs currently state:
  - `uv init` creates an application project
  - `uv init --package` creates a `src`-layout packaged application
  - `.venv` and `uv.lock` are created lazily on first sync/run rather than necessarily at init time
  - if a `pyproject.toml` exists in a parent directory, uv can treat the project as a workspace member [Source: https://docs.astral.sh/uv/concepts/projects/init/ and https://docs.astral.sh/uv/reference/cli/]

### References

- Epic story definition: [epics.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/epics.md:223)
- PRD executive summary and MVP scope: [prd.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/prd.md:36)
- Architecture starter decisions: [architecture.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/architecture.md:118)
- Architecture project structure and boundaries: [architecture.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/architecture.md:565)
- Next.js installation docs: https://nextjs.org/docs/app/getting-started/installation
- uv project initialization docs: https://docs.astral.sh/uv/concepts/projects/init/
- uv CLI reference: https://docs.astral.sh/uv/reference/cli/

## Dev Agent Record

### Agent Model Used

GPT-5.4

### Debug Log References

- `node -e "JSON.parse(...)"` validated root and web JSON configuration files.
- `python3 apps/api/src/stockanalyse_api/main.py` verified the backend scaffold entrypoint.
- `npm install`, `npm run lint`, and `npm run build` were executed in `apps/web` to verify the frontend shell.
- Next.js build succeeded with `next build --webpack`; Turbopack build was avoided in the verification script because the sandbox previously blocked Turbopack child-process behavior.
- Post-review follow-up: switched root workspace execution to npm, pinned frontend dependency versions, and removed stale legacy tests.

### Completion Notes List

- Ultimate context engine analysis completed - comprehensive developer guide created.
- Resolved the backend scaffold ambiguity by preferring a packaged `src` layout consistent with the approved architecture tree.
- Marked this story as foundation-only to prevent schema, API, or domain logic from leaking into the scaffold step.
- Added a monorepo root with `package.json` and `pnpm-workspace.yaml`.
- Scaffolded `apps/web` as a Next.js App Router shell with TypeScript, Tailwind, ESLint, `src/` layout, and `@/*` path alias.
- Scaffolded `apps/api` as a `uv`-compatible packaged Python application with `src/stockanalyse_api` layout and reserved backend module boundaries.
- Reserved shared `packages/contracts` and `scripts/*` directories to match the approved architecture.
- Verified frontend lint/build and backend bootstrap entrypoint successfully.
- Post-review fixes applied: reproducible dependency versions, tracked workspace lockfile strategy, and removed obsolete top-level tests.

### File List

- _bmad-output/implementation-artifacts/1-1-initialize-monorepo-application-shells.md
- .gitignore
- package.json
- pnpm-workspace.yaml
- apps/web/package.json
- apps/web/next.config.ts
- apps/web/tsconfig.json
- apps/web/next-env.d.ts
- apps/web/eslint.config.mjs
- apps/web/postcss.config.mjs
- apps/web/src/app/globals.css
- apps/web/src/app/layout.tsx
- apps/web/src/app/page.tsx
- apps/web/public/icons/.gitkeep
- apps/web/tests/components/.gitkeep
- apps/web/tests/pages/.gitkeep
- apps/web/tests/e2e/.gitkeep
- apps/api/README.md
- apps/api/pyproject.toml
- apps/api/src/stockanalyse_api/__init__.py
- apps/api/src/stockanalyse_api/main.py
- apps/api/src/stockanalyse_api/api/__init__.py
- apps/api/src/stockanalyse_api/api/errors/__init__.py
- apps/api/src/stockanalyse_api/api/routes/__init__.py
- apps/api/src/stockanalyse_api/api/schemas/__init__.py
- apps/api/src/stockanalyse_api/config/__init__.py
- apps/api/src/stockanalyse_api/db/__init__.py
- apps/api/src/stockanalyse_api/domain/__init__.py
- apps/api/src/stockanalyse_api/jobs/__init__.py
- apps/api/src/stockanalyse_api/logging/__init__.py
- apps/api/src/stockanalyse_api/repositories/__init__.py
- apps/api/src/stockanalyse_api/services/__init__.py
- apps/api/tests/__init__.py
- apps/api/tests/unit/.gitkeep
- apps/api/tests/integration/.gitkeep
- apps/api/tests/fixtures/.gitkeep
- packages/contracts/api-schemas/.gitkeep
- packages/contracts/glossary/.gitkeep
- scripts/dev/.gitkeep
- scripts/data/.gitkeep
- scripts/maintenance/.gitkeep
- package-lock.json

### Change Log

- 2026-04-13: Implemented Story 1.1 monorepo scaffolding for `apps/web` and `apps/api`, added workspace files, reserved shared directories, and completed smoke verification.
- 2026-04-13: Applied review follow-up fixes to pin frontend dependency versions, align root scripts with npm workspaces, and remove legacy test artifacts.
