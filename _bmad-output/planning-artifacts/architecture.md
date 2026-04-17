---
stepsCompleted:
  - 1
  - 2
  - 3
  - 4
  - 5
  - 6
  - 7
  - 8
inputDocuments:
  - /Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/prd.md
  - /Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/research/technical-stock-backtesting-jp-us-research-2026-04-13.md
workflowType: 'architecture'
project_name: 'stockAnalyse'
user_name: 'Adam'
date: '2026-04-13'
lastStep: 8
status: 'complete'
completedAt: '2026-04-13'
---

# Architecture Decision Document

_This document builds collaboratively through step-by-step discovery. Sections are appended as we work through each architectural decision together._

## Project Context Analysis

### Requirements Overview

**Functional Requirements:**
The product defines 54 functional requirements across strategy configuration, market data and universe management, indicator evaluation, screening results, chart review, watchlist management, backtesting, data health visibility, and future extension boundaries. Architecturally, this is not a simple charting app or screener. It requires a coordinated flow from historical data ingestion to derived-factor computation, screen execution, explainable result serving, and reproducible backtesting.

The MVP scope is intentionally narrow but structurally demanding:

- Japan equities only
- end-of-day data only
- two core strategy conditions in MVP
- browser-based research workflow
- watchlist state with contextual notes
- reproducible screen and backtest runs

This implies at least these architectural capability groups:

- data ingestion and normalization
- factor computation and materialization
- screening and run tracking
- chart and stock-detail serving
- watchlist persistence
- backtest execution and result persistence
- operational status and data freshness visibility

**Non-Functional Requirements:**
The strongest architectural drivers are reproducibility, data integrity, explainability, and operational trustworthiness. Performance matters at interactive-research scale, but low-latency real-time architecture is not required. Security is relevant mainly around provider credentials and future broker isolation, not customer payments or multi-tenant enterprise controls.

The most architecture-shaping NFRs are:

- identical inputs must yield identical backtest outputs
- stale or partial data must be surfaced, not hidden
- screening, charting, and backtesting must remain aligned to the same stored dataset
- result qualification must remain traceable to stored values and thresholds
- browser clients must not directly hold privileged provider credentials

**Scale & Complexity:**
This is a high-complexity product despite the narrow MVP scope because it combines data engineering, financial time-series logic, explainability requirements, and browser workflows in one system. The complexity is driven more by correctness and consistency than by user volume or infrastructure scale.

- Primary domain: web-based fintech research platform
- Complexity level: high
- Estimated architectural components: 7-9 major components

### Technical Constraints & Dependencies

- The MVP must support Japan equities only.
- The MVP must use end-of-day historical data only.
- Earnings surprise continuity is explicitly out of MVP and should not drive the first architecture unnecessarily.
- Broker integration is out of MVP and must remain isolated from research workflows later.
- Historical market data must be ingested from external providers and normalized locally before use in screening and backtesting.
- A single market-data normalization policy must be used across charts, screen logic, and backtests.
- The product must expose data freshness and failed-update states clearly enough for user trust.

### Cross-Cutting Concerns Identified

- Data normalization consistency
- Reproducibility of screen and backtest runs
- Explainability of qualified results
- Separation of research workflows from future broker workflows
- Operational observability for data freshness, failed runs, and incomplete updates
- Browser experience for chart-heavy research flows

## Starter Template Evaluation

### Primary Technology Domain

Full-stack web application with a Python analytics and backend core plus a TypeScript browser frontend.

### Starter Options Considered

**Option 1: Next.js official starter for the frontend plus custom Python backend**

- Strong fit for the interactive web UI
- Keeps the frontend foundation modern and well-supported
- Avoids forcing the backend into a heavyweight full-stack template
- Best fit for a local-first MVP with a custom data and analysis core

**Option 2: Vite React frontend plus custom Python backend**

- Also viable
- Simpler frontend foundation than Next.js
- Less opinionated for routing and app structure
- Reasonable alternative, but less compelling than Next.js for a richer application shell

**Option 3: FastAPI full-stack template**

- Includes FastAPI, React, SQLModel, PostgreSQL, Docker, auth, email, testing, and deployment defaults
- Too heavyweight for this MVP because it makes decisions around authentication, deployment, and infrastructure that are not currently required
- Better suited to a broader SaaS-style application than a local-first, single-user research tool

### Selected Starter: Next.js Official Starter for Frontend Only

**Rationale for Selection:**
The project benefits from a strong frontend starter but not from a heavy backend starter. The frontend needs a reliable TypeScript application shell for parameter editing, result navigation, chart pages, and watchlist workflows. The backend needs flexibility more than scaffolding because the hard part of this system is custom market-data ingestion, factor computation, screen execution, and reproducible backtesting.

The recommended approach is:

- use Next.js as the frontend starter
- build the Python backend as a custom service
- keep the backend and data model architecture driven by product requirements rather than by a generic full-stack template

**Initialization Command:**

```bash
pnpm create next-app@latest stockanalyse-web --ts --eslint --tailwind --app --src-dir --import-alias "@/*"
```

**Architectural Decisions Provided by Starter:**

**Language & Runtime:**

- TypeScript
- React
- Next.js App Router
- Node.js 20.9+ runtime requirement from current Next.js docs

**Styling Solution:**

- Tailwind CSS from the official starter path

**Build Tooling:**

- create-next-app
- Next.js build pipeline
- modern browser support aligned with current Next.js defaults

**Testing Framework:**

- not forced by the starter and can be added deliberately later

**Code Organization:**

- `src/`-based frontend application layout
- route-oriented structure via App Router
- alias support with `@/*`

**Development Experience:**

- modern local development workflow
- strong TypeScript defaults
- established React and Next.js conventions
- good fit for a chart-heavy browser application shell

### Backend Foundation Decision

The backend should start as a custom Python application instead of a heavyweight starter template.

**Recommended backend initialization path:**

```bash
uv init stockanalyse-api
```

**Rationale:**

- the project's complexity sits in custom domain logic, not generic CRUD scaffolding
- the MVP does not require auth, email, or multi-tenant admin features
- uv is current, well-maintained, and suitable for fast local-first Python project setup
- a custom FastAPI-based backend can be introduced cleanly without inheriting unnecessary product assumptions

### Database Foundation Decision

The initial architecture should prefer a simple local relational database for MVP development.

**Recommended default:**

- SQLite for MVP local development

**Planned migration path:**

- preserve a later transition path to PostgreSQL if the product expands into multi-user or server-hosted operation

### Starter Decision Summary

- Frontend starter: **Yes**
- Selected frontend starter: **Next.js official starter**
- Backend starter: **No heavyweight starter**
- Backend foundation: **Custom Python project initialized with uv**
- Database default: **SQLite**
- Docker: **Optional later, not required for MVP initialization**

## Core Architectural Decisions

### Decision Priority Analysis

**Critical Decisions (Block Implementation):**

- Use SQLite as the MVP system-of-record database
- Use a custom Python API service as the backend boundary
- Materialize derived screening facts instead of computing everything on demand in the UI
- Separate ingestion, factor computation, screening, chart serving, watchlists, and backtesting as distinct architectural modules
- Treat research workflows and any future broker workflows as separate architectural boundaries

**Important Decisions (Shape Architecture):**

- Use REST-style HTTP APIs between frontend and backend
- Use batch-oriented daily data refresh rather than streaming architecture
- Use a local job execution model for refresh and backtest tasks
- Keep the frontend as a thin interaction and visualization layer over stored outputs
- Keep the MVP single-user and local-first

**Deferred Decisions (Post-MVP):**

- Broker integration details
- Multi-user authentication model
- PostgreSQL production migration timing
- Earnings-surprise ingestion architecture
- Dockerized deployment defaults

### Data Architecture

**Database Choice:**

- SQLite for MVP local development
- Preserve a later migration path to PostgreSQL

**Data Modeling Approach:**

- normalized relational model for securities, price history, screen runs, watchlist records, and backtest runs
- explicit derived-facts tables for RPS values, 52-week-high proximity, and screen pass or fail states

**Data Validation Strategy:**

- validate provider payloads before persistence
- mark stale, partial, and unavailable data states explicitly
- preserve run-time traceability from each qualified result back to stored values and parameters
- apply the frozen RPS semantic contract in `_bmad-output/planning-artifacts/rps-semantics-contract.md` across derived facts, stock detail payloads, and backtest inputs

**Migration Approach:**

- schema migration tooling is required from the start even in SQLite-backed MVP
- schema must allow later PostgreSQL migration with minimal model rewrites

**Caching Strategy:**

- avoid a separate caching system in MVP
- rely on stored derived facts and persisted run outputs as the primary performance strategy

### Authentication & Security

**Authentication Method:**

- no end-user authentication required in MVP single-user local mode

**Authorization Pattern:**

- local trusted-user model for MVP
- future auth must remain separable from core research logic

**Security Boundary:**

- provider credentials stored only on the backend
- no secrets exposed to the browser
- broker credentials, if introduced later, must live in a stricter isolated boundary than historical-data provider keys

**Operational Security:**

- server-side logging for failed updates, failed runs, and data-integrity investigations
- encrypted secret storage in transit and at rest where applicable to the chosen local environment

### API & Communication Patterns

**API Design Pattern:**

- REST-style HTTP API between Next.js frontend and Python backend

**Core API Boundaries:**

- screening configuration and execution
- screening execution may default to the latest derived-fact trade date, but future historical replay must select only from persisted derived-fact dates
- stock result list and stock detail retrieval
- chart data retrieval
- watchlist management
- backtest execution and result retrieval
- data freshness and update health reporting

**Error Handling Standard:**

- every API response involved in screening, charting, or backtesting must make incomplete-data or failed-run states explicit
- backend responses must preserve traceability context for investigation workflows
- chart and explainability payloads must preserve the distinction between authoritative screening signals and explanatory-only visual annotations

**Communication Style:**

- synchronous HTTP for quick reads and writes
- async job-style execution for screening and backtest operations that exceed interactive thresholds

### Frontend Architecture

**Application Shell:**

- Next.js App Router frontend
- desktop-first interface with responsive review support for smaller screens

**State Management Approach:**

- server-state concerns and transient UI state remain distinct
- avoid a heavyweight global state layer unless cross-page complexity proves it necessary

**UI Boundaries:**

- parameter configuration view
- screening results view
- stock detail and chart view
- watchlist view
- backtest results view
- data health and run-status indicators

**Rendering Strategy:**

- frontend remains primarily a consumer of backend-provided stored outputs and chart-ready data
- explainability data is rendered alongside chart workflows rather than hidden in separate diagnostics pages

### Infrastructure & Local Development

**Runtime Shape:**

- one frontend app
- one Python backend service
- one SQLite database
- one local job execution path for refresh and backtest work

**Refresh Model:**

- scheduled or manually triggered end-of-day batch updates
- no real-time event infrastructure in MVP

**Observability:**

- local logs for data refresh, screening runs, backtests, and data-integrity warnings
- visible data freshness indicators in the product UI

**Containerization:**

- not required for MVP local development
- can be introduced later if local environment setup becomes too inconsistent

### Decision Impact Analysis

**Implementation Sequence:**

1. establish backend data model and migration approach
2. implement ingestion and normalization pipeline
3. implement derived-facts computation
4. implement screening and result persistence
5. implement stock detail and chart-serving APIs
6. implement watchlist workflows
7. implement backtest execution and result persistence
8. connect frontend flows to backend APIs

**Cross-Component Dependencies:**

- chart views depend on the same normalized dataset used by screening and backtesting
- result explainability depends on persisted derived facts
- watchlist workflows depend on stable stock-detail identity and result context
- backtesting depends on stored parameters, stored data, and reproducible derived inputs

## Implementation Patterns & Consistency Rules

### Pattern Categories Defined

**Critical Conflict Points Identified:**
The highest-risk implementation conflicts are data naming, API payload shape, date and market-data normalization, derived-facts storage, error handling, and screen/backtest reproducibility rules.

### Naming Patterns

**Database Naming Conventions:**

- use `snake_case` for all tables and columns
- use plural table names for entity collections
- use `_id` suffix for foreign keys
- use explicit names for market-data fields such as `close`, `adj_close`, `high_252`, and `rps_50`
- do not mix semantic aliases for the same concept across tables

**API Naming Conventions:**

- use resource-oriented REST paths in lowercase nouns
- use plural resource paths for collections
- use path parameters for stable identifiers
- use query parameters for filters, date ranges, and run selectors
- keep API field names in `snake_case` unless a deliberate translation layer is introduced later

**Code Naming Conventions:**

- Python modules, functions, variables, and filenames use `snake_case`
- TypeScript React components use `PascalCase`
- TypeScript utility files and route-support files use `kebab-case` or framework-standard naming where required
- maintain canonical domain terms such as `screen_run`, `backtest_run`, `watchlist_entry`, and `instrument`

### Structure Patterns

**Project Organization:**

- keep frontend and backend as separate top-level applications
- organize backend by domain module, not by technical layer only
- keep ingestion, factors, screening, watchlists, backtests, and operational status as separate backend domains
- place tests close to the application they validate while keeping naming consistent within each app

**File Structure Patterns:**

- backend domain modules own models, service logic, repository access, and API serializers for that domain
- frontend routes map to user workflows, not backend service boundaries
- shared constants and domain types must be placed in explicit shared modules, not duplicated ad hoc across features

### Format Patterns

**API Response Formats:**

- read endpoints return direct domain-shaped JSON objects or arrays for successful responses
- mutation and job-triggering endpoints return explicit run or operation records
- error responses use a single structure with machine-readable `code`, human-readable `message`, and optional `details`
- incomplete-data and stale-data conditions must be surfaced explicitly where relevant

**Data Exchange Formats:**

- use ISO 8601 strings for timestamps
- use exchange-trading dates as explicit date strings for market-day fields
- use booleans as `true` and `false`
- use `null` instead of sentinel placeholder values
- preserve separate fields for raw and adjusted price concepts

### Communication Patterns

**Run and Job Patterns:**

- screening runs and backtest runs are first-class records with stable identifiers
- long-running operations return a persisted run record rather than ephemeral-only status
- data refresh operations produce explicit status outputs that can be inspected later

**State Management Patterns:**

- frontend server-state concerns are separated from local UI state
- backend remains the source of truth for screening outputs, watchlist records, run records, and chart-ready datasets
- frontend must not recompute authoritative screening results independently from backend-provided facts

### Process Patterns

**Error Handling Patterns:**

- every user-visible failure distinguishes between invalid input, missing data, stale data, and system failure
- backend logs capture diagnostic detail while frontend surfaces concise actionable messages
- suspicious outputs must be investigable through run metadata and condition breakdown, not generic errors

**Loading State Patterns:**

- screening, backtest, and refresh workflows expose explicit loading or in-progress states
- UI loading states map to real backend operation states where applicable
- if a result depends on stale or partial data, the UI shows that state alongside the result

### Enforcement Guidelines

**All AI Agents MUST:**

- use the same canonical domain terms across frontend, backend, database, and API layers
- treat derived screening facts as persisted backend outputs, not ad hoc frontend computations
- preserve traceability from stock qualification back to stored values, thresholds, and run context
- keep raw-price and adjusted-price concepts distinct
- keep research workflows isolated from future broker-specific concerns

**Pattern Enforcement:**

- architecture and PRD terminology are the source of truth for domain naming
- new features must map to an existing domain module or explicitly justify a new one
- any deviation in API shape, naming, or market-data semantics must be corrected before merge
- implementation tasks should reference these pattern rules directly

### Pattern Examples

**Good Examples:**

- `screen_runs`, `backtest_runs`, `watchlist_entries`, `market_data_daily`
- `rps_50`, `rps_120`, `rps_250`, `high_252`, `high_proximity_ratio`
- API responses that include explicit data freshness or trust signals where relevant
- stock detail payloads that include both chart data and condition breakdown sourced from the same stored facts

**Anti-Patterns:**

- mixing `camelCase` and `snake_case` for the same backend-facing payload family
- recomputing pass or fail logic in the frontend independently from the backend
- collapsing raw and adjusted prices into one ambiguous field
- returning screen results without enough context to explain why a stock qualified
- treating refresh failures as silent background events with no user-visible status

## Project Structure & Boundaries

### Complete Project Directory Structure

```text
stockAnalyse/
├── README.md
├── .gitignore
├── .env.example
├── docs/
├── data/
│   ├── raw/
│   ├── derived/
│   └── exports/
├── apps/
│   ├── web/
│   │   ├── package.json
│   │   ├── next.config.ts
│   │   ├── tsconfig.json
│   │   ├── postcss.config.js
│   │   ├── eslint.config.js
│   │   ├── public/
│   │   │   └── icons/
│   │   ├── src/
│   │   │   ├── app/
│   │   │   │   ├── layout.tsx
│   │   │   │   ├── page.tsx
│   │   │   │   ├── screen/
│   │   │   │   │   └── page.tsx
│   │   │   │   ├── stocks/
│   │   │   │   │   └── [instrumentId]/
│   │   │   │   │       └── page.tsx
│   │   │   │   ├── watchlist/
│   │   │   │   │   └── page.tsx
│   │   │   │   └── backtests/
│   │   │   │       ├── page.tsx
│   │   │   │       └── [runId]/
│   │   │   │           └── page.tsx
│   │   │   ├── components/
│   │   │   │   ├── chart/
│   │   │   │   ├── screen/
│   │   │   │   ├── watchlist/
│   │   │   │   ├── backtest/
│   │   │   │   ├── forms/
│   │   │   │   └── ui/
│   │   │   ├── lib/
│   │   │   │   ├── api/
│   │   │   │   ├── format/
│   │   │   │   ├── constants/
│   │   │   │   └── utils/
│   │   │   ├── hooks/
│   │   │   ├── types/
│   │   │   └── styles/
│   │   └── tests/
│   │       ├── components/
│   │       ├── pages/
│   │       └── e2e/
│   └── api/
│       ├── pyproject.toml
│       ├── uv.lock
│       ├── alembic.ini
│       ├── migrations/
│       ├── src/
│       │   └── stockanalyse_api/
│       │       ├── main.py
│       │       ├── config/
│       │       ├── db/
│       │       │   ├── base.py
│       │       │   ├── session.py
│       │       │   └── migrations/
│       │       ├── domain/
│       │       │   ├── instruments/
│       │       │   ├── market_data/
│       │       │   ├── indicators/
│       │       │   ├── screens/
│       │       │   ├── watchlists/
│       │       │   ├── backtests/
│       │       │   └── operations/
│       │       ├── services/
│       │       │   ├── ingestion/
│       │       │   ├── normalization/
│       │       │   ├── factor_materialization/
│       │       │   ├── screening/
│       │       │   ├── chart_data/
│       │       │   ├── watchlists/
│       │       │   ├── backtesting/
│       │       │   └── health/
│       │       ├── repositories/
│       │       ├── api/
│       │       │   ├── routes/
│       │       │   ├── schemas/
│       │       │   └── errors/
│       │       ├── jobs/
│       │       │   ├── refresh_market_data.py
│       │       │   ├── materialize_facts.py
│       │       │   └── run_backtest.py
│       │       └── logging/
│       └── tests/
│           ├── unit/
│           ├── integration/
│           └── fixtures/
├── packages/
│   └── contracts/
│       ├── api-schemas/
│       └── glossary/
├── scripts/
│   ├── dev/
│   ├── data/
│   └── maintenance/
└── _bmad-output/
```

### Architectural Boundaries

**API Boundaries:**

- `apps/web` never reads the database directly
- `apps/web` talks only to `apps/api` over HTTP
- `apps/api` owns screening, chart, watchlist, backtest, and health endpoints
- provider integrations remain behind backend service boundaries only

**Component Boundaries:**

- `screen`, `stocks`, `watchlist`, and `backtests` are separate UI workflow areas
- chart components do not compute authoritative factor logic
- UI components render backend-provided facts and statuses

**Service Boundaries:**

- `ingestion` pulls provider data
- `normalization` transforms provider data into canonical records
- `factor_materialization` computes stored derived facts such as `rps_50`, `rps_120`, `rps_250`, and `high_proximity_ratio`
- `screening` executes parameterized screen runs from stored facts
- future screening date selection must reuse persisted derived-fact dates rather than query raw market data ad hoc
- `chart_data` assembles stock detail and chart-ready payloads
- `backtesting` runs reproducible historical simulations against stored inputs
- `health` exposes data freshness and operational status

**Data Boundaries:**

- raw provider payloads and normalized market records are distinct concerns
- derived facts are persisted separately from raw market data
- screen runs and backtest runs are persisted records, not transient-only outputs
- watchlist entries are independent user records linked to canonical instrument identifiers

### Requirements to Structure Mapping

**Feature Mapping:**

- Strategy configuration → `apps/web/src/app/screen`, `apps/api/src/stockanalyse_api/domain/screens`
- Market data and universe → `apps/api/src/stockanalyse_api/domain/instruments`, `market_data`, `services/ingestion`, `services/normalization`
- Indicator evaluation → `apps/api/src/stockanalyse_api/domain/indicators`, `services/factor_materialization`
- Screening results → `apps/api/src/stockanalyse_api/domain/screens`, `services/screening`, `apps/web/src/components/screen`
- Chart review → `apps/api/src/stockanalyse_api/services/chart_data`, `apps/web/src/components/chart`
- Watchlist management → `apps/api/src/stockanalyse_api/domain/watchlists`, `apps/web/src/components/watchlist`
- Backtesting → `apps/api/src/stockanalyse_api/domain/backtests`, `services/backtesting`, `jobs/run_backtest.py`, `apps/web/src/components/backtest`
- Data health and status → `apps/api/src/stockanalyse_api/domain/operations`, `services/health`

**Cross-Cutting Concerns:**

- API schemas and shared naming glossary → `packages/contracts`
- migration and database lifecycle → `apps/api/migrations`, `apps/api/src/stockanalyse_api/db`
- operational scripts and local workflow helpers → `scripts/`

### Integration Points

**Internal Communication:**

- frontend requests backend HTTP endpoints
- backend services communicate through domain services and repositories, not direct frontend coupling
- job modules call the same domain services used by the HTTP API

**External Integrations:**

- market-data providers connect only through backend ingestion services
- future earnings or broker integrations remain separate modules under backend service boundaries

**Data Flow:**

1. ingest raw provider data
2. normalize into canonical market records
3. materialize derived screening facts
4. execute screen or backtest against stored facts and parameters
5. serve chart, result, watchlist, and run data to the web app

### File Organization Patterns

**Configuration Files:**

- root-level shared repo config
- frontend config under `apps/web`
- backend config under `apps/api`
- environment examples at root and app-local when needed

**Source Organization:**

- frontend organized by user workflow plus shared UI primitives
- backend organized by business domain first, then services, repositories, and API adapters

**Test Organization:**

- frontend UI and page tests in `apps/web/tests`
- backend unit, integration, and fixture tests in `apps/api/tests`
- no mixed frontend and backend test folders

**Asset Organization:**

- frontend public assets under `apps/web/public`
- exported datasets and offline artifacts under `data/exports`
- raw and derived data separated under `data/`

### Development Workflow Integration

**Development Server Structure:**

- frontend runs independently for UI iteration
- backend runs independently for API and job iteration
- local development uses SQLite-backed API by default

**Build Process Structure:**

- frontend build and backend packaging remain separate
- shared contracts and glossary stay lightweight and source-controlled

**Deployment Structure:**

- the structure supports later containerization, but does not require Docker for MVP local development
- later PostgreSQL migration can be handled within the backend app boundary without restructuring the repository

## Architecture Validation Results

### Coherence Validation ✅

**Decision Compatibility:**
All major decisions are compatible. Next.js and a custom Python backend align with the chosen single-user, local-first, end-of-day research workflow. SQLite is acceptable for MVP because the architecture treats persistence, migrations, and derived-facts materialization explicitly rather than as throwaway prototype concerns.

**Pattern Consistency:**
The implementation patterns support the architectural decisions. Naming, API payloads, state ownership, and data traceability rules are aligned with the requirement for reproducibility and explainability.

**Structure Alignment:**
The project structure supports the chosen architecture. Domain boundaries, service modules, job execution paths, and frontend workflow boundaries are all defined in a way that matches the PRD and avoids overlapping ownership.

### Requirements Coverage Validation ✅

**Feature Coverage:**
All MVP feature groups are architecturally supported: screening, chart review, explainability, watchlists, backtesting, and data health visibility all map to explicit domains and modules.

**Functional Requirements Coverage:**
All functional requirement categories have architectural support:

- strategy configuration
- market data and universe
- indicator and signal evaluation
- screening results
- chart review and explainability
- watchlist management
- backtesting
- data health and operational visibility
- future extension boundaries

**Non-Functional Requirements Coverage:**
The architecture addresses the key NFRs:

- performance through stored derived facts and persisted run outputs
- reliability through explicit run records, migration support, and data-state visibility
- security through backend-only secret handling and future broker isolation
- accessibility through frontend workflow boundaries and explicit non-color signal summaries
- data integrity through one normalization policy and traceable result generation

### Implementation Readiness Validation ✅

**Decision Completeness:**
Critical decisions are complete enough to begin implementation. The remaining deferred items are intentionally post-MVP and do not block initial development.

**Structure Completeness:**
The project tree is concrete enough for implementation agents to place code consistently without reinterpreting the architecture.

**Pattern Completeness:**
The highest-risk consistency points are covered: naming, API format, data semantics, derived-facts ownership, and error and loading patterns.

### Gap Analysis Results

**Critical Gaps:** None for MVP implementation.

**Important Gaps:**

- specific charting library selection is still deferred
- exact Python ORM and migration tooling package choices are implied but not yet named explicitly in the architecture body
- exact job runner mechanism for local execution can be finalized during implementation

**Nice-to-Have Gaps:**

- a dedicated glossary document under `packages/contracts/glossary`
- a lightweight ADR folder for future post-MVP decision tracking

### Validation Issues Addressed

- The architecture explicitly narrows MVP scope to Japan equities only
- Earnings surprise continuity remains out of MVP so the architecture does not overfit to unstable data acquisition paths
- Research workflows and future broker workflows remain structurally isolated
- The same stored facts are the basis for screening, chart explainability, and backtesting

### Architecture Completeness Checklist

**✅ Requirements Analysis**

- [x] Project context thoroughly analyzed
- [x] Scale and complexity assessed
- [x] Technical constraints identified
- [x] Cross-cutting concerns mapped

**✅ Architectural Decisions**

- [x] Critical decisions documented
- [x] Technology stack fully specified
- [x] Integration patterns defined
- [x] Performance considerations addressed

**✅ Implementation Patterns**

- [x] Naming conventions established
- [x] Structure patterns defined
- [x] Communication patterns specified
- [x] Process patterns documented

**✅ Project Structure**

- [x] Complete directory structure defined
- [x] Component boundaries established
- [x] Integration points mapped
- [x] Requirements to structure mapping complete

### Architecture Readiness Assessment

**Overall Status:** READY FOR IMPLEMENTATION

**Confidence Level:** High

**Key Strengths:**

- MVP scope is deliberately constrained
- the architecture is driven by reproducibility and explainability
- the backend structure naturally supports derived-facts materialization and persisted run history
- future growth paths are preserved without polluting MVP implementation

**Areas for Future Enhancement:**

- multi-user auth and hosted deployment path
- PostgreSQL production migration
- earnings-surprise ingestion design
- broker integration boundary implementation

### Implementation Handoff

**AI Agent Guidelines:**

- follow architectural boundaries exactly as documented
- treat backend-derived facts as authoritative
- preserve traceability from every qualifying stock result to stored values and run context
- keep frontend workflow code separate from backend domain logic
- do not introduce broker-specific concerns into MVP research modules

**First Implementation Priority:**

- initialize the frontend and backend application shells
- establish the backend data model and migration path
- implement ingestion, normalization, and derived-facts materialization before chart and backtest UX

## 2026-04-15 Follow-Up Architecture Addendum

This addendum records approved scope adjustments discovered during real usage after the initial planning set was completed.

### Chart Data Serving Boundaries

- Stock detail chart payloads must provide enough historical depth for routine pattern review rather than only a narrow recent slice.
- The preferred default is a larger backend-defined history window.
- If payload size or response latency becomes a constraint, the backend may instead expose a bounded incremental loading contract for older chart history.
- The frontend must not infer authoritative chart history windows on its own.

### Chart Readability Rules

- RPS line labels must not cover the most recent plotted values where the user validates the latest setup.
- Label placement is a presentation concern, but the backend remains the source of truth for the plotted lines and official threshold state.
- End-of-day chart date presentation should use localized date-only formatting in the frontend contract surface.

### Screening Parameterization Boundaries

- The approved RPS business definition remains fixed.
- What becomes configurable is the active set of approved RPS lookback windows that participate in screening, and every selected line must satisfy the threshold.
- This change does not authorize arbitrary user-defined factor semantics.
- Implementation must choose one bounded strategy:
  - pre-materialize an approved set of windows, or
  - compute additional approved windows from stored prices on demand, or
  - support a constrained hybrid approach
- The selected strategy must preserve consistency across screening, chart review, and backtesting.

### Operations and Refresh Automation

- Refresh execution state can no longer be treated as purely manual or observational.
- The backend runtime may initialize or advance refresh execution status automatically at startup.
- If the backend remains running, it must support a daily automation path that advances refresh execution status on the expected cadence.
- This automation must be designed to coexist safely with:
  - SQLite locking behavior
  - existing manual maintenance commands
  - explicit refresh jobs

### Data Health Semantics

- Data health responses must report these concepts separately:
  - stored market-data coverage
  - approved common-stock universe size
  - universe manifest last-updated timestamp
  - refresh execution state
- Raw local filesystem paths are implementation details and should not be the primary trust signal in user-facing health summaries.
- Common-stock universe counts must be derived from the approved manifest semantics rather than incidental display formatting or partial subsets.

### Watchlist Workflow Continuity

- Watchlist entries are not terminal records; they are part of the research workflow.
- The architecture therefore treats watchlist-to-stock-detail navigation as a first-class continuity path rather than a convenience-only link.

### Impacted Domains

- `operations` / `services/health`
- `screens` and derived-indicator evaluation
- `chart_data`
- `watchlists`
- shared frontend shell and stock-detail components
