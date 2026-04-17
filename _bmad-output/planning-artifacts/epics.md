---
stepsCompleted:
  - step-01-validate-prerequisites.md
  - step-02-design-epics.md
  - step-03-create-stories.md
inputDocuments:
  - /Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/prd.md
  - /Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/architecture.md
---

# stockAnalyse - Epic Breakdown

## Overview

This document provides the complete epic and story breakdown for stockAnalyse, decomposing the requirements from the PRD, UX Design if it exists, and Architecture requirements into implementable stories.

## Requirements Inventory

### Functional Requirements

FR1: The user can create a screening configuration for Japan equities.
FR2: The user can edit the parameter values of a screening configuration.
FR3: The user can define the RPS threshold used by the strategy.
FR4: The user can define the 52-week-high proximity threshold used by the strategy.
FR5: The user can run the strategy using either parameters supplied directly from the current form or a saved configuration version, without requiring the form parameters to be saved as a configuration first.
FR6: The user can rerun the strategy after changing parameters.
FR7: The product can preserve an independent parameter snapshot on each screen run that records the exact values used, regardless of whether those values came from an ad-hoc form submission or a saved configuration version.
FR8: The product can preserve the parameter values used for each backtest run.
FR9: The product can maintain a Japan equity universe for screening and backtesting.
FR10: The product can store end-of-day historical price data for supported securities.
FR11: The product can update historical market data for the supported universe.
FR12: The product can expose the freshness state, universe manifest freshness, and refresh execution state of the stored market data.
FR13: The product can identify when market data for a security or date range is incomplete or unavailable.
FR14: The product can keep screening, charting, and backtesting aligned to the same stored market dataset.
FR15: The product can calculate approved RPS-related values for supported securities for the configured lookback windows required by screening, chart review, and backtesting.
FR16: The product can determine whether every user-selected RPS line satisfies the strategy threshold condition.
FR17: The product can calculate each security's proximity to its 52-week high.
FR18: The product can determine whether a security satisfies the configured 52-week-high proximity condition.
FR19: The product can evaluate whether a security passes the full MVP strategy based on the active conditions.
FR20: The product can retain the indicator and condition values that caused a security to pass or fail a run.
FR21: The user can run a screen across the supported Japan equity universe.
FR22: The product can return the list of securities that satisfy the active screen conditions.
FR23: The user can open an individual screening result for deeper inspection.
FR24: The product can show the exact rule breakdown for a screened security.
FR25: The product can show the underlying values used to determine whether each condition passed.
FR26: The user can tell from the result detail why a stock qualified for the screen.
FR27: The product can associate each result set with the run date and parameter set that produced it.
FR28: The user can view a candlestick chart for a supported security with sufficient historical context for routine chart review.
FR29: The user can view RPS information in a panel below the main price chart without important recent data being obscured by fixed labels.
FR30: The product can visually distinguish RPS conditions that meet the configured threshold.
FR31: The user can inspect chart-adjacent summaries of the strategy condition values.
FR32: The user can review the stock's 52-week-high proximity state from the stock detail workflow.
FR33: The product can present chart review and condition breakdown information as part of the same stock analysis flow.
FR34: The user can add a screened security to a watchlist.
FR35: The user can remove a security from a watchlist.
FR36: The user can view the securities currently stored in the watchlist and navigate from an entry into the corresponding stock detail workflow.
FR37: The user can record a note for a watchlist entry.
FR38: The user can record an observation reason for a watchlist entry.
FR39: The product can retain the date when a security was added to the watchlist.
FR40: The user can review saved watchlist notes, reasons, and added dates later.
FR41: The user can launch a portfolio-return backtest that takes a completed screen run as its input and simulates the subsequent performance of the qualified securities under an explicit entry, sizing, holding, and stop-loss policy.
FR42: The user can select the historical date range used for a backtest.
FR43: The product can run the backtest using the same parameterized conditions used by the screen.
FR44: The product can return a reproducible backtest result for the same input screen run, holding parameters, stop-loss parameters, and stored dataset, recording a dataset-version identifier per run.
FR45: The user can review the result of a completed backtest, including portfolio cumulative return, win rate (defined as the share of closed positions whose realized return is strictly greater than zero), maximum drawdown (defined as the largest peak-to-trough decline of the portfolio equity curve), the portfolio equity curve, and the per-security return distribution. (Normative source for win-rate and max-drawdown definitions across the product.)
FR46: The product can associate a backtest result with the originating screen_run_id, strategy parameter snapshot, holding parameters, stop-loss parameters, portfolio cap, ranking policy, and simulation date range that produced it.
FR47: The user can use portfolio-return backtest outputs to compare strategy adjustments across runs, including differences in holding period, stop-loss threshold, portfolio cap, and source screen run.
FR48: The user can see whether market data and the approved universe manifest are current enough for routine post-close use.
FR49: The user can see when a data update has failed, is incomplete, stale, or has not been automatically advanced as expected.
FR50: The user can identify whether a suspicious screen or backtest result may be caused by stale or incomplete data.
FR51: The product can expose enough run and data context to investigate unexpected outputs.
FR52: The product can keep research workflows separate from any future broker integration workflows.
FR53: The product can support future addition of new strategy conditions without invalidating the core workflow structure.
FR54: The product can support future expansion from Japan equities to other supported markets.
FR55: The user can select an available historical trade date for a screening run when reviewing past market states.
FR56: The user can configure which approved RPS lookback windows participate in the active screening rule.
FR57: The user can configure which RPS lines participate in the threshold condition for a security to qualify.
FR58: The backend can trigger or maintain refresh execution state automatically at startup and on the expected daily cadence.
FR59: The product can display the last-updated timestamp of the approved universe manifest without exposing unnecessary local file path details in the primary UI.
FR60: The product can present chart dates in a localized, date-only format appropriate for the primary user workflow.
FR61: The user can navigate directly from a watchlist entry to the corresponding stock detail workflow.
FR62: The product distinguishes between the user's current form parameters, the currently active saved configuration, and the parameter snapshot that governed a specific screen run, and exposes this distinction in run-detail and result-list traceability surfaces.
FR63: The product can save the current form parameters as a new configuration version through an explicit user action that is independent from launching a screen run, and rejects no-op saves identical to the latest existing version.
FR64: When a user adds a stock to the watchlist from a screen result, the product automatically attaches the originating screen_run_id and screen trade date to that watchlist entry; re-adding the same instrument from a different screen run updates the attached run reference rather than creating a duplicate; non-screening additions record screen_run_id as null and the UI distinguishes that case explicitly.
FR65: The user can review a screened security's price chart, valuation indicators (PE, PB), and fiscal-year net income history directly inside the screening result panel, without navigating to the stock detail page.
FR66: The screening result panel can incrementally load the inline analysis data of additional result cards as the user scrolls, rather than loading every card's analysis payload at first render; result sets below an explicit threshold may load all at once.
FR67: The product reuses the existing charting library and stock-detail data contracts to render inline screening-result analysis cards, extending those contracts where required for valuation and fiscal-year data, and does not introduce a separate visualization framework for that purpose.
FR68: The product executes backtest entries at the opening price of the trading day immediately after the screening trade date (T+1 open) and never uses screening-day post-close information; if T+1 is invalid for a security, the product defers entry to the next valid trading day within a configurable entry-deferral window (MVP default = 5 trading days), or excludes the security from the simulated portfolio with a recorded exclusion reason if no valid open price arrives within that window or if a corporate-action / delisting event invalidates entry across the window.
FR69: The product sizes backtest positions by equal weighting up to a configurable portfolio cap; the MVP ranking policy for cap exclusion is descending RPS composite score with ticker tie-breaker; fractional shares are allowed as an MVP simplification; no rebalancing, re-entry, or position adds occur after initial entry; an empty qualified set after exclusions yields a fully populated empty-portfolio result rather than a failure.
FR70: The product enforces a configurable per-security stop-loss threshold against each position's own entry price, computes the breach signal from the daily adjusted close, closes the position at the next valid trading day's opening price (including gap-down opens), and does not redeploy released cash within the same backtest.
FR71: The user can configure backtest holding period (in trading days), per-security stop-loss threshold, portfolio cap, and entry-deferral window (in trading days) at launch; MVP defaults are 20 trading days holding, -8% stop-loss, 20 securities, and 5 trading days entry-deferral window; validation requires holding ≥ 1 (integer), stop-loss ∈ (-1, 0), cap ≥ 1 (integer), and entry-deferral window ≥ 1 (integer); effective values are persisted on every run.
FR72: The user can launch a backtest as a single action that both persists the backtest record and executes the simulation; the launch is debounced to prevent duplicate runs from a double click; persistence success without execution start marks the run as failed-recoverable so the user can retry; the MVP exposes neither a separate "execute" action for an already-created run nor a cancel action mid-execution.
FR73: Every backtest run record carries a `backtest_lifecycle` field whose values include `portfolio_return` (default for runs produced by the portfolio-return execution model) and `legacy_condition_hit` (any run that predates the portfolio-return model); the introducing migration backfills pre-existing runs with `legacy_condition_hit` rather than null, and result-list, comparison, and aggregation surfaces never mix the two lifecycle classes into a single portfolio-return statistic.

### NonFunctional Requirements

NFR1: The system shall return a completed Japan equity screen within 5 minutes for normal end-of-day usage under the MVP data universe.
NFR2: The system shall open a stock detail view, including chart-ready data and condition breakdown, within 3 seconds for 95% of requests under normal usage.
NFR3: The system shall persist watchlist add, edit, and remove actions within 2 seconds for 95% of requests under normal usage.
NFR4: The system shall present explicit in-progress status for screen and backtest operations that cannot complete within 3 seconds.
NFR5: The system shall avoid hour-scale waits for routine screening and backtesting tasks in the MVP workflow.
NFR6: The system shall produce identical backtest outputs for identical historical ranges, parameter sets, and underlying stored datasets.
NFR7: The system shall detect and surface failed, partial, or stale market-data updates before those outputs are presented as normal screening or backtest results.
NFR8: The system shall preserve the parameter set, run context, and output association for every screening run and backtest run.
NFR9: The system shall prevent silent divergence between screening outputs, chart views, and condition breakdown values derived from the same stored dataset.
NFR10: The system shall retain watchlist entries, notes, observation reasons, and added dates without loss during normal operation.
NFR11: The system shall restrict all provider credentials and any future broker credentials to server-side storage and execution paths.
NFR12: The system shall encrypt sensitive credentials and secrets at rest and in transit.
NFR13: The system shall prevent browser clients from directly invoking privileged provider or broker operations with embedded secrets.
NFR14: The system shall maintain separate credential boundaries for historical-data providers and any future broker integrations.
NFR15: The system shall record sufficient server-side logs to investigate failed updates, failed runs, and data integrity issues.
NFR16: The system shall support keyboard access to primary workflows including parameter editing, result navigation, stock detail access, and watchlist editing.
NFR17: The system shall not rely on color alone to communicate whether a condition passed or failed.
NFR18: The system shall provide text-visible summaries for key signal states displayed on or near charts.
NFR19: The system shall keep parameter forms, watchlist forms, and result details readable and operable on supported desktop browsers without requiring pointer-only interaction.
NFR20: The system shall apply one consistent market-data normalization policy across screening, charting, and backtesting within the MVP scope.
NFR21: The system shall identify the source and freshness of stored market data used for screening and backtesting.
NFR22: The system shall distinguish complete data, partial data, and unavailable data states in a way the user can inspect.
NFR23: The system shall preserve traceability from each qualified stock result back to the stored values and thresholds that produced it.
NFR24: The system shall keep future broker integration concerns isolated from the MVP research data workflows so that research reproducibility is not degraded.
NFR25: The system shall render the first batch of inline analysis cards on the screening result panel within 3 seconds for result sets up to 50 qualified securities under normal usage; for result sets above 50, the system shall render the result-list skeleton (without inline analysis payloads) within 3 seconds and then progressively populate analysis cards as the user scrolls.

### Additional Requirements

- Frontend foundation shall use the Next.js official starter with TypeScript, App Router, Tailwind CSS, `src/` layout, and `@/*` import alias support.
- Backend foundation shall use a custom Python application initialized with `uv`, rather than a heavyweight full-stack template.
- The MVP system-of-record database shall start with SQLite and preserve a later migration path to PostgreSQL.
- The architecture shall follow an `ingest -> normalize -> materialize facts -> serve` flow.
- The backend shall persist derived screening facts instead of relying on on-demand frontend computation.
- Screening, charting, and backtesting shall consume the same normalized dataset and stored derived facts.
- Screening runs and backtest runs shall be persisted as first-class records with stable identifiers.
- The backend shall expose REST-style HTTP APIs for screening, stock detail, chart data, watchlists, backtests, and data health.
- The frontend shall remain a thin interaction and visualization layer over backend-provided outputs.
- MVP local development shall run as one frontend app, one Python backend service, one SQLite database, and one local job execution path.
- The repository shall follow a monorepo structure with separate `apps/web` and `apps/api` applications.
- The backend shall be organized by business domains including instruments, market_data, indicators, screens, watchlists, backtests, and operations.
- Provider integrations shall live only behind backend ingestion services.
- Broker integration concerns shall remain structurally isolated from MVP research workflows.
- Database and API naming shall use canonical `snake_case` domain terminology.
- Frontend components shall not recompute authoritative screening or qualification logic independently from backend-provided facts.
- The system shall preserve separate raw-price and adjusted-price concepts throughout storage and API boundaries.
- The architecture shall support schema migration tooling from the start, even with SQLite-backed MVP development.
- The architecture shall defer earnings-surprise continuity, multi-user authentication, broker integration, PostgreSQL production migration, and Dockerized deployment defaults to post-MVP work.

### UX Design Requirements

- Use the lightweight UX supplement at `_bmad-output/planning-artifacts/ux-followups-2026-04-15.md` for chart history, chart readability, shared navigation, data health hierarchy, screening parameter controls, and watchlist drill-down behavior.

### FR Coverage Map

FR1: Epic 2 - Strategy configuration for Japan equity screening
FR2: Epic 2 - Strategy configuration for Japan equity screening
FR3: Epic 2 - Strategy configuration for Japan equity screening
FR4: Epic 2 - Strategy configuration for Japan equity screening
FR5: Epic 2 - Strategy configuration for Japan equity screening
FR6: Epic 2 - Strategy configuration for Japan equity screening
FR7: Epic 2 - Strategy configuration for Japan equity screening
FR8: Epic 5 - Reproducible backtest parameter tracking

FR9: Epic 1 - Japan equity universe and data backbone
FR10: Epic 1 - Japan equity universe and data backbone
FR11: Epic 1 - Japan equity universe and data backbone
FR12: Epic 1 - Japan equity universe and data backbone
FR13: Epic 1 - Japan equity universe and data backbone
FR14: Epic 1 - Japan equity universe and data backbone

FR15: Epic 2 - Indicator materialization and screening execution
FR16: Epic 2 - Indicator materialization and screening execution
FR17: Epic 2 - Indicator materialization and screening execution
FR18: Epic 2 - Indicator materialization and screening execution
FR19: Epic 2 - Indicator materialization and screening execution
FR20: Epic 2 - Indicator materialization and screening execution
FR21: Epic 2 - Indicator materialization and screening execution
FR22: Epic 2 - Indicator materialization and screening execution
FR24: Epic 2 - Indicator materialization and screening execution
FR25: Epic 2 - Indicator materialization and screening execution
FR27: Epic 2 - Indicator materialization and screening execution
FR55: Epic 2 - Indicator materialization and screening execution

FR23: Epic 3 - Stock detail, chart review, and explainability
FR26: Epic 3 - Stock detail, chart review, and explainability
FR28: Epic 3 - Stock detail, chart review, and explainability
FR29: Epic 3 - Stock detail, chart review, and explainability
FR30: Epic 3 - Stock detail, chart review, and explainability
FR31: Epic 3 - Stock detail, chart review, and explainability
FR32: Epic 3 - Stock detail, chart review, and explainability
FR33: Epic 3 - Stock detail, chart review, and explainability

FR34: Epic 4 - Watchlist and research workflow continuity
FR35: Epic 4 - Watchlist and research workflow continuity
FR36: Epic 4 - Watchlist and research workflow continuity
FR37: Epic 4 - Watchlist and research workflow continuity
FR38: Epic 4 - Watchlist and research workflow continuity
FR39: Epic 4 - Watchlist and research workflow continuity
FR40: Epic 4 - Watchlist and research workflow continuity

FR41: Epic 5 - Backtesting and strategy iteration
FR42: Epic 5 - Backtesting and strategy iteration
FR43: Epic 5 - Backtesting and strategy iteration
FR44: Epic 5 - Backtesting and strategy iteration
FR45: Epic 5 - Backtesting and strategy iteration
FR46: Epic 5 - Backtesting and strategy iteration
FR47: Epic 5 - Backtesting and strategy iteration

FR48: Epic 1 - Japan equity universe and data backbone
FR49: Epic 1 - Japan equity universe and data backbone
FR50: Epic 1 - Japan equity universe and data backbone
FR51: Epic 1 - Japan equity universe and data backbone
FR58: Epic 1 - Japan equity universe and data backbone
FR59: Epic 1 - Japan equity universe and data backbone

FR52: Epic 6 - Product hardening and extension boundaries
FR53: Epic 6 - Product hardening and extension boundaries
FR54: Epic 6 - Product hardening and extension boundaries
FR56: Epic 2 - Indicator materialization and screening execution
FR57: Epic 2 - Indicator materialization and screening execution
FR60: Epic 3 - Stock detail, chart review, and explainability
FR61: Epic 4 - Watchlist and research workflow continuity

FR62: Epic 2 - Strategy configuration for Japan equity screening
FR63: Epic 2 - Strategy configuration for Japan equity screening
FR64: Epic 4 - Watchlist and research workflow continuity
FR65: Epic 3 - Inline screening-result analysis cards
FR66: Epic 3 - Inline screening-result analysis cards
FR67: Epic 3 - Inline screening-result analysis cards
FR68: Epic 5 - Portfolio-return backtest definition
FR69: Epic 5 - Portfolio-return backtest definition
FR70: Epic 5 - Portfolio-return backtest definition
FR71: Epic 5 - Portfolio-return backtest definition
FR72: Epic 5 - Portfolio-return backtest definition

## Epic List

### Epic 1: Project Foundation and Historical Data Backbone
Establish the monorepo structure, frontend and backend application shells, database and migration setup, Japan equity universe model, end-of-day market data ingestion, normalization, and operational data freshness visibility so all later product flows run on a trustworthy data foundation.
**FRs covered:** FR9, FR10, FR11, FR12, FR13, FR14, FR48, FR49, FR50, FR51, FR58, FR59

### Epic 2: Indicator Materialization and Screening Execution
Enable the user to define strategy parameters, compute MVP indicators, execute Japan equity screens, and persist explainable screen runs based on stored derived facts rather than ad hoc UI calculations.
**FRs covered:** FR1, FR2, FR3, FR4, FR5, FR6, FR7, FR15, FR16, FR17, FR18, FR19, FR20, FR21, FR22, FR24, FR25, FR27, FR55, FR56, FR57, FR62, FR63

### Epic 3: Stock Detail, Chart Review, and Explainability
Enable the user to open a screened stock, inspect candlestick data, review RPS panels, validate 52-week-high proximity, understand exactly why the stock qualified, and compare candidates inline within the screening result panel.
**FRs covered:** FR23, FR26, FR28, FR29, FR30, FR31, FR32, FR33, FR60, FR65, FR66, FR67

### Epic 4: Watchlist and Research Workflow Continuity
Enable the user to capture qualified stocks into a watchlist, preserve research context through notes and observation reasons, automatically retain the screening context that produced each entry, and revisit watchlist entries as part of the daily review workflow.
**FRs covered:** FR34, FR35, FR36, FR37, FR38, FR39, FR40, FR61, FR64

### Epic 5: Backtesting and Strategy Iteration
Enable the user to run reproducible portfolio-return backtests driven by completed screen runs, with explicit entry, sizing, holding, and stop-loss policies, and inspect realized portfolio-level outcomes.
**FRs covered:** FR8, FR41, FR42, FR43, FR44, FR45, FR46, FR47, FR68, FR69, FR70, FR71, FR72

### Epic 6: Product Hardening and Extension Boundaries
Harden the MVP for consistent use by enforcing trust, accessibility, security, and future-safe architectural boundaries, while preserving clean separation from post-MVP broker and market expansion concerns.
**FRs covered:** FR52, FR53, FR54

## Epic 1: Project Foundation and Historical Data Backbone

Establish the monorepo structure, frontend and backend application shells, database and migration setup, Japan equity universe model, end-of-day market data ingestion, normalization, and operational data freshness visibility so all later product flows run on a trustworthy data foundation.

### Story 1.1: Initialize Monorepo Application Shells

As a developer,
I want a monorepo with separate web and API applications initialized,
So that all subsequent features have a stable implementation foundation.

**FRs implemented:** Additional Architecture Requirements (frontend starter, backend foundation, monorepo structure)

**Acceptance Criteria:**

**Given** a fresh repository
**When** the project is initialized
**Then** separate `apps/web` and `apps/api` applications exist following the approved architecture structure
**And** the frontend uses the approved Next.js TypeScript starter conventions
**And** the backend uses the approved custom Python project structure

**Given** the initialized repository
**When** a developer reviews the root structure
**Then** shared configuration, data, scripts, and `_bmad-output` locations are present or reserved according to the architecture document

### Story 1.2: Establish Database Schema and Migration Workflow

As a developer,
I want the initial database schema baseline and migration workflow in place,
So that market data foundations can be stored consistently and later stories can evolve the schema safely.

**FRs implemented:** FR9, FR10, Additional Architecture Requirements (SQLite foundation, schema migration tooling)

**Acceptance Criteria:**

**Given** the backend application
**When** the persistence layer is implemented
**Then** the SQLite-backed schema supports instruments and daily market data records needed by the historical data backbone
**And** schema migration tooling is configured and usable in local development

**Given** a new local environment
**When** migrations are applied
**Then** the database can be created from migrations without manual schema editing

### Story 1.3: Ingest and Normalize Japan Equity End-of-Day Data

As a user,
I want Japan equity end-of-day market data ingested and normalized,
So that screening and backtesting run on a trustworthy dataset.

**FRs implemented:** FR9, FR10, FR11, FR13, FR14, FR48, FR49, FR50, FR51

**Acceptance Criteria:**

**Given** a configured market-data provider
**When** a market-data refresh is executed
**Then** the system stores normalized Japan equity instrument records and daily market data records
**And** raw-price and adjusted-price concepts remain distinct in storage

**Given** provider data contains incomplete or unavailable values
**When** normalization runs
**Then** the system marks incomplete or unavailable states explicitly instead of silently treating them as complete data

### Story 1.4: Expose Data Freshness and Refresh Status

As a user,
I want to see whether stored market data is fresh and usable,
So that I can trust or question screening and backtest outputs appropriately.

**FRs implemented:** FR12, FR13, FR48, FR49, FR50, FR51

**Acceptance Criteria:**

**Given** market-data refresh jobs have run
**When** the user requests data health information
**Then** the system returns refresh status, freshness state, and failure or incompleteness indicators

**Given** a refresh fails or only partially completes
**When** the user views the product status
**Then** the failed or partial state is visible and not masked as normal success

### Story 1.6: Correct Universe Manifest Freshness Display and Common-Stock Count Semantics

As a user,
I want the data health view to report the approved universe manifest clearly and correctly,
So that I can trust the displayed common-stock coverage and manifest freshness signals.

**FRs implemented:** FR12, FR48, FR49, FR50, FR51, FR59

**Acceptance Criteria:**

**Given** the data health summary is rendered
**When** the product shows universe-related trust information
**Then** it displays the approved common-stock universe count using the correct manifest semantics
**And** it displays the manifest last-updated timestamp as the primary freshness signal

**Given** the manifest source is a local file
**When** the main data health summary is shown
**Then** the UI does not rely on exposing the raw local filesystem path as the primary user-facing trust indicator

### Story 1.7: Maintain Refresh Execution State Automatically on Startup and Daily Cadence

As a user,
I want refresh execution state to advance automatically when the backend starts and continues running,
So that the product's operational trust view reflects reality without relying on manual intervention.

**FRs implemented:** FR12, FR48, FR49, FR51, FR58

**Acceptance Criteria:**

**Given** the backend service starts
**When** the runtime initializes
**Then** the system can create or advance refresh execution state according to the approved automation rules

**Given** the backend remains running across the expected refresh cadence
**When** the daily automation point is reached
**Then** the refresh execution state advances automatically
**And** the resulting status is visible through the data health workflow

**Given** an automatic refresh transition fails or is skipped
**When** the user inspects product status
**Then** the state is surfaced explicitly rather than appearing as a silent success

## Epic 2: Indicator Materialization and Screening Execution

Enable the user to define strategy parameters, compute MVP indicators, execute Japan equity screens, and persist explainable screen runs based on stored derived facts rather than ad hoc UI calculations.

### Story 2.1: Create Strategy Configuration Workflow with Decoupled Try-Run and Save

As a user,
I want to define and edit MVP strategy parameters and run them either as an ad-hoc try-run or after explicitly saving them as a configuration version,
So that I can iterate on parameters quickly without having to create a saved version on every change.

**FRs implemented:** FR1, FR2, FR3, FR4, FR5, FR6, FR7, FR62 (primary), FR63

**Acceptance Criteria:**

**Given** the screen configuration view
**When** the user enters an RPS threshold and a 52-week-high proximity threshold
**Then** the system accepts and validates the parameter values for a screen run

**Given** a saved or current parameter set
**When** the user updates the values
**Then** the new values are preserved for the next screen run

**Given** the user has unsaved form parameter values that pass validation (including the at-least-one-RPS-window rule from Story 2.8)
**When** the user clicks `用当前参数试跑`
**Then** the system launches a screen run using those form values directly
**And** no new saved configuration version is created as a side effect

**Given** the user has form parameter values that fail validation
**When** the form is in that state
**Then** the `用当前参数试跑` action remains disabled with an explicit reason and no run is created

**Given** the user wants to preserve the current form parameters as a reusable version
**When** the user clicks `保存为参数集`
**Then** the system creates a new saved configuration version
**And** no screen run is launched as a side effect

**Given** the current form values are identical to the latest existing saved configuration version
**When** the user clicks `保存为参数集`
**Then** the system rejects the no-op save with an explicit message and does not create a duplicate version

**Given** both actions coexist on the screen configuration UI
**When** the user reads the page
**Then** `用当前参数试跑` is rendered as the primary action and `保存为参数集` is rendered as a visually distinct secondary action

**Given** the user clicks `用当前参数试跑` rapidly more than once before the first response returns
**When** the UI processes those clicks
**Then** the action is debounced so only one screen run is created

### Story 2.2: Materialize RPS and 52-Week-High Derived Facts

As a system,
I want to compute and persist derived screening facts from normalized market data,
So that screening, charting, and backtesting all use the same authoritative values.

**FRs implemented:** FR14, FR15, FR16, FR17, FR18, FR20

**Acceptance Criteria:**

**Given** normalized daily market data exists
**When** derived-facts materialization runs
**Then** the system computes and stores 50-day, 120-day, and 250-day RPS-related values for supported securities using the approved business definition
**And** the system computes and stores 52-week-high proximity values for supported securities

**Given** derived facts have been computed
**When** later services query them
**Then** the same stored values are available for screening, chart detail, and backtesting
**And** non-computable RPS dates or securities are surfaced explicitly rather than silently approximated

### Story 2.3: Execute Screen Runs and Persist Independent Parameter Snapshot

As a user,
I want to run the MVP screen against the Japan equity universe and have each run record its own independent parameter snapshot,
So that I can retrieve the stocks that satisfy the active strategy and later trace which exact values produced any given result.

**FRs implemented:** FR5, FR7, FR19, FR20, FR21, FR22, FR24, FR25, FR27, FR62 (supporting)

**Acceptance Criteria:**

**Given** a valid parameter set and available derived facts
**When** the user launches a screen run
**Then** the system evaluates the configured strategy across the supported Japan equity universe
**And** the system persists a screen run record with the parameter set and run context

**Given** a completed screen run
**When** results are returned
**Then** each qualified stock is linked to the stored values that caused it to pass
**And** the persisted run remains traceable to the approved RPS semantic contract and threshold semantics active at execution time

**Given** any screen run is executed
**When** the system persists the run record
**Then** it stores an independent parameter snapshot containing rps_threshold, selected_rps_windows, price_proximity_threshold, trade_date, rps_definition_version, and source

**Given** a run launched via `用当前参数试跑`
**When** the snapshot is persisted
**Then** the source field is recorded as the literal value `ad_hoc_form`

**Given** a run launched from a saved configuration version
**When** the snapshot is persisted
**Then** the source field references that configuration version explicitly via a stable foreign-key-style identifier (for example `saved_configuration:{config_version_id}`) so that future edits or deletions of the configuration record cannot silently change what the run originally referred to

**Given** a saved configuration version is referenced by at least one screen run
**When** an edit attempt is made on that configuration version
**Then** the system either creates a new version or freezes the referenced version, never overwriting it in place

**Given** screen run records exist that predate the introduction of the source field
**When** the migration runs
**Then** their source field is backfilled with the literal value `legacy` rather than being inferred as `ad_hoc_form` or any specific configuration version

### Story 2.4: Display Screen Result List with Qualification Summary

As a user,
I want a result list of qualified stocks with immediate qualification context,
So that I can choose which candidates to inspect further.

**FRs implemented:** FR22, FR24, FR25, FR27

**Acceptance Criteria:**

**Given** a completed screen run with qualifying securities
**When** the user views the result list
**Then** the system displays the qualified stocks from that run
**And** the result list is associated with the run date and parameter set that produced it

**Given** a stock appears in the result list
**When** the user views its summary
**Then** the summary indicates that the stock passed and provides enough context to open the detailed explanation flow

### Story 2.5: Freeze RPS Business Definition and Derived-Fact Contract

As a product and engineering team,
I want the RPS business definition, ranking semantics, and derived-fact contract to be explicitly frozen,
So that future screening, charting, and backtesting changes do not drift away from the intended method.

**FRs implemented:** FR14, FR15, FR16, FR20

**Acceptance Criteria:**

**Given** the MVP RPS workflow is under review
**When** the team finalizes the approved RPS semantics
**Then** the project documents define the exact formula, ranking universe policy, normalization rule, and non-computable-data handling for RPS 50, 120, and 250
**And** the approved definition is frozen in a single contract document that future stories can reference directly

**Given** the approved RPS semantics exist
**When** derived facts, screen runs, stock detail payloads, or backtests consume RPS data
**Then** they all reference the same documented contract and no client-side approximation is treated as authoritative

### Story 2.6: Persist RPS Definition Version with Screen Runs

As an operator,
I want each screen run to record which approved RPS definition version it used,
So that historical results remain explainable even after the RPS contract evolves.

**FRs implemented:** FR7, FR20, FR27, FR51

**Acceptance Criteria:**

**Given** an approved RPS semantic contract version exists
**When** the user executes a screen run
**Then** the persisted run records the RPS definition version or equivalent contract identifier used at execution time

**Given** a historical screen result is investigated later
**When** the operator reviews the run context and qualification values
**Then** they can identify which RPS definition version governed that result without inferring it from source code history

### Story 2.7: Select Screening Trade Date from Available Derived-Fact Dates

As a user,
I want to choose an available historical screening trade date,
So that I can replay the screen against a past market state rather than always using the latest derived facts.

**FRs implemented:** FR21, FR22, FR27, FR55

**Acceptance Criteria:**

**Given** persisted derived facts exist for multiple trade dates
**When** the user opens the screening workflow
**Then** the product can present an explicit screening trade-date choice sourced from available derived-fact dates

**Given** the user selects a historical trade date and launches a screen run
**When** the run completes
**Then** the system evaluates only the stored derived facts for that selected trade date
**And** the persisted run context records the selected date without implying that arbitrary calendar dates are supported

**Given** the user does not manually choose a trade date
**When** a screen run is launched
**Then** the product continues to default to the latest available derived-fact trade date

### Story 2.8: Parameterize Included RPS Windows

As a user,
I want to choose which RPS windows are included in the strategy,
So that the screening rule matches the way I actually iterate on the method.

**FRs implemented:** FR3, FR5, FR6, FR7, FR15, FR16, FR19, FR20, FR56, FR57

**Acceptance Criteria:**

**Given** the screening configuration workflow
**When** the user defines the active RPS rule
**Then** the product accepts a selectable set of RPS lookback windows that can be included in the strategy

**Given** the user configures the active RPS rule
**When** the configuration is validated
**Then** the system requires at least one RPS window to be selected for inclusion
**And** the system does not expose a separate “minimum satisfied-line count” parameter

**Given** a configured screen run is executed
**When** the backend evaluates the strategy
**Then** it uses the selected RPS windows as the authoritative RPS condition set
**And** every selected RPS window must satisfy the threshold for the RPS condition to pass
**And** the persisted run context records the selected windows clearly enough for later investigation

## Epic 3: Stock Detail, Chart Review, and Explainability

Enable the user to open a screened stock, inspect candlestick data, review RPS panels, validate 52-week-high proximity, and understand exactly why the stock qualified. Also enable the user to compare candidates inline within the screening result panel via inline analysis cards, with progressive loading for larger result sets, before drilling into a single stock detail.

### Story 3.1: Serve Stock Detail and Chart Data from Stored Facts

As a user,
I want stock detail data assembled from the same stored dataset used by screening,
So that chart review and qualification logic stay aligned.

**FRs implemented:** FR14, FR23, FR24, FR25, FR31, FR32, FR33

**Acceptance Criteria:**

**Given** a stock from a completed screen run
**When** the stock detail is requested
**Then** the backend returns candlestick data, relevant RPS values, historical RPS series, 52-week-high proximity state, and rule breakdown from stored data

**Given** the stock detail payload
**When** it is compared with the originating screen run
**Then** the qualification values remain consistent with the run that produced the result
**And** missing or partial RPS history is represented explicitly instead of being fabricated in the client

### Story 3.2: Build Stock Detail Page with Candlestick and RPS Panels

As a user,
I want a stock detail view with price and RPS visualization,
So that I can inspect the setup visually in one workflow.

**FRs implemented:** FR23, FR28, FR29, FR30, FR31, FR32, FR33

**Acceptance Criteria:**

**Given** a selected qualified stock
**When** the stock detail page loads
**Then** the page displays a candlestick chart for the stock
**And** the page displays an RPS panel below the main price chart using traceable backend-derived history

**Given** the RPS panel is shown
**When** the active threshold is applied
**Then** RPS conditions meeting the threshold are visually distinguishable without relying on color alone
**And** explanatory chart annotations are visually separated from official screening signals

### Story 3.3: Show Rule Breakdown and Exact Qualifying Values

As a user,
I want to see exactly why a stock passed the screen,
So that I can validate the result before adding it to my watchlist.

**FRs implemented:** FR24, FR25, FR26, FR31, FR33

**Acceptance Criteria:**

**Given** a stock detail page for a qualified stock
**When** the explainability section is displayed
**Then** the system shows the exact rule breakdown for the stock
**And** the system shows the underlying values used to determine whether each condition passed
**And** the system labels whether each displayed RPS visual state is part of official screen logic or explanation-only context

**Given** the user reviews the stock detail
**When** they inspect the explainability section
**Then** they can determine why the stock qualified without leaving the stock analysis flow

### Story 3.4: Replace Manual Stock Detail SVG Charts with Lightweight Charts

As a user,
I want the stock detail page to use mature charting components for price and RPS history,
So that the visual analysis workflow feels trustworthy and does not rely on fabricated chart geometry.

**FRs implemented:** FR28, FR29, FR30, FR33

**Acceptance Criteria:**

**Given** a stock detail page loads
**When** the price panel renders
**Then** candlesticks are drawn by a mature charting library instead of custom SVG geometry

**Given** a stock detail page loads
**When** the RPS panel renders
**Then** it uses true historical derived-indicator values from the backend rather than a frontend-generated decay curve

**Given** the active threshold is shown in the RPS panel
**When** users inspect the chart
**Then** the threshold is represented directly in the chart and remains visually distinguishable

### Story 3.5: Clarify RPS Chart Semantics and Explainability Boundaries

As a user,
I want the RPS chart to distinguish official screening signals from explanatory visual annotations,
So that I do not mistake a helpful chart cue for a rule that actually drove qualification.

**FRs implemented:** FR24, FR25, FR29, FR30, FR33

**Acceptance Criteria:**

**Given** the stock detail page shows RPS history
**When** the user reviews the chart and rule breakdown together
**Then** the UI explicitly separates threshold-driven screening logic from explanatory-only visual annotations

**Given** the system presents RPS history or status annotations
**When** a displayed state is not part of official screen logic
**Then** the product labels it as explanatory-only and does not imply that it affected qualification

### Story 3.6: Expand Stock Detail Chart History and Improve Chart Readability

As a user,
I want the stock detail charts to show enough history and remain readable near the latest data,
So that I can use the detail page as a trustworthy review surface instead of fighting the visualization.

**FRs implemented:** FR28, FR29, FR30, FR31, FR33, FR60

**Acceptance Criteria:**

**Given** the stock detail page loads
**When** the candlestick chart is rendered
**Then** it displays enough historical context for routine chart review instead of only a narrow recent slice
**And** any later incremental loading behavior follows a defined backend-supported contract

**Given** the RPS panel is rendered
**When** the latest visible data is inspected
**Then** fixed labels do not obscure the most recent important plotted values

**Given** chart-adjacent dates are displayed in the stock detail workflow
**When** the user reviews them
**Then** they use a localized date-only format appropriate for end-of-day analysis

**Given** shared page shell elements are reused around the stock detail workflow
**When** the user navigates between major pages
**Then** the top navigation remains structurally consistent and does not collapse into descriptive page copy

### Story 3.7: Inline Screening Result Analysis Cards

As a user,
I want each screening result card to show a price chart, valuation indicators, and fiscal-year net income history in-line,
So that I can compare candidates without opening every stock detail page.

**FRs implemented:** FR65, FR67 (primary)

**Acceptance Criteria:**

**Given** a completed screen run with qualified securities
**When** the user views a result card
**Then** the card shows a 1-year candlestick chart for that security

**Given** a security with less than one full year of available history (for example a recent IPO)
**When** its inline candlestick renders
**Then** only the available portion is plotted
**And** the card explicitly indicates that the displayed window is shorter than 1 year rather than padding or stretching the data

**Given** a result card
**When** its analysis area renders
**Then** it displays a net-income bar chart covering up to the most recent 5 fiscal years
**And** each fiscal-year bar is rendered together with that year's PE and PB values on the same chart so the user can read profitability and valuation against the same time axis

**Given** a fiscal year reports a net loss
**When** PE for that year is rendered
**Then** the card displays a negative or "N/A" PE value using a single explicit convention rather than silently omitting the bar's PE label or substituting a positive value

**Given** companies with different fiscal-year-end conventions appear across cards
**When** their fiscal-year bars render
**Then** each card labels its bars by the company's own reported fiscal year (not normalized to a calendar year), and the labeling makes the FY end month visible to avoid cross-card misreading

**Given** PE, PB, or net income data is missing for a fiscal year
**When** the card renders
**Then** the missing data is surfaced explicitly (for example a labeled gap or "数据缺失" marker) instead of being fabricated, interpolated, or silently omitted

**Given** the inline analysis area renders
**When** charting is required
**Then** the implementation reuses lightweight-charts and the existing stock-detail data contracts
**And** any required additional fields for valuation or fiscal-year data are added to the existing data contract rather than a new bespoke charting framework

### Story 3.8: Incremental Result-Panel Data Loading

As a user,
I want additional result cards to load their analysis data only when I scroll near them,
So that a long result list does not force the first screen to load every chart and financial panel.

**FRs implemented:** FR66, FR67 (supporting)

**Acceptance Criteria:**

**Given** a screen run produces 20 or more qualified securities
**When** the result panel first renders
**Then** only cards within or near the viewport request their inline analysis payload

**Given** a screen run produces fewer than 20 qualified securities
**When** the result panel first renders
**Then** all cards request their inline analysis payload at first render and incremental loading is not engaged

**Given** the user scrolls down the result panel
**When** additional cards approach the viewport
**Then** the system incrementally loads the next batch of analysis data for those cards

**Given** the user changes the result-list ordering or filters
**When** the new arrangement is applied
**Then** the incremental-load state is reset so that the cards now within the viewport are the ones whose payloads load first

**Given** an analysis-card payload fails to load
**When** the affected card is on screen
**Then** the card shows an explicit error state and exposes a retry affordance
**And** the failure does not block other cards from loading or rendering

**Given** a card whose payload previously failed re-enters the viewport after the user scrolls back to it
**When** the card is rendered
**Then** the card does not silently re-fetch on every scroll; the user retries via the explicit retry affordance instead

## Epic 4: Watchlist and Research Workflow Continuity

Enable the user to capture qualified stocks into a watchlist, preserve research context through notes and observation reasons, and revisit watchlist entries as part of the daily review workflow. Adding a qualified stock to the watchlist automatically retains the originating screen run context (parameter set and trade date) by reference, so the user does not have to re-enter screening provenance, and that context is surfaced and re-bound on every subsequent re-add.

### Story 4.1: Add and Remove Qualified Stocks from the Watchlist with Auto-Attached Screening Context

As a user,
I want adding a screened stock to the watchlist to automatically retain the originating screen run context,
So that I do not have to remember or re-enter which parameter set or trade date the addition was based on.

**FRs implemented:** FR34, FR35, FR36, FR64 (primary)

**Acceptance Criteria:**

**Given** a screened or reviewed stock
**When** the user adds it to the watchlist
**Then** the stock is stored as a watchlist entry linked to the canonical instrument identity

**Given** a stock is already in the watchlist
**When** the user removes it
**Then** the watchlist no longer includes that entry

**Given** the user adds a qualified stock to the watchlist from a screen result
**When** the watchlist entry is created
**Then** the system automatically attaches the originating screen_run_id and the screen trade date to the entry by reference
**And** no duplicate parameter snapshot is stored on the watchlist entry itself

**Given** the same canonical instrument is later added to the watchlist from a different screen run
**When** the system processes the addition
**Then** the existing entry's attached screen_run_id and screen trade date are updated to point to the most recent addition
**And** no duplicate watchlist entry is created for that instrument

**Given** the user adds a stock to the watchlist from a non-screening surface (for example the stock detail page reached without a screen context)
**When** the watchlist entry is created
**Then** the screen_run_id is recorded as null
**And** the UI distinguishes that case explicitly so the user can tell that the entry has no attached screening context

### Story 4.2: Persist Watchlist Notes, Observation Reasons, Added Dates, and Surface Attached Screening Context

As a user,
I want each watchlist entry to include my own research context as well as the screening parameters that originally produced it,
So that I can remember why the stock matters later without losing the trail back to the screen run that surfaced it.

**FRs implemented:** FR37, FR38, FR39, FR40, FR64 (supporting)

**Acceptance Criteria:**

**Given** a watchlist entry
**When** the user saves a note and observation reason
**Then** the system persists those fields with the watchlist entry

**Given** a watchlist entry is created
**When** it is stored
**Then** the system preserves the date the stock was added to the watchlist

**Given** a watchlist entry has an attached screen_run_id
**When** the user reviews that entry
**Then** the UI surfaces the key parameters used by that screen run (RPS threshold, selected RPS windows, 52-week proximity threshold, trade date) by following the reference to the screen run's snapshot
**And** the surfaced parameters reflect the most recent screen_run_id bound to that entry per Story 4.1's overwrite-on-re-add semantics — the displayed context is "the latest re-binding," not "the first add" — and the UI does not retain a history of prior bindings on the watchlist entry

**Given** the referenced screen run record is no longer reachable (deleted, archived, or otherwise unavailable)
**When** the user reviews that watchlist entry
**Then** the UI displays an explicit "原筛选记录不可用" state instead of erroring or showing empty parameter fields

**Given** existing watchlist entries that predate the introduction of the auto-attached screening context
**When** the migration runs
**Then** their screen_run_id field is recorded as null
**And** the UI surfaces those legacy entries explicitly as having no attached screening context

### Story 4.3: View and Review the Watchlist

As a user,
I want to revisit my watchlist entries with their saved context,
So that my daily research workflow continues across sessions.

**FRs implemented:** FR36, FR40

**Acceptance Criteria:**

**Given** one or more watchlist entries exist
**When** the user opens the watchlist view
**Then** the system displays the stored watchlist securities

**Given** a watchlist entry is displayed
**When** the user reviews it
**Then** the saved note, observation reason, and added date are visible

### Story 4.4: Navigate from Watchlist Entries to Stock Detail

As a user,
I want each watchlist entry to lead directly to the corresponding stock detail view,
So that I can continue the research workflow without manually re-searching the symbol.

**FRs implemented:** FR36, FR40, FR61

**Acceptance Criteria:**

**Given** one or more watchlist entries are displayed
**When** the user selects the stock symbol or primary detail affordance
**Then** the product opens the corresponding stock detail workflow for that canonical instrument

**Given** a watchlist entry contains saved note and observation context
**When** the user drills into stock detail
**Then** the navigation preserves enough context that the user can continue analysis without losing the relationship to the watchlist review flow

## Epic 5: Backtesting and Strategy Iteration

Enable the user to run reproducible portfolio-return backtests driven by completed screen runs, using the same stored data and parameterized conditions as the screen, with a single execution model (T+1 open entry, equal-weight portfolio with fractional shares, configurable holding period and per-security stop-loss, configurable entry-deferral window, no intra-run cash redeployment) and a defined `backtest_lifecycle` field that separates portfolio-return runs from any pre-existing legacy condition-hit runs. The user can inspect results, compare strategy adjustments across runs, and trace each run back to its originating screen run and RPS definition version without backtests independently storing strategy definitions.

### Story 5.1: Launch Portfolio-Return Backtest from a Screen Run as a Single Action

As a user,
I want to launch a portfolio-return backtest in a single action starting from a chosen screen run,
So that I can evaluate what would have happened after that screen without sequencing backend task steps myself.

**FRs implemented:** FR8, FR41, FR42, FR46, FR71, FR72

**Acceptance Criteria:**

**Given** a completed screen run and the backtest configuration form
**When** the user clicks the single backtest launch action
**Then** the system creates and begins executing the backtest in one operation

**Given** a backtest is running
**When** execution takes longer than an immediate request cycle
**Then** the UI exposes a single in-progress state
**And** the UI does not expose any separate "execute" action for an already-created run
**And** the MVP does not expose a cancel action mid-execution

**Given** the user did not override the MVP defaults
**When** the backtest is launched
**Then** the system applies the shipped MVP defaults defined in the Story 5.6 portfolio-return backtest anchor (`_bmad-output/planning-artifacts/portfolio-backtest-anchor.md`) — this story does not redefine those default values
**And** the effective values are persisted on the backtest run record

**Given** the user overrides defaults at launch
**When** the form is submitted
**Then** the system validates each parameter against the rules stated in FR71 (holding period, stop-loss threshold, portfolio cap, entry-deferral window) — this story does not restate those validation bounds
**And** the launch action is disabled with explicit per-field reasons until validation passes

**Given** the user clicks the single launch action more than once before the first response returns
**When** the UI processes those clicks
**Then** the action is debounced so only one backtest run is created

**Given** persistence of the backtest record succeeds but execution does not start
**When** the failure is detected
**Then** the run is recorded with a failed-recoverable status so the user can retry from the same record without manual cleanup

### Story 5.2: Execute Portfolio-Return Backtest with Entry, Holding, and Stop-Loss Rules

As a user,
I want the backtest to simulate the subsequent portfolio performance of a screen run's qualified securities under explicit entry, holding, and stop-loss rules,
So that the result tells me how the screen would have performed as a strategy, not how often its conditions historically hit.

**FRs implemented:** FR43, FR44, FR68, FR69, FR70, FR71

**Acceptance Criteria:**

**Given** a backtest input consisting of a screen_run_id and configured holding parameters
**When** the backtest executes
**Then** the system opens equal-weighted positions in the qualified securities at the opening price of the trading day immediately after the screening trade date (T+1 open)
**And** if the qualified set exceeds the portfolio cap, the system retains the top entries ranked by descending RPS composite score with ticker as the deterministic tie-breaker, and records both the ranking policy identifier and the excluded securities on the run

**Given** the configured T+1 day is not a valid trading day for a security or its T+1 open price is unavailable
**When** entry is attempted for that security
**Then** entry is deferred to the next trading day with a valid open price, bounded by the run's `entry_deferral_window_days` parameter (MVP default = 5 trading days, per FR71 and the Story 5.6 anchor)
**And** if no valid open price is available within that deferral window, that security is excluded from the simulated portfolio with the exclusion reason recorded

**Given** a security is suspended, halted, delisted, or undergoes a corporate action that invalidates a tradable open price across the entire deferral window
**When** the entry phase completes
**Then** the security is excluded from the simulated portfolio with the exclusion reason recorded on the run

**Given** equal weighting is applied across N qualified securities
**When** position sizes are computed
**Then** the system uses fractional share sizing where each security receives `portfolio_value / N`, with `portfolio_value` defined as a unitless constant of `1.0` for the MVP (so per-security weight equals `1/N` and all reported portfolio-level returns are weight-based ratios, not currency)
**And** if N equals zero after exclusions, the system produces a fully populated empty-portfolio result rather than failing

**Given** positions are open
**When** the simulation advances day by day
**Then** the stop-loss breach signal is computed once per day from the daily adjusted close price as the single authoritative input
**And** when the breach signal fires, the position is closed at the next valid trading day's opening price (including a gap-down open below the stop-loss level)
**And** if the next trading day is itself a halt or has no valid open, closure is deferred to the next available trading day with a valid open price
**And** the released cash is not redeployed within the same backtest

**Given** the configured holding period elapses (counted in trading days)
**When** any positions remain open
**Then** they are closed at the opening price of the next trading day after the holding period ends, applying the same deferral rules as stop-loss closure when the next day lacks a valid open

**Given** the holding period would extend beyond the latest available historical trading day at execution time
**When** the simulation runs
**Then** the system returns the run with an explicit "数据不足以完成持有期" status rather than truncating the holding period silently or marking the run as completed

**Given** no rebalancing, re-entry, or position add is permitted within a single backtest
**When** the simulation runs
**Then** no logic path adds shares or opens new positions after the initial T+1 entry phase

**Given** the same screen_run_id, holding parameters, stop-loss parameters, and stored dataset (identified by dataset-version identifier)
**When** the backtest is executed again
**Then** the result is identical
**And** if the underlying market data has been corrected since the original run, the dataset-version identifier change is surfaced explicitly on the new run rather than silently producing a different result for the same inputs

### Story 5.3: Review Portfolio-Return Backtest Results and Compare Strategy Adjustments

As a user,
I want to inspect completed portfolio-return backtest results with all the indicators that matter for an investment view, and to compare runs across the dimensions I actually adjust,
So that I can iterate on my strategy with evidence rather than intuition alone.

**FRs implemented:** FR45, FR46, FR47

**Acceptance Criteria:**

**Given** one or more completed backtest runs
**When** the user opens a backtest result
**Then** the system displays the completed backtest output linked to the run record

**Given** a completed portfolio-return backtest
**When** the user opens the result
**Then** the UI shows portfolio cumulative return, win rate, maximum drawdown, the portfolio equity curve, and the per-security return distribution for that run, using the win-rate and maximum-drawdown definitions stated in FR45 (the normative source) — this story does not redefine those metrics

**Given** multiple completed backtest runs driven by different holding periods, stop-loss thresholds, portfolio caps, or source screen runs
**When** the user reviews them side by side
**Then** the result view exposes those differences as first-class comparison dimensions

**Given** two compared runs whose source screen runs have different trade dates
**When** their equity curves are displayed together
**Then** the curves are aligned on a "trading days since T+1 entry" axis (not on calendar date) so that the comparison reflects strategy performance, not coincidental timing

**Given** legacy backtest runs created under the prior "historical condition-hit" execution model exist in storage
**When** the result list and comparison view render
**Then** those runs are explicitly tagged as `legacy_condition_hit` and visually segregated from portfolio-return runs so they are not mixed into portfolio-return comparisons

### Story 5.4: Verify Backtest Alignment with Approved RPS Semantics via Source Screen Run

As a user,
I want every portfolio-return backtest's RPS semantics to be verifiable by tracing the run back to its originating screen run,
So that historical evaluation cannot drift away from the screen and chart workflows and we do not maintain a parallel RPS definition under the backtest record.

**FRs implemented:** FR14, FR43, FR44, FR46

**Acceptance Criteria:**

**Given** a portfolio-return backtest run was driven by a completed screen run
**When** the run context is inspected
**Then** the backtest record exposes its source `screen_run_id`, and the RPS definition / semantic contract version used is read from that source screen run rather than being independently re-stored on the backtest record

**Given** a suspicious portfolio-return backtest result is investigated
**When** the operator follows the `screen_run_id` reference
**Then** the originating screen run's RPS definition version, parameter snapshot, and stored dataset version are sufficient to demonstrate that the same RPS semantics were used by screening, stock detail, and backtesting — without reverse-engineering the code

**Given** the approved RPS semantic contract changes between two backtest runs whose source screen runs were created on different definition versions
**When** the runs are compared
**Then** the difference is observable through the source `screen_run_id` traceback (because each screen run already records its definition version), not through a separately stored definition version on the backtest record

### Story 5.5: Reference Screen-Run Provenance from Backtest Runs Without Re-Storing Strategy Definition

As an operator,
I want each portfolio-return backtest run to reference its source screen run as the single source of truth for strategy definition (RPS version, parameter snapshot, stored dataset version),
So that historical simulations remain reproducible and comparable after contract updates without the backtest record diverging from the screen run that drove it.

**FRs implemented:** FR8, FR44, FR46, FR51

**Acceptance Criteria:**

**Given** a portfolio-return backtest is launched from a completed screen run
**When** the backtest record is persisted
**Then** the record stores `screen_run_id` plus its own execution-model parameters (holding period, stop-loss threshold, portfolio cap, entry-deferral window, ranking policy identifier, dataset-version identifier as observed at execution time, `backtest_lifecycle = portfolio_return`), but does **not** independently re-store the screening strategy definition or RPS contract version — those remain owned by the source screen run

**Given** the source screen run is required to interpret a backtest record's strategy semantics
**When** any result, comparison, or operator-investigation surface renders the backtest run
**Then** that surface either resolves the strategy definition through `screen_run_id` or makes the missing source-screen-run state explicit (rather than synthesizing a definition locally)

**Given** the source screen run becomes unavailable (deleted, archived, or otherwise unresolvable)
**When** the backtest record is rendered
**Then** the UI surfaces an explicit "originating screen run unavailable — strategy definition cannot be resolved" state rather than a silent partial render

**Given** two portfolio-return backtest runs whose source screen runs used different RPS definition versions are compared
**When** their outputs differ
**Then** the version difference is observable through their respective `screen_run_id` references (each screen run already carries its own definition version), so the operator does not need to inspect repository history to attribute the difference

### Story 5.6: Anchor Portfolio-Return Backtest Definition

As a product and engineering team,
I want the backtest definition to be explicitly anchored to portfolio-return simulation with defined entry, sizing, holding, stop-loss, ranking, and benchmark boundaries,
So that future stories, implementations, and test criteria cannot quietly drift back to a historical condition-hit statistic.

**FRs implemented:** FR41, FR44, FR68, FR69, FR70, FR71, FR72, FR73

**Acceptance Criteria:**

**Given** the MVP backtest semantics are being locked
**When** the team finalizes the approved definition
**Then** a single anchor document is produced at the path `_bmad-output/planning-artifacts/portfolio-backtest-anchor.md` (filename `portfolio-backtest-anchor.md`) and records all of the following without ambiguity:
- entry at T+1 open with the deferral and exclusion rules from FR68 (configurable `entry_deferral_window_days`, MVP default = 5 trading days, per FR71)
- equal-weighted sizing with a configurable portfolio cap and an MVP default of 20 securities
- MVP ranking policy for cap exclusion = descending RPS composite score with ticker tie-breaker
- configurable holding period in trading days, MVP default 20
- configurable per-security stop-loss threshold against own entry price, MVP default -8%, breach signal computed once per day from the daily adjusted close
- closure (both stop-loss and holding-period exit) at the next valid trading day's open with the deferral rules from FR70
- no rebalancing, re-entry, or position adds within a single backtest
- released cash is not redeployed within the same backtest
- fractional share sizing is the MVP simplification, with the initial `portfolio_value` defined as a unitless constant of `1.0` so each of N securities receives weight `1/N` and all reported portfolio-level returns are weight-based ratios (not currency)
- the win-rate and maximum-drawdown definitions used for result review are stated by FR45 (the normative source); this anchor links to FR45 rather than re-stating them
- MVP does not include benchmark comparison
- every run records its source `screen_run_id`, parameter snapshot, holding parameters, stop-loss parameters, portfolio cap, `entry_deferral_window_days`, ranking policy identifier, dataset-version identifier, effective default values, and `backtest_lifecycle` field per FR73 (default `portfolio_return`)
- the strategy definition (RPS contract version, screening parameters) is **not** re-stored on the backtest record; it is resolved through the `screen_run_id` reference per Stories 5.4 and 5.5

**Given** any future story touches backtest execution, sizing, holding, or stop-loss behavior
**When** it is drafted or reviewed
**Then** it must reference this anchor explicitly
**And** any deviation must be stated explicitly rather than implicitly regressing to the historical condition-hit model

**Given** this anchor exists
**When** Stories 5.1, 5.2, 5.3, 5.4, and 5.5 are implemented or revised
**Then** their acceptance criteria trace back to this anchor as their semantic source

**Given** legacy backtest runs created under the historical condition-hit model exist
**When** the migration runs as part of adopting this anchor
**Then** they are tagged with the literal value `legacy_condition_hit` on the `backtest_lifecycle` field defined by FR73
**And** the result-list, comparison, and aggregation surfaces never mix them into portfolio-return run statistics

## Epic 6: Product Hardening and Extension Boundaries

Harden the MVP for consistent use by enforcing trust, accessibility, security, and future-safe architectural boundaries, while preserving clean separation from post-MVP broker and market expansion concerns.

### Story 6.1: Enforce Trust, Accessibility, and Error-State Standards

As a user,
I want the product to surface trust and usability signals consistently,
So that I can use the tool confidently during daily research.

**FRs implemented:** FR48, FR49, FR50, FR51, NFR4, NFR7, NFR16, NFR17, NFR18, NFR19

**Acceptance Criteria:**

**Given** a screen, stock detail, watchlist, or backtest workflow
**When** stale data, partial data, invalid input, or a failed run occurs
**Then** the UI presents a clear explicit state instead of a silent or misleading success state

**Given** primary workflows in the web app
**When** the user navigates them with keyboard-only interaction
**Then** the main parameter, result, stock detail, and watchlist flows remain operable
**And** important pass or fail states are not communicated by color alone

### Story 6.2: Preserve Security and Future Extension Boundaries

As a developer,
I want MVP research boundaries and extension boundaries enforced,
So that future broker or market expansion work does not compromise current correctness.

**FRs implemented:** FR52, FR53, FR54, NFR11, NFR13, NFR14, NFR24

**Acceptance Criteria:**

**Given** the MVP implementation
**When** provider credentials are configured
**Then** they are handled only on the backend and never exposed to the browser

**Given** future post-MVP concerns such as broker integration or non-Japan market expansion
**When** the MVP modules are reviewed
**Then** research workflows remain structurally separate from those deferred concerns
**And** the MVP architecture still supports future addition of new strategy conditions and markets without invalidating the core workflow structure
