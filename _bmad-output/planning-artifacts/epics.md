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
FR5: The user can run the strategy using the currently selected parameter set.  
FR6: The user can rerun the strategy after changing parameters.  
FR7: The product can preserve the parameter values used for each screen run.  
FR8: The product can preserve the parameter values used for each backtest run.  
FR9: The product can maintain a Japan equity universe for screening and backtesting.  
FR10: The product can store end-of-day historical price data for supported securities.  
FR11: The product can update historical market data for the supported universe.  
FR12: The product can expose the freshness state of the stored market data.  
FR13: The product can identify when market data for a security or date range is incomplete or unavailable.  
FR14: The product can keep screening, charting, and backtesting aligned to the same stored market dataset.  
FR15: The product can calculate 50-day, 120-day, and 250-day RPS-related values for supported securities.  
FR16: The product can determine whether at least one supported RPS line satisfies the strategy threshold condition.  
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
FR28: The user can view a candlestick chart for a supported security.  
FR29: The user can view RPS information in a panel below the main price chart.  
FR30: The product can visually distinguish RPS conditions that meet the configured threshold.  
FR31: The user can inspect chart-adjacent summaries of the strategy condition values.  
FR32: The user can review the stock's 52-week-high proximity state from the stock detail workflow.  
FR33: The product can present chart review and condition breakdown information as part of the same stock analysis flow.  
FR34: The user can add a screened security to a watchlist.  
FR35: The user can remove a security from a watchlist.  
FR36: The user can view the securities currently stored in the watchlist.  
FR37: The user can record a note for a watchlist entry.  
FR38: The user can record an observation reason for a watchlist entry.  
FR39: The product can retain the date when a security was added to the watchlist.  
FR40: The user can review saved watchlist notes, reasons, and added dates later.  
FR41: The user can launch a historical backtest for the MVP strategy.  
FR42: The user can select the historical date range used for a backtest.  
FR43: The product can run the backtest using the same parameterized conditions used by the screen.  
FR44: The product can return a reproducible backtest result for the same historical range and parameter set.  
FR45: The user can review the result of a completed backtest.  
FR46: The product can associate a backtest result with the parameter set and historical range that produced it.  
FR47: The user can use backtest outputs to compare strategy adjustments across runs.  
FR48: The user can see whether market data is current enough for routine post-close use.  
FR49: The user can see when a data update has failed, is incomplete, or may affect output trustworthiness.  
FR50: The user can identify whether a suspicious screen or backtest result may be caused by stale or incomplete data.  
FR51: The product can expose enough run and data context to investigate unexpected outputs.  
FR52: The product can keep research workflows separate from any future broker integration workflows.  
FR53: The product can support future addition of new strategy conditions without invalidating the core workflow structure.  
FR54: The product can support future expansion from Japan equities to other supported markets.

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

No UX Design document was provided for extraction.

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

FR52: Epic 6 - Product hardening and extension boundaries  
FR53: Epic 6 - Product hardening and extension boundaries  
FR54: Epic 6 - Product hardening and extension boundaries

## Epic List

### Epic 1: Project Foundation and Historical Data Backbone
Establish the monorepo structure, frontend and backend application shells, database and migration setup, Japan equity universe model, end-of-day market data ingestion, normalization, and operational data freshness visibility so all later product flows run on a trustworthy data foundation.
**FRs covered:** FR9, FR10, FR11, FR12, FR13, FR14, FR48, FR49, FR50, FR51

### Epic 2: Indicator Materialization and Screening Execution
Enable the user to define strategy parameters, compute MVP indicators, execute Japan equity screens, and persist explainable screen runs based on stored derived facts rather than ad hoc UI calculations.
**FRs covered:** FR1, FR2, FR3, FR4, FR5, FR6, FR7, FR15, FR16, FR17, FR18, FR19, FR20, FR21, FR22, FR24, FR25, FR27

### Epic 3: Stock Detail, Chart Review, and Explainability
Enable the user to open a screened stock, inspect candlestick data, review RPS panels, validate 52-week-high proximity, and understand exactly why the stock qualified.
**FRs covered:** FR23, FR26, FR28, FR29, FR30, FR31, FR32, FR33

### Epic 4: Watchlist and Research Workflow Continuity
Enable the user to capture qualified stocks into a watchlist, preserve research context through notes and observation reasons, and revisit watchlist entries as part of the daily review workflow.
**FRs covered:** FR34, FR35, FR36, FR37, FR38, FR39, FR40

### Epic 5: Backtesting and Strategy Iteration
Enable the user to run reproducible historical backtests using the same stored data and parameterized conditions as the screen, inspect results, and compare strategy adjustments across runs.
**FRs covered:** FR8, FR41, FR42, FR43, FR44, FR45, FR46, FR47

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

## Epic 2: Indicator Materialization and Screening Execution

Enable the user to define strategy parameters, compute MVP indicators, execute Japan equity screens, and persist explainable screen runs based on stored derived facts rather than ad hoc UI calculations.

### Story 2.1: Create Strategy Configuration Workflow

As a user,
I want to define and edit MVP strategy parameters,
So that I can control the screen logic without changing code.

**FRs implemented:** FR1, FR2, FR3, FR4, FR5, FR6, FR7

**Acceptance Criteria:**

**Given** the screen configuration view  
**When** the user enters an RPS threshold and a 52-week-high proximity threshold  
**Then** the system accepts and validates the parameter values for a screen run  

**Given** a saved or current parameter set  
**When** the user updates the values  
**Then** the new values are preserved for the next screen run  

### Story 2.2: Materialize RPS and 52-Week-High Derived Facts

As a system,
I want to compute and persist derived screening facts from normalized market data,
So that screening, charting, and backtesting all use the same authoritative values.

**FRs implemented:** FR14, FR15, FR16, FR17, FR18, FR20

**Acceptance Criteria:**

**Given** normalized daily market data exists  
**When** derived-facts materialization runs  
**Then** the system computes and stores 50-day, 120-day, and 250-day RPS-related values for supported securities  
**And** the system computes and stores 52-week-high proximity values for supported securities  

**Given** derived facts have been computed  
**When** later services query them  
**Then** the same stored values are available for screening, chart detail, and backtesting  

### Story 2.3: Execute Screen Runs and Persist Results

As a user,
I want to run the MVP screen against the Japan equity universe,
So that I can retrieve the stocks that satisfy the active strategy.

**FRs implemented:** FR5, FR7, FR19, FR20, FR21, FR22, FR24, FR25, FR27

**Acceptance Criteria:**

**Given** a valid parameter set and available derived facts  
**When** the user launches a screen run  
**Then** the system evaluates the configured strategy across the supported Japan equity universe  
**And** the system persists a screen run record with the parameter set and run context  

**Given** a completed screen run  
**When** results are returned  
**Then** each qualified stock is linked to the stored values that caused it to pass  

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

## Epic 3: Stock Detail, Chart Review, and Explainability

Enable the user to open a screened stock, inspect candlestick data, review RPS panels, validate 52-week-high proximity, and understand exactly why the stock qualified.

### Story 3.1: Serve Stock Detail and Chart Data from Stored Facts

As a user,
I want stock detail data assembled from the same stored dataset used by screening,
So that chart review and qualification logic stay aligned.

**FRs implemented:** FR14, FR23, FR24, FR25, FR31, FR32, FR33

**Acceptance Criteria:**

**Given** a stock from a completed screen run  
**When** the stock detail is requested  
**Then** the backend returns candlestick data, relevant RPS values, 52-week-high proximity state, and rule breakdown from stored data  

**Given** the stock detail payload  
**When** it is compared with the originating screen run  
**Then** the qualification values remain consistent with the run that produced the result  

### Story 3.2: Build Stock Detail Page with Candlestick and RPS Panels

As a user,
I want a stock detail view with price and RPS visualization,
So that I can inspect the setup visually in one workflow.

**FRs implemented:** FR23, FR28, FR29, FR30, FR31, FR32, FR33

**Acceptance Criteria:**

**Given** a selected qualified stock  
**When** the stock detail page loads  
**Then** the page displays a candlestick chart for the stock  
**And** the page displays an RPS panel below the main price chart  

**Given** the RPS panel is shown  
**When** the active threshold is applied  
**Then** RPS conditions meeting the threshold are visually distinguishable without relying on color alone  

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

**Given** the user reviews the stock detail  
**When** they inspect the explainability section  
**Then** they can determine why the stock qualified without leaving the stock analysis flow  

## Epic 4: Watchlist and Research Workflow Continuity

Enable the user to capture qualified stocks into a watchlist, preserve research context through notes and observation reasons, and revisit watchlist entries as part of the daily review workflow.

### Story 4.1: Add and Remove Qualified Stocks from the Watchlist

As a user,
I want to add or remove qualified stocks from a watchlist,
So that I can maintain a focused list of candidates worth monitoring.

**FRs implemented:** FR34, FR35, FR36

**Acceptance Criteria:**

**Given** a screened or reviewed stock  
**When** the user adds it to the watchlist  
**Then** the stock is stored as a watchlist entry linked to the canonical instrument identity  

**Given** a stock is already in the watchlist  
**When** the user removes it  
**Then** the watchlist no longer includes that entry  

### Story 4.2: Persist Watchlist Notes, Observation Reasons, and Added Dates

As a user,
I want each watchlist entry to include my research context,
So that I can remember why the stock matters later.

**FRs implemented:** FR37, FR38, FR39, FR40

**Acceptance Criteria:**

**Given** a watchlist entry  
**When** the user saves a note and observation reason  
**Then** the system persists those fields with the watchlist entry  

**Given** a watchlist entry is created  
**When** it is stored  
**Then** the system preserves the date the stock was added to the watchlist  

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

## Epic 5: Backtesting and Strategy Iteration

Enable the user to run reproducible historical backtests using the same stored data and parameterized conditions as the screen, inspect results, and compare strategy adjustments across runs.

### Story 5.1: Launch and Persist Backtest Runs

As a user,
I want to launch a historical backtest using my strategy parameters,
So that I can evaluate the strategy with historical evidence.

**FRs implemented:** FR8, FR41, FR42, FR46

**Acceptance Criteria:**

**Given** a valid strategy parameter set  
**When** the user selects a historical range and starts a backtest  
**Then** the system creates and persists a backtest run record with the parameter set and historical range  

**Given** a backtest run is started  
**When** execution takes longer than an immediate request cycle  
**Then** the system exposes an explicit in-progress state for that run  

### Story 5.2: Execute Reproducible Backtests from Stored Inputs

As a user,
I want backtests to run from the same stored facts used by screening,
So that the results are reproducible and trustworthy.

**FRs implemented:** FR43, FR44

**Acceptance Criteria:**

**Given** a backtest run with a historical range and parameter set  
**When** the system executes the backtest  
**Then** it uses the same normalized dataset and parameterized conditions as the screen logic  

**Given** the same historical range, parameter set, and stored dataset  
**When** the backtest is run again  
**Then** the system returns the same result  

### Story 5.3: Review Backtest Results and Compare Strategy Adjustments

As a user,
I want to inspect completed backtest results and compare runs,
So that I can iterate on my strategy with evidence rather than intuition alone.

**FRs implemented:** FR45, FR46, FR47

**Acceptance Criteria:**

**Given** one or more completed backtest runs  
**When** the user opens a backtest result  
**Then** the system displays the completed backtest output linked to the run record  

**Given** multiple backtest runs exist  
**When** the user reviews them  
**Then** the system provides enough run context to compare parameter adjustments across runs  

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
