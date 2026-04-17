---
stepsCompleted:
  - step-01-init.md
  - step-02-discovery.md
  - step-02b-vision.md
  - step-02c-executive-summary.md
  - step-03-success.md
  - step-04-journeys.md
  - step-05-domain.md
  - step-07-project-type.md
  - step-08-scoping.md
  - step-09-functional.md
  - step-10-nonfunctional.md
  - step-11-polish.md
  - step-12-complete.md
inputDocuments:
  - /Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/research/technical-stock-backtesting-jp-us-research-2026-04-13.md
workflowType: 'prd'
documentCounts:
  briefCount: 0
  researchCount: 1
  brainstormingCount: 0
  projectDocsCount: 0
classification:
  projectType: web_app
  domain: fintech
  complexity: high
  projectContext: greenfield
---

# Product Requirements Document - stockAnalyse

**Author:** Adam
**Date:** 2026-04-13

## Executive Summary

stockAnalyse is a web-based stock screening and backtesting product designed first for a self-directed investor who cannot find an existing tool that matches a specific investing method closely enough. It combines historical market data, rule-based screening, visual chart validation, and backtesting in a single workflow so the user can test, refine, and reuse a personal strategy instead of adapting to generic platforms.

The MVP focuses on Japan equities and end-of-day research rather than real-time trading. Its initial strategy workflow centers on cross-sectional RPS strength and proximity to 52-week highs, with chart-level inspection and historical backtesting in the browser. Earnings surprise continuity is a planned post-MVP expansion, not an MVP dependency.

The core problem is not a lack of stock tools. The real problem is that existing tools either do not support the required strategy logic, do not expose enough flexibility to evolve that logic, or split screening, chart validation, and backtesting across separate systems. stockAnalyse exists to provide a single research workspace built around the user's own method.

### What Makes This Special

The product is differentiated by strategy ownership. Instead of forcing the user into a fixed indicator set or a generic screener, it is designed to evolve around a personally defined investing method. Users can express a strategy, inspect how each condition appears on the chart, and verify its historical behavior through backtesting inside one product.

Its first differentiator is the integration of cross-sectional screening logic with chart-native verification. The user does not only see that a stock passed a rule. The user can inspect the underlying RPS curves, threshold states, 52-week-high proximity, and exact qualifying values directly with the chart. This closes the gap between a screening result and a conviction-building review process.

The long-term differentiator is extensibility. The product is intended to become a reusable personal investing research platform where new conditions, derived metrics, and backtest rules can be added without rebuilding the workflow from scratch.

## Project Classification

- Project Type: Web App
- Domain: Fintech
- Complexity: High
- Project Context: Greenfield

## Success Criteria

### User Success

The user can define a Japan-equity screening strategy with configurable parameters, run it against historical data, inspect the result on a chart, and decide whether to add a stock to a watchlist for continued observation.

The product is successful for the primary user when it becomes part of a repeatable workflow:

1. configure strategy parameters
2. run a screen on historical or latest end-of-day data
3. inspect candlestick charts and RPS panels
4. review near-52-week-high behavior
5. add candidates to a watchlist
6. use backtest results and chart review to refine the strategy

The product must support conviction-building, not just signal generation. A passing screen result is only useful if the user can visually verify why the stock passed and whether the setup matches the intended method.

### Business Success

The product is successful as a personal-use product if it replaces the user's need to combine multiple websites and tools for the same workflow.

Within the first usable release, the product should:

- support daily use for Japan equity screening and review
- support repeated strategy adjustment without code changes for the core parameters
- reduce the need to manually switch between screeners, chart tools, and separate backtest tools

A later business success milestone is extensibility: the product can absorb new screening logic, additional metrics, and earnings-based conditions without requiring a redesign of the workflow.

### Technical Success

The system reliably ingests and updates Japan end-of-day equity data and produces reproducible screening and backtest results.

Technical success requires:

- daily data updates complete without manual repair in normal operation
- identical parameters and historical ranges produce identical backtest results
- chart data and screening outputs stay consistent with each other
- RPS and 52-week-high calculations remain stable and explainable
- the user can complete a screen or backtest in an interactive timeframe rather than waiting at hour scale

### Measurable Outcomes

- The user can run a Japan equity screen with parameter changes and receive results within minutes, not hours.
- The user can open a candidate stock and inspect price plus RPS indicators in one workflow without switching tools.
- The user can add screened candidates to a watchlist and revisit them later.
- The user can run the same backtest twice with the same settings and obtain the same result.
- The user can validate why a stock passed the screen directly from the chart and indicator display.
- Daily end-of-day updates succeed consistently enough that the tool is usable for routine post-close review.

## Product Scope

### MVP - Minimum Viable Product

- Japan equities only
- End-of-day historical data ingestion and update workflow
- Strategy parameter input from the web UI
- RPS calculation and visualization using one approved business definition across screening, charting, and backtesting, while allowing the active strategy to select from an approved set of configurable lookback windows
- threshold-based highlighting for RPS conditions using traceable backend-derived indicator history rather than fabricated frontend geometry
- 52-week-high proximity screening
- candlestick chart with RPS panel below the main chart
- screen results list
- watchlist management
- direct navigation from watchlist entries into the stock detail workflow
- historical backtesting for the supported strategy conditions
- reproducible backtest outputs for the same parameter set

### Growth Features (Post-MVP)

- earnings surprise continuity screening
- scraping-based comparison workflow for earnings-announcement-day data versus external sources
- richer watchlist review workflows
- saved strategy presets
- broader filtering rules and composite conditions
- performance analytics beyond basic backtest output
- support for additional Japanese tradable instrument filters

### Vision (Future)

- expansion from Japan equities to US equities
- a reusable personal investing research platform rather than a single-purpose screener
- continuous addition of new custom strategy logic
- deeper fundamentals and earnings-event workflows
- optional broker-side integrations after research and backtesting workflows are stable

## User Journeys

### Journey 1: Primary User - Discover, Validate, and Save a Candidate

The primary user is a self-directed investor using the product to research Japan equities after the market close. The user opens the web app with a specific strategy in mind and adjusts the screening parameters for RPS and proximity to 52-week highs. The user runs the screen and receives a list of matching stocks.

The user opens an individual stock from the result list and reviews the candlestick chart together with the RPS panel. The product shows not only that the stock passed, but why it passed. The user can inspect the exact condition values, including the relevant RPS values, threshold states, and the stock's proximity to its 52-week high. Any displayed RPS history must come from the same approved semantic definition used by screening and backtesting, so the chart remains a trustworthy verification surface rather than an illustrative approximation. This is the critical trust-building moment: the product turns a screening output into something the user can verify visually and numerically.

If the setup looks valid, the user adds the stock to a watchlist and records a note, the observation reason, and the date it was added. The journey succeeds when the user leaves the session with a smaller set of candidates worth monitoring and enough context to remember why each one matters.

### Journey 2: Primary User - Challenge a Screening Result

The primary user sometimes sees a stock in the result list that appears wrong, weak, or inconsistent with the intended method. Instead of abandoning the screen, the user opens the stock detail view to investigate.

The product shows the exact rule breakdown for the stock. The user can see which conditions passed, which thresholds were used, and the underlying values that caused the stock to qualify. The user reviews the chart, RPS behavior, and near-52-week-high context to determine whether the problem is in the strategy definition, the selected parameters, or the data itself. If the UI shows observational annotations on the RPS chart, the product must distinguish them clearly from the official screening signals.

This journey succeeds when the user can explain why the stock qualified and decide what action to take next: keep it, reject it, or refine the strategy. The emotional outcome is confidence rather than doubt.

### Journey 3: Primary User - Iterate on a Strategy Through Backtesting

The primary user does not treat the first parameter set as final. After reviewing candidates and chart behavior, the user adjusts the strategy configuration and launches a backtest using historical Japan equity data.

The product returns a reproducible backtest result for the selected date range and parameter set. The user compares the new run with prior expectations and uses the result to decide whether the strategy is becoming more usable or less reliable. The user then runs the screen again using the refined settings.

This journey succeeds when the user can move from intuition to evidence. The product helps the user evolve a personal method instead of manually repeating the same work across separate tools.

### Journey 4: Operations and Data Reliability - Keep the Research Workflow Usable

Even in a single-user product, the system has an operational journey. End-of-day data for Japan equities must update reliably. If an update fails, produces stale data, or leaves part of the universe incomplete, the user needs clear visibility into that state.

The product exposes data freshness or update status clearly enough that the user knows whether current screen and backtest outputs are trustworthy. If needed, the user can re-run an update or identify that a screen result may be based on incomplete data. The same operational view must also communicate whether the approved universe manifest is current and whether refresh execution has advanced automatically as expected.

This journey succeeds when the product avoids silent failure. The user should not have to guess whether suspicious output is caused by strategy logic or broken data.

### Journey Requirements Summary

These journeys reveal the need for the following capability areas:

- strategy parameter configuration from the web UI
- Japan equity screening runs using end-of-day data
- result lists with drill-down into stock detail
- candlestick chart plus RPS visualization
- explicit condition breakdown with the exact values that caused a stock to pass
- watchlist creation and maintenance
- watchlist notes, observation reason, and added date
- reproducible backtesting with adjustable parameters
- visibility into data freshness, universe manifest freshness, and refresh execution health
- investigation workflow for suspicious or unexpected results

## Domain-Specific Requirements

### Compliance & Regulatory

The product is a research and backtesting tool, not an order-execution or brokerage system in its MVP scope. The product must avoid presenting itself as an execution platform, account-management tool, or automated advisory service.

The product must clearly communicate that screening and backtest outputs are research artifacts derived from historical data and user-defined parameters. Product language and UI behavior must not imply guaranteed outcomes, portfolio advice, or broker-like execution capabilities.

### Technical Constraints

The product must preserve data integrity across screening, charting, watchlist review, and backtesting. A stock that passes a screen must be explainable from the same stored data used by the chart and the backtest engine.

The system must surface stale, incomplete, or failed updates instead of presenting normal-looking results without warning.

The product must use a consistent market-data normalization policy for Japan equities. RPS calculations, 52-week-high proximity checks, chart rendering, and backtest logic must use consistent definitions for dates, adjustments, and security identity.

### Integration Requirements

The product must support a provider model where historical market data can be ingested, stored, and normalized locally before use in screening and backtesting.

The MVP does not require broker integration. Any later broker integration must remain separate from the historical research data layer so that execution concerns do not change the meaning or reproducibility of research outputs.

Any later earnings-surprise workflow that depends on scraping or third-party sites must mark source provenance clearly and distinguish inferred or partial data from fully structured provider data.

### Risk Mitigations

The product must reduce false confidence. Users need to see why a stock passed a rule, what values caused that result, and whether the underlying data was current and complete.

The product must avoid silent failure in three areas:

- stale or partial market data
- inconsistent indicator calculations across views
- non-reproducible backtest results

The product must make it possible to investigate suspicious outputs by exposing the condition breakdown, parameter values, and relevant chart context for each qualified stock.

## Web App Specific Requirements

### Project-Type Overview

stockAnalyse is a browser-based research product for Japan equity screening, chart validation, watchlist tracking, and historical backtesting. It is an interactive web application rather than a content site or SEO-oriented property. The product should prioritize fast parameter iteration, chart inspection, and smooth movement between screen results, stock detail, and backtest output.

### Technical Architecture Considerations

The web application should behave like an interactive single-page experience for the core workflow. Users need to move quickly between parameter configuration, result lists, stock detail views, watchlist updates, and backtest results without page-heavy transitions.

The application must keep chart rendering, condition breakdowns, and screening outputs synchronized.

Because the product is end-of-day driven rather than real-time, the frontend does not require streaming infrastructure. It does require clear status communication for data freshness, universe manifest freshness, refresh execution status, backtest progress, and failed or incomplete updates.

### Browser Matrix

The MVP must support current desktop versions of Chrome, Safari, and Edge.

The MVP should also support mobile and tablet browser access for review workflows, but desktop remains the primary operating environment for parameter editing, chart analysis, and backtesting.

### Responsive Design

The product must be usable on desktop first and adapt cleanly to smaller screens. On narrower screens, the layout may reduce density, but the user must still be able to:

- review screening results
- open a stock detail view
- inspect key condition values
- read watchlist notes and observation reasons

Chart usability on smaller screens must remain adequate for review, even if deep strategy editing is optimized for desktop.

### Performance Targets

The interface must feel interactive for core research tasks. Parameter updates, screen launches, stock detail transitions, and watchlist actions should complete in a timeframe that supports focused research sessions.

The product does not need millisecond behavior, but it must avoid long blocking waits and unclear loading states. If a screen or backtest requires more time, the UI must show explicit progress or status.

### SEO Strategy

SEO is not required for the MVP. The product is a logged-in or user-directed research tool rather than a discovery-oriented public website.

### Accessibility Level

The MVP should meet practical usability standards rather than enterprise-grade compliance targets. At minimum:

- primary actions must be keyboard reachable
- important values shown by color must also have textual or structural indicators
- chart-adjacent signal summaries must be readable outside the chart itself
- forms for parameters, notes, and watchlist editing must be clear and operable without relying only on visual styling

### Implementation Considerations

The web layer must expose rule breakdown, threshold values, and relevant signal states in a readable structure near the chart and stock detail content.

The watchlist workflow must behave like a research notebook rather than a bookmark list. Adding a stock must support note entry, observation reason, and recorded added date as first-class fields.

## Project Scoping & Phased Development

### MVP Strategy & Philosophy

**MVP Approach:** Problem-solving MVP for a single primary user. The goal is not broad market launch, but to create a reliable Japan-equity research workflow that replaces the user's current patchwork of tools.

**Resource Requirements:** One technically capable builder can deliver the MVP if scope remains constrained to Japan equities, end-of-day data, RPS, 52-week-high proximity, watchlist workflows, and reproducible backtesting. Additional help becomes more valuable when expanding into richer data acquisition, advanced UX polish, or multi-market support.

### MVP Feature Set (Phase 1)

**Core User Journeys Supported:**

- run a Japan equity screen with configurable strategy parameters
- inspect why a stock passed by viewing chart, indicators, and rule breakdown
- save candidates to a watchlist with note, reason, added date, and direct drill-down to stock detail
- rerun the strategy as a backtest and refine parameters based on results
- verify whether current outputs are trustworthy based on data freshness, universe manifest freshness, and refresh execution status

**Must-Have Capabilities:**

- Japan equities only
- end-of-day data ingestion, storage, and refresh workflow
- parameterized screening UI
- configurable approved RPS window calculation and display
- threshold highlighting and pass/fail logic for RPS conditions
- 52-week-high proximity calculation and pass/fail logic
- stock result list and stock detail workflow
- candlestick chart with RPS panel below the main chart
- explicit rule breakdown with exact values for each qualified stock
- watchlist management with note, observation reason, added date, and direct stock-detail access
- historical backtesting for the MVP strategy conditions
- reproducible backtest outputs
- visible data freshness, universe manifest freshness, and update health status

### Post-MVP Features

**Phase 2 (Post-MVP):**

- earnings surprise continuity screening
- scraping-assisted earnings comparison workflows
- saved strategy presets
- stronger watchlist review and filtering workflows
- richer backtest analytics and comparative result views
- broader condition composition beyond the initial strategy

**Phase 3 (Expansion):**

- US equity support
- broader custom strategy framework
- deeper fundamentals workflows
- broker-side integration after research workflows are stable
- platform evolution from single-strategy tool to general personal investing research workspace

### Risk Mitigation Strategy

**Technical Risks:**

- historical data quality or normalization errors could invalidate screens and backtests
- the explainability layer could diverge from the actual screening engine
- earnings-surprise workflows are likely to be data-fragile

**Mitigation Approach:**

- keep MVP limited to Japan equities and end-of-day workflows
- use one consistent normalization policy across charting, screening, and backtesting
- defer earnings-surprise continuity until after core screening and backtesting are stable
- require rule breakdown visibility for every qualified stock

**Market Risks:**

- the product may solve the workflow technically but still feel slower or more awkward than existing tools
- the product may generate outputs that are correct but not trusted

**Validation Approach:**

- optimize the MVP around the user's actual daily research loop
- treat trust and explainability as core value, not secondary polish
- measure success by replacement of existing manual workflow, not by feature count

**Resource Risks:**

- scope expansion into US markets, broker integration, and richer fundamentals could delay a usable release
- overbuilding the platform too early could slow the delivery of a useful v1

**Contingency Approach:**

- lock MVP to one market and two core strategy conditions
- keep broker integration out of MVP
- keep platform architecture extensible, but keep v1 feature scope narrow

## Functional Requirements

### Strategy Configuration

- FR1: The user can create a screening configuration for Japan equities.
- FR2: The user can edit the parameter values of a screening configuration.
- FR3: The user can define the RPS threshold used by the strategy.
- FR4: The user can define the 52-week-high proximity threshold used by the strategy.
- FR5: The user can run the strategy using either parameters supplied directly from the current form or a saved configuration version, without requiring the form parameters to be saved as a configuration first.
- FR6: The user can rerun the strategy after changing parameters.
- FR7: The product can preserve an independent parameter snapshot on each screen run that records the exact values used, regardless of whether those values came from an ad-hoc form submission or a saved configuration version.
- FR8: The product can preserve the parameter values used for each backtest run.
- FR62: The product distinguishes between the user's current form parameters, the currently active saved configuration, and the parameter snapshot that governed a specific screen run, and exposes this distinction in run-detail and result-list traceability surfaces.
- FR63: The product can save the current form parameters as a new configuration version through an explicit user action that is independent from launching a screen run, and rejects no-op saves whose values are identical to the latest existing version.

### Market Data & Universe

- FR9: The product can maintain a Japan equity universe for screening and backtesting.
- FR10: The product can store end-of-day historical price data for supported securities.
- FR11: The product can update historical market data for the supported universe.
- FR12: The product can expose the freshness state, universe manifest freshness, and refresh execution state of the stored market data.
- FR13: The product can identify when market data for a security or date range is incomplete or unavailable.
- FR14: The product can keep screening, charting, and backtesting aligned to the same stored market dataset.

### Indicator & Signal Evaluation

- FR15: The product can calculate approved RPS-related values for supported securities for the configured lookback windows required by screening, chart review, and backtesting, while preserving a documented business definition, a fixed ranking universe policy, and a consistent normalization rule.
- FR16: The product can determine whether every user-selected RPS line satisfies the strategy threshold condition using the same approved definition and data semantics used by charting and backtesting.
- FR17: The product can calculate each security's proximity to its 52-week high.
- FR18: The product can determine whether a security satisfies the configured 52-week-high proximity condition.
- FR19: The product can evaluate whether a security passes the full MVP strategy based on the active conditions.
- FR20: The product can retain the indicator and condition values that caused a security to pass or fail a run.

### Screening Results

- FR21: The user can run a screen across the supported Japan equity universe.
- FR22: The product can return the list of securities that satisfy the active screen conditions.
- FR23: The user can open an individual screening result for deeper inspection.
- FR24: The product can show the exact rule breakdown for a screened security.
- FR25: The product can show the underlying values used to determine whether each condition passed.
- FR26: The user can tell from the result detail why a stock qualified for the screen.
- FR27: The product can associate each result set with the run date and parameter set that produced it.
- FR55: The user can select an available historical trade date for a screening run when reviewing past market states.
- FR65: The user can review a screened security's price chart, valuation indicators (PE, PB), and fiscal-year net income history directly inside the screening result panel, without navigating to the stock detail page.
- FR66: The screening result panel can incrementally load the inline analysis data of additional result cards as the user scrolls, rather than loading every card's analysis payload at first render; result sets below an explicit threshold may load all at once.
- FR67: The product reuses the existing charting library and stock-detail data contracts to render inline screening-result analysis cards, extending those contracts where required for valuation and fiscal-year data, and does not introduce a separate visualization framework for that purpose.

### Chart Review & Explainability

- FR28: The user can view a candlestick chart for a supported security with sufficient historical context for routine chart review.
- FR29: The user can view RPS information in a panel below the main price chart using traceable backend-derived indicator history without important recent data being obscured by fixed labels.
- FR30: The product can visually distinguish RPS conditions that meet the configured threshold while keeping official screening signals separate from explanatory-only chart annotations.
- FR31: The user can inspect chart-adjacent summaries of the strategy condition values.
- FR32: The user can review the stock's 52-week-high proximity state from the stock detail workflow.
- FR33: The product can present chart review and condition breakdown information as part of the same stock analysis flow.

### Watchlist Management

- FR34: The user can add a screened security to a watchlist.
- FR35: The user can remove a security from a watchlist.
- FR36: The user can view the securities currently stored in the watchlist and navigate from an entry into the corresponding stock detail workflow.
- FR37: The user can record a note for a watchlist entry.
- FR38: The user can record an observation reason for a watchlist entry.
- FR39: The product can retain the date when a security was added to the watchlist.
- FR40: The user can review saved watchlist notes, reasons, and added dates later.
- FR64: When a user adds a stock to the watchlist from a screen result, the product automatically attaches the originating screen_run_id and screen trade date to that watchlist entry; if the same canonical instrument is added from a different screen run, the product updates the entry's attached screen_run_id and trade date to the most recent addition rather than creating a duplicate entry; if the entry is added from a non-screening surface (e.g., stock detail), the screen_run_id is recorded as null and the UI distinguishes that case explicitly.

### Backtesting

- FR41: The user can launch a portfolio-return backtest that takes a completed screen run as its input and simulates the subsequent performance of the qualified securities under an explicit entry, sizing, holding, and stop-loss policy.
- FR42: The user can select the historical date range used for a backtest.
- FR43: The product can run the backtest using the same parameterized conditions used by the screen.
- FR44: The product can return a reproducible backtest result for the same input screen run, holding parameters, stop-loss parameters, and stored dataset, and records a dataset-version identifier on every backtest run so that later corrections to the underlying market data are detectable rather than silently changing historical outputs.
- FR45: The user can review the result of a completed backtest, including portfolio cumulative return, win rate (defined as the share of closed positions whose realized return is strictly greater than zero), maximum drawdown (defined as the largest peak-to-trough decline of the portfolio equity curve), the portfolio equity curve, and the per-security return distribution.
- FR46: The product can associate a backtest result with the originating screen_run_id, strategy parameter snapshot, holding parameters, stop-loss parameters, portfolio cap, ranking policy used for cap exclusion, and simulation date range that produced it.
- FR47: The user can use portfolio-return backtest outputs to compare strategy adjustments across runs, including differences in holding period, stop-loss threshold, portfolio cap, and the source screen run.
- FR68: The product executes backtest entries at the opening price of the trading day immediately after the screening trade date (T+1 open) and does not use post-close information from the screening day itself to simulate an entry; if T+1 is a non-trading day for that security or its T+1 open price is unavailable, the product defers entry to the next trading day with a valid open price within a configurable entry-deferral window measured in trading days (MVP default = 5 trading days); if a security is suspended, halted, delisted, or undergoes a corporate action that invalidates a tradable open price across the entire deferral window, the product excludes that security from the simulated portfolio and records the exclusion reason on the run.
- FR69: The product sizes backtest positions using equal weighting across the qualified securities up to a configurable portfolio cap; when the qualified set exceeds the cap, the product retains the top entries ranked by descending RPS composite score (with ticker as a deterministic tie-breaker) as the MVP ranking policy and records the policy identifier on the run; the product allows fractional share sizing as an MVP simplification; the product does not rebalance, re-enter, or add positions once the initial entry is executed; if the qualified set is empty after exclusions, the product produces a fully populated empty-portfolio result rather than failing.
- FR70: The product enforces a configurable per-security stop-loss threshold measured against each position's own entry price; the breach signal is computed using the daily adjusted close price as the single authoritative input; on breach, the product closes that position at the next trading day's opening price (including any gap below the stop-loss level); if the next trading day is a halt, suspension, or otherwise lacks a valid open, the product defers closure to the next available trading day with a valid open price; the released cash is not redeployed within the same backtest.
- FR71: The user can configure the backtest holding period (in trading days), per-security stop-loss threshold, portfolio cap, and entry-deferral window (in trading days) at launch time; the MVP ships explicit default values of 20 trading days holding, -8% per-security stop loss, 20-security portfolio cap, and 5 trading days entry-deferral window, validates that holding period is a positive integer, stop-loss threshold lies in (-1, 0), portfolio cap is an integer ≥ 1, and entry-deferral window is an integer ≥ 1, and persists the effective values on every backtest run.
- FR72: The user can launch a backtest as a single action that both persists the backtest record and executes the simulation; the launch action is debounced so a duplicate click before the first response does not produce a second run; if persistence succeeds but execution does not start, the resulting run is marked as failed with a recoverable status so the user can retry from the same record; the MVP does not expose a separate "execute" action for an already-created run and does not expose a cancel action mid-execution.
- FR73: The product preserves a `backtest_lifecycle` field on every backtest run record whose values include `portfolio_return` (the MVP default for any run produced by the portfolio-return execution model) and `legacy_condition_hit` (any run that predates the portfolio-return execution model); the schema migration that introduces this field backfills all pre-existing backtest run records with `legacy_condition_hit` rather than leaving the field null, and the result-list, comparison, and aggregation surfaces never mix the two lifecycle classes into a single portfolio-return statistic.

### Data Health & Operational Visibility

- FR48: The user can see whether market data and the approved universe manifest are current enough for routine post-close use.
- FR49: The user can see when a data update has failed, is incomplete, stale, or has not been automatically advanced as expected.
- FR50: The user can identify whether a suspicious screen or backtest result may be caused by stale or incomplete data.
- FR51: The product can expose enough run and data context to investigate unexpected outputs.
- FR56: The user can configure which approved RPS lookback windows participate in the active screening rule.
- FR57: The user can configure which selected RPS lines participate in the threshold condition for a security to qualify.
- FR58: The backend can trigger or maintain refresh execution state automatically at startup and on the expected daily cadence.
- FR59: The product can display the last-updated timestamp of the approved universe manifest without exposing unnecessary local file path details in the primary UI.
- FR60: The product can present chart dates in a localized, date-only format appropriate for the primary user workflow.
- FR61: The user can navigate directly from a watchlist entry to the corresponding stock detail workflow.

### Product Boundary & Future Extension

- FR52: The product can keep research workflows separate from any future broker integration workflows.
- FR53: The product can support future addition of new strategy conditions without invalidating the core workflow structure.
- FR54: The product can support future expansion from Japan equities to other supported markets.

## Non-Functional Requirements

### Performance

- The system shall return a completed Japan equity screen within 5 minutes for normal end-of-day usage under the MVP data universe.
- The system shall open a stock detail view, including chart-ready data and condition breakdown, within 3 seconds for 95% of requests under normal usage.
- The system shall persist watchlist add, edit, and remove actions within 2 seconds for 95% of requests under normal usage.
- The system shall present explicit in-progress status for screen and backtest operations that cannot complete within 3 seconds.
- The system shall avoid hour-scale waits for routine screening and backtesting tasks in the MVP workflow.
- NFR25: The system shall render the first batch of inline analysis cards on the screening result panel within 3 seconds for result sets up to 50 qualified securities under normal usage; for result sets above 50, the system shall render the result-list skeleton (without inline analysis payloads) within 3 seconds and then progressively populate analysis cards as the user scrolls.

### Reliability

- The system shall produce identical backtest outputs for identical historical ranges, parameter sets, and underlying stored datasets.
- The system shall detect and surface failed, partial, or stale market-data updates before those outputs are presented as normal screening or backtest results.
- The system shall preserve the parameter set, run context, and output association for every screening run and backtest run.
- The system shall prevent silent divergence between screening outputs, chart views, and condition breakdown values derived from the same stored dataset.
- The system shall retain watchlist entries, notes, observation reasons, and added dates without loss during normal operation.

### Security

- The system shall restrict all provider credentials and any future broker credentials to server-side storage and execution paths.
- The system shall encrypt sensitive credentials and secrets at rest and in transit.
- The system shall prevent browser clients from directly invoking privileged provider or broker operations with embedded secrets.
- The system shall maintain separate credential boundaries for historical-data providers and any future broker integrations.
- The system shall record sufficient server-side logs to investigate failed updates, failed runs, and data integrity issues.

### Accessibility

- The system shall support keyboard access to primary workflows including parameter editing, result navigation, stock detail access, and watchlist editing.
- The system shall not rely on color alone to communicate whether a condition passed or failed.
- The system shall provide text-visible summaries for key signal states displayed on or near charts.
- The system shall keep parameter forms, watchlist forms, and result details readable and operable on supported desktop browsers without requiring pointer-only interaction.

### Integration & Data Integrity

- The system shall apply one consistent market-data normalization policy across screening, charting, and backtesting within the MVP scope.
- The system shall identify the source and freshness of stored market data used for screening and backtesting.
- The system shall distinguish complete data, partial data, and unavailable data states in a way the user can inspect.
- The system shall preserve traceability from each qualified stock result back to the stored values and thresholds that produced it.
- The system shall keep future broker integration concerns isolated from the MVP research data workflows so that research reproducibility is not degraded.
- The authoritative MVP definition for RPS semantics, ranking universe, price policy, and non-computable handling is frozen in `_bmad-output/planning-artifacts/rps-semantics-contract.md`.
