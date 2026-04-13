---
stepsCompleted: [1, 2, 3, 4, 5, 6]
inputDocuments: []
workflowType: 'research'
lastStep: 1
research_type: 'technical'
research_topic: 'Web-based stock strategy backtesting tool for Japan and US tradable equities with RPS, 52-week-high proximity, and earnings surprise continuity screening'
research_goals: 'Evaluate feasible data sources and broker APIs, define architecture and implementation options for a web-based backtesting tool covering Japan and US equities, and assess how SBI Securities and Interactive Brokers Japan can fit into the design'
user_name: 'Adam'
date: '2026-04-13'
web_research_enabled: true
source_verification: true
---

# Research Report: technical

**Date:** 2026-04-13
**Author:** Adam
**Research Type:** technical

---

## Research Overview

This report evaluates the technical feasibility of building a **web-based stock strategy backtesting tool** for **Japan and US tradable equities** using **end-of-day historical data**, with a strategy centered on:

- RPS50/120/250 cross-sectional strength
- proximity to 52-week highs
- continuity of earnings surprises versus expectations

The research focused on four practical questions:

1. Which historical-data APIs or platforms are viable for Japan and US equities when real-time data is not required?
2. How should the system separate historical data, derived indicators, backtesting, and future broker connectivity?
3. Which parts of the requested strategy are straightforward to implement, and which parts are materially constrained by data availability?
4. What implementation sequence minimizes risk while still producing a useful web product quickly?

Methodology:

- current-source web verification
- preference for official documentation and primary provider sources
- cross-checking provider fit against the specific workload rather than abstract popularity
- explicit confidence statements where coverage or licensing remains uncertain

---

## Technical Research Scope Confirmation

**Research Topic:** Web-based stock strategy backtesting tool for Japan and US tradable equities with RPS, 52-week-high proximity, and earnings surprise continuity screening
**Research Goals:** Evaluate feasible historical-data sources, broker APIs, and existing tools for a web-based backtesting tool covering Japan and US equities; define architecture and implementation options; assess how Yahoo Finance/yfinance, SBI Securities, and Interactive Brokers Japan can fit into the design

**Technical Research Scope:**

- Architecture Analysis - system layering, indicator engine, screening engine, backtesting engine, and web visualization
- Implementation Approaches - batch updates, historical calculations, storage options, and operational workflows
- Technology Stack - backend, frontend, storage, charting, scheduling, and external data tooling
- Integration Patterns - market data, fundamentals, earnings surprise data, and broker API boundaries
- Performance Considerations - precomputation, caching, universe-scale scans, and responsive UI queries

**Research Methodology:**

- Current web data with rigorous source verification
- Multi-source validation for critical technical claims
- Confidence level framework for uncertain information
- Comprehensive technical coverage with architecture-specific insights

**Scope Confirmed:** 2026-04-13

---

<!-- Content will be appended sequentially through research workflow steps -->

## Technology Stack Analysis

### Programming Languages

For a web-based backtesting tool focused on end-of-day equities data, Python remains the strongest implementation language for the quantitative core, while TypeScript is the strongest choice for the web application layer. Python has the richest ecosystem for market-data ingestion, batch calculations, vectorized analytics, and backtesting frameworks. TypeScript is better suited for browser-based charting, parameter forms, job status UIs, and results exploration.

For your specific use case, the language split should be:

- **Python** for data ingestion, universe preparation, RPS calculation, 52-week-high proximity scans, earnings-surprise continuity screening, and the backtest engine.
- **TypeScript** for the web UI, API contracts, form validation, chart rendering, and user-facing backtest workflows.
- **SQL** for screening queries, incremental refresh bookkeeping, and result serving.

This split also reduces risk: the indicator and backtest logic can be tested independently from the UI, and the UI can remain thin while reading precomputed datasets and simulation results.

Confidence: High.
Sources:
- https://vectorbt.dev/
- https://www.backtrader.com/
- https://kernc.github.io/backtesting.py/doc/examples/Quick%20Start%20User%20Guide.html

### Development Frameworks and Libraries

The current market strongly favors a "Python quant core + browser UI" architecture over an all-in-one desktop quant app for this kind of project.

Relevant frameworks and libraries fall into three buckets:

- **Backtesting engines**
  - `vectorbt`: very fast, array/vectorization-oriented, strong for parameter sweeps and multi-asset research. It is especially attractive if you want to test many thresholds such as RPS cutoff, near-high percentage, and earnings-surprise lookback in bulk.
  - `backtrader`: mature and flexible, event-driven, easier to reason about when modeling order flow, rebalancing, commissions, and future broker integration.
  - `backtesting.py`: lightweight and fast for single-asset or simpler systems, but its own docs note it is not ideal for stock-picking or multi-asset portfolio rebalancing, which matters for your cross-sectional strategy.
- **Data acquisition**
  - `yfinance`: fast to prototype, easy symbol download, includes some fundamentals/analyst-related fields, but explicitly positions itself as an unofficial Yahoo wrapper intended for research and educational use, and points back to Yahoo terms stating personal-use restrictions.
- **Web visualization**
  - A JS/TS web stack with a charting library is the right fit. For K-line plus indicator panes, browser-native chart packages are more appropriate than notebook-centric plotting.

Practical implication: if you want to get a usable first version online quickly, a strong path is:

- Python service for ingestion/screening/backtest
- Lightweight HTTP API
- React/Next.js or another TS web UI
- Browser charting package for candlestick + lower indicator panels

Confidence: High for the backtesting-framework comparison; medium for the exact frontend framework choice because that depends on team preference and hosting constraints.
Sources:
- https://vectorbt.dev/getting-started/features/
- https://www.backtrader.com/
- https://kernc.github.io/backtesting.py/doc/examples/Quick%20Start%20User%20Guide.html
- https://ranaroussi.github.io/yfinance/

### Database and Storage Technologies

For this project, a relational database is a better default than a document store. The workload is highly structured:

- security master / ticker mapping
- daily OHLCV history
- splits and dividends
- fundamental snapshots
- earnings estimates and actuals
- precomputed indicators and screening states
- backtest runs and parameter sets

The main storage options break down like this:

- **PostgreSQL**
  - Best default choice for long-term maintainability.
  - Good fit for normalized market/fundamental tables plus indexed daily-series queries.
  - Easier future evolution for result history, user accounts, and saved screens.
- **SQLite / DuckDB**
  - Very attractive for a personal or single-user prototype.
  - Lower ops burden.
  - DuckDB is especially strong for analytics scans on local columnar data.
- **MySQL**
  - Usable, but your current prototype shows direct-coupled scripts and less ergonomic analytics flow compared with PostgreSQL/DuckDB options for quant workloads.

For a web application that will repeatedly draw chart panes and run stock-universe filters, a hybrid approach is likely best:

- PostgreSQL for app/system-of-record data
- Parquet/DuckDB for batch analytics and local research jobs
- object files or compressed snapshots for cached screening outputs

Confidence: Medium-high. This is an architecture recommendation inferred from the workload and the current ecosystem, not a vendor-mandated requirement.
Sources:
- https://vectorbt.dev/
- https://www.quantconnect.com/docs/v2/writing-algorithms/historical-data/asset-classes/us-equities

### Development Tools and Platforms

The main toolchain should optimize for repeatable daily refreshes, not real-time streaming.

Recommended tool categories:

- **Batch and scheduling**
  - Cron or a simple scheduler is sufficient at the beginning because you only need previous close data and post-close refresh.
  - If the project grows, a workflow orchestrator can be added later for exchange-by-exchange refresh jobs and retry handling.
- **Testing**
  - Unit tests for indicator calculations: RPS ranking, rolling 252-day high proximity, earnings-beat streak logic.
  - fixture-based regression tests for cross-market symbol normalization and delisting behavior.
- **API platform**
  - A Python API service is suitable because the quant core is already in Python.
- **Charting / browser delivery**
  - Browser-based chart rendering is the correct platform decision for your visualization requirement.

For the first production-capable version, the tooling goal should be:

- deterministic daily ingest
- deterministic recomputation of derived metrics
- stored parameterized screen snapshots
- reproducible backtest runs

Confidence: Medium-high.
Sources:
- https://vectorbt.dev/getting-started/usage/
- https://www.backtrader.com/docu/dataautoref/

### Cloud Infrastructure and Deployment

This project does not need a heavy cloud-native design on day one. Because the system is EOD-driven, a simpler deployment model is enough:

- one backend service
- one scheduled ingestion/calculation worker
- one relational database
- one web frontend

That makes a small VPS or simple container deployment viable. You do not need streaming infrastructure, low-latency pub/sub, or exchange-colocated services for the initial scope.

The only part that may need scaling is bulk recomputation across large universes:

- Japan + US equity universe scans
- recalculating RPS50/120/250 across each trading date
- materializing screen states for multiple parameter combinations

Those workloads are batch-friendly and can be handled with vectorized Python plus precomputed tables rather than distributed microservices.

Confidence: High.
Sources:
- https://vectorbt.dev/
- https://www.backtrader.com/

### Technology Adoption Trends

The strongest current pattern for a project like yours is:

- use unofficial/free data sources to prototype
- move to paid structured EOD/fundamental datasets once logic is validated
- keep broker APIs separate from historical-research data plumbing

The web research supports this pattern:

- **Yahoo Finance / yfinance**
  - Yahoo Finance provides downloadable historical data on the website, but Yahoo notes download availability depends on licensing and subscription context.
  - yfinance explicitly states it is not affiliated with Yahoo and that Yahoo data is intended for personal use only.
  - This makes Yahoo/yfinance suitable for a prototype, local testing, and rapid exploration, but risky as the long-term system-of-record for a web product.
- **Interactive Brokers (IBKR)**
  - IBKR officially supports historical bar requests through TWS/API and Client Portal workflows.
  - However, IBKR historical data has market-data subscription requirements and documented filtering/limitations, so it is better treated as a broker/integration layer than the canonical historical research dataset.
- **Structured historical-data vendors**
  - EODHD publicly markets 30+ years of end-of-day data across 60 exchanges, plus fundamentals and financial events/news APIs, which aligns well with a Japan+US EOD screening/backtest product.
  - Alpha Vantage documents global equity time series plus fundamental endpoints, earnings calendar, and earnings estimates. It is attractive for experimentation and some production use, though practical rate limits and market coverage details still need validation during integration testing.
  - Polygon is strong for US equities, but the public pricing/docs are much more US-centric and therefore less suitable as the single-provider answer for your Japan+US coverage.
  - FMP exposes historical market data and earnings surprise endpoints, but the surfaced materials in this research are clearer on US coverage than on Japan equities, so confidence is lower for using it as the unified provider.

This yields a practical recommendation:

- **Prototype phase**
  - `yfinance` or Yahoo CSV download for fast validation
- **Production-oriented phase**
  - a structured EOD vendor such as `EODHD` as the main historical/fundamental layer
  - optionally `Alpha Vantage` or `FMP` as supplementary earnings/estimate validation sources
- **Broker integration phase**
  - `IBKR Japan` for later execution/account integration
  - `SBI` only if an officially suitable API surface for your target instruments and workflow is confirmed

Confidence: High on Yahoo/yfinance and IBKR positioning; medium on exact vendor ranking between EODHD, Alpha Vantage, and FMP because that depends on hands-on symbol coverage tests for Japanese equities and analyst-estimate fields.
Sources:
- https://ranaroussi.github.io/yfinance/
- https://legal.yahoo.com/us/en/yahoo/terms/product-atos/apiforydn/index.html
- https://help.yahoo.com/kb/finance-app-for-ios/download-historical-data-yahoo-finance-sln2311.html
- https://interactivebrokers.github.io/tws-api/historical_bars.html
- https://interactivebrokers.github.io/tws-api/historical_data.html
- https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/
- https://eodhd.com/
- https://eodhd.com/lp/historical-eod-api
- https://www.alphavantage.co/documentation/
- https://polygon.io/stocks
- https://site.financialmodelingprep.com/datasets/market-data-historic
- https://site.financialmodelingprep.com/developer/docs/earnings-surprises-api

## Integration Patterns Analysis

### API Design Patterns

For your project, the correct API pattern is not "one external provider directly feeds the web app." The correct pattern is a layered integration model:

- **Provider adapters** for each external data/broker source
- **Canonical domain schema** inside your system
- **Internal service APIs** for screen results, chart data, and backtest runs

This protects the strategy engine from provider churn. It matters because Yahoo, EOD vendors, Alpha Vantage, IBKR, and possible future sources expose different identifiers, field names, date semantics, adjustment rules, and rate-limit constraints.

The cleanest boundary is:

- `market_data_provider` interface
  - daily OHLCV
  - splits/dividends
  - symbol metadata
- `fundamentals_provider` interface
  - statements
  - earnings dates
  - analyst estimate / actual / surprise fields where available
- `broker_provider` interface
  - account positions
  - order placement
  - execution reports

This means your web UI and backtest engine only talk to your own internal API, not directly to Yahoo or IBKR.

Confidence: High.
Sources:
- https://www.interactivebrokers.com/campus/api
- https://www.interactivebrokers.com/campus/ibkr-api-page/cpapi-v1/
- https://eodhd.com/financial-apis
- https://www.alphavantage.co/documentation/

### Communication Protocols

The dominant protocol split across the researched sources is:

- **REST/HTTP + JSON or CSV** for historical data, fundamentals, and batch-oriented retrieval
- **WebSocket** only for real-time/streaming use cases

Because your project only needs post-close historical data, you should bias heavily toward REST pull jobs rather than streaming subscriptions.

Protocol implications by provider:

- **EODHD**
  - JSON/HTTP API model fits nightly ingestion and incremental updates.
- **Alpha Vantage**
  - HTTP endpoints with JSON and some CSV responses; this also fits end-of-day workflows.
- **Yahoo/yfinance**
  - Python library abstracts HTTP calls, but you still depend on an unofficial access path.
- **IBKR Client Portal API**
  - RESTful API plus websocket/event-driven support, but the official docs frame it around trading functionality and live account interaction, not as a clean bulk historical warehouse substitute.
- **IBKR TWS API**
  - local gateway/socket-based style is workable for broker automation, but operationally heavier than a simple historical EOD pull API.

For your system, the internal protocol split should be:

- REST for UI-driven chart/screen/backtest requests
- background batch jobs for daily refresh
- no internal streaming requirement in the first version

Confidence: High.
Sources:
- https://www.interactivebrokers.com/campus/ibkr-api-page/cpapi-v1/
- https://www.interactivebrokers.com/campus/trading-course/ibkrs-client-portal-api/
- https://eodhd.com/financial-apis/quick-start-with-our-financial-data-apis/
- https://www.alphavantage.co/documentation/

### Data Formats and Standards

You should define one internal canonical format regardless of upstream source. This is especially important because your strategy depends on cross-sectional ranking and rolling windows, which are sensitive to symbol and date inconsistencies.

Recommended canonical structures:

- **Security master**
  - internal instrument ID
  - provider ticker
  - exchange
  - country
  - asset type
  - active/delisted flag
- **Daily price table**
  - instrument ID
  - trade date
  - open/high/low/close/adj_close/volume
  - split factor
  - dividend cash amount
  - provider source and load timestamp
- **Fundamental / earnings-surprise table**
  - fiscal period end
  - report date
  - EPS actual
  - EPS estimate
  - revenue actual
  - revenue estimate
  - surprise %
  - source quality flag

Critical normalization rules:

- store all dates in exchange-local trading date semantics
- distinguish raw close vs adjusted close
- track whether provider already adjusted for splits/dividends
- maintain provider symbol mapping because JP tickers and US tickers follow different conventions

This is more important than it looks. RPS, 52-week highs, and earnings-beat streaks all become unreliable if adjustment policies differ across providers.

Confidence: High.
Sources:
- https://ranaroussi.github.io/yfinance/advanced/price_repair.html
- https://interactivebrokers.github.io/tws-api/historical_data.html
- https://eodhd.com/financial-apis
- https://www.alphavantage.co/documentation/

### System Interoperability Approaches

The strongest interoperability approach for your project is an **ETL/adapter architecture**, not direct federation at query time.

That means:

1. Pull external data into your storage layer.
2. Normalize it into your canonical schema.
3. Compute derived metrics internally.
4. Expose only internal APIs to the UI and backtest engine.

Why this matters:

- provider APIs have different uptime and throttling behavior
- historical data can be corrected or repaired later
- backtests must be reproducible, which is hard if every run hits live third-party endpoints
- you need stable snapshots for ranking metrics like RPS by date

For interoperability with brokers:

- treat broker APIs as **execution connectors**, not historical-data primitives
- optionally import portfolio/account state into your app
- do not let broker-specific conid/order models leak into strategy and screening logic

Confidence: High.
Sources:
- https://interactivebrokers.github.io/tws-api/historical_data.html
- https://www.interactivebrokers.com/campus/ibkr-api-page/market-data-subscriptions/
- https://eodhd.com/

### Microservices Integration Patterns

A full microservices architecture is unnecessary for v1. The better pattern is:

- one application backend
- one ingestion/calculation worker
- one database
- optional task queue if refresh jobs become slow

Internally, logical modules are still useful:

- `security-master`
- `market-data`
- `fundamentals`
- `indicators`
- `screening`
- `backtesting`
- `broker-sync`

But they can remain modules inside one deployable service at first.

This is especially appropriate because your data is EOD and batch-oriented. The complexity cost of separate microservices would not buy enough value early on.

Confidence: Medium-high.
Sources:
- https://www.interactivebrokers.com/campus/api
- https://eodhd.com/financial-apis

### Event-Driven Integration

Event-driven patterns are optional rather than required.

Useful internal events could include:

- `daily_prices_loaded`
- `fundamentals_loaded`
- `earnings_surprises_loaded`
- `indicators_materialized`
- `screen_snapshot_ready`
- `backtest_completed`

These can initially be implemented without Kafka or similar infrastructure. A database job table or lightweight queue is enough.

Where event-driven thinking does help:

- triggering downstream recalculation only when fresh data lands
- avoiding full recomputation of all indicators on every run
- keeping the web UI decoupled from long-running backtests

But there is no need for a full event-streaming platform in the first implementation.

Confidence: Medium-high.
Sources:
- https://www.interactivebrokers.com/campus/ibkr-api-page/cpapi-v1/
- https://eodhd.com/financial-apis

### Integration Security Patterns

Security requirements are straightforward but non-trivial:

- keep provider API keys and broker credentials out of the browser
- all external provider access must happen server-side
- broker API credentials require a stricter boundary than market-data API keys
- backtest users should never be able to invoke raw provider calls directly

Provider-specific security observations:

- **IBKR Client Portal API**
  - official docs support OAuth, SSO, and CP Gateway-related auth options; this is much heavier than typical market-data REST APIs.
  - active account requirements apply.
- **IBKR market data**
  - API market data generally requires an active account and appropriate subscriptions, with additional requirements documented by IBKR.
- **Yahoo/yfinance**
  - because access is unofficial, operational predictability and entitlement clarity are weaker.

Security design recommendation:

- keep broker integration in a separate module/service boundary from historical data ingestion
- maintain separate secret stores for market-data providers and broker credentials
- audit all trade-intent actions even if live execution is deferred to a later phase

Confidence: High.
Sources:
- https://www.interactivebrokers.com/campus/ibkr-api-page/cpapi-v1/
- https://www.interactivebrokers.com/campus/ibkr-api-page/market-data-subscriptions/
- https://ranaroussi.github.io/yfinance/

### Broker and Provider Fit for This System

The integration research makes the positioning clearer:

- **Yahoo/yfinance**
  - best as prototype ingestion and idea-validation input
  - not the long-term canonical integration for a durable web product
- **EODHD**
  - strong fit as a primary historical-data and fundamentals adapter for a Japan+US end-of-day system
- **Alpha Vantage**
  - useful supplemental provider, especially because it exposes earnings-related endpoints in a structured way
- **IBKR Japan**
  - strong fit as later broker/execution integration
  - weak fit as the primary historical backtest warehouse because of subscription and access constraints
- **SBI**
  - based on the official public material surfaced here, the clearly documented API offering is for futures/options connectivity rather than the Japan+US cash-equity historical-data use case
  - therefore SBI should not be assumed to solve your core backtest data problem

Confidence: High on the architectural fit; medium on SBI because this conclusion is based on publicly surfaced official pages and may not cover private or partner-only offerings.
Sources:
- https://www.sbisec.co.jp/ETGate/WPLETmgR001Control?OutSide=on&burl=search_op&cat1=op&cat2=service&dir=service&file=op_service_05.html&getFlg=on
- https://www.interactivebrokers.com/campus/api
- https://www.interactivebrokers.com/campus/trading-lessons/requesting-market-data/
- https://interactivebrokers.github.io/tws-api/historical_data.html
- https://eodhd.com/
- https://www.alphavantage.co/documentation/

## Architectural Patterns and Design

### System Architecture Patterns

The best-fit architecture for your project is a **modular monolith with batch-oriented analytics**, not a desktop script bundle and not a full microservices estate.

The system naturally splits into these layers:

- **Ingestion layer**
  - pulls end-of-day prices, corporate actions, symbol metadata, fundamentals, and earnings-related data
- **Normalization layer**
  - standardizes identifiers, exchange calendars, corporate-action handling, and report-date semantics
- **Derived-data layer**
  - computes RPS50/120/250, 52-week-high proximity, and earnings-surprise continuity signals
- **Screening layer**
  - applies user thresholds and materializes daily candidate lists
- **Backtesting layer**
  - simulates rebalance rules, position management, costs, and portfolio equity curves
- **Serving/UI layer**
  - exposes screen results, chart series, and backtest outputs to the web app

This architecture maps well to end-of-day research systems because it separates expensive calculations from interactive reads. The user-facing web app should mostly query already-normalized and already-computed data, rather than recalculating long rolling windows on demand.

This is also where your current prototype falls short: the old scripts mix raw data download, storage, and indicator logic too tightly.

Confidence: High.
Sources:
- https://vectorbt.dev/getting-started/features/
- https://www.backtrader.com/
- https://www.quantconnect.com/docs/v2/writing-algorithms/securities/asset-classes/us-equity/requesting-data

### Design Principles and Best Practices

Several design principles are especially important for this project:

- **Canonical internal model first**
  - provider schemas must be translated into one internal schema before any strategy logic runs
- **Reproducibility over convenience**
  - every backtest should run against stored snapshots, not fresh provider responses
- **Corporate-action explicitness**
  - raw close, adjusted close, splits, and dividends must be modeled deliberately
- **Exchange-aware date handling**
  - Japan and US markets have different holidays and sessions; trade dates cannot be treated as a single global timeline
- **Precompute expensive cross-sectional metrics**
  - RPS requires ranking within the active universe for each date; this should be materialized, not recomputed ad hoc in UI requests

One particularly important lesson comes from LEAN/QuantConnect documentation: data normalization mode changes the meaning of the historical series. That same principle applies to your own system. If your RPS is computed on adjusted close but your 52-week-high scan or visual chart is using raw close, your signals will drift and users will not trust the tool.

Confidence: High.
Sources:
- https://www.quantconnect.com/docs/v2/writing-algorithms/securities/asset-classes/us-equity/requesting-data
- https://www.lean.io/docs/v2/lean-engine/class-reference/namespaceQuantConnect.html
- https://ranaroussi.github.io/yfinance/advanced/price_repair.html

### Scalability and Performance Patterns

The main scalability challenge is not the web frontend. It is the derived-data pipeline.

Your heaviest workloads are:

- calculating rolling 50/120/250-day returns for large universes
- ranking those returns cross-sectionally by date to produce RPS
- scanning rolling 252-day highs
- joining earnings estimate/actual data to determine consecutive beats
- running parameterized backtests over many dates and candidate lists

The right performance pattern is:

1. ingest raw daily data
2. compute rolling metrics in batch
3. materialize date-level screening facts
4. let the web layer query precomputed facts and chart-ready series

For RPS in particular, a robust architecture is:

- store per-symbol rolling return series
- for each trade date, rank all eligible symbols in the universe
- store the percentile or 0-1000 normalized rank
- derive display value from the stored normalized field rather than recomputing in charts

VectorBT's documented strength in vectorized, Numba-accelerated simulation makes it a good candidate for the research/backtest engine when you need to test many parameter combinations quickly. Backtrader remains valuable if execution-path realism becomes more important than parameter-grid throughput.

Confidence: High.
Sources:
- https://vectorbt.dev/
- https://vectorbt.dev/getting-started/features/
- https://www.backtrader.com/

### Integration and Communication Patterns

The architectural boundary between analytics and presentation should be strict.

Recommended internal service boundaries:

- `GET /instruments/:id/chart`
  - OHLCV + splits/dividends + RPS pane + high-proximity series
- `GET /screens`
  - screen definitions and parameter metadata
- `POST /screens/run`
  - runs or retrieves a screening snapshot for specific parameters and date
- `POST /backtests/run`
  - starts a backtest job
- `GET /backtests/:id`
  - returns summary statistics, equity curve, and holdings history

This is a better fit than having the browser orchestrate multiple data-source calls because:

- provider rate limits stay server-side
- symbol normalization remains centralized
- long-running computations can be asynchronous
- chart responses can be shaped specifically for your UI

Confidence: Medium-high. This is a design recommendation inferred from the workload.
Sources:
- https://www.interactivebrokers.com/campus/ibkr-api-page/cpapi-v1/
- https://vectorbt.dev/

### Security Architecture Patterns

Even before live trading, there are two distinct trust zones:

- **historical data zone**
  - API keys for market/fundamental providers
- **broker zone**
  - credentials and permissions capable of touching real accounts

These should not share the same secret lifecycle or code path.

Recommended pattern:

- broker connectors disabled by default in development
- no broker credentials in the same environment as public web serving unless required
- all trade-capable operations isolated behind explicit server-side policies and audit logging

This matters because once `IBKR Japan` is added, the system stops being "just a backtester" and becomes partially safety-sensitive.

Confidence: High.
Sources:
- https://www.interactivebrokers.com/campus/ibkr-api-page/cpapi-v1/
- https://www.interactivebrokers.com/campus/ibkr-api-page/market-data-subscriptions/

### Data Architecture Patterns

The data model should center on four durable datasets:

- **security master**
- **daily market data**
- **fundamentals / earnings surprise events**
- **derived screening facts**

Derived screening facts are the most important addition missing from the current prototype.

A practical derived-facts table would include:

- trade date
- instrument ID
- return_50 / return_120 / return_250
- rps_50 / rps_120 / rps_250
- rps_red_flags for each threshold crossing rule
- high_252
- high_proximity_ratio
- high_proximity_pass flag
- earnings_actual
- earnings_estimate
- earnings_surprise_pct
- consecutive_earnings_beats_count
- composite_strategy_pass flag under a named rule version

This architecture gives you three benefits:

- charts can read exact values shown in screens
- backtests can reference historically frozen facts
- strategy rule changes can be versioned without rewriting raw price history

Confidence: High.
Sources:
- https://www.quantconnect.com/docs/v2/writing-algorithms/datasets/quantconnect/us-equity-security-master
- https://www.quantconnect.com/docs/v2/research-environment/datasets/us-equity

### Deployment and Operations Architecture

The deployment model should start simple:

- one backend container/service
- one worker process for daily ingest and recalculation
- one PostgreSQL instance
- one web frontend

Add a task queue only when one of these becomes true:

- daily refresh jobs exceed your acceptable window
- backtest runs become too slow for synchronous requests
- multiple users begin launching overlapping computations

Operationally, the most important routine is the nightly pipeline:

1. refresh security master changes
2. ingest new OHLCV and corporate actions
3. ingest latest fundamentals / earnings events
4. recompute incremental rolling metrics
5. materialize screen facts
6. invalidate UI caches

This batch pipeline is the operational heart of the system. The web app is mostly a consumer of its outputs.

Confidence: High.
Sources:
- https://vectorbt.dev/
- https://www.backtrader.com/

### Architectural Recommendation for This Project

The architecture that best matches your requirements is:

- **Application style**
  - modular monolith
- **Core language**
  - Python for ingestion, analytics, and backtesting
- **UI**
  - TypeScript web frontend
- **Primary data flow**
  - nightly pull -> normalize -> precompute -> serve
- **Primary data source class**
  - structured EOD/fundamental vendor
- **Broker role**
  - optional later-stage execution connector

This design is more defensible than continuing to grow the current `src` prototype because it directly addresses:

- Japan + US market support
- reproducible historical screening
- web-first visualization
- future broker compatibility
- stable indicator definitions

Confidence: High.
Sources:
- https://vectorbt.dev/
- https://www.backtrader.com/
- https://www.quantconnect.com/docs/

## Implementation Approaches and Technology Adoption

### Technology Adoption Strategies

The implementation should be staged instead of trying to solve everything in one pass.

The right adoption sequence is:

1. **Build a historical screening and charting core first**
   - daily OHLCV
   - splits/dividends
   - security master
   - RPS50/120/250
   - 52-week-high proximity
2. **Add backtesting on top of frozen screening facts**
   - rebalance rules
   - commission/slippage
   - equity curve and holdings history
3. **Add earnings-surprise continuity only after the data source is validated**
   - because this is the least stable and most provider-dependent requirement
4. **Add broker integration later**
   - especially if live trading or account sync is needed

This staged path lowers risk substantially. The main reason is that your first two conditions are derived from daily price history and are fully under your control once ingested. The third condition depends on analyst-estimate coverage and historical surprise data quality, which varies by vendor and market.

Confidence: High.
Sources:
- https://www.alphavantage.co/documentation/
- https://eodhd.com/lp/fundamental-data-api
- https://site.financialmodelingprep.com/developer/docs/earnings-surprises-api

### Development Workflows and Tooling

A practical development workflow for this project is:

- **Data contracts first**
  - define canonical schemas for instruments, daily bars, earnings events, and derived facts
- **Indicator tests before UI**
  - write deterministic tests for RPS ranking, threshold coloring, and 52-week-high logic
- **Batch jobs before browser interactivity**
  - make sure nightly refresh and recomputation are stable before optimizing the frontend
- **Snapshot-backed UI**
  - the web app should consume stored output from screen runs and backtests, not ad hoc calculations

Recommended implementation modules:

- `ingest/`
- `normalize/`
- `factors/`
- `screens/`
- `backtests/`
- `api/`
- `web/`

This modular split is much better than continuing the current layout where download scripts and database calls directly embed business rules.

Confidence: High.
Sources:
- https://vectorbt.dev/getting-started/usage/
- https://www.backtrader.com/docu/

### Testing and Quality Assurance

Testing needs to focus on financial correctness rather than generic CRUD behavior.

Minimum test scope:

- **RPS**
  - verify rolling-return windows for 50/120/250
  - verify cross-sectional ranking by date
  - verify 0-1000 normalized rank and displayed `/10` value
  - verify "翻红" rule based on your chosen definition
- **52-week-high proximity**
  - verify rolling 252-day high
  - verify pass/fail at thresholds like `0.95`
- **Earnings surprise continuity**
  - verify consecutive beat counts
  - verify missing-estimate handling
  - verify that market-specific gaps do not silently produce false positives
- **Backtest execution**
  - verify next-bar execution assumptions
  - verify commissions and slippage
  - verify delisting and symbol-change handling

The research also reinforces that backtest realism requires explicit modeling of slippage and corporate actions. Backtrader’s broker docs show how slippage assumptions directly change fill prices, which is a useful reminder for your own implementation even if you do not use Backtrader as the final engine.

Confidence: High.
Sources:
- https://www.backtrader.com/docu/slippage/slippage/
- https://www.quantconnect.com/docs/v2/writing-algorithms/securities/asset-classes/us-equity/corporate-actions

### Deployment and Operations Practices

The daily batch pipeline should be treated as the primary operational product.

Recommended operational flow:

1. load security master updates
2. ingest latest daily bars and corporate actions
3. ingest/update earnings event data
4. recompute incremental factor tables
5. refresh named screens
6. expire and rebuild chart caches for affected symbols

Operational practices worth adopting early:

- job status table with retries
- per-provider load logs
- data-quality flags for missing/partial vendor responses
- immutable backtest-run records with parameter hashes

This is particularly important for Yahoo/yfinance usage. The yfinance docs themselves document repair logic for missing or incorrect Yahoo data, including cases where price data may need reconstruction. That is acceptable for experiments, but it means your pipeline should track provider quality and never assume perfect data.

Confidence: High.
Sources:
- https://ranaroussi.github.io/yfinance/advanced/price_repair.html
- https://eodhd.com/

### Team Organization and Skills

This project can be built efficiently even by one developer if the implementation order is disciplined.

Required capability areas are:

- Python data engineering
- financial time-series modeling
- SQL/data modeling
- web frontend/charting
- API and background-job operations

The highest-skill portion is not the chart UI. It is the definition and validation of historically correct screening facts across two markets.

Confidence: High.
Sources:
- https://vectorbt.dev/
- https://www.backtrader.com/

### Cost Optimization and Resource Management

Because you only need historical close-driven workflows, you can optimize cost aggressively:

- avoid real-time entitlements in v1
- avoid streaming infrastructure
- prefer one primary structured EOD provider plus one fallback/validation source
- cache chart-ready series and screen outputs
- run nightly jobs instead of intraday refresh loops

The likely cost drivers are:

- historical/fundamental vendor subscription
- storage of daily series and derived facts
- backtest compute for multi-parameter runs

Relative cost guidance:

- `Yahoo/yfinance`: lowest cost, highest operational/legal fragility
- `Alpha Vantage`: low to moderate cost, attractive for experimentation
- `EODHD`: stronger unified product fit for Japan+US EOD + fundamentals
- `IBKR`: not cost-optimal as the core historical-data warehouse because of subscription and account constraints

Confidence: Medium-high.
Sources:
- https://eodhd.com/
- https://www.alphavantage.co/documentation/
- https://help.yahoo.com/kb/index?id=SLN2311&locale=en_US&page=content&y=PROD_FIN

### Risk Assessment and Mitigation

The main implementation risks are:

- **Risk 1: earnings surprise coverage mismatch**
  - Japan and US coverage may differ significantly
  - Mitigation: treat earnings-surprise continuity as a provider-qualified module with explicit coverage checks
- **Risk 2: provider normalization differences**
  - adjusted vs raw prices may differ by source
  - Mitigation: store raw and adjusted fields separately and define one strategy normalization policy
- **Risk 3: survivorship bias**
  - current naive stock-pool construction can bias results
  - Mitigation: maintain listing/delisting-aware security master
- **Risk 4: symbol mapping complexity**
  - JP and US ticker/exchange semantics differ
  - Mitigation: internal instrument IDs and provider mapping tables
- **Risk 5: product overreach**
  - doing broker connectivity too early will slow down core research functionality
  - Mitigation: defer broker integration until historical research workflows are solid

LEAN/QuantConnect documentation is a useful benchmark here because it highlights survivorship-bias handling, corporate actions, symbol changes, and universe selection as first-class concerns. Your implementation should adopt the same seriousness even if you do not use LEAN directly.

Confidence: High.
Sources:
- https://www.lean.io/
- https://www.quantconnect.com/docs/v2/writing-algorithms/securities/asset-classes/us-equity/corporate-actions

## Technical Research Recommendations

### Implementation Roadmap

Recommended execution order:

1. Build new canonical data model and retire the old prototype scripts from `src`
2. Implement daily OHLCV ingestion for Japan and US
3. Implement corporate-action-aware return windows
4. Implement RPS50/120/250 and threshold-color logic
5. Implement 52-week-high proximity screen
6. Build chart API and web K-line + RPS pane
7. Implement backtest engine with costs/slippage
8. Add earnings-surprise continuity if and only if provider coverage is validated
9. Add broker integration only after research/backtest quality is acceptable

### Technology Stack Recommendations

Best current-fit stack:

- Python backend and analytics engine
- PostgreSQL as primary relational store
- optional DuckDB/Parquet for analytical batch workflows
- TypeScript/React web frontend
- one structured EOD/fundamental provider as primary source
- one secondary source for validation of fragile datasets such as earnings surprises

### Skill Development Requirements

Implementation requires competence in:

- historical data normalization
- cross-sectional factor computation
- corporate-action-aware backtesting
- chart-oriented API shaping
- provider contract testing

### Success Metrics and KPIs

Key success metrics:

- nightly refresh success rate
- number of symbols with complete 250-day history
- percentage of universe with valid RPS50/120/250 facts
- chart response latency for a single symbol
- backtest reproducibility for identical parameter sets
- earnings-surprise coverage ratio by market
- divergence rate between primary and validation data source on sampled symbols

## Executive Summary

The strongest technical direction is to build a **Python-centered historical research platform with a web frontend**, backed by a **canonical local data store** and a **nightly batch pipeline**. The system should not rely on broker APIs as its historical-data foundation. Instead, it should ingest end-of-day market and fundamental data from one structured data vendor, normalize it internally, compute screening facts in batch, and serve only those internal results to the UI and backtest engine.

The strategy components are not equal in implementation difficulty. `RPS` and `52-week-high proximity` are straightforward once daily OHLCV, corporate actions, and exchange calendars are modeled correctly. By contrast, `earnings surprise continuity` is the highest-risk requirement because it depends on the quality and historical depth of consensus-estimate versus actual-report data, which can vary by vendor and by market, especially for Japan. This requirement should therefore be implemented as a provider-qualified module, not as a hard dependency for the first production-capable release.

Yahoo Finance and `yfinance` are suitable for rapid prototyping and idea validation, but not a durable foundation for the long-term product because of unofficial access patterns, personal-use framing, and documented data-quality repair issues. `IBKR Japan` is best positioned as a later broker/execution integration, not as the canonical historical backtest warehouse, because official API usage depends on account status, subscriptions, and market-data entitlements. Based on the researched provider landscape, a structured EOD/fundamental vendor is the most defensible primary data layer for this project.

**Key Findings**

- The right application shape is a `modular monolith`, not a script pile and not a microservices-heavy build.
- The right operational heart is a `nightly pull -> normalize -> precompute -> serve` pipeline.
- The most important missing concept in the current prototype is a durable `derived screening facts` layer.
- Broker integration should be deferred until the historical research workflow is correct and reproducible.
- The data contract for prices, corporate actions, symbols, and earnings events matters more than the choice of frontend framework.

**Top Recommendations**

- Start with `Japan + US daily OHLCV + corporate actions + RPS + 52-week-high proximity`.
- Use one structured historical-data provider as primary source, and keep `Yahoo/yfinance` only for prototyping or cross-checking.
- Design a canonical schema before coding new ingestion logic.
- Make all backtests run on stored historical snapshots, not fresh provider responses.
- Treat earnings-surprise continuity as an optional v1.5/v2 module unless coverage is validated early.

## Table of Contents

1. Technical Research Introduction and Methodology
2. Technology Stack Analysis
3. Integration Patterns Analysis
4. Architectural Patterns and Design
5. Implementation Approaches and Technology Adoption
6. Strategic Technical Recommendations
7. Source Notes and Confidence Boundaries

## 1. Technical Research Introduction and Methodology

### Technical Research Significance

For this product category, the critical differentiator is not real-time speed but **historical correctness**. A web-based screening and backtesting tool only becomes useful if the price history, corporate actions, ranking universe, and earnings-event joins are internally consistent across time. Official documentation from Yahoo and IBKR also reinforces that access rights, licensing, and market-data entitlements materially affect what can be downloaded and how reliably it can be used.

Technical importance in this project therefore centers on:

- historical data reproducibility
- cross-market symbol normalization
- corporate-action-aware indicator construction
- stable, queryable derived facts for web visualization and backtesting

Sources:
- https://help.yahoo.com/kb/finance-app-for-ios/download-historical-data-yahoo-finance-sln2311.html
- https://www.interactivebrokers.com/campus/ibkr-api-page/market-data-subscriptions/

### Technical Research Methodology

This research used:

- official provider/API documentation where available
- provider product pages for coverage and capability assessment
- framework documentation for backtesting architecture fit
- architecture inference based on your specific workload: Japan + US equities, EOD-only, web-first display, and factor-like screening

Confidence framework used:

- **High** when conclusions are directly supported by official provider documentation and closely match your use case
- **Medium** when conclusions depend partly on vendor fit inference or uncovered hands-on validation gaps
- **Lower** when public documentation is incomplete for the target market or instrument set

### Technical Research Goals and Objectives

**Original Technical Goals:** Evaluate feasible historical-data sources, broker APIs, and existing tools for a web-based backtesting tool covering Japan and US equities; define architecture and implementation options; assess how Yahoo Finance/yfinance, SBI Securities, and Interactive Brokers Japan can fit into the design

**Achieved Technical Objectives:**

- Identified that EOD-oriented structured vendors fit the historical-data layer better than broker APIs
- Determined that Yahoo/yfinance is useful for prototype work but too fragile for long-term product centrality
- Established an architecture centered on canonical data storage and precomputed screening facts
- Isolated earnings-surprise continuity as the main data-coverage risk
- Defined a staged implementation roadmap that reduces delivery risk

## 6. Strategic Technical Recommendations

### Recommended v1 Scope

The highest-value v1 is:

- daily historical prices for Japan and US equities
- RPS50/120/250 computation and visualization
- 52-week-high proximity screening
- web-based chart and screening UI
- simple but correct portfolio backtesting on stored signals

This scope is already substantial and useful. It avoids the biggest data-quality dependency while preserving the core idea of your strategy workflow.

### Recommended v1.5 / v2 Scope

Add later:

- earnings surprise continuity
- richer parameter templates and saved screen versions
- portfolio-level analytics and factor attribution
- broker connectivity with IBKR Japan if needed

### Preferred Provider Positioning

- **Primary historical/fundamental layer**
  - structured EOD provider
- **Prototype/validation layer**
  - Yahoo Finance / `yfinance`
- **Supplementary validation layer**
  - secondary fundamentals/earnings source for sampled cross-checks
- **Broker layer**
  - `IBKR Japan`
- **Not assumed as core historical solution**
  - `SBI`, unless a suitable official stock API is concretely confirmed later

## 7. Source Notes and Confidence Boundaries

### High-Confidence Conclusions

- Yahoo historical download availability is constrained by licensing/subscription context.
- yfinance is unofficial and framed around personal/research usage.
- IBKR API market data requires account and subscription conditions.
- A broker API is not the cleanest canonical historical warehouse for your use case.
- RPS and 52-week-high logic are easier and more controllable than earnings-surprise continuity.

### Medium-Confidence Conclusions

- EODHD is likely the best-fit single provider category from the researched set, but exact fit still needs symbol-level validation on your target Japan universe.
- Alpha Vantage and FMP are useful supporting candidates, but their practical Japan coverage for your exact earnings-surprise workflow still needs direct integration tests.
- The final choice between `vectorbt` and a custom/backtrader-like engine depends on whether bulk parameter sweeps or execution realism matters more in your early milestones.

### Remaining Open Questions

- What exact rule defines `翻红` for the RPS curves in your UI and screen logic?
- Should the 52-week-high proximity use adjusted or raw close?
- What exact earnings-surprise definition do you want:
  - EPS beat only
  - EPS and revenue both beat
  - absolute beat
  - beat by minimum percentage
- How should Japan and US stock universes be filtered:
  - all common equities
  - exclude ETFs/REITs/ADR/preferreds
  - minimum liquidity rules

These questions should be resolved in the PRD and architecture phases before implementation begins.
