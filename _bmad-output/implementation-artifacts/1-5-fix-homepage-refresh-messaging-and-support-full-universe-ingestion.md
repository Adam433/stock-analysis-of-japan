# Story 1.5: Fix Homepage Refresh Messaging and Support Full-Universe Ingestion

Status: in-progress

## Story

As a user,  
I want the homepage trust signals to describe refresh state accurately and the data backbone to ingest the full Japan equity universe from a real provider,  
so that I can trust the operational status shown on the homepage and run screening, charting, and backtesting against the market scope the product promises.

## Acceptance Criteria

1. Given the health API is reachable but no refresh run has ever been recorded, when the homepage renders refresh status, then the UI states that no refresh record exists yet and does not describe the condition as API unreachability.
2. Given stored market data exists but the latest refresh metadata is absent, when the homepage renders coverage and refresh cards, then the page distinguishes between data availability, refresh provenance, and API connectivity with explicit text.
3. Given a real Japan equity end-of-day provider is configured, when a full-universe refresh is executed, then the system ingests and normalizes the supported Japan equity universe rather than only fixture symbols.
4. Given a full-universe refresh completes with complete, partial, or failed outcomes, when the health API is requested, then the payload includes enough summary context for the UI to explain provider name, universe scope, and refresh provenance without implying success when no refresh has run.
5. Given the full-universe ingestion path is in place, when screening and stock-detail workflows run, then they continue to consume the same stored normalized dataset and no browser-side provider credentials or direct provider calls are introduced.

## Tasks / Subtasks

- [x] Correct homepage refresh-state semantics. (AC: 1, 2, 4)
  - [x] Replace the homepage fallback copy that currently conflates `last_refresh === null` with API unreachability.
  - [x] Add an explicit "no refresh recorded yet" presentation path for refresh provider, refresh status, and related trust text.
  - [x] Keep genuine API connectivity failures visually distinct from reachable-but-uninitialized backend states.
- [x] Expand health-domain payload semantics for provenance. (AC: 2, 4)
  - [x] Review `MarketDataHealthSnapshot` and extend it only as needed to communicate refresh provenance cleanly.
  - [x] Preserve backward-safe semantics for stale, partial, failed, and missing states.
  - [x] Add backend tests for "reachable API + no refresh history" and "stored data + no refresh history" cases.
- [x] Design and implement a full-universe ingestion path for Japan equities backed by local CSV + JPX universe sync. (AC: 3, 5)
  - [x] Replace fixture-only assumptions in the ingestion entrypoint with a provider contract that can enumerate or accept the supported Japan equity universe.
  - [x] Add provider configuration and operational guidance for full-universe refresh execution.
  - [x] Preserve backend-only credential handling and explicit provider metadata boundaries.
- [x] Make full-universe refresh operationally visible. (AC: 3, 4)
  - [x] Record refresh-run metadata that can explain which provider and universe scope were used.
  - [x] Ensure health responses and homepage copy remain truthful for complete, partial, failed, and never-run states.
- [x] Validate end-to-end behavior. (AC: 1, 2, 3, 4, 5)
  - [x] Run backend tests covering health-state semantics and full-universe ingestion paths.
  - [x] Run frontend lint/build for the homepage trust-state updates.
  - [x] Verify a refresh execution updates homepage trust signals without showing misleading "API unreachable" language.

## Dev Notes

- Story 1.5 extends Epic 1 because the observed issue is not only visual copy; it is a data-foundation and operational-trust mismatch between stored market data, refresh provenance, and the product's promised universe scope. [Source: _bmad-output/planning-artifacts/epics.md:262-299]
- Story 1.4 already established that freshness visibility must reflect backend-provided trust signals rather than UI inference. This story should refine those semantics, not introduce a parallel client-only interpretation layer. [Source: _bmad-output/implementation-artifacts/1-4-expose-data-freshness-and-refresh-status.md]
- The current implementation uses a static fixture provider and a symbol-list-driven refresh CLI. That is acceptable for deterministic validation but does not satisfy the user's requirement that the product cover the tradable Japan equity universe in normal operation. [Source: apps/api/src/stockanalyse_api/services/ingestion/providers/static_provider.py, apps/api/src/stockanalyse_api/jobs/refresh_market_data.py, _bmad-output/implementation-artifacts/1-3-ingest-and-normalize-japan-equity-end-of-day-data.md]
- The architecture requires provider integrations to remain inside backend ingestion services, with the browser rendering only backend-supplied facts and trust states. Real-provider work must preserve this boundary. [Source: _bmad-output/planning-artifacts/architecture.md:622-647,681-686]
- Epic 6 boundary work explicitly keeps provider credentials backend-only and constrains the MVP to Japan-equity end-of-day research. A real full-universe provider is in-scope only if it stays within those boundaries. [Source: _bmad-output/implementation-artifacts/6-2-preserve-security-and-future-extension-boundaries.md]
- The homepage currently shows provider text from `refresh?.provider ?? "接口不可达，刷新状态未知"` even when the API is reachable and `last_refresh` is simply absent. Fix the semantics at the source and in the UI copy. [Source: apps/web/src/app/page.tsx, apps/api/src/stockanalyse_api/services/health.py]

## Implementation Guidance

- Prefer additive health payload changes over breaking shape changes unless all consuming routes are updated in the same story.
- Keep fixture-based ingestion support available for tests; do not regress deterministic testability while adding the real provider path.
- Introduce explicit naming for "no refresh recorded", "refresh unavailable due to API failure", and "refresh failed after execution". These are distinct states and must remain distinct in both backend semantics and UI copy.
- If the chosen real provider has pagination, rate limits, or exchange-specific quirks, capture those rules in the provider module and operational docs rather than scattering them into route handlers or UI code.
- Preserve SQLite compatibility for local development unless a provider integration makes a broader persistence change unavoidable.

## References

- Epic story definition: [epics.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/epics.md:262)
- Previous story: [1-4-expose-data-freshness-and-refresh-status.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/implementation-artifacts/1-4-expose-data-freshness-and-refresh-status.md)
- Related hardening story: [6-1-enforce-trust-accessibility-and-error-state-standards.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/implementation-artifacts/6-1-enforce-trust-accessibility-and-error-state-standards.md)

## Open Questions

- Which real provider should be the first supported Japan equity full-universe source, and what credentials or entitlement model does it require?
- What cadence should a future remote provider use for incremental upstream pulls once backend-only credentials are introduced?

## Implementation Notes

- The current operational path is `JPX manifest sync -> local CSV provider -> normalized SQLite store`.
- The legacy local CSV source has been downgraded to an archived seed dataset under `data/archive/local_seed_csv`; the database is now the authoritative store and the remote provider is the intended path for new coverage and new dates.
- "Full universe" is now concretely defined as active TSE common stocks only.
- Homepage counts can legitimately diverge. For example, `3757 / 3836` means the JPX-approved universe has 3836 symbols while only 3757 matching CSV files currently exist in the local source directory.
- Missing coverage is therefore currently a source-completeness issue, not a homepage-calculation issue.
- Refresh runs now use batched commits and provider-aware incremental ingestion. The local CSV provider only emits rows later than each symbol's latest stored trade date, so repeat runs do not rewrite the entire database history.
- A remote `yahoo_finance_chart` provider is now available for full-universe backfill and nightly incremental updates, which removes the previous dependency on local CSV presence for missing symbols.
