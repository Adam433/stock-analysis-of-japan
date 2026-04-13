# ADR 0001: MVP Research Boundaries

## Status

Accepted

## Context

The MVP is intentionally limited to Japan-equity end-of-day research workflows. The architecture and PRD both require:

- provider credentials remain backend-only
- browser clients never invoke privileged provider or future broker operations directly
- research workflows remain structurally separate from any future broker integration
- future market expansion must not invalidate current research semantics

## Decision

The codebase enforces the following boundaries for the MVP:

1. Web routes use server-side API base configuration only. The browser is not treated as a secret-bearing integration layer.
2. Ingestion providers declare explicit `market_scope` and `credential_boundary` metadata.
3. Provider construction goes through a registry that rejects providers that are not `backend_only` and `jp_equities_eod`.
4. Research workflows remain limited to screening, stock detail, watchlist, backtests, and data health. No broker execution modules are introduced into those flows.

## Consequences

- Future broker integration must land behind a separate backend boundary rather than being added to current research modules.
- Future multi-market support requires an intentional scope change instead of silently broadening current provider contracts.
- MVP research behavior stays reproducible and insulated from execution-side concerns.
