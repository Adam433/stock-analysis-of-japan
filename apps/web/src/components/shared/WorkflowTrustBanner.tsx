import type { MarketDataHealthResponse } from "@/lib/marketDataHealth";

type WorkflowTrustBannerProps = {
  workflowLabel: string;
  health: MarketDataHealthResponse | null;
  error: string | null;
};

function bannerTone(health: MarketDataHealthResponse | null, error: string | null): string {
  if (error || !health) {
    return "workflow-banner--bad";
  }
  if (
    health.freshness_state === "stale" ||
    health.coverage_status === "partial" ||
    health.coverage_status === "failed"
  ) {
    return "workflow-banner--warn";
  }
  return "workflow-banner--good";
}

export function WorkflowTrustBanner({
  workflowLabel,
  health,
  error,
}: WorkflowTrustBannerProps) {
  const toneClass = bannerTone(health, error);
  const explicitState = error
    ? "connection issue"
    : !health
      ? "unavailable"
      : health.coverage_status === "failed"
        ? "failed refresh"
        : health.freshness_state === "stale"
          ? "stale data"
          : health.coverage_status === "partial"
            ? "partial coverage"
            : "trusted for routine review";

  return (
    <section className={`workflow-banner ${toneClass}`} aria-live="polite">
      <div>
        <p className="status-label">Trust State</p>
        <h2>{workflowLabel}: {explicitState}</h2>
      </div>
      <div className="workflow-banner__copy">
        {error || !health ? (
          <p className="status-copy">
            {error ?? "Data-health context is unavailable, so this workflow should not be treated as normal success."}
          </p>
        ) : (
          <>
            <p className="status-copy">
              Freshness {health.freshness_state}, coverage {health.coverage_status}, latest trade date{" "}
              {health.latest_trade_date ?? "unavailable"}.
            </p>
            <p className="status-copy">
              Partial rows {health.partial_rows}, unavailable rows {health.unavailable_rows}, last refresh{" "}
              {health.last_refresh?.status ?? "missing"}.
            </p>
          </>
        )}
      </div>
    </section>
  );
}
