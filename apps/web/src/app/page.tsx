type RefreshPayload = {
  status: string;
  provider: string;
  started_at: string;
  completed_at: string | null;
  rows_processed: number;
  rows_inserted: number;
  rows_updated: number;
  partial_rows: number;
  unavailable_rows: number;
  latest_trade_date: string | null;
  error_message: string | null;
  requested_symbols: string[];
};

type MarketDataHealthResponse = {
  freshness_state: string;
  latest_trade_date: string | null;
  age_in_days: number | null;
  coverage_status: string;
  total_instruments: number;
  partial_rows: number;
  unavailable_rows: number;
  last_refresh: RefreshPayload | null;
};

const workflowSteps = [
  "Ingest and normalize Japan equity end-of-day bars",
  "Track refresh outcome, partial rows, and failed runs",
  "Use stored trust signals before screening and backtesting",
];

const apiBaseUrl = process.env.STOCKANALYSE_API_BASE_URL ?? "http://127.0.0.1:8000";

export const dynamic = "force-dynamic";

async function getMarketDataHealth(): Promise<MarketDataHealthResponse | null> {
  try {
    const response = await fetch(`${apiBaseUrl}/health/market-data`, {
      cache: "no-store",
    });

    if (!response.ok) {
      return null;
    }

    return (await response.json()) as MarketDataHealthResponse;
  } catch {
    return null;
  }
}

function formatTimestamp(value: string | null): string {
  if (!value) {
    return "Not available";
  }

  return new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

function toneClassForStatus(status: string): string {
  if (status === "fresh" || status === "succeeded" || status === "complete") {
    return "status-card--good";
  }
  if (status === "partial") {
    return "status-card--warn";
  }
  return "status-card--bad";
}

export default async function HomePage() {
  const health = await getMarketDataHealth();
  const refresh = health?.last_refresh;
  const freshnessTone = toneClassForStatus(health?.freshness_state ?? "failed");
  const coverageTone = toneClassForStatus(health?.coverage_status ?? "failed");
  const refreshTone = toneClassForStatus(refresh?.status ?? "failed");

  return (
    <main className="dashboard-shell">
      <section className="hero-panel">
        <div className="hero-copy">
          <p className="eyebrow">stockAnalyse</p>
          <h1>Operational trust view for Japan equity data.</h1>
          <p className="hero-text">
            Story 1.4 turns the shell into a live health surface: freshness,
            partial coverage, and failed refresh runs are visible before any
            screening or backtest output is trusted.
          </p>
        </div>

        <div className="hero-orbit">
          {workflowSteps.map((step, index) => (
            <article key={step} className="orbit-card">
              <p className="orbit-step">Track {index + 1}</p>
              <p>{step}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="status-grid">
        <article className={`status-card ${freshnessTone}`}>
          <p className="status-label">Freshness</p>
          <h2>{health?.freshness_state ?? "unavailable"}</h2>
          <p className="status-copy">
            Latest trade date: {health?.latest_trade_date ?? "No stored data"}
          </p>
          <p className="status-meta">
            Age: {health?.age_in_days ?? "-"} day{health?.age_in_days === 1 ? "" : "s"}
          </p>
        </article>

        <article className={`status-card ${coverageTone}`}>
          <p className="status-label">Coverage</p>
          <h2>{health?.coverage_status ?? "unavailable"}</h2>
          <p className="status-copy">
            {health
              ? `${health.total_instruments} instruments tracked in stored daily bars`
              : "API not reachable, so coverage cannot be confirmed"}
          </p>
          <p className="status-meta">
            Partial rows: {health?.partial_rows ?? "-"} | Unavailable rows:{" "}
            {health?.unavailable_rows ?? "-"}
          </p>
        </article>

        <article className={`status-card ${refreshTone}`}>
          <p className="status-label">Last Refresh</p>
          <h2>{refresh?.status ?? "unavailable"}</h2>
          <p className="status-copy">
            Provider: {refresh?.provider ?? "No refresh run recorded"}
          </p>
          <p className="status-meta">
            Completed: {formatTimestamp(refresh?.completed_at ?? null)}
          </p>
        </article>
      </section>

      <section className="detail-grid">
        <article className="detail-panel">
          <p className="detail-label">Run Detail</p>
          <h3>Refresh execution state</h3>
          <dl className="detail-list">
            <div>
              <dt>Started</dt>
              <dd>{formatTimestamp(refresh?.started_at ?? null)}</dd>
            </div>
            <div>
              <dt>Completed</dt>
              <dd>{formatTimestamp(refresh?.completed_at ?? null)}</dd>
            </div>
            <div>
              <dt>Rows processed</dt>
              <dd>{refresh?.rows_processed ?? "-"}</dd>
            </div>
            <div>
              <dt>Rows inserted / updated</dt>
              <dd>
                {refresh ? `${refresh.rows_inserted} / ${refresh.rows_updated}` : "-"}
              </dd>
            </div>
          </dl>
        </article>

        <article className="detail-panel">
          <p className="detail-label">Trust Signals</p>
          <h3>What would block confidence</h3>
          <ul className="signal-list">
            <li>Freshness older than 3 days moves the dataset to stale.</li>
            <li>Any partial or unavailable bar marks coverage as partial.</li>
            <li>A failed refresh remains visible until a later run succeeds.</li>
          </ul>
        </article>

        <article className="detail-panel">
          <p className="detail-label">Scope</p>
          <h3>Requested symbols</h3>
          <p className="symbol-cloud">
            {refresh?.requested_symbols?.length
              ? refresh.requested_symbols.join(" · ")
              : "No symbol list captured yet"}
          </p>
          <p className="error-callout">
            {refresh?.error_message ?? "No refresh error recorded."}
          </p>
        </article>
      </section>
    </main>
  );
}
