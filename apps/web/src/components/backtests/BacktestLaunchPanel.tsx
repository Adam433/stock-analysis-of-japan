"use client";

import { FormEvent, useState } from "react";

type BacktestRun = {
  id: number;
  strategy_configuration_id: number;
  status: string;
  start_date: string;
  end_date: string;
  started_at: string;
  completed_at: string | null;
  error_message: string | null;
  parameter_set: {
    id: number;
    version: number;
    rps_threshold: number;
    high_proximity_threshold_pct: string;
  };
};

type BacktestLaunchPanelProps = {
  apiBaseUrl: string;
  initialRun: BacktestRun | null;
  initialError: string | null;
};

function formatTimestamp(value: string | null): string {
  if (!value) {
    return "Not completed yet";
  }

  return new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

export function BacktestLaunchPanel({
  apiBaseUrl,
  initialRun,
  initialError,
}: BacktestLaunchPanelProps) {
  const [startDate, setStartDate] = useState("2024-01-01");
  const [endDate, setEndDate] = useState("2024-12-31");
  const [latestRun, setLatestRun] = useState<BacktestRun | null>(initialRun);
  const [message, setMessage] = useState(
    initialError ?? "Select a historical range and launch a persisted backtest run.",
  );
  const [launchState, setLaunchState] = useState<"idle" | "launching" | "ready" | "error">(
    initialError ? "error" : initialRun ? "ready" : "idle",
  );

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (startDate > endDate) {
      setLaunchState("error");
      setMessage("Start date must be on or before end date.");
      return;
    }

    setLaunchState("launching");
    setMessage("Launching backtest run and persisting run context...");

    try {
      const response = await fetch(`${apiBaseUrl}/backtests/runs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          start_date: startDate,
          end_date: endDate,
        }),
      });

      if (!response.ok) {
        const payload = (await response.json()) as { detail?: string };
        throw new Error(payload.detail ?? `Request failed with ${response.status}`);
      }

      const payload = (await response.json()) as { backtest_run: BacktestRun };
      setLatestRun(payload.backtest_run);
      setLaunchState("ready");
      setMessage(
        `Backtest run #${payload.backtest_run.id} is persisted and currently ${payload.backtest_run.status}.`,
      );
    } catch (error) {
      setLaunchState("error");
      setMessage(error instanceof Error ? error.message : "Unable to launch backtest run.");
    }
  }

  return (
    <section className="screen-panel">
      <div className="screen-panel__header">
        <p className="eyebrow">Backtest Launch</p>
        <h1>Persist a historical backtest run before execution begins.</h1>
        <p className="hero-text">
          Story 5.1 establishes the backtest run record, associates it with the active parameter
          set, and surfaces an explicit in-progress state for longer-running work.
        </p>
      </div>

      <div className="screen-summary-grid">
        <article className="screen-summary-card">
          <p className="status-label">Latest Run</p>
          <h2>{latestRun ? `#${latestRun.id}` : "Unavailable"}</h2>
          <p className="status-copy">Persisted backtest run identifier for future result retrieval.</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">Status</p>
          <h2>{latestRun?.status ?? "No run"}</h2>
          <p className="status-copy">Backtest launch state remains explicit instead of implying instant completion.</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">Parameter Set</p>
          <h2>{latestRun ? `v${latestRun.parameter_set.version}` : "Unavailable"}</h2>
          <p className="status-copy">Every backtest run stays linked to the exact active strategy configuration.</p>
        </article>
      </div>

      <form className="strategy-form" onSubmit={handleSubmit}>
        <label className="strategy-field">
          <span>Start date</span>
          <input
            name="start_date"
            type="date"
            value={startDate}
            onChange={(event) => setStartDate(event.target.value)}
          />
          <small>Beginning of the historical range for this run.</small>
        </label>

        <label className="strategy-field">
          <span>End date</span>
          <input
            name="end_date"
            type="date"
            value={endDate}
            onChange={(event) => setEndDate(event.target.value)}
          />
          <small>End of the historical range for this run.</small>
        </label>

        <div className="strategy-actions">
          <div className="strategy-button-row">
            <button type="submit" className="strategy-button" disabled={launchState === "launching"}>
              {launchState === "launching" ? "Launching..." : "Launch Backtest"}
            </button>
          </div>
          <p className={`strategy-message strategy-message--${launchState === "error" ? "error" : "ready"}`}>
            {message}
          </p>
        </div>
      </form>

      <section className="result-panel">
        <div className="result-panel__header">
          <p className="eyebrow">Run Context</p>
          <h2>Most recent persisted backtest run.</h2>
        </div>
        {latestRun ? (
          <div className="run-metadata-grid">
            <article className="run-metadata-card">
              <p className="status-label">Range</p>
              <h3>
                {latestRun.start_date} to {latestRun.end_date}
              </h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">Started</p>
              <h3>{formatTimestamp(latestRun.started_at)}</h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">Completed</p>
              <h3>{formatTimestamp(latestRun.completed_at)}</h3>
            </article>
          </div>
        ) : (
          <p className="empty-state">No persisted backtest run is available yet.</p>
        )}
      </section>
    </section>
  );
}
