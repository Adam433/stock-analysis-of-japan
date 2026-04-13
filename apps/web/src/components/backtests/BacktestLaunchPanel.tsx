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
  result_summary: {
    trade_dates_evaluated: number;
    total_candidates_evaluated: number;
    qualifying_observations: number;
    unique_qualified_instruments: number;
    first_qualified_trade_date: string | null;
    last_qualified_trade_date: string | null;
    result_checksum: string | null;
  };
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
  initialRuns: BacktestRun[];
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
  initialRuns,
  initialError,
}: BacktestLaunchPanelProps) {
  const [startDate, setStartDate] = useState("2024-01-01");
  const [endDate, setEndDate] = useState("2024-12-31");
  const [latestRun, setLatestRun] = useState<BacktestRun | null>(initialRun);
  const [runs, setRuns] = useState<BacktestRun[]>(initialRuns);
  const [message, setMessage] = useState(
    initialError ?? "Select a historical range and launch a persisted backtest run.",
  );
  const [launchState, setLaunchState] = useState<"idle" | "launching" | "executing" | "ready" | "error">(
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
      setRuns((current) => [payload.backtest_run, ...current.filter((run) => run.id !== payload.backtest_run.id)]);
      setLaunchState("ready");
      setMessage(
        `Backtest run #${payload.backtest_run.id} is persisted and currently ${payload.backtest_run.status}.`,
      );
    } catch (error) {
      setLaunchState("error");
      setMessage(error instanceof Error ? error.message : "Unable to launch backtest run.");
    }
  }

  async function handleExecuteLatestRun() {
    if (!latestRun) {
      setLaunchState("error");
      setMessage("Launch a backtest run before executing it.");
      return;
    }

    setLaunchState("executing");
    setMessage(`Executing backtest run #${latestRun.id} from stored derived facts...`);

    try {
      const response = await fetch(`${apiBaseUrl}/backtests/runs/${latestRun.id}/execute`, {
        method: "POST",
      });

      if (!response.ok) {
        const payload = (await response.json()) as { detail?: string };
        throw new Error(payload.detail ?? `Request failed with ${response.status}`);
      }

      const payload = (await response.json()) as { backtest_run: BacktestRun };
      setLatestRun(payload.backtest_run);
      setRuns((current) => [payload.backtest_run, ...current.filter((run) => run.id !== payload.backtest_run.id)]);
      setLaunchState("ready");
      setMessage(
        `Backtest run #${payload.backtest_run.id} completed with checksum ${payload.backtest_run.result_summary.result_checksum ?? "unavailable"}.`,
      );
    } catch (error) {
      setLaunchState("error");
      setMessage(error instanceof Error ? error.message : "Unable to execute backtest run.");
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
            <button
              type="button"
              className="strategy-button strategy-button--secondary"
              disabled={!latestRun || launchState === "executing" || launchState === "launching"}
              onClick={handleExecuteLatestRun}
            >
              {launchState === "executing" ? "Executing..." : "Execute Latest Run"}
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

        {latestRun?.status === "completed" ? (
          <div className="run-metadata-grid backtest-summary-grid">
            <article className="run-metadata-card">
              <p className="status-label">Trade Dates</p>
              <h3>{latestRun.result_summary.trade_dates_evaluated}</h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">Qualified Snapshots</p>
              <h3>{latestRun.result_summary.qualifying_observations}</h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">Qualified Instruments</p>
              <h3>{latestRun.result_summary.unique_qualified_instruments}</h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">First / Last Qualified</p>
              <h3>
                {latestRun.result_summary.first_qualified_trade_date ?? "-"} /{" "}
                {latestRun.result_summary.last_qualified_trade_date ?? "-"}
              </h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">Checksum</p>
              <h3>{latestRun.result_summary.result_checksum ?? "Unavailable"}</h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">Candidates Evaluated</p>
              <h3>{latestRun.result_summary.total_candidates_evaluated}</h3>
            </article>
          </div>
        ) : (
          <p className="empty-state">
            Execute a persisted run to materialize a reproducible backtest summary from stored inputs.
          </p>
        )}
      </section>

      <section className="result-panel">
        <div className="result-panel__header">
          <p className="eyebrow">Result Review</p>
          <h2>Completed runs and strategy-adjustment comparison.</h2>
          <p className="status-copy">
            Review run-linked outputs and compare parameter versions, ranges, and persisted summary changes
            without leaving the backtests workflow.
          </p>
        </div>

        {runs.filter((run) => run.status === "completed").length ? (
          <>
            <div className="run-metadata-grid backtest-summary-grid">
              {runs
                .filter((run) => run.status === "completed")
                .slice(0, 2)
                .map((run) => (
                  <article key={`compare-${run.id}`} className="run-metadata-card">
                    <p className="status-label">Run #{run.id}</p>
                    <h3>v{run.parameter_set.version}</h3>
                    <p className="status-copy">
                      Range {run.start_date} to {run.end_date}
                    </p>
                    <p className="status-copy">
                      Qualified snapshots {run.result_summary.qualifying_observations} | Instruments{" "}
                      {run.result_summary.unique_qualified_instruments}
                    </p>
                    <p className="status-copy">
                      RPS {run.parameter_set.rps_threshold} / High proximity{" "}
                      {run.parameter_set.high_proximity_threshold_pct}%
                    </p>
                  </article>
                ))}
            </div>

            <div className="result-list">
              {runs
                .filter((run) => run.status === "completed")
                .map((run, index, completedRuns) => {
                  const previousRun = completedRuns[index + 1] ?? null;
                  const qualifyingDelta = previousRun
                    ? run.result_summary.qualifying_observations - previousRun.result_summary.qualifying_observations
                    : null;
                  const instrumentDelta = previousRun
                    ? run.result_summary.unique_qualified_instruments -
                      previousRun.result_summary.unique_qualified_instruments
                    : null;

                  return (
                    <article key={run.id} className="result-card">
                      <div className="result-card__title">
                        <div>
                          <p className="status-label">Completed Backtest</p>
                          <h3>Run #{run.id}</h3>
                        </div>
                        <p className="result-pass-flag">v{run.parameter_set.version}</p>
                      </div>

                      <div className="result-summary-grid">
                        <div>
                          <dt>Range</dt>
                          <dd>
                            {run.start_date} to {run.end_date}
                          </dd>
                        </div>
                        <div>
                          <dt>Qualified snapshots</dt>
                          <dd>{run.result_summary.qualifying_observations}</dd>
                        </div>
                        <div>
                          <dt>Qualified instruments</dt>
                          <dd>{run.result_summary.unique_qualified_instruments}</dd>
                        </div>
                        <div>
                          <dt>Checksum</dt>
                          <dd>{run.result_summary.result_checksum ?? "Unavailable"}</dd>
                        </div>
                      </div>

                      <ul className="signal-list">
                        <li>
                          Parameter set: RPS {run.parameter_set.rps_threshold} / high proximity{" "}
                          {run.parameter_set.high_proximity_threshold_pct}%
                        </li>
                        <li>
                          Qualified date span: {run.result_summary.first_qualified_trade_date ?? "-"} to{" "}
                          {run.result_summary.last_qualified_trade_date ?? "-"}
                        </li>
                        <li>
                          Compared with previous completed run: qualified snapshot delta{" "}
                          {qualifyingDelta === null ? "n/a" : qualifyingDelta >= 0 ? `+${qualifyingDelta}` : qualifyingDelta}
                          , instrument delta{" "}
                          {instrumentDelta === null ? "n/a" : instrumentDelta >= 0 ? `+${instrumentDelta}` : instrumentDelta}
                        </li>
                      </ul>
                    </article>
                  );
                })}
            </div>
          </>
        ) : (
          <p className="empty-state">
            Execute one or more backtest runs to unlock completed-result review and cross-run comparison.
          </p>
        )}
      </section>
    </section>
  );
}
