"use client";

import { FormEvent, useEffect, useState } from "react";
import Link from "next/link";

import { WatchlistToggleButton } from "@/components/watchlist/WatchlistToggleButton";

type StrategyConfiguration = {
  id: number;
  version: number;
  rps_threshold: number;
  high_proximity_threshold_pct: string;
};

type StrategyConfigurationResponse = {
  configuration: StrategyConfiguration;
  validation?: {
    rps_threshold: { min: number; max: number; default: number };
    high_proximity_threshold_pct: { min: string; max: string; default: string };
  };
};

type ScreenRunResult = {
  instrument_id: number;
  symbol: string;
  exchange: string;
  trade_date: string;
  best_rps_value: string | null;
  rps_threshold: number;
  high_proximity_ratio: string | null;
  high_proximity_threshold_pct: string;
  max_drawdown_from_high_pct: string | null;
  rps_condition_passed: boolean;
  high_proximity_condition_passed: boolean;
};

type ScreenRun = {
  id: number;
  strategy_configuration_id: number;
  trade_date: string;
  executed_at: string;
  total_candidates: number;
  qualified_count: number;
  status: string;
  parameter_set: {
    id: number;
    version: number;
    rps_threshold: number;
    high_proximity_threshold_pct: string;
  };
  qualified_results: ScreenRunResult[];
};

type StrategyConfigPanelProps = {
  apiBaseUrl: string;
  initialData: StrategyConfigurationResponse | null;
  initialError: string | null;
  initialRun: ScreenRun | null;
  initialRunError: string | null;
};

type SaveState = "idle" | "saving" | "saved" | "error";
type RunState = "idle" | "running" | "ready" | "error";

function formatTimestamp(value: string): string {
  return new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

export function StrategyConfigPanel({
  apiBaseUrl,
  initialData,
  initialError,
  initialRun,
  initialRunError,
}: StrategyConfigPanelProps) {
  const [watchlistInstrumentIds, setWatchlistInstrumentIds] = useState<number[]>([]);
  const [rpsThreshold, setRpsThreshold] = useState(initialData?.configuration.rps_threshold ?? 90);
  const [highProximityThresholdPct, setHighProximityThresholdPct] = useState(
    initialData?.configuration.high_proximity_threshold_pct ?? "5.00",
  );
  const [activeVersion, setActiveVersion] = useState(initialData?.configuration.version ?? 0);
  const [message, setMessage] = useState(
    initialError ?? "Edit the thresholds, save the set, and launch a screen run.",
  );
  const [saveState, setSaveState] = useState<SaveState>(initialError ? "error" : "idle");
  const [runState, setRunState] = useState<RunState>(initialRun ? "ready" : initialRunError ? "error" : "idle");
  const [runMessage, setRunMessage] = useState(
    initialRunError ?? "No screen run has been launched from this workflow yet.",
  );
  const [latestRun, setLatestRun] = useState<ScreenRun | null>(initialRun);
  const hasLoadedConfiguration = Boolean(initialData);

  useEffect(() => {
    if (initialData) {
      setRpsThreshold(initialData.configuration.rps_threshold);
      setHighProximityThresholdPct(initialData.configuration.high_proximity_threshold_pct);
      setActiveVersion(initialData.configuration.version);
    }
  }, [initialData]);

  useEffect(() => {
    let cancelled = false;

    async function loadWatchlist() {
      try {
        const response = await fetch(`${apiBaseUrl}/watchlist`);
        if (!response.ok) {
          throw new Error();
        }

        const payload = (await response.json()) as {
          entries: Array<{ instrument_id: number }>;
        };
        if (!cancelled) {
          setWatchlistInstrumentIds(payload.entries.map((entry) => entry.instrument_id));
        }
      } catch {
        if (!cancelled) {
          setWatchlistInstrumentIds([]);
        }
      }
    }

    void loadWatchlist();

    return () => {
      cancelled = true;
    };
  }, [apiBaseUrl]);

  function validateInputs(): string | null {
    if (Number.isNaN(rpsThreshold) || rpsThreshold < 0 || rpsThreshold > 100) {
      return "RPS threshold must be between 0 and 100.";
    }

    const parsedHighThreshold = Number(highProximityThresholdPct);
    if (Number.isNaN(parsedHighThreshold) || parsedHighThreshold < 0 || parsedHighThreshold > 100) {
      return "52-week-high proximity threshold must be between 0.00 and 100.00.";
    }

    return null;
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const validationError = validateInputs();

    if (validationError) {
      setSaveState("error");
      setMessage(validationError);
      return;
    }

    setSaveState("saving");
    setMessage("Saving strategy configuration...");

    try {
      const response = await fetch(`${apiBaseUrl}/screen/configuration`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          rps_threshold: rpsThreshold,
          high_proximity_threshold_pct: highProximityThresholdPct,
        }),
      });

      if (!response.ok) {
        const payload = (await response.json()) as { detail?: string };
        throw new Error(payload.detail ?? `Request failed with ${response.status}`);
      }

      const payload = (await response.json()) as StrategyConfigurationResponse;
      setActiveVersion(payload.configuration.version);
      setRpsThreshold(payload.configuration.rps_threshold);
      setHighProximityThresholdPct(payload.configuration.high_proximity_threshold_pct);
      setSaveState("saved");
      setMessage("Configuration saved. The new parameter set is now active for the next screen run.");
    } catch (error) {
      setSaveState("error");
      setMessage(
        error instanceof Error
          ? error.message
          : "Unable to save the strategy configuration.",
      );
    }
  }

  async function handleRunScreen() {
    setRunState("running");
    setRunMessage("Launching screen run against the latest derived facts...");

    try {
      const response = await fetch(`${apiBaseUrl}/screen/runs`, {
        method: "POST",
      });

      if (!response.ok) {
        const payload = (await response.json()) as { detail?: string };
        throw new Error(payload.detail ?? `Run failed with ${response.status}`);
      }

      const payload = (await response.json()) as { screen_run: ScreenRun };
      setLatestRun(payload.screen_run);
      setRunState("ready");
      setRunMessage(
        `Run #${payload.screen_run.id} completed. ${payload.screen_run.qualified_count} qualified stock(s) out of ${payload.screen_run.total_candidates} candidates.`,
      );
    } catch (error) {
      setRunState("error");
      setRunMessage(
        error instanceof Error ? error.message : "Unable to launch the screen run.",
      );
    }
  }

  return (
    <section className="screen-panel">
      <div className="screen-panel__header">
        <p className="eyebrow">Strategy Configuration</p>
        <h1>Run the MVP screen and review the qualified list.</h1>
        <p className="hero-text">
          This workflow now covers both parameter editing and the first result-list
          surface. The list below is tied to the exact parameter set and run date
          that produced it.
        </p>
      </div>

      <div className="screen-summary-grid">
        <article className="screen-summary-card">
          <p className="status-label">Active Version</p>
          <h2>{hasLoadedConfiguration ? `v${activeVersion}` : "Unavailable"}</h2>
          <p className="status-copy">Saved parameter sets stay traceable for future run history.</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">RPS Rule</p>
          <h2>{hasLoadedConfiguration ? rpsThreshold : "Unavailable"}</h2>
          <p className="status-copy">At least one supported RPS line must meet this threshold.</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">52-Week High Rule</p>
          <h2>{hasLoadedConfiguration ? `${highProximityThresholdPct}%` : "Unavailable"}</h2>
          <p className="status-copy">Maximum allowed drawdown from the rolling 52-week high.</p>
        </article>
      </div>

      <form className="strategy-form" onSubmit={handleSubmit}>
        <label className="strategy-field">
          <span>RPS threshold</span>
          <input
            name="rps_threshold"
            type="number"
            min={0}
            max={100}
            value={rpsThreshold}
            aria-invalid={saveState === "error" && message.includes("RPS threshold")}
            onChange={(event) => setRpsThreshold(Number(event.target.value))}
          />
          <small>Integer from 0 to 100.</small>
        </label>

        <label className="strategy-field">
          <span>52-week-high proximity threshold (%)</span>
          <input
            name="high_proximity_threshold_pct"
            type="number"
            min={0}
            max={100}
            step="0.01"
            value={highProximityThresholdPct}
            aria-invalid={saveState === "error" && message.includes("52-week-high proximity threshold")}
            onChange={(event) => setHighProximityThresholdPct(event.target.value)}
          />
          <small>Percentage distance below the 52-week high, from 0.00 to 100.00.</small>
        </label>

        <div className="strategy-actions">
          <div className="strategy-button-row">
            <button type="submit" className="strategy-button" disabled={saveState === "saving"}>
              {saveState === "saving" ? "Saving..." : "Save Parameter Set"}
            </button>
            <button
              type="button"
              className="strategy-button strategy-button--secondary"
              disabled={!hasLoadedConfiguration || runState === "running"}
              onClick={handleRunScreen}
            >
              {runState === "running" ? "Running..." : "Run Screen"}
            </button>
          </div>
          <p className={`strategy-message strategy-message--${saveState}`} role={saveState === "error" ? "alert" : "status"}>{message}</p>
          <p className={`strategy-message strategy-message--${runState}`} role={runState === "error" ? "alert" : "status"}>{runMessage}</p>
        </div>
      </form>

      <section className="result-panel">
        <div className="result-panel__header">
          <p className="eyebrow">Result List</p>
          <h2>Qualified stocks with immediate context.</h2>
          <p className="status-copy">
            {latestRun
              ? `Run #${latestRun.id} executed ${formatTimestamp(latestRun.executed_at)} with parameter set v${latestRun.parameter_set.version}.`
              : "Launch a screen run to populate the result list."}
          </p>
        </div>

        {latestRun ? (
          <>
            <div className="run-metadata-grid">
              <article className="run-metadata-card">
                <p className="status-label">Run Date</p>
                <h3>{latestRun.trade_date}</h3>
              </article>
              <article className="run-metadata-card">
                <p className="status-label">Qualified</p>
                <h3>{latestRun.qualified_count}</h3>
              </article>
              <article className="run-metadata-card">
                <p className="status-label">Candidates</p>
                <h3>{latestRun.total_candidates}</h3>
              </article>
            </div>

            {latestRun.qualified_results.length ? (
              <div className="result-list">
                {latestRun.qualified_results.map((result) => (
                  <article key={`${latestRun.id}-${result.instrument_id}`} className="result-card">
                    <div className="result-card__title">
                      <div>
                        <p className="status-label">{result.exchange}</p>
                        <h3>
                          <Link
                            href={`/stocks/${result.instrument_id}?screen_run_id=${latestRun.id}`}
                            className="result-link"
                          >
                            {result.symbol}
                          </Link>
                        </h3>
                      </div>
                      <div className="result-card__actions">
                        <p className="result-pass-flag">Qualified</p>
                        <WatchlistToggleButton
                          apiBaseUrl={apiBaseUrl}
                          instrumentId={result.instrument_id}
                          symbol={result.symbol}
                          className="strategy-button strategy-button--secondary"
                          initialIsInWatchlist={watchlistInstrumentIds.includes(result.instrument_id)}
                          loadOnMount={false}
                          onToggleComplete={(nextValue) =>
                            setWatchlistInstrumentIds((current) =>
                              nextValue
                                ? [...current, result.instrument_id]
                                : current.filter((instrumentId) => instrumentId !== result.instrument_id),
                            )
                          }
                        />
                      </div>
                    </div>

                    <div className="result-summary-grid">
                      <div>
                        <dt>Best RPS</dt>
                        <dd>
                          {result.best_rps_value} vs threshold {result.rps_threshold}
                        </dd>
                      </div>
                      <div>
                        <dt>Drawdown From High</dt>
                        <dd>
                          {result.max_drawdown_from_high_pct}% vs limit {result.high_proximity_threshold_pct}%
                        </dd>
                      </div>
                    </div>

                    <ul className="signal-list">
                      <li>
                        RPS condition: {result.rps_condition_passed ? "passed" : "failed"}
                      </li>
                      <li>
                        52-week-high proximity:{" "}
                        {result.high_proximity_condition_passed ? "passed" : "failed"}
                      </li>
                      <li>High proximity ratio: {result.high_proximity_ratio}</li>
                    </ul>
                  </article>
                ))}
              </div>
            ) : (
              <p className="empty-state">
                This run completed with no qualified stocks for the current parameter set.
              </p>
            )}
          </>
        ) : (
          <p className="empty-state">No persisted screen run is available yet.</p>
        )}
      </section>
    </section>
  );
}
