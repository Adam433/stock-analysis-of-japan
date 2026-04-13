"use client";

import { FormEvent, useEffect, useState } from "react";

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

type StrategyConfigPanelProps = {
  apiBaseUrl: string;
  initialData: StrategyConfigurationResponse | null;
  initialError: string | null;
};

type SaveState = "idle" | "saving" | "saved" | "error";

export function StrategyConfigPanel({
  apiBaseUrl,
  initialData,
  initialError,
}: StrategyConfigPanelProps) {
  const [rpsThreshold, setRpsThreshold] = useState(initialData?.configuration.rps_threshold ?? 90);
  const [highProximityThresholdPct, setHighProximityThresholdPct] = useState(
    initialData?.configuration.high_proximity_threshold_pct ?? "5.00",
  );
  const [activeVersion, setActiveVersion] = useState(initialData?.configuration.version ?? 0);
  const [message, setMessage] = useState(initialError ?? "Edit the thresholds and save the set for the next screen run.");
  const [saveState, setSaveState] = useState<SaveState>(initialError ? "error" : "idle");
  const hasLoadedConfiguration = Boolean(initialData);

  useEffect(() => {
    if (initialData) {
      setRpsThreshold(initialData.configuration.rps_threshold);
      setHighProximityThresholdPct(initialData.configuration.high_proximity_threshold_pct);
      setActiveVersion(initialData.configuration.version);
    }
  }, [initialData]);

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

  return (
    <section className="screen-panel">
      <div className="screen-panel__header">
        <p className="eyebrow">Strategy Configuration</p>
        <h1>Create and refine the MVP parameter set.</h1>
        <p className="hero-text">
          This workflow owns the editable thresholds that later screening and
          backtesting stories will consume. Each save produces a new active
          version instead of mutating the old set in place.
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
          <p className="status-copy">At least one supported RPS line will need to meet this threshold.</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">52-Week High Rule</p>
          <h2>{hasLoadedConfiguration ? `${highProximityThresholdPct}%` : "Unavailable"}</h2>
          <p className="status-copy">Maximum allowed distance below the 52-week high.</p>
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
            onChange={(event) => setHighProximityThresholdPct(event.target.value)}
          />
          <small>Percentage distance below the 52-week high, from 0.00 to 100.00.</small>
        </label>

        <div className="strategy-actions">
          <button type="submit" className="strategy-button" disabled={saveState === "saving"}>
            {saveState === "saving" ? "Saving..." : "Save Parameter Set"}
          </button>
          <p className={`strategy-message strategy-message--${saveState}`}>{message}</p>
        </div>
      </form>
    </section>
  );
}
