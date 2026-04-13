import Link from "next/link";

import { StrategyConfigPanel } from "@/components/screen/StrategyConfigPanel";

type StrategyConfigurationResponse = {
  configuration: {
    id: number;
    version: number;
    rps_threshold: number;
    high_proximity_threshold_pct: string;
  };
  validation?: {
    rps_threshold: { min: number; max: number; default: number };
    high_proximity_threshold_pct: { min: string; max: string; default: string };
  };
};

type LoadStrategyConfigurationResult = {
  data: StrategyConfigurationResponse | null;
  error: string | null;
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
  qualified_results: Array<{
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
  }>;
};

type LoadLatestRunResult = {
  data: ScreenRun | null;
  error: string | null;
};

const apiBaseUrl =
  process.env.STOCKANALYSE_API_BASE_URL ??
  process.env.NEXT_PUBLIC_STOCKANALYSE_API_BASE_URL ??
  "http://127.0.0.1:8000";

export const dynamic = "force-dynamic";

async function loadStrategyConfiguration(): Promise<LoadStrategyConfigurationResult> {
  try {
    const response = await fetch(`${apiBaseUrl}/screen/configuration`, {
      cache: "no-store",
    });

    if (!response.ok) {
      return {
        data: null,
        error: `Unable to load strategy configuration (${response.status}).`,
      };
    }

    return {
      data: (await response.json()) as StrategyConfigurationResponse,
      error: null,
    };
  } catch {
    return {
      data: null,
      error: "Strategy configuration API is unreachable. Check backend availability and API base URL.",
    };
  }
}

async function loadLatestScreenRun(): Promise<LoadLatestRunResult> {
  try {
    const response = await fetch(`${apiBaseUrl}/screen/runs/latest`, {
      cache: "no-store",
    });

    if (!response.ok) {
      return {
        data: null,
        error: `Unable to load the latest screen run (${response.status}).`,
      };
    }

    const payload = (await response.json()) as { screen_run: ScreenRun | null };
    return {
      data: payload.screen_run,
      error: null,
    };
  } catch {
    return {
      data: null,
      error: "Latest screen run API is unreachable. Launch will still work once backend connectivity is restored.",
    };
  }
}

export default async function ScreenConfigurationPage() {
  const [{ data, error }, { data: latestRun, error: latestRunError }] = await Promise.all([
    loadStrategyConfiguration(),
    loadLatestScreenRun(),
  ]);

  return (
    <main className="dashboard-shell">
      <nav className="top-nav">
        <Link href="/">Data Health</Link>
        <span>/</span>
        <span>Screen Configuration</span>
        <span>/</span>
        <Link href="/watchlist">Watchlist</Link>
      </nav>
      <StrategyConfigPanel
        apiBaseUrl={apiBaseUrl}
        initialData={data}
        initialError={error}
        initialRun={latestRun}
        initialRunError={latestRunError}
      />
    </main>
  );
}
