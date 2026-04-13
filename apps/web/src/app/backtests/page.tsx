import Link from "next/link";

import { WorkflowTrustBanner } from "@/components/shared/WorkflowTrustBanner";
import { BacktestLaunchPanel } from "@/components/backtests/BacktestLaunchPanel";
import { loadMarketDataHealth } from "@/lib/marketDataHealth";

type BacktestRun = Parameters<typeof BacktestLaunchPanel>[0]["initialRun"];

const apiBaseUrl =
  process.env.STOCKANALYSE_API_BASE_URL ??
  process.env.NEXT_PUBLIC_STOCKANALYSE_API_BASE_URL ??
  "http://127.0.0.1:8000";

export const dynamic = "force-dynamic";

async function loadLatestBacktestRun(): Promise<{ data: BacktestRun; error: string | null }> {
  try {
    const response = await fetch(`${apiBaseUrl}/backtests/runs/latest`, {
      cache: "no-store",
    });
    if (!response.ok) {
      return {
        data: null,
        error: `Unable to load the latest backtest run (${response.status}).`,
      };
    }

    const payload = (await response.json()) as { backtest_run: BacktestRun };
    return { data: payload.backtest_run, error: null };
  } catch {
    return {
      data: null,
      error: "Backtest API is unreachable. Launch will work once backend connectivity is restored.",
    };
  }
}

async function loadBacktestRuns(): Promise<{ data: NonNullable<Parameters<typeof BacktestLaunchPanel>[0]["initialRuns"]>; error: string | null }> {
  try {
    const response = await fetch(`${apiBaseUrl}/backtests/runs`, {
      cache: "no-store",
    });
    if (!response.ok) {
      return {
        data: [],
        error: `Unable to load backtest runs (${response.status}).`,
      };
    }

    const payload = (await response.json()) as { backtest_runs: NonNullable<Parameters<typeof BacktestLaunchPanel>[0]["initialRuns"]> };
    return { data: payload.backtest_runs, error: null };
  } catch {
    return {
      data: [],
      error: "Backtest run list is unreachable. Comparison will work once backend connectivity is restored.",
    };
  }
}

export default async function BacktestsPage() {
  const [{ data, error }, { data: runs, error: runsError }, { health, error: healthError }] = await Promise.all([
    loadLatestBacktestRun(),
    loadBacktestRuns(),
    loadMarketDataHealth(apiBaseUrl),
  ]);

  return (
    <main className="dashboard-shell">
      <nav className="top-nav">
        <Link href="/">Data Health</Link>
        <span>/</span>
        <Link href="/screen">Screen Configuration</Link>
        <span>/</span>
        <Link href="/watchlist">Watchlist</Link>
        <span>/</span>
        <span>Backtests</span>
      </nav>
      <WorkflowTrustBanner workflowLabel="Backtest workflow" health={health} error={healthError} />
      <BacktestLaunchPanel
        apiBaseUrl={apiBaseUrl}
        initialRun={data}
        initialRuns={runs}
        initialError={error ?? runsError}
      />
    </main>
  );
}
