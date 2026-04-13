import Link from "next/link";

import { BacktestLaunchPanel } from "@/components/backtests/BacktestLaunchPanel";

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

export default async function BacktestsPage() {
  const { data, error } = await loadLatestBacktestRun();

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
      <BacktestLaunchPanel apiBaseUrl={apiBaseUrl} initialRun={data} initialError={error} />
    </main>
  );
}
