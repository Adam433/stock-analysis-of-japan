import Link from "next/link";

import { WorkflowTrustBanner } from "@/components/shared/WorkflowTrustBanner";
import { BacktestLaunchPanel } from "@/components/backtests/BacktestLaunchPanel";
import { resolveApiBaseUrl } from "@/lib/apiBaseUrl";
import { loadMarketDataHealth } from "@/lib/marketDataHealth";

type BacktestRun = Parameters<typeof BacktestLaunchPanel>[0]["initialRun"];

export const dynamic = "force-dynamic";

async function loadLatestBacktestRun(
  apiBaseUrl: string,
): Promise<{ data: BacktestRun; error: string | null }> {
  try {
    const response = await fetch(`${apiBaseUrl}/backtests/runs/latest`, {
      cache: "no-store",
    });
    if (!response.ok) {
      return {
        data: null,
        error: `无法加载最近一次回测记录（${response.status}）。`,
      };
    }

    const payload = (await response.json()) as { backtest_run: BacktestRun };
    return { data: payload.backtest_run, error: null };
  } catch {
    return {
      data: null,
      error: "回测接口不可达，后端恢复后即可启动新的回测。",
    };
  }
}

async function loadBacktestRuns(
  apiBaseUrl: string,
): Promise<{ data: NonNullable<Parameters<typeof BacktestLaunchPanel>[0]["initialRuns"]>; error: string | null }> {
  try {
    const response = await fetch(`${apiBaseUrl}/backtests/runs`, {
      cache: "no-store",
    });
    if (!response.ok) {
      return {
        data: [],
        error: `无法加载回测记录列表（${response.status}）。`,
      };
    }

    const payload = (await response.json()) as { backtest_runs: NonNullable<Parameters<typeof BacktestLaunchPanel>[0]["initialRuns"]> };
    return { data: payload.backtest_runs, error: null };
  } catch {
    return {
      data: [],
      error: "回测记录列表接口不可达，后端恢复后即可查看对比。",
    };
  }
}

export default async function BacktestsPage() {
  const apiBaseUrl = await resolveApiBaseUrl();
  const [{ data, error }, { data: runs, error: runsError }, { health, error: healthError }] = await Promise.all([
    loadLatestBacktestRun(apiBaseUrl),
    loadBacktestRuns(apiBaseUrl),
    loadMarketDataHealth(apiBaseUrl),
  ]);

  return (
    <main className="dashboard-shell">
      <nav className="top-nav">
        <Link href="/">数据健康</Link>
        <span>/</span>
        <Link href="/screen">策略配置</Link>
        <span>/</span>
        <Link href="/watchlist">观察列表</Link>
        <span>/</span>
        <span>回测</span>
      </nav>
      <WorkflowTrustBanner workflowLabel="回测工作流" health={health} error={healthError} />
      <BacktestLaunchPanel
        apiBaseUrl={apiBaseUrl}
        initialRun={data}
        initialRuns={runs}
        initialError={error ?? runsError}
      />
    </main>
  );
}
