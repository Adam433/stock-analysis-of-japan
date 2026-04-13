import Link from "next/link";

import { WorkflowTrustBanner } from "@/components/shared/WorkflowTrustBanner";
import { StrategyConfigPanel } from "@/components/screen/StrategyConfigPanel";
import { loadMarketDataHealth } from "@/lib/marketDataHealth";

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
  process.env.STOCKANALYSE_API_BASE_URL ?? "http://127.0.0.1:8000";

export const dynamic = "force-dynamic";

async function loadStrategyConfiguration(): Promise<LoadStrategyConfigurationResult> {
  try {
    const response = await fetch(`${apiBaseUrl}/screen/configuration`, {
      cache: "no-store",
    });

    if (!response.ok) {
      return {
        data: null,
        error: `无法加载策略配置（${response.status}）。`,
      };
    }

    return {
      data: (await response.json()) as StrategyConfigurationResponse,
      error: null,
    };
  } catch {
    return {
      data: null,
      error: "策略配置接口不可达，请检查后端服务与 API 地址。",
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
        error: `无法加载最近一次筛选结果（${response.status}）。`,
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
      error: "最近筛选结果接口不可达，后端恢复后即可正常启动筛选。",
    };
  }
}

export default async function ScreenConfigurationPage() {
  const [{ data, error }, { data: latestRun, error: latestRunError }, { health, error: healthError }] = await Promise.all([
    loadStrategyConfiguration(),
    loadLatestScreenRun(),
    loadMarketDataHealth(apiBaseUrl),
  ]);

  return (
    <main className="dashboard-shell">
      <nav className="top-nav">
        <Link href="/">数据健康</Link>
        <span>/</span>
        <span>策略配置</span>
        <span>/</span>
        <Link href="/watchlist">观察列表</Link>
        <span>/</span>
        <Link href="/backtests">回测</Link>
      </nav>
      <WorkflowTrustBanner workflowLabel="筛选工作流" health={health} error={healthError} />
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
