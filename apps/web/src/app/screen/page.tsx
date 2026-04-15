import Link from "next/link";

import { WorkflowTrustBanner } from "@/components/shared/WorkflowTrustBanner";
import { StrategyConfigPanel } from "@/components/screen/StrategyConfigPanel";
import { resolveApiBaseUrl } from "@/lib/apiBaseUrl";
import { loadMarketDataHealth } from "@/lib/marketDataHealth";

type StrategyConfigurationResponse = {
  configuration: {
    id: number;
    version: number;
    rps_threshold: number;
    high_proximity_threshold_pct: string;
    selected_rps_windows: number[];
    min_rps_lines_required: number;
  };
  validation?: {
    rps_threshold: { min: number; max: number; default: number };
    high_proximity_threshold_pct: { min: string; max: string; default: string };
    selected_rps_windows: { approved: number[]; default: number[] };
    min_rps_lines_required: { min: number; max: number; default: number };
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
    selected_rps_windows: number[];
    min_rps_lines_required: number;
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

type ScreeningTradeDateOption = {
  trade_date: string;
};

type LoadTradeDatesResult = {
  data: ScreeningTradeDateOption[];
  error: string | null;
};

export const dynamic = "force-dynamic";

async function loadStrategyConfiguration(
  apiBaseUrl: string,
): Promise<LoadStrategyConfigurationResult> {
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

async function loadLatestScreenRun(apiBaseUrl: string): Promise<LoadLatestRunResult> {
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

async function loadScreeningTradeDates(apiBaseUrl: string): Promise<LoadTradeDatesResult> {
  try {
    const response = await fetch(`${apiBaseUrl}/screen/trade-dates`, {
      cache: "no-store",
    });

    if (!response.ok) {
      return {
        data: [],
        error: `无法加载筛选交易日列表（${response.status}）。`,
      };
    }

    const payload = (await response.json()) as { trade_dates: ScreeningTradeDateOption[] };
    return {
      data: payload.trade_dates,
      error: null,
    };
  } catch {
    return {
      data: [],
      error: "筛选交易日接口不可达，将继续默认使用最新可用交易日。",
    };
  }
}

export default async function ScreenConfigurationPage() {
  const apiBaseUrl = await resolveApiBaseUrl();
  const [
    { data, error },
    { data: latestRun, error: latestRunError },
    { data: tradeDates, error: tradeDateError },
    { health, error: healthError },
  ] = await Promise.all([
    loadStrategyConfiguration(apiBaseUrl),
    loadLatestScreenRun(apiBaseUrl),
    loadScreeningTradeDates(apiBaseUrl),
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
        initialTradeDates={tradeDates}
        initialTradeDateError={tradeDateError}
      />
    </main>
  );
}
