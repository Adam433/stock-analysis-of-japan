import Link from "next/link";

import { WorkflowTrustBanner } from "@/components/shared/WorkflowTrustBanner";
import { StrategyConfigPanel } from "@/components/screen/StrategyConfigPanel";
import { resolveApiBaseUrl } from "@/lib/apiBaseUrl";
import { fetchWithRetry } from "@/lib/fetchWithRetry";
import { loadMarketDataHealth } from "@/lib/marketDataHealth";
import type { StrategyConfigurationResponse, ScreenRun } from "@/lib/types";

type LoadStrategyConfigurationResult = {
  data: StrategyConfigurationResponse | null;
  error: string | null;
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
    const response = await fetchWithRetry(`${apiBaseUrl}/screen/configuration`, {
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

async function loadScreenRunContext(
  apiBaseUrl: string,
  requestedRunId: string | undefined,
): Promise<LoadLatestRunResult> {
  const trimmedRunId = requestedRunId?.trim();
  if (trimmedRunId && !/^\d+$/.test(trimmedRunId)) {
    return {
      data: null,
      error: "run_id 必须是整数。",
    };
  }

  const endpoint = trimmedRunId
    ? `${apiBaseUrl}/screen/runs/${trimmedRunId}`
    : `${apiBaseUrl}/screen/runs/latest`;

  try {
    const response = await fetchWithRetry(endpoint, {
      cache: "no-store",
    });

    if (!response.ok) {
      return {
        data: null,
        error: trimmedRunId
          ? `无法加载指定的筛选结果（${response.status}）。`
          : `无法加载最近一次筛选结果（${response.status}）。`,
      };
    }

    const payload = (await response.json()) as { screen_run: ScreenRun | null };
    return {
      data: payload.screen_run,
      error:
        payload.screen_run === null
          ? trimmedRunId
            ? "指定的筛选记录不存在。"
            : null
          : null,
    };
  } catch {
    return {
      data: null,
      error: trimmedRunId
        ? "指定筛选结果接口不可达，后端恢复后即可查看来源筛选。"
        : "最近筛选结果接口不可达，后端恢复后即可正常启动筛选。",
    };
  }
}

async function loadScreeningTradeDates(apiBaseUrl: string): Promise<LoadTradeDatesResult> {
  try {
    const response = await fetchWithRetry(`${apiBaseUrl}/screen/trade-dates`, {
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

export default async function ScreenConfigurationPage(props: {
  searchParams?: Promise<{ run_id?: string }>;
}) {
  const apiBaseUrl = await resolveApiBaseUrl();
  const resolvedSearchParams = (await props?.searchParams) ?? {};
  const [
    { data, error },
    { data: latestRun, error: latestRunError },
    { data: tradeDates, error: tradeDateError },
    { health, error: healthError },
  ] = await Promise.all([
    loadStrategyConfiguration(apiBaseUrl),
    loadScreenRunContext(apiBaseUrl, resolvedSearchParams.run_id),
    loadScreeningTradeDates(apiBaseUrl),
    loadMarketDataHealth(apiBaseUrl),
  ]);

  return (
    <main id="main-content" className="dashboard-shell">
      <nav className="top-nav">
        <Link href="/">数据健康</Link>
        <span>/</span>
        <span>策略配置</span>
        <span>/</span>
        <Link href="/watchlist">观察列表</Link>
        <span>/</span>
        <Link href="/backtests">回测</Link>
        <span>/</span>
        <Link href="/x-signal-tracker">X 信号追踪</Link>
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
