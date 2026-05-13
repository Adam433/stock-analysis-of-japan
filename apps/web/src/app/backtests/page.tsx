import Link from "next/link";

import { WorkflowTrustBanner } from "@/components/shared/WorkflowTrustBanner";
import { BacktestLaunchPanel } from "@/components/backtests/BacktestLaunchPanel";
import { resolveApiBaseUrl } from "@/lib/apiBaseUrl";
import { fetchWithRetry } from "@/lib/fetchWithRetry";
import { loadMarketDataHealth } from "@/lib/marketDataHealth";
import type { BacktestRun, ScreenRun } from "@/lib/types";

export const dynamic = "force-dynamic";

type LoadScreenRunContextResult = {
  data: ScreenRun | null;
  error: string | null;
};

function selectInitialRunForContext(
  latestRun: BacktestRun | null,
  runs: BacktestRun[],
  screenRunId: number | null,
): BacktestRun | null {
  if (screenRunId === null) {
    return latestRun;
  }

  const candidates = latestRun && runs.every((run) => run.id !== latestRun.id) ? [latestRun, ...runs] : runs;
  return candidates.find((run) => run.source_screen_run_id === screenRunId) ?? null;
}

async function loadLatestBacktestRun(
  apiBaseUrl: string,
): Promise<{ data: BacktestRun | null; error: string | null }> {
  try {
    const response = await fetchWithRetry(`${apiBaseUrl}/backtests/runs/latest`, {
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
): Promise<{ data: BacktestRun[]; error: string | null }> {
  try {
    const response = await fetchWithRetry(`${apiBaseUrl}/backtests/runs`, {
      cache: "no-store",
    });
    if (!response.ok) {
      return {
        data: [],
        error: `无法加载回测记录列表（${response.status}）。`,
      };
    }

    const payload = (await response.json()) as { backtest_runs: BacktestRun[] };
    return { data: payload.backtest_runs, error: null };
  } catch {
    return {
      data: [],
      error: "回测记录列表接口不可达，后端恢复后即可查看对比。",
    };
  }
}

async function loadScreenRunContext(
  apiBaseUrl: string,
  requestedScreenRunId: string | undefined,
): Promise<LoadScreenRunContextResult> {
  const trimmedScreenRunId = requestedScreenRunId?.trim();
  if (trimmedScreenRunId && !/^\d+$/.test(trimmedScreenRunId)) {
    return {
      data: null,
      error: "screen_run_id 必须是整数。",
    };
  }

  const endpoint = trimmedScreenRunId
    ? `${apiBaseUrl}/screen/runs/${trimmedScreenRunId}`
    : `${apiBaseUrl}/screen/runs/latest`;

  try {
    const response = await fetchWithRetry(endpoint, {
      cache: "no-store",
    });
    if (!response.ok) {
      return {
        data: null,
        error: trimmedScreenRunId
          ? `无法加载指定的 screen run（${response.status}）。`
          : `无法加载最近一次 screen run（${response.status}）。`,
      };
    }

    const payload = (await response.json()) as { screen_run: ScreenRun | null };
    if (payload.screen_run === null) {
      return {
        data: null,
        error: trimmedScreenRunId
          ? "指定的 screen run 不存在。"
          : "尚无已完成的 screen run，完成一次筛选后即可启动 portfolio-return 回测。",
      };
    }

    return {
      data: payload.screen_run,
      error: payload.screen_run.status === "completed"
        ? null
        : `当前 screen run #${payload.screen_run.id} 状态为 ${payload.screen_run.status}，只能从 completed 结果启动回测。`,
    };
  } catch {
    return {
      data: null,
      error: trimmedScreenRunId
        ? "指定 screen run 接口不可达，请检查后端服务与 API 地址。"
        : "最近一次 screen run 接口不可达，请检查后端服务与 API 地址。",
    };
  }
}

export default async function BacktestsPage({
  searchParams,
}: {
  searchParams: Promise<{ screen_run_id?: string }>;
}) {
  const apiBaseUrl = await resolveApiBaseUrl();
  const resolvedSearchParams = await searchParams;
  const [
    { data, error },
    { data: runs, error: runsError },
    { data: screenRun, error: screenRunError },
    { health, error: healthError },
  ] = await Promise.all([
    loadLatestBacktestRun(apiBaseUrl),
    loadBacktestRuns(apiBaseUrl),
    loadScreenRunContext(apiBaseUrl, resolvedSearchParams.screen_run_id),
    loadMarketDataHealth(apiBaseUrl),
  ]);
  const visibleRuns = data && runs.every((run) => run.id !== data.id) ? [data, ...runs] : runs;
  const legacyRunCount = visibleRuns.filter(
    (run) => run.backtest_lifecycle === "legacy_condition_hit",
  ).length;
  const screenRunId = screenRun?.status === "completed" ? screenRun.id : null;
  const contextualRun = selectInitialRunForContext(data, visibleRuns, screenRunId);

  return (
    <main id="main-content" className="dashboard-shell">
      <nav className="top-nav">
        <Link href="/">数据健康</Link>
        <span>/</span>
        <Link href="/screen">策略配置</Link>
        <span>/</span>
        <Link href="/watchlist">观察列表</Link>
        <span>/</span>
        <span>回测</span>
        <span>/</span>
        <Link href="/experiments">实验</Link>
        <span>/</span>
        <Link href="/x-signal-tracker">X 信号追踪</Link>
      </nav>
      <WorkflowTrustBanner workflowLabel="回测工作流" health={health} error={healthError} />
      {legacyRunCount ? (
        <p className="hero-text">
          当前页面检测到 {legacyRunCount} 条历史 condition-hit runs；它们会和
          portfolio-return lifecycle 分开展示，且不会进入跨-run 组合层汇总。
        </p>
      ) : null}
      <BacktestLaunchPanel
        apiBaseUrl={apiBaseUrl}
        screenRunId={screenRunId}
        initialRun={contextualRun}
        initialRuns={visibleRuns}
        initialError={screenRunError ?? error ?? runsError}
      />
    </main>
  );
}
