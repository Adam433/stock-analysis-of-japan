import Link from "next/link";

import { PortfolioReturnComparePanel } from "@/components/backtests/PortfolioReturnComparePanel";
import { WorkflowTrustBanner } from "@/components/shared/WorkflowTrustBanner";
import { resolveApiBaseUrl } from "@/lib/apiBaseUrl";
import {
  isSourceScreenRunUnavailablePayload,
  SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE,
} from "@/lib/backtestErrors";
import { fetchWithRetry } from "@/lib/fetchWithRetry";
import { loadMarketDataHealth } from "@/lib/marketDataHealth";
import type { PortfolioReturnRunComparison } from "@/lib/types";

export const dynamic = "force-dynamic";

async function loadPortfolioReturnComparison(
  apiBaseUrl: string,
  requestedIds: string | undefined,
): Promise<{ data: PortfolioReturnRunComparison[]; error: string | null }> {
  const trimmedIds = requestedIds?.trim();
  if (!trimmedIds) {
    return { data: [], error: "请至少提供两个 run id 进入对比。" };
  }
  if (!/^\d+(,\d+)+$/.test(trimmedIds)) {
    return { data: [], error: "ids 必须是用逗号分隔的整数列表。" };
  }

  try {
    const response = await fetchWithRetry(
      `${apiBaseUrl}/backtests/portfolio-return/runs/compare?ids=${trimmedIds}`,
      {
        cache: "no-store",
      },
    );
    if (!response.ok) {
      const payload = (await response.json().catch(() => null)) as {
        detail?: string;
        error?: string;
        error_code?: string;
      } | null;
      return {
        data: [],
        error: isSourceScreenRunUnavailablePayload(payload)
          ? SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE
          : payload?.detail ?? `无法加载 portfolio-return 回测对比（${response.status}）。`,
      };
    }

    const payload = (await response.json()) as { runs: PortfolioReturnRunComparison[] };
    return { data: payload.runs, error: null };
  } catch {
    return {
      data: [],
      error: "portfolio-return 对比接口不可达，请检查后端服务与 API 地址。",
    };
  }
}

export default async function PortfolioReturnComparePage({
  searchParams,
}: {
  searchParams: Promise<{ ids?: string }>;
}) {
  const apiBaseUrl = await resolveApiBaseUrl();
  const resolvedSearchParams = await searchParams;
  const [{ data, error }, { health, error: healthError }] = await Promise.all([
    loadPortfolioReturnComparison(apiBaseUrl, resolvedSearchParams.ids),
    loadMarketDataHealth(apiBaseUrl),
  ]);

  return (
    <main id="main-content" className="dashboard-shell">
      <nav className="top-nav">
        <Link href="/">数据健康</Link>
        <span>/</span>
        <Link href="/screen">策略配置</Link>
        <span>/</span>
        <Link href="/backtests">回测</Link>
        <span>/</span>
        <span>跨 run 对比</span>
      </nav>
      <WorkflowTrustBanner workflowLabel="回测工作流" health={health} error={healthError} />
      <PortfolioReturnComparePanel initialRuns={data} initialError={error} />
    </main>
  );
}
