import Link from "next/link";

import { PortfolioReturnResultPanel } from "@/components/backtests/PortfolioReturnResultPanel";
import { WorkflowTrustBanner } from "@/components/shared/WorkflowTrustBanner";
import { resolveApiBaseUrl } from "@/lib/apiBaseUrl";
import {
  isSourceScreenRunUnavailablePayload,
  SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE,
} from "@/lib/backtestErrors";
import { fetchWithRetry } from "@/lib/fetchWithRetry";
import { loadMarketDataHealth } from "@/lib/marketDataHealth";
import type { PortfolioReturnRunResult } from "@/lib/types";

export const dynamic = "force-dynamic";

async function loadPortfolioReturnResult(
  apiBaseUrl: string,
  runId: string,
): Promise<{ data: PortfolioReturnRunResult | null; error: string | null }> {
  if (!/^\d+$/.test(runId)) {
    return { data: null, error: "runId 必须是整数。" };
  }

  try {
    const response = await fetchWithRetry(`${apiBaseUrl}/backtests/portfolio-return/runs/${runId}/result`, {
      cache: "no-store",
    });
    if (!response.ok) {
      const payload = (await response.json().catch(() => null)) as {
        detail?: string;
        error?: string;
        error_code?: string;
      } | null;
      return {
        data: null,
        error: isSourceScreenRunUnavailablePayload(payload)
          ? SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE
          : payload?.detail ?? `无法加载 portfolio-return 回测结果（${response.status}）。`,
      };
    }

    const payload = (await response.json()) as { result: PortfolioReturnRunResult };
    return { data: payload.result, error: null };
  } catch {
    return {
      data: null,
      error: "portfolio-return 回测结果接口不可达，请检查后端服务与 API 地址。",
    };
  }
}

export default async function PortfolioReturnResultPage({
  params,
}: {
  params: Promise<{ runId: string }>;
}) {
  const apiBaseUrl = await resolveApiBaseUrl();
  const { runId } = await params;
  const [{ data, error }, { health, error: healthError }] = await Promise.all([
    loadPortfolioReturnResult(apiBaseUrl, runId),
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
        <span>结果详情</span>
      </nav>
      <WorkflowTrustBanner workflowLabel="回测工作流" health={health} error={healthError} />
      <PortfolioReturnResultPanel initialResult={data} initialError={error} />
    </main>
  );
}
