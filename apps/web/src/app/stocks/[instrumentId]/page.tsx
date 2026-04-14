import Link from "next/link";

import { WorkflowTrustBanner } from "@/components/shared/WorkflowTrustBanner";
import { StockDetailView } from "@/components/stocks/StockDetailView";
import { resolveApiBaseUrl } from "@/lib/apiBaseUrl";
import { loadMarketDataHealth } from "@/lib/marketDataHealth";

type StockDetailResponse = {
  stock_detail: Parameters<typeof StockDetailView>[0]["detail"];
};

export const dynamic = "force-dynamic";

async function loadStockDetail(
  apiBaseUrl: string,
  instrumentId: string,
  screenRunId: string | undefined,
): Promise<{ data: StockDetailResponse["stock_detail"] | null; error: string | null }> {
  if (!screenRunId) {
    return {
      data: null,
      error: "缺少 screen_run_id，无法将个股详情与对应的筛选结果对齐。",
    };
  }

  try {
    const response = await fetch(
      `${apiBaseUrl}/stocks/${instrumentId}/detail?screen_run_id=${screenRunId}`,
      { cache: "no-store" },
    );

    if (!response.ok) {
      return {
        data: null,
        error: `无法加载个股详情（${response.status}）。`,
      };
    }

    const payload = (await response.json()) as StockDetailResponse;
    return { data: payload.stock_detail, error: null };
  } catch {
    return {
      data: null,
      error: "个股详情接口不可达，请检查后端服务与 API 地址。",
    };
  }
}

export default async function StockDetailPage({
  params,
  searchParams,
}: {
  params: Promise<{ instrumentId: string }>;
  searchParams: Promise<{ screen_run_id?: string }>;
}) {
  const apiBaseUrl = await resolveApiBaseUrl();
  const resolvedParams = await params;
  const resolvedSearchParams = await searchParams;
  const [{ data, error }, { health, error: healthError }] = await Promise.all([
    loadStockDetail(
      apiBaseUrl,
      resolvedParams.instrumentId,
      resolvedSearchParams.screen_run_id,
    ),
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
        <Link href="/backtests">回测</Link>
        <span>/</span>
        <span>个股详情</span>
      </nav>
      <WorkflowTrustBanner workflowLabel="个股详情工作流" health={health} error={healthError} />

      {data ? (
        <StockDetailView apiBaseUrl={apiBaseUrl} detail={data} />
      ) : (
        <section className="screen-panel">
          <p className="eyebrow">个股详情</p>
          <h1>详情数据暂不可用。</h1>
          <p className="hero-text">{error}</p>
        </section>
      )}
    </main>
  );
}
