import Link from "next/link";

import { WorkflowTrustBanner } from "@/components/shared/WorkflowTrustBanner";
import { WatchlistReviewPanel } from "@/components/watchlist/WatchlistReviewPanel";
import { loadMarketDataHealth } from "@/lib/marketDataHealth";

type WatchlistEntry = {
  id: number;
  instrument_id: number;
  symbol: string;
  exchange: string;
  name: string | null;
  note: string | null;
  observation_reason: string | null;
  added_date: string;
  added_at: string;
};

const apiBaseUrl =
  process.env.STOCKANALYSE_API_BASE_URL ?? "http://127.0.0.1:8000";

export const dynamic = "force-dynamic";

async function loadWatchlist(): Promise<{ entries: WatchlistEntry[]; error: string | null }> {
  try {
    const response = await fetch(`${apiBaseUrl}/watchlist`, { cache: "no-store" });
    if (!response.ok) {
      return {
        entries: [],
        error: `无法加载观察列表（${response.status}）。`,
      };
    }

    const payload = (await response.json()) as { entries: WatchlistEntry[] };
    return { entries: payload.entries, error: null };
  } catch {
    return {
      entries: [],
      error: "观察列表接口不可达，请检查后端服务与 API 地址。",
    };
  }
}

export default async function WatchlistPage() {
  const [{ entries, error }, { health, error: healthError }] = await Promise.all([
    loadWatchlist(),
    loadMarketDataHealth(apiBaseUrl),
  ]);

  return (
    <main className="dashboard-shell">
      <nav className="top-nav">
        <Link href="/">数据健康</Link>
        <span>/</span>
        <Link href="/screen">策略配置</Link>
        <span>/</span>
        <span>观察列表</span>
        <span>/</span>
        <Link href="/backtests">回测</Link>
      </nav>
      <WorkflowTrustBanner workflowLabel="观察列表工作流" health={health} error={healthError} />
      <WatchlistReviewPanel apiBaseUrl={apiBaseUrl} initialEntries={entries} initialError={error} />
    </main>
  );
}
