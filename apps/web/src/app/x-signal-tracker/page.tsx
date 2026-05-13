import Link from "next/link";

import { XSignalTrackerPanel } from "@/components/x-signals/XSignalTrackerPanel";
import { resolveApiBaseUrl } from "@/lib/apiBaseUrl";
import { fetchWithRetry } from "@/lib/fetchWithRetry";
import type { XSignalDashboard } from "@/lib/types";

export const dynamic = "force-dynamic";

const emptyDashboard: XSignalDashboard = {
  authors: [],
  mentions: [],
  total_posts: 0,
  total_mentions: 0,
  latest_fetch_request: null,
};

async function loadXSignalDashboard(
  apiBaseUrl: string,
): Promise<{ dashboard: XSignalDashboard; error: string | null }> {
  try {
    const response = await fetchWithRetry(`${apiBaseUrl}/x-signals/dashboard`, {
      cache: "no-store",
    });
    if (!response.ok) {
      return {
        dashboard: emptyDashboard,
        error: `无法加载 X Signal Tracker（${response.status}）。`,
      };
    }

    const payload = (await response.json()) as { dashboard: XSignalDashboard };
    return { dashboard: payload.dashboard, error: null };
  } catch {
    return {
      dashboard: emptyDashboard,
      error: "X Signal Tracker 接口不可达，请检查后端服务与 API 地址。",
    };
  }
}

export default async function XSignalTrackerPage() {
  const apiBaseUrl = await resolveApiBaseUrl();
  const { dashboard, error } = await loadXSignalDashboard(apiBaseUrl);

  return (
    <main id="main-content" className="dashboard-shell">
      <nav className="top-nav">
        <Link href="/">数据健康</Link>
        <span>/</span>
        <Link href="/screen">策略配置</Link>
        <span>/</span>
        <Link href="/watchlist">观察列表</Link>
        <span>/</span>
        <Link href="/backtests">回测</Link>
        <span>/</span>
        <Link href="/experiments">实验</Link>
        <span>/</span>
        <span>X 信号追踪</span>
      </nav>
      <XSignalTrackerPanel
        apiBaseUrl={apiBaseUrl}
        initialDashboard={dashboard}
        initialError={error}
      />
    </main>
  );
}
