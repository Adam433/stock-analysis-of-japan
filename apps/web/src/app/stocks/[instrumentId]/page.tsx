import Link from "next/link";

import { StockDetailView } from "@/components/stocks/StockDetailView";

type StockDetailResponse = {
  stock_detail: Parameters<typeof StockDetailView>[0]["detail"];
};

const apiBaseUrl =
  process.env.STOCKANALYSE_API_BASE_URL ??
  process.env.NEXT_PUBLIC_STOCKANALYSE_API_BASE_URL ??
  "http://127.0.0.1:8000";

export const dynamic = "force-dynamic";

async function loadStockDetail(
  instrumentId: string,
  screenRunId: string | undefined,
): Promise<{ data: StockDetailResponse["stock_detail"] | null; error: string | null }> {
  if (!screenRunId) {
    return {
      data: null,
      error: "screen_run_id is required to keep stock detail aligned with the originating result set.",
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
        error: `Unable to load stock detail (${response.status}).`,
      };
    }

    const payload = (await response.json()) as StockDetailResponse;
    return { data: payload.stock_detail, error: null };
  } catch {
    return {
      data: null,
      error: "Stock detail API is unreachable. Check backend availability and API base URL.",
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
  const resolvedParams = await params;
  const resolvedSearchParams = await searchParams;
  const { data, error } = await loadStockDetail(
    resolvedParams.instrumentId,
    resolvedSearchParams.screen_run_id,
  );

  return (
    <main className="dashboard-shell">
      <nav className="top-nav">
        <Link href="/">Data Health</Link>
        <span>/</span>
        <Link href="/screen">Screen</Link>
        <span>/</span>
        <Link href="/watchlist">Watchlist</Link>
        <span>/</span>
        <Link href="/backtests">Backtests</Link>
        <span>/</span>
        <span>Stock Detail</span>
      </nav>

      {data ? (
        <StockDetailView apiBaseUrl={apiBaseUrl} detail={data} />
      ) : (
        <section className="screen-panel">
          <p className="eyebrow">Stock Detail</p>
          <h1>Detail payload unavailable.</h1>
          <p className="hero-text">{error}</p>
        </section>
      )}
    </main>
  );
}
