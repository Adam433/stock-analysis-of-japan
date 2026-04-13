import Link from "next/link";

import { WatchlistReviewPanel } from "@/components/watchlist/WatchlistReviewPanel";

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
  process.env.STOCKANALYSE_API_BASE_URL ??
  process.env.NEXT_PUBLIC_STOCKANALYSE_API_BASE_URL ??
  "http://127.0.0.1:8000";

export const dynamic = "force-dynamic";

async function loadWatchlist(): Promise<{ entries: WatchlistEntry[]; error: string | null }> {
  try {
    const response = await fetch(`${apiBaseUrl}/watchlist`, { cache: "no-store" });
    if (!response.ok) {
      return {
        entries: [],
        error: `Unable to load watchlist (${response.status}).`,
      };
    }

    const payload = (await response.json()) as { entries: WatchlistEntry[] };
    return { entries: payload.entries, error: null };
  } catch {
    return {
      entries: [],
      error: "Watchlist API is unreachable. Check backend availability and API base URL.",
    };
  }
}

export default async function WatchlistPage() {
  const { entries, error } = await loadWatchlist();

  return (
    <main className="dashboard-shell">
      <nav className="top-nav">
        <Link href="/">Data Health</Link>
        <span>/</span>
        <Link href="/screen">Screen Configuration</Link>
        <span>/</span>
        <span>Watchlist</span>
        <span>/</span>
        <Link href="/backtests">Backtests</Link>
      </nav>
      <WatchlistReviewPanel apiBaseUrl={apiBaseUrl} initialEntries={entries} initialError={error} />
    </main>
  );
}
