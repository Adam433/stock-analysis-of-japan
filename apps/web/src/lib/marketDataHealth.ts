export type RefreshPayload = {
  status: string;
  provider: string;
  universe_scope: string;
  universe_filter: string;
  requested_symbol_count: number;
  started_at: string;
  completed_at: string | null;
  rows_processed: number;
  rows_inserted: number;
  rows_updated: number;
  partial_rows: number;
  unavailable_rows: number;
  latest_trade_date: string | null;
  error_message: string | null;
  requested_symbols: string[];
};

export type MarketDataHealthResponse = {
  freshness_state: string;
  latest_trade_date: string | null;
  age_in_days: number | null;
  coverage_status: string;
  total_instruments: number;
  partial_rows: number;
  unavailable_rows: number;
  last_refresh: RefreshPayload | null;
  universe_manifest: {
    universe_filter: string;
    symbol_count: number;
    updated_at: string | null;
  } | null;
};

export async function loadMarketDataHealth(
  apiBaseUrl: string,
): Promise<{ health: MarketDataHealthResponse | null; error: string | null }> {
  try {
    const { fetchWithRetry } = await import("@/lib/fetchWithRetry");
    const response = await fetchWithRetry(`${apiBaseUrl}/health/market-data`, {
      cache: "no-store",
    });

    if (!response.ok) {
      return {
        health: null,
        error: `健康检查接口返回 ${response.status}。`,
      };
    }

    return {
      health: (await response.json()) as MarketDataHealthResponse,
      error: null,
    };
  } catch {
    return {
      health: null,
      error: `无法访问健康检查接口：${apiBaseUrl}/health/market-data。请检查 STOCKANALYSE_API_BASE_URL 与后端服务状态。`,
    };
  }
}
