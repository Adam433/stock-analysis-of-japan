export function apiPaths(baseUrl: string) {
  return {
    healthMarketData: `${baseUrl}/health/market-data`,
    screenConfiguration: `${baseUrl}/screen/configuration`,
    screenRuns: `${baseUrl}/screen/runs`,
    screenRunsLatest: `${baseUrl}/screen/runs/latest`,
    screenTradeDates: `${baseUrl}/screen/trade-dates`,
    watchlist: `${baseUrl}/watchlist`,
    watchlistEntry: (instrumentId: number) => `${baseUrl}/watchlist/${instrumentId}`,
    backtestRuns: `${baseUrl}/backtests/runs`,
    backtestRunsLatest: `${baseUrl}/backtests/runs/latest`,
    backtestRunExecute: (runId: number) => `${baseUrl}/backtests/runs/${runId}/execute`,
    stockDetail: (instrumentId: string | number, screenRunId?: string) => {
      const query = screenRunId ? `?screen_run_id=${screenRunId}` : "";
      return `${baseUrl}/stocks/${instrumentId}/detail${query}`;
    },
  } as const;
}
