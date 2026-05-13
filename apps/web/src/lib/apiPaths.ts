export function apiPaths(baseUrl: string) {
  return {
    healthMarketData: `${baseUrl}/health/market-data`,
    screenConfiguration: `${baseUrl}/screen/configuration`,
    screenRuns: `${baseUrl}/screen/runs`,
    screenRunsLatest: `${baseUrl}/screen/runs/latest`,
    screenTradeDates: `${baseUrl}/screen/trade-dates`,
    watchlist: `${baseUrl}/watchlist`,
    watchlistEntry: (instrumentId: number) => `${baseUrl}/watchlist/${instrumentId}`,
    xSignalDashboard: `${baseUrl}/x-signals/dashboard`,
    xSignalAuthors: `${baseUrl}/x-signals/authors`,
    xSignalFetchRequests: (authorId: number) => `${baseUrl}/x-signals/authors/${authorId}/fetch-requests`,
    xSignalImportPosts: (authorId: number) => `${baseUrl}/x-signals/authors/${authorId}/posts/import`,
    xSignalAnalyzeAuthor: (authorId: number) => `${baseUrl}/x-signals/authors/${authorId}/analyze`,
    backtestDefaults: `${baseUrl}/backtests/defaults`,
    backtestRuns: `${baseUrl}/backtests/runs`,
    backtestRunsLatest: `${baseUrl}/backtests/runs/latest`,
    portfolioReturnBacktestRuns: `${baseUrl}/backtests/portfolio-return/runs`,
    portfolioReturnBacktestResult: (runId: number) => `${baseUrl}/backtests/portfolio-return/runs/${runId}/result`,
    portfolioReturnBacktestCompare: (ids: number[]) =>
      `${baseUrl}/backtests/portfolio-return/runs/compare?ids=${ids.join(",")}`,
    backtestRunExecute: (runId: number) => `${baseUrl}/backtests/runs/${runId}/execute`,
    stockDetail: (instrumentId: string | number, screenRunId?: string) => {
      const query = screenRunId ? `?screen_run_id=${screenRunId}` : "";
      return `${baseUrl}/stocks/${instrumentId}/detail${query}`;
    },
    stockInlineAnalysis: (instrumentId: string | number, screenRunId?: string | number) => {
      const query = screenRunId ? `?screen_run_id=${screenRunId}` : "";
      return `${baseUrl}/stocks/${instrumentId}/inline-analysis${query}`;
    },
  } as const;
}
