import { apiPaths } from "@/lib/apiPaths";

describe("apiPaths", () => {
  it("builds static endpoint paths from the base url", () => {
    const paths = apiPaths("http://localhost:8000");

    expect(paths.healthMarketData).toBe("http://localhost:8000/health/market-data");
    expect(paths.screenConfiguration).toBe("http://localhost:8000/screen/configuration");
    expect(paths.screenRuns).toBe("http://localhost:8000/screen/runs");
    expect(paths.screenRunsLatest).toBe("http://localhost:8000/screen/runs/latest");
    expect(paths.screenTradeDates).toBe("http://localhost:8000/screen/trade-dates");
    expect(paths.watchlist).toBe("http://localhost:8000/watchlist");
    expect(paths.backtestDefaults).toBe("http://localhost:8000/backtests/defaults");
    expect(paths.backtestRuns).toBe("http://localhost:8000/backtests/runs");
    expect(paths.backtestRunsLatest).toBe("http://localhost:8000/backtests/runs/latest");
    expect(paths.portfolioReturnBacktestRuns).toBe("http://localhost:8000/backtests/portfolio-return/runs");
  });

  it("builds dynamic endpoint paths with identifiers and optional query", () => {
    const paths = apiPaths("http://localhost:8000");

    expect(paths.watchlistEntry(61)).toBe("http://localhost:8000/watchlist/61");
    expect(paths.backtestRunExecute(9)).toBe("http://localhost:8000/backtests/runs/9/execute");
    expect(paths.portfolioReturnBacktestResult(9)).toBe("http://localhost:8000/backtests/portfolio-return/runs/9/result");
    expect(paths.portfolioReturnBacktestCompare([9, 11])).toBe(
      "http://localhost:8000/backtests/portfolio-return/runs/compare?ids=9,11",
    );
    expect(paths.stockDetail(61)).toBe("http://localhost:8000/stocks/61/detail");
    expect(paths.stockDetail("61", "8")).toBe("http://localhost:8000/stocks/61/detail?screen_run_id=8");
    expect(paths.stockInlineAnalysis(61)).toBe("http://localhost:8000/stocks/61/inline-analysis");
    expect(paths.stockInlineAnalysis("61", 8)).toBe("http://localhost:8000/stocks/61/inline-analysis?screen_run_id=8");
  });
});
