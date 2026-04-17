import { render, screen } from "@testing-library/react";

import BacktestsPage from "@/app/backtests/page";
import HomePage from "@/app/page";
import ScreenConfigurationPage from "@/app/screen/page";
import StockDetailPage from "@/app/stocks/[instrumentId]/page";
import WatchlistPage from "@/app/watchlist/page";

vi.mock("next/link", () => ({
  default: ({ href, children }: { href: string; children: React.ReactNode }) => (
    <a href={href}>{children}</a>
  ),
}));

vi.mock("@/lib/apiBaseUrl", () => ({
  resolveApiBaseUrl: vi.fn(),
  describeApiBaseUrlResolution: vi.fn(),
}));

vi.mock("@/lib/fetchWithRetry", () => ({
  fetchWithRetry: vi.fn(),
}));

vi.mock("@/lib/marketDataHealth", () => ({
  loadMarketDataHealth: vi.fn(),
}));

vi.mock("@/components/shared/WorkflowTrustBanner", () => ({
  WorkflowTrustBanner: ({ workflowLabel }: { workflowLabel: string }) => <div>banner:{workflowLabel}</div>,
}));

vi.mock("@/components/screen/StrategyConfigPanel", () => ({
  StrategyConfigPanel: () => <div>strategy-panel</div>,
}));

vi.mock("@/components/watchlist/WatchlistReviewPanel", () => ({
  WatchlistReviewPanel: () => <div>watchlist-panel</div>,
}));

vi.mock("@/components/backtests/BacktestLaunchPanel", () => ({
  BacktestLaunchPanel: () => <div>backtest-panel</div>,
}));

vi.mock("@/components/stocks/StockDetailView", () => ({
  StockDetailView: () => <div>stock-detail-view</div>,
}));

describe("workflow smoke", () => {
  beforeEach(async () => {
    const { resolveApiBaseUrl, describeApiBaseUrlResolution } = await import("@/lib/apiBaseUrl");
    const { fetchWithRetry } = await import("@/lib/fetchWithRetry");
    const { loadMarketDataHealth } = await import("@/lib/marketDataHealth");

    vi.mocked(resolveApiBaseUrl).mockResolvedValue("http://localhost:8000");
    vi.mocked(describeApiBaseUrlResolution).mockReturnValue("resolved from env");
    vi.mocked(loadMarketDataHealth).mockResolvedValue({
      health: {
        freshness_state: "fresh",
        latest_trade_date: "2026-04-16",
        age_in_days: 0,
        coverage_status: "complete",
        total_instruments: 100,
        partial_rows: 0,
        unavailable_rows: 0,
        last_refresh: null,
        universe_manifest: null,
      },
      error: null,
    });
    vi.mocked(fetchWithRetry).mockImplementation(async (url: string | URL | Request) => {
      const value = String(url);

      if (value.endsWith("/screen/configuration")) {
        return new Response(
          JSON.stringify({
            configuration: {
              id: 3,
              version: 4,
              rps_threshold: 90,
              high_proximity_threshold_pct: "5.00",
              selected_rps_windows: [50, 120, 250],
            },
          }),
          { status: 200 },
        );
      }
      if (value.endsWith("/screen/runs/latest")) {
        return new Response(JSON.stringify({ screen_run: null }), { status: 200 });
      }
      if (value.endsWith("/screen/trade-dates")) {
        return new Response(JSON.stringify({ trade_dates: [{ trade_date: "2026-04-15" }] }), {
          status: 200,
        });
      }
      if (value.endsWith("/watchlist")) {
        return new Response(JSON.stringify({ entries: [] }), { status: 200 });
      }
      if (value.endsWith("/backtests/runs/latest")) {
        return new Response(JSON.stringify({ backtest_run: null }), { status: 200 });
      }
      if (value.endsWith("/backtests/runs")) {
        return new Response(JSON.stringify({ backtest_runs: [] }), { status: 200 });
      }
      if (value.includes("/stocks/61/detail")) {
        return new Response(
          JSON.stringify({
            stock_detail: {
              instrument: { id: 61, symbol: "7203", exchange: "TSE", name: "Toyota", currency: "JPY" },
              screen_run: { id: 8, trade_date: "2026-04-15", executed_at: "2026-04-16T09:30:00Z", status: "completed", strategy_configuration_version: 4 },
              rule_breakdown: {
                passed: true,
                rps_condition: { passed: true, best_rps_value: "97.10", threshold: 90, rps_50: "97.10", rps_120: "91.20", rps_250: "88.00" },
                high_proximity_condition: { passed: true, high_proximity_ratio: "0.98", threshold_pct: "5.00", max_drawdown_from_high_pct: "2.05" },
              },
              latest_indicator_snapshot: { trade_date: "2026-04-15", rps_50: "97.10", rps_120: "91.20", rps_250: "88.00", fifty_two_week_high: "3200", high_proximity_ratio: "0.98" },
              candlesticks: [],
              indicator_history: [],
            },
          }),
          { status: 200 },
        );
      }

      return new Response(null, { status: 404 });
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders every workflow page shell without crashing", async () => {
    const { unmount: unmountHome } = render(await HomePage());
    expect(screen.getByText("日股数据的运营可信视图。")).toBeInTheDocument();
    unmountHome();

    const { unmount: unmountScreen } = render(await ScreenConfigurationPage());
    expect(screen.getByText("strategy-panel")).toBeInTheDocument();
    expect(screen.getByText("banner:筛选工作流")).toBeInTheDocument();
    unmountScreen();

    const { unmount: unmountWatchlist } = render(await WatchlistPage());
    expect(screen.getByText("watchlist-panel")).toBeInTheDocument();
    expect(screen.getByText("banner:观察列表工作流")).toBeInTheDocument();
    unmountWatchlist();

    const { unmount: unmountBacktests } = render(
      await BacktestsPage({ searchParams: Promise.resolve({}) }),
    );
    expect(screen.getByText("backtest-panel")).toBeInTheDocument();
    expect(screen.getByText("banner:回测工作流")).toBeInTheDocument();
    unmountBacktests();

    render(
      await StockDetailPage({
        params: Promise.resolve({ instrumentId: "61" }),
        searchParams: Promise.resolve({ screen_run_id: "8" }),
      }),
    );
    expect(screen.getByText("stock-detail-view")).toBeInTheDocument();
    expect(screen.getByText("banner:个股详情工作流")).toBeInTheDocument();
  });
});
