import { render, screen } from "@testing-library/react";

import BacktestsPage from "@/app/backtests/page";
import ScreenConfigurationPage from "@/app/screen/page";
import WatchlistPage from "@/app/watchlist/page";

vi.mock("next/link", () => ({
  default: ({ href, children }: { href: string; children: React.ReactNode }) => (
    <a href={href}>{children}</a>
  ),
}));

vi.mock("@/lib/apiBaseUrl", () => ({
  resolveApiBaseUrl: vi.fn(),
}));

vi.mock("@/lib/fetchWithRetry", () => ({
  fetchWithRetry: vi.fn(),
}));

vi.mock("@/lib/marketDataHealth", () => ({
  loadMarketDataHealth: vi.fn(),
}));

vi.mock("@/components/shared/WorkflowTrustBanner", () => ({
  WorkflowTrustBanner: ({ workflowLabel, health, error }: { workflowLabel: string; health: unknown; error: string | null }) => (
    <div>
      banner:{workflowLabel}:{health ? "health" : "no-health"}:{error ?? "ok"}
    </div>
  ),
}));

vi.mock("@/components/screen/StrategyConfigPanel", () => ({
  StrategyConfigPanel: (props: {
    apiBaseUrl: string;
    initialError: string | null;
    initialRunError: string | null;
    initialTradeDateError: string | null;
    initialTradeDates: Array<{ trade_date: string }>;
  }) => <div>screen-panel:{props.apiBaseUrl}:{props.initialError ?? "ok"}:{props.initialRunError ?? "ok"}:{props.initialTradeDateError ?? "ok"}:{props.initialTradeDates.length}</div>,
}));

vi.mock("@/components/watchlist/WatchlistReviewPanel", () => ({
  WatchlistReviewPanel: (props: {
    apiBaseUrl: string;
    initialEntries: Array<{ symbol: string }>;
    initialError: string | null;
  }) => <div>watchlist-panel:{props.apiBaseUrl}:{props.initialEntries.map((entry) => entry.symbol).join(",")}:{props.initialError ?? "ok"}</div>,
}));

vi.mock("@/components/backtests/BacktestLaunchPanel", () => ({
  BacktestLaunchPanel: (props: {
    apiBaseUrl: string;
    initialRun: { id: number } | null;
    initialRuns: Array<{ id: number }>;
    initialError: string | null;
  }) => <div>backtest-panel:{props.apiBaseUrl}:{props.initialRun?.id ?? "none"}:{props.initialRuns.length}:{props.initialError ?? "ok"}</div>,
}));

describe("Workflow pages", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("wires screen page data into the trust banner and strategy panel", async () => {
    const { resolveApiBaseUrl } = await import("@/lib/apiBaseUrl");
    const { fetchWithRetry } = await import("@/lib/fetchWithRetry");
    const { loadMarketDataHealth } = await import("@/lib/marketDataHealth");

    vi.mocked(resolveApiBaseUrl).mockResolvedValue("http://localhost:8000");
    vi.mocked(fetchWithRetry)
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            configuration: {
              id: 3,
              version: 4,
              rps_threshold: 90,
              high_proximity_threshold_pct: "5.00",
              selected_rps_windows: [50, 120, 250],
              min_rps_lines_required: 2,
            },
          }),
          { status: 200 },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ screen_run: { id: 8, trade_date: "2026-04-15", executed_at: "2026-04-16T09:30:00Z", total_candidates: 10, qualified_count: 2, status: "completed", parameter_set: { id: 3, version: 4, rps_threshold: 90, high_proximity_threshold_pct: "5.00", selected_rps_windows: [50], min_rps_lines_required: 1 }, qualified_results: [] } }),
          { status: 200 },
        ),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ trade_dates: [{ trade_date: "2026-04-15" }] }), { status: 200 }),
      );
    vi.mocked(loadMarketDataHealth).mockResolvedValue({ health: { freshness_state: "fresh" }, error: null } as never);

    render(await ScreenConfigurationPage());

    expect(screen.getByText("banner:筛选工作流:health:ok")).toBeInTheDocument();
    expect(screen.getByText("screen-panel:http://localhost:8000:ok:ok:ok:1")).toBeInTheDocument();
  });

  it("wires watchlist page error state into the panel", async () => {
    const { resolveApiBaseUrl } = await import("@/lib/apiBaseUrl");
    const { fetchWithRetry } = await import("@/lib/fetchWithRetry");
    const { loadMarketDataHealth } = await import("@/lib/marketDataHealth");

    vi.mocked(resolveApiBaseUrl).mockResolvedValue("http://localhost:8000");
    vi.mocked(fetchWithRetry).mockResolvedValue(new Response(null, { status: 503 }));
    vi.mocked(loadMarketDataHealth).mockResolvedValue({ health: null, error: "health error" });

    render(await WatchlistPage());

    expect(screen.getByText("banner:观察列表工作流:no-health:health error")).toBeInTheDocument();
    expect(screen.getByText("watchlist-panel:http://localhost:8000::无法加载观察列表（503）。")).toBeInTheDocument();
  });

  it("wires backtests page latest run, run list and health data", async () => {
    const { resolveApiBaseUrl } = await import("@/lib/apiBaseUrl");
    const { fetchWithRetry } = await import("@/lib/fetchWithRetry");
    const { loadMarketDataHealth } = await import("@/lib/marketDataHealth");

    vi.mocked(resolveApiBaseUrl).mockResolvedValue("http://localhost:8000");
    vi.mocked(fetchWithRetry)
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ backtest_run: { id: 9 } }), { status: 200 }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ backtest_runs: [{ id: 9 }, { id: 8 }] }), { status: 200 }),
      );
    vi.mocked(loadMarketDataHealth).mockResolvedValue({ health: { freshness_state: "fresh" }, error: null } as never);

    render(await BacktestsPage());

    expect(screen.getByText("banner:回测工作流:health:ok")).toBeInTheDocument();
    expect(screen.getByText("backtest-panel:http://localhost:8000:9:2:ok")).toBeInTheDocument();
  });
});
