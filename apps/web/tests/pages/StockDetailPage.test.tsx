import { render, screen } from "@testing-library/react";

import StockDetailPage from "@/app/stocks/[instrumentId]/page";

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
  WorkflowTrustBanner: ({ workflowLabel }: { workflowLabel: string }) => <div>banner:{workflowLabel}</div>,
}));

vi.mock("@/components/stocks/StockDetailView", () => ({
  StockDetailView: ({ apiBaseUrl, detail }: { apiBaseUrl: string; detail: { instrument: { symbol: string } } }) => (
    <div>detail-view:{apiBaseUrl}:{detail.instrument.symbol}</div>
  ),
}));

describe("StockDetailPage", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders stock detail view when detail data loads successfully", async () => {
    const { resolveApiBaseUrl } = await import("@/lib/apiBaseUrl");
    const { fetchWithRetry } = await import("@/lib/fetchWithRetry");
    const { loadMarketDataHealth } = await import("@/lib/marketDataHealth");

    vi.mocked(resolveApiBaseUrl).mockResolvedValue("http://localhost:8000");
    vi.mocked(fetchWithRetry).mockResolvedValue(
      new Response(
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
      ),
    );
    vi.mocked(loadMarketDataHealth).mockResolvedValue({ health: null, error: null });

    render(
      await StockDetailPage({
        params: Promise.resolve({ instrumentId: "61" }),
        searchParams: Promise.resolve({ screen_run_id: "8" }),
      }),
    );

    expect(fetchWithRetry).toHaveBeenCalledWith("http://localhost:8000/stocks/61/detail?screen_run_id=8", {
      cache: "no-store",
    });
    expect(screen.getByText("banner:个股详情工作流")).toBeInTheDocument();
    expect(screen.getByText("detail-view:http://localhost:8000:7203")).toBeInTheDocument();
  });

  it("renders fallback copy when stock detail cannot be loaded", async () => {
    const { resolveApiBaseUrl } = await import("@/lib/apiBaseUrl");
    const { fetchWithRetry } = await import("@/lib/fetchWithRetry");
    const { loadMarketDataHealth } = await import("@/lib/marketDataHealth");

    vi.mocked(resolveApiBaseUrl).mockResolvedValue("http://localhost:8000");
    vi.mocked(fetchWithRetry).mockResolvedValue(new Response(null, { status: 404 }));
    vi.mocked(loadMarketDataHealth).mockResolvedValue({ health: null, error: null });

    render(
      await StockDetailPage({
        params: Promise.resolve({ instrumentId: "61" }),
        searchParams: Promise.resolve({}),
      }),
    );

    expect(screen.getByText("详情数据暂不可用。")).toBeInTheDocument();
    expect(screen.getByText("无法加载个股详情（404）。")).toBeInTheDocument();
  });
});
