import { render, screen } from "@testing-library/react";

import HomePage from "@/app/page";

vi.mock("next/link", () => ({
  default: ({ href, children }: { href: string; children: React.ReactNode }) => (
    <a href={href}>{children}</a>
  ),
}));

vi.mock("@/lib/apiBaseUrl", () => ({
  resolveApiBaseUrl: vi.fn(),
  describeApiBaseUrlResolution: vi.fn(),
}));

vi.mock("@/lib/marketDataHealth", () => ({
  loadMarketDataHealth: vi.fn(),
}));

describe("HomePage", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders health status cards from the market data payload", async () => {
    const { resolveApiBaseUrl, describeApiBaseUrlResolution } = await import("@/lib/apiBaseUrl");
    const { loadMarketDataHealth } = await import("@/lib/marketDataHealth");

    vi.mocked(resolveApiBaseUrl).mockResolvedValue("http://localhost:8000");
    vi.mocked(describeApiBaseUrlResolution).mockReturnValue("resolved from env");
    vi.mocked(loadMarketDataHealth).mockResolvedValue({
      health: {
        freshness_state: "fresh",
        latest_trade_date: "2026-04-16",
        age_in_days: 0,
        coverage_status: "complete",
        total_instruments: 1234,
        partial_rows: 0,
        unavailable_rows: 0,
        last_refresh: {
          status: "succeeded",
          provider: "local_csv_directory",
          universe_scope: "full_universe",
          universe_filter: "tse_common_stock",
          requested_symbol_count: 1234,
          started_at: "2026-04-16T00:00:00Z",
          completed_at: "2026-04-16T00:10:00Z",
          rows_processed: 1000,
          rows_inserted: 800,
          rows_updated: 200,
          partial_rows: 0,
          unavailable_rows: 0,
          latest_trade_date: "2026-04-16",
          error_message: null,
          requested_symbols: ["7203", "6758"],
        },
        universe_manifest: {
          universe_filter: "tse_common_stock",
          symbol_count: 1234,
          updated_at: "2026-04-16T00:00:00Z",
        },
      },
      error: null,
    });

    render(await HomePage());

    expect(screen.getByText("日股数据的运营可信视图。")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "策略配置" })).toHaveAttribute("href", "/screen");
    expect(screen.getByText("API 基址：http://localhost:8000")).toBeInTheDocument();
    expect(screen.getByText("fresh")).toBeInTheDocument();
    expect(screen.getByText("complete")).toBeInTheDocument();
    expect(screen.getByText("succeeded")).toBeInTheDocument();
    expect(screen.getByText(/已存日频行情涵盖 1234 只标的/)).toBeInTheDocument();
    expect(screen.getByText("7203 · 6758")).toBeInTheDocument();
  });

  it("renders API error state when health loading fails", async () => {
    const { resolveApiBaseUrl, describeApiBaseUrlResolution } = await import("@/lib/apiBaseUrl");
    const { loadMarketDataHealth } = await import("@/lib/marketDataHealth");

    vi.mocked(resolveApiBaseUrl).mockResolvedValue("http://localhost:8000");
    vi.mocked(describeApiBaseUrlResolution).mockReturnValue("resolved from env");
    vi.mocked(loadMarketDataHealth).mockResolvedValue({
      health: null,
      error: "健康检查接口不可达。",
    });

    render(await HomePage());

    expect(screen.getByText("api-unreachable")).toBeInTheDocument();
    expect(screen.getAllByText("connection-issue")).toHaveLength(2);
    expect(screen.getAllByText(/健康检查接口不可达。 resolved from env/)).toHaveLength(2);
    expect(screen.getByText("接口不可达，刷新状态未知")).toBeInTheDocument();
  });
});
