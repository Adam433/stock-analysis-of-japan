import { render, screen } from "@testing-library/react";

import OptimizationResultDetailPage from "@/app/experiments/optimization-results/[resultId]/page";

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

describe("OptimizationResultDetailPage", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders optimization result detail and trades", async () => {
    const { resolveApiBaseUrl } = await import("@/lib/apiBaseUrl");
    const { fetchWithRetry } = await import("@/lib/fetchWithRetry");

    vi.mocked(resolveApiBaseUrl).mockResolvedValue("http://localhost:8000");
    vi.mocked(fetchWithRetry).mockResolvedValue(
      new Response(
        JSON.stringify({
          detail: {
            optimization_run: {
              id: 41,
              market: "us",
              objective: "spy_alpha",
              train_start_date: "2025-01-01",
              train_end_date: "2025-12-31",
              validation_start_date: null,
              validation_end_date: null,
            },
            optimization_result: {
              id: 100,
              rank: 1,
              score: "0.128000",
              status: "completed",
              failure_reason: null,
              completed_at: "2026-05-11T10:30:00Z",
            },
            parameters: {
              rps_threshold: 80,
              selected_rps_windows: [120, 250],
              stop_loss_pct: "-0.0800",
              portfolio_cap: 20,
              holding_days: 130,
            },
            train: {
              summary: {
                total_return: "0.100000",
                max_drawdown: "-0.050000",
                spy_average_trade_excess_return: "0.020000",
                completed_trades: 30,
                win_rate: "0.600000",
              },
              trades: [
                {
                  period: "train",
                  symbol: "MSFT",
                  entry_date: "2025-01-02",
                  entry_price: "100.000000",
                  exit_date: "2025-02-01",
                  exit_price: "120.000000",
                  realized_return: "0.200000",
                  exit_reason_label: "持有期结束",
                },
              ],
            },
            validation: null,
          },
        }),
        { status: 200 },
      ),
    );

    render(
      await OptimizationResultDetailPage({
        params: Promise.resolve({ resultId: "100" }),
      }),
    );

    expect(fetchWithRetry).toHaveBeenCalledWith(
      "http://localhost:8000/backtests/optimization/results/100/detail?max_trades_returned=120",
      { cache: "no-store" },
    );
    expect(screen.getByText("结果 #100")).toBeInTheDocument();
    expect(screen.getByText("0.128000")).toBeInTheDocument();
    expect(screen.getByText("MSFT")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "实验" })).toHaveAttribute("href", "/experiments");
  });
});
