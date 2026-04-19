import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { ResultAnalysisCard } from "@/components/screen/ResultAnalysisCard";
import type { InlineAnalysisPayload } from "@/lib/types";

vi.mock("lightweight-charts", () => ({
  createChart: vi.fn(() => ({
    applyOptions: vi.fn(),
    addSeries: vi.fn(() => ({ setData: vi.fn() })),
    remove: vi.fn(),
    timeScale: vi.fn(() => ({
      fitContent: vi.fn(),
    })),
  })),
  CandlestickSeries: "CandlestickSeries",
  HistogramSeries: "HistogramSeries",
  ColorType: { Solid: "solid" },
  CrosshairMode: { Normal: 0 },
  LineStyle: { Solid: 0 },
}));

function buildPayload(overrides: Partial<InlineAnalysisPayload> = {}): InlineAnalysisPayload {
  return {
    instrument: {
      id: 61,
      symbol: "7203",
      exchange: "TSE",
      name: "Toyota",
      currency: "JPY",
    },
    screen_run_ref: {
      id: 8,
      trade_date: "2026-04-15",
    },
    candlesticks: [
      {
        trade_date: "2026-04-14",
        open: "100.000000",
        high: "103.000000",
        low: "99.000000",
        close: "102.000000",
        adj_close: "102.000000",
        volume: 1000,
        data_status: "complete",
      },
    ],
    candlestick_window_days_available: 252,
    valuation_by_fiscal_year: [
      {
        fiscal_year_label: "FY2024",
        fiscal_year_end_month: 3,
        net_income: "1000000.00",
        net_income_currency: "JPY",
        pe: "10.1000",
        pb: "1.1200",
        data_status: "complete",
      },
    ],
    generated_at: "2026-04-17T12:00:00Z",
    ...overrides,
  };
}

describe("ResultAnalysisCard", () => {
  it("renders a loading skeleton for idle and loading states", () => {
    const { rerender } = render(
      <ResultAnalysisCard
        instrumentId={61}
        symbol="7203"
        screenRunId={8}
        analysisPayload="idle"
      />,
    );

    expect(screen.getByRole("status", { name: "内联分析加载中" })).toBeInTheDocument();

    rerender(
      <ResultAnalysisCard
        instrumentId={61}
        symbol="7203"
        screenRunId={8}
        analysisPayload="loading"
      />,
    );

    expect(screen.getByText(/正在加载 7203 的 1 年 K 线与财务概览/)).toBeInTheDocument();
  });

  it("locks the loss-year PE convention to N/A and shows the short-history hint", () => {
    render(
      <ResultAnalysisCard
        instrumentId={61}
        symbol="7203"
        screenRunId={8}
        analysisPayload={buildPayload({
          candlestick_window_days_available: 30,
          valuation_by_fiscal_year: [
            {
              fiscal_year_label: "FY2024",
              fiscal_year_end_month: 3,
              net_income: "-1000000.00",
              net_income_currency: "JPY",
              pe: null,
              pb: "0.9800",
              data_status: "partial",
            },
          ],
        })}
      />,
    );

    expect(screen.getByText("历史数据仅覆盖 30 个交易日。")).toBeInTheDocument();
    expect(screen.getByText("N/A")).toBeInTheDocument();
    expect(screen.getByText("亏损")).toBeInTheDocument();
  });

  it("renders explicit missing placeholders for fiscal-year data gaps", () => {
    render(
      <ResultAnalysisCard
        instrumentId={61}
        symbol="7203"
        screenRunId={8}
        analysisPayload={buildPayload({
          valuation_by_fiscal_year: [
            {
              fiscal_year_label: "FY2024",
              fiscal_year_end_month: 3,
              net_income: null,
              net_income_currency: "JPY",
              pe: null,
              pb: null,
              data_status: "missing",
            },
          ],
        })}
      />,
    );

    expect(screen.getAllByText("数据缺失").length).toBeGreaterThan(0);
    expect(screen.getByText("FY2024（03 月结束）")).toBeInTheDocument();
    expect(screen.queryByText("+数据缺失")).not.toBeInTheDocument();
  });

  it("shows a retry button with the concrete error message when loading fails", async () => {
    const user = userEvent.setup();
    const onRetry = vi.fn();

    render(
      <ResultAnalysisCard
        instrumentId={61}
        symbol="7203"
        screenRunId={8}
        analysisPayload="failed"
        errorMessage="请求失败（429）"
        onRetry={onRetry}
      />,
    );

    expect(screen.getByText("请求失败（429）")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "重试加载" }));
    expect(onRetry).toHaveBeenCalledTimes(1);
  });
});
