import { render, screen } from "@testing-library/react";

import { StockDetailView } from "@/components/stocks/StockDetailView";
import type { StockDetailPayload } from "@/lib/types";

vi.mock("@/components/watchlist/WatchlistToggleButton", () => ({
  WatchlistToggleButton: ({ symbol }: { symbol: string }) => <button type="button">观察 {symbol}</button>,
}));

vi.mock("@/components/stocks/StockDetailCharts", () => ({
  StockDetailCharts: ({ rpsThreshold }: { rpsThreshold: number }) => (
    <div>图表组件阈值 {rpsThreshold}</div>
  ),
}));

function buildDetail(overrides: Partial<StockDetailPayload> = {}): StockDetailPayload {
  return {
    instrument: {
      id: 61,
      symbol: "7203",
      exchange: "TSE",
      name: "Toyota Motor",
      currency: "JPY",
    },
    screen_run: {
      id: 8,
      trade_date: "2026-04-15",
      executed_at: "2026-04-16T09:30:00Z",
      status: "completed",
      strategy_configuration_version: 4,
    },
    rule_breakdown: {
      passed: true,
      rps_condition: {
        passed: true,
        best_rps_value: "97.10",
        threshold: 90,
        rps_50: "97.10",
        rps_120: "91.20",
        rps_250: "88.00",
      },
      high_proximity_condition: {
        passed: true,
        high_proximity_ratio: "0.9795",
        threshold_pct: "5.00",
        max_drawdown_from_high_pct: "2.05",
      },
    },
    latest_indicator_snapshot: {
      trade_date: "2026-04-15",
      rps_50: "97.10",
      rps_120: "91.20",
      rps_250: "88.00",
      fifty_two_week_high: "3200.00",
      high_proximity_ratio: "0.9795",
    },
    candlesticks: [
      {
        trade_date: "2026-03-01",
        open: "3000.00",
        high: "3050.00",
        low: "2980.00",
        close: "3040.00",
        adj_close: "3040.00",
        volume: 1000,
        data_status: "complete",
      },
      {
        trade_date: "2026-04-15",
        open: "3110.00",
        high: "3140.00",
        low: "3090.00",
        close: "3134.40",
        adj_close: "3134.40",
        volume: 1200,
        data_status: "complete",
      },
    ],
    indicator_history: [
      {
        trade_date: "2026-03-01",
        rps_50: "94.50",
        rps_120: "89.00",
        rps_250: "86.00",
        high_proximity_ratio: "0.9650",
      },
      {
        trade_date: "2026-04-15",
        rps_50: "97.10",
        rps_120: "91.20",
        rps_250: "88.00",
        high_proximity_ratio: "0.9795",
      },
    ],
    ...overrides,
  };
}

describe("StockDetailView", () => {
  it("renders passed qualification summary and mirrors official rule data", () => {
    render(
      <StockDetailView apiBaseUrl="http://localhost:8000" detail={buildDetail()} />,
    );

    expect(screen.getByText("个股详情")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /7203/ })).toBeInTheDocument();
    expect(screen.getByText(/Toyota Motor/)).toBeInTheDocument();
    expect(screen.getByText(/已存行情、派生指标与筛选结果/)).toBeInTheDocument();
    expect(screen.getByText("图表组件阈值 90")).toBeInTheDocument();
    expect(screen.getByText("已入选")).toBeInTheDocument();
    expect(screen.getByText("观察 7203")).toBeInTheDocument();
    expect(screen.getByText(/入选：最佳 RPS 97.10 已突破 90 阈值/)).toBeInTheDocument();
    expect(screen.getByText("价格距 52 周高点在允许范围内")).toBeInTheDocument();
  });

  it("renders failed qualification copy when screening rules do not pass", () => {
    render(
      <StockDetailView
        apiBaseUrl="http://localhost:8000"
        detail={buildDetail({
          rule_breakdown: {
            passed: false,
            rps_condition: {
              passed: false,
              best_rps_value: "84.00",
              threshold: 90,
              rps_50: "84.00",
              rps_120: "79.00",
              rps_250: "75.00",
            },
            high_proximity_condition: {
              passed: false,
              high_proximity_ratio: "0.9200",
              threshold_pct: "5.00",
              max_drawdown_from_high_pct: "8.00",
            },
          },
        })}
      />,
    );

    expect(screen.getByText("未入选")).toBeInTheDocument();
    expect(screen.getByText(/未入选：RPS 未通过，距 52 周高点未通过。/)).toBeInTheDocument();
    expect(screen.getByText("最佳 RPS 低于阈值")).toBeInTheDocument();
    expect(screen.getByText("价格距 52 周高点已超出允许范围")).toBeInTheDocument();
  });
});
