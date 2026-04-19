import { render, screen } from "@testing-library/react";

import { PortfolioReturnComparePanel } from "@/components/backtests/PortfolioReturnComparePanel";
import { SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE } from "@/lib/backtestErrors";
import type { PortfolioReturnRunComparison } from "@/lib/types";

vi.mock("next/link", () => ({
  default: ({ href, children, className }: { href: string; children: React.ReactNode; className?: string }) => (
    <a href={href} className={className}>
      {children}
    </a>
  ),
}));

function buildComparison(overrides: Partial<PortfolioReturnRunComparison> = {}): PortfolioReturnRunComparison {
  return {
    run: {
      id: 11,
      source_screen_run_id: 8,
      strategy_configuration_id: 3,
      status: "completed",
      backtest_lifecycle: "portfolio_return",
      start_date: "2026-04-15",
      end_date: "2026-04-15",
      started_at: "2026-04-16T09:00:00Z",
      completed_at: "2026-04-16T09:05:00Z",
      rps_definition_version: null,
      dataset_trade_date_start: "2026-04-16",
      dataset_trade_date_end: "2026-04-18",
      dataset_checksum: "dataset-11",
      effective_holding_days: 2,
      effective_stop_loss_pct: "-0.0800",
      effective_portfolio_cap: 20,
      effective_entry_deferral_window_days: 5,
      ranking_policy_id: "rps_desc_ticker_asc_v1",
      excluded_securities: [],
      portfolio_value: "1.000000",
      position_count_after_exclusions: 1,
      cumulative_return: "0.045000",
      equity_curve: [
        { trade_date: "2026-04-16", equity: "1.000000" },
        { trade_date: "2026-04-17", equity: "1.020000" },
      ],
      per_security_returns: [
        {
          instrument_id: 61,
          symbol: "7203",
          entry_date: "2026-04-16",
          exit_date: "2026-04-18",
          exit_reason: "holding_period_elapsed",
          realized_return: "0.045000",
        },
      ],
      error_message: null,
      result_summary: {
        trade_dates_evaluated: 2,
        total_candidates_evaluated: 1,
        qualifying_observations: 1,
        unique_qualified_instruments: 1,
        first_qualified_trade_date: "2026-04-16",
        last_qualified_trade_date: "2026-04-16",
        result_checksum: "result-11",
      },
      parameter_set: {
        id: 7,
        version: 4,
        rps_threshold: 90,
        high_proximity_threshold_pct: "5.00",
        selected_rps_windows: [50, 120, 250],
      },
    },
    cumulative_return: "0.045000",
    win_rate: "1.000000",
    max_drawdown: "0.010000",
    equity_curve: [
      { trade_date: "2026-04-16", equity: "1.000000" },
      { trade_date: "2026-04-17", equity: "1.020000" },
    ],
    per_security_returns: [
      {
        instrument_id: 61,
        symbol: "7203",
        entry_date: "2026-04-16",
        exit_date: "2026-04-18",
        exit_reason: "holding_period_elapsed",
        realized_return: "0.045000",
      },
    ],
    source_screen_run: {
      id: 8,
      trade_date: "2026-04-15",
      strategy_configuration_version: 4,
      status: "completed",
    },
    compare_dimensions: {
      holding_days: 2,
      stop_loss_pct: "-0.0800",
      portfolio_cap: 20,
      source_screen_run_id: 8,
      source_trade_date: "2026-04-15",
      strategy_configuration_version: 4,
      rps_definition_version: "rps-v1-2026-04-14",
    },
    aligned_equity_curve: [
      { days_since_entry: 0, equity: "1.000000" },
      { days_since_entry: 1, equity: "1.020000" },
    ],
    ...overrides,
  };
}

describe("PortfolioReturnComparePanel", () => {
  it("shows first-class comparison dimensions and aligned x-axis copy", () => {
    render(
      <PortfolioReturnComparePanel
        initialRuns={[
          buildComparison(),
          buildComparison({
            run: { ...buildComparison().run, id: 12, source_screen_run_id: 9, cumulative_return: "0.020000" },
            cumulative_return: "0.020000",
            win_rate: "0.500000",
            max_drawdown: "0.030000",
            source_screen_run: {
              id: 9,
              trade_date: "2026-04-18",
              strategy_configuration_version: 5,
              status: "completed",
            },
            compare_dimensions: {
              holding_days: 5,
              stop_loss_pct: "-0.1000",
              portfolio_cap: 10,
              source_screen_run_id: 9,
              source_trade_date: "2026-04-18",
              strategy_configuration_version: 5,
              rps_definition_version: "rps-v2-2026-04-17",
            },
            aligned_equity_curve: [
              { days_since_entry: 0, equity: "1.000000" },
              { days_since_entry: 1, equity: "0.980000" },
            ],
          }),
        ]}
        initialError={null}
      />,
    );

    expect(screen.getByLabelText("对齐组合权益曲线图")).toBeInTheDocument();
    expect(screen.getByText("交易日（自 T+1 入场起）")).toBeInTheDocument();
    expect(screen.getAllByText("holding_days").length).toBeGreaterThan(0);
    expect(screen.getAllByText("stop_loss_pct").length).toBeGreaterThan(0);
    expect(screen.getByText("#8 / 2026年4月15日 / v4")).toBeInTheDocument();
    expect(screen.getByText("#9 / 2026年4月18日 / v5")).toBeInTheDocument();
    expect(screen.getByText("存在差异")).toBeInTheDocument();
    expect(screen.getAllByTitle("RPS 定义版本不同")).toHaveLength(2);
    expect(screen.getByText("rps-v1-2026-04-14")).toBeInTheDocument();
    expect(screen.getByText("rps-v2-2026-04-17")).toBeInTheDocument();
  });

  it("does not highlight the version badge when all compared runs share the same RPS definition version", () => {
    render(
      <PortfolioReturnComparePanel
        initialRuns={[
          buildComparison(),
          buildComparison({
            run: { ...buildComparison().run, id: 12, source_screen_run_id: 9 },
            source_screen_run: {
              id: 9,
              trade_date: "2026-04-18",
              strategy_configuration_version: 5,
              status: "completed",
            },
            compare_dimensions: {
              holding_days: 5,
              stop_loss_pct: "-0.1000",
              portfolio_cap: 10,
              source_screen_run_id: 9,
              source_trade_date: "2026-04-18",
              strategy_configuration_version: 5,
              rps_definition_version: "rps-v1-2026-04-14",
            },
          }),
        ]}
        initialError={null}
      />,
    );

    expect(screen.getByText("一致")).toBeInTheDocument();
    expect(screen.queryByTitle("RPS 定义版本不同")).not.toBeInTheDocument();
    expect(screen.getAllByTitle("RPS 定义版本一致")).toHaveLength(2);
  });

  it("shows the compare error state for invalid or rejected selections", () => {
    render(<PortfolioReturnComparePanel initialRuns={[]} initialError="Only portfolio_return runs support this endpoint." />);

    expect(screen.getByText("portfolio-return 对比当前不可用。")).toBeInTheDocument();
    expect(screen.getByText("Only portfolio_return runs support this endpoint.")).toBeInTheDocument();
  });

  it("renders the provenance-unavailable downgrade state for 410 responses", () => {
    render(<PortfolioReturnComparePanel initialRuns={[]} initialError={SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE} />);

    expect(screen.getByRole("heading", { name: SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE })).toBeInTheDocument();
    expect(screen.getByText("source provenance 已断开")).toBeInTheDocument();
    expect(
      screen.getByText("只要被选 runs 中任意一条失去 source screen run，这个对比页就不会再做 partial render。"),
    ).toBeInTheDocument();
  });
});
