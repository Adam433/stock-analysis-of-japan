import { render, screen } from "@testing-library/react";

import { PortfolioReturnResultPanel } from "@/components/backtests/PortfolioReturnResultPanel";
import { SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE } from "@/lib/backtestErrors";
import type { PortfolioReturnRunResult } from "@/lib/types";

const setDataMock = vi.fn();
const applyOptionsMock = vi.fn();
const fitContentMock = vi.fn();
const removeMock = vi.fn();
const observeMock = vi.fn();
const disconnectMock = vi.fn();

vi.mock("next/link", () => ({
  default: ({ href, children, className }: { href: string; children: React.ReactNode; className?: string }) => (
    <a href={href} className={className}>
      {children}
    </a>
  ),
}));

vi.mock("lightweight-charts", () => ({
  ColorType: { Solid: "solid" },
  CrosshairMode: { Normal: 0 },
  LineStyle: { Solid: 0 },
  LineSeries: "LineSeries",
  createChart: vi.fn(() => ({
    applyOptions: applyOptionsMock,
    addSeries: vi.fn(() => ({ setData: setDataMock })),
    timeScale: vi.fn(() => ({ fitContent: fitContentMock })),
    remove: removeMock,
  })),
}));

function buildResult(overrides: Partial<PortfolioReturnRunResult> = {}): PortfolioReturnRunResult {
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
      rps_definition_version: "rps-v2",
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
      position_count_after_exclusions: 2,
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
          realized_return: "0.040000",
        },
        {
          instrument_id: 62,
          symbol: "6758",
          entry_date: "2026-04-16",
          exit_date: "2026-04-18",
          exit_reason: "stop_loss",
          realized_return: "-0.020000",
        },
      ],
      error_message: null,
      result_summary: {
        trade_dates_evaluated: 2,
        total_candidates_evaluated: 2,
        qualifying_observations: 2,
        unique_qualified_instruments: 2,
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
    win_rate: "0.500000",
    max_drawdown: "0.030000",
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
        realized_return: "0.040000",
      },
      {
        instrument_id: 62,
        symbol: "6758",
        entry_date: "2026-04-16",
        exit_date: "2026-04-18",
        exit_reason: "stop_loss",
        realized_return: "-0.020000",
      },
    ],
    source_screen_run: {
      id: 8,
      trade_date: "2026-04-15",
      strategy_configuration_version: 4,
      status: "completed",
    },
    ...overrides,
  };
}

describe("PortfolioReturnResultPanel", () => {
  beforeEach(() => {
    setDataMock.mockClear();
    applyOptionsMock.mockClear();
    fitContentMock.mockClear();
    removeMock.mockClear();
    observeMock.mockClear();
    disconnectMock.mockClear();

    vi.stubGlobal(
      "ResizeObserver",
      class {
        observe = observeMock;
        disconnect = disconnectMock;
      },
    );
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("renders FR45 metrics, the equity chart, and the source screen run link", () => {
    render(<PortfolioReturnResultPanel initialResult={buildResult()} initialError={null} />);

    expect(screen.getByText("4.50%")).toBeInTheDocument();
    expect(screen.getByText("50.00%")).toBeInTheDocument();
    expect(screen.getByText("3.00%")).toBeInTheDocument();
    expect(screen.getByLabelText("组合权益曲线")).toBeInTheDocument();
    expect(screen.getByText("查看来源筛选 #8")).toBeInTheDocument();
    expect(screen.getByText("7203")).toBeInTheDocument();
    expect(screen.getByText("止损平仓")).toBeInTheDocument();
    expect(setDataMock).toHaveBeenCalled();
    expect(observeMock).toHaveBeenCalled();
  });

  it("renders empty-portfolio and missing-source fallback copy", () => {
    render(
      <PortfolioReturnResultPanel
        initialResult={buildResult({
          run: {
            ...buildResult().run,
            source_screen_run_id: 999,
            position_count_after_exclusions: 0,
            excluded_securities: [{ instrument_id: 61, symbol: "7203", exclusion_reason: "cap_overflow" }],
            equity_curve: [],
            per_security_returns: [],
          },
          cumulative_return: "0.000000",
          win_rate: "0.000000",
          max_drawdown: "0.000000",
          equity_curve: [],
          per_security_returns: [],
          source_screen_run: null,
        })}
        initialError={null}
      />,
    );

    expect(screen.getByRole("heading", { name: "原筛选记录不可用 — 策略定义无法解析" })).toBeInTheDocument();
    expect(screen.getByText("查看来源筛选（不可用）")).toBeInTheDocument();
    expect(screen.getByText("原筛选记录不可用，策略定义无法解析，因此不再显示 partial result。")).toBeInTheDocument();
  });

  it("renders the provenance-unavailable downgrade state and disables trace-back link copy", () => {
    render(<PortfolioReturnResultPanel initialResult={null} initialError={SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE} />);

    expect(screen.getByRole("heading", { name: SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE })).toBeInTheDocument();
    expect(screen.getByText("查看来源筛选（不可用）")).toBeInTheDocument();
    expect(
      screen.getByText("该 run 的执行结果仍已持久化，但来源 screen run 当前不可解析，因此策略定义无法再解释。"),
    ).toBeInTheDocument();
  });
});
