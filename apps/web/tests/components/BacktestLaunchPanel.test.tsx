import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { BacktestLaunchPanel } from "@/components/backtests/BacktestLaunchPanel";
import type { BacktestRun } from "@/lib/types";

const mockDefaults = {
  holding_days: 20,
  stop_loss_pct: -0.08,
  portfolio_cap: 20,
  entry_deferral_window_days: 5,
};

function buildBacktestRun(overrides: Partial<BacktestRun> = {}): BacktestRun {
  return {
    id: 9,
    source_screen_run_id: 8,
    strategy_configuration_id: 3,
    status: "running",
    backtest_lifecycle: "portfolio_return",
    start_date: "2026-04-15",
    end_date: "2026-04-15",
    started_at: "2026-04-16T09:00:00Z",
    completed_at: null,
    rps_definition_version: "rps-v2",
    dataset_trade_date_start: "2026-01-05",
    dataset_trade_date_end: "2026-04-15",
    dataset_checksum: "dataset-123",
    effective_holding_days: mockDefaults.holding_days,
    effective_stop_loss_pct: mockDefaults.stop_loss_pct.toFixed(4),
    effective_portfolio_cap: mockDefaults.portfolio_cap,
    effective_entry_deferral_window_days: mockDefaults.entry_deferral_window_days,
    error_message: null,
    result_summary: {
      trade_dates_evaluated: 0,
      total_candidates_evaluated: 0,
      qualifying_observations: 0,
      unique_qualified_instruments: 0,
      first_qualified_trade_date: null,
      last_qualified_trade_date: null,
      result_checksum: null,
    },
    parameter_set: {
      id: 7,
      version: 4,
      rps_threshold: 90,
      high_proximity_threshold_pct: "5.00",
      selected_rps_windows: [50, 120, 250],
    },
    ...overrides,
  };
}

describe("BacktestLaunchPanel", () => {
  function getInput(name: "holding_days" | "stop_loss_pct" | "portfolio_cap" | "entry_deferral_window_days") {
    const input = document.querySelector(`input[name="${name}"]`);
    if (!(input instanceof HTMLInputElement)) {
      throw new Error(`Missing input ${name}`);
    }

    return input;
  }

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("loads defaults from the endpoint and fills the launch fields", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ defaults: mockDefaults }), { status: 200 }),
    );

    render(
      <BacktestLaunchPanel
        apiBaseUrl="http://localhost:8000"
        screenRunId={8}
        initialRun={null}
        initialRuns={[]}
        initialError={null}
      />,
    );

    await waitFor(() => {
      expect(getInput("holding_days").value).toBe(String(mockDefaults.holding_days));
    });

    expect(getInput("stop_loss_pct").value).toBe(String(mockDefaults.stop_loss_pct));
    expect(getInput("portfolio_cap").value).toBe(String(mockDefaults.portfolio_cap));
    expect(getInput("entry_deferral_window_days").value).toBe(String(mockDefaults.entry_deferral_window_days));
    expect(fetchMock).toHaveBeenCalledWith("http://localhost:8000/backtests/defaults");
  });

  it("shows validation errors and disables launch when fields are invalid", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ defaults: mockDefaults }), { status: 200 }),
    );

    render(
      <BacktestLaunchPanel
        apiBaseUrl="http://localhost:8000"
        screenRunId={8}
        initialRun={null}
        initialRuns={[]}
        initialError={null}
      />,
    );

    await waitFor(() => {
      expect(getInput("holding_days").value).toBe(String(mockDefaults.holding_days));
    });

    fireEvent.change(getInput("holding_days"), { target: { value: "0" } });
    fireEvent.change(getInput("stop_loss_pct"), { target: { value: "0" } });
    fireEvent.change(getInput("portfolio_cap"), { target: { value: "0" } });
    fireEvent.change(getInput("entry_deferral_window_days"), { target: { value: "0" } });

    expect(screen.getByText("持有期必须是不小于 1 的整数。")).toBeInTheDocument();
    expect(screen.getByText("止损阈值必须大于 -1 且小于 0。")).toBeInTheDocument();
    expect(screen.getByText("组合上限必须是不小于 1 的整数。")).toBeInTheDocument();
    expect(screen.getByText("入场顺延窗口必须是不小于 1 的整数。")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "启动并执行回测" })).toBeDisabled();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("launches a portfolio-return backtest as a single action and disables the button while waiting", async () => {
    const user = userEvent.setup();
    const launchedRun = buildBacktestRun({ id: 11 });
    let resolveLaunch: ((value: Response) => void) | null = null;
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/backtests/defaults")) {
        return Promise.resolve(new Response(JSON.stringify({ defaults: mockDefaults }), { status: 200 }));
      }
      if (url.endsWith("/backtests/portfolio-return/runs")) {
        return new Promise<Response>((resolve) => {
          resolveLaunch = resolve;
        });
      }
      throw new Error(`Unexpected URL ${url}`);
    });

    render(
      <BacktestLaunchPanel
        apiBaseUrl="http://localhost:8000"
        screenRunId={8}
        initialRun={null}
        initialRuns={[]}
        initialError={null}
      />,
    );

    await waitFor(() => {
      expect(getInput("holding_days").value).toBe(String(mockDefaults.holding_days));
    });

    const button = screen.getByRole("button", { name: "启动并执行回测" });
    await user.click(button);

    expect(button).toBeDisabled();
    expect(screen.getByText("正在从 screen run #8 启动并开始执行 portfolio-return 回测……")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "执行最新任务" })).not.toBeInTheDocument();

    resolveLaunch?.(new Response(JSON.stringify({ backtest_run: launchedRun }), { status: 200 }));

    await waitFor(() => {
      expect(screen.getByText("portfolio-return 回测 #11 已启动，当前状态为 running。")).toBeInTheDocument();
    });

    expect(fetchMock).toHaveBeenCalledWith("http://localhost:8000/backtests/defaults");
    expect(fetchMock).toHaveBeenCalledWith("http://localhost:8000/backtests/portfolio-return/runs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        screen_run_id: 8,
        holding_days: mockDefaults.holding_days,
        stop_loss_pct: mockDefaults.stop_loss_pct,
        portfolio_cap: mockDefaults.portfolio_cap,
        entry_deferral_window_days: mockDefaults.entry_deferral_window_days,
      }),
    });
    expect(screen.getByRole("heading", { name: "#11" })).toBeInTheDocument();
    expect(screen.getByText("running")).toBeInTheDocument();
  });

  it("renders a legacy lifecycle badge for historical condition-hit runs", () => {
    const legacyRun = buildBacktestRun({ backtest_lifecycle: "legacy_condition_hit" });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ defaults: mockDefaults }), { status: 200 }),
    );

    render(
      <BacktestLaunchPanel
        apiBaseUrl="http://localhost:8000"
        screenRunId={8}
        initialRun={legacyRun}
        initialRuns={[legacyRun]}
        initialError={null}
      />,
    );

    expect(screen.getAllByText("历史 condition-hit 模型").length).toBeGreaterThan(0);
    expect(
      screen.getByText(/当前任务属于历史 condition-hit lifecycle；它会保留用于追溯/),
    ).toBeInTheDocument();
  });

  it("excludes legacy runs from portfolio-return aggregation cards", () => {
    const portfolioRun = buildBacktestRun({
      id: 21,
      status: "completed",
      completed_at: "2026-04-16T09:15:00Z",
      result_summary: {
        trade_dates_evaluated: 42,
        total_candidates_evaluated: 320,
        qualifying_observations: 18,
        unique_qualified_instruments: 7,
        first_qualified_trade_date: "2026-02-10",
        last_qualified_trade_date: "2026-03-28",
        result_checksum: "checksum-21",
      },
    });
    const legacyRun = buildBacktestRun({
      id: 8,
      status: "completed",
      backtest_lifecycle: "legacy_condition_hit",
      completed_at: "2026-03-16T09:15:00Z",
      result_summary: {
        trade_dates_evaluated: 40,
        total_candidates_evaluated: 300,
        qualifying_observations: 12,
        unique_qualified_instruments: 5,
        first_qualified_trade_date: "2026-01-10",
        last_qualified_trade_date: "2026-02-28",
        result_checksum: "checksum-08",
      },
    });

    render(
      <BacktestLaunchPanel
        apiBaseUrl="http://localhost:8000"
        screenRunId={8}
        initialRun={portfolioRun}
        initialRuns={[portfolioRun, legacyRun]}
        initialError={null}
      />,
    );

    const portfolioCard = screen.getByText("portfolio-return 已完成任务").closest("article");
    const legacyCard = screen.getByText("历史 condition-hit 任务").closest("article");

    expect(portfolioCard).not.toBeNull();
    expect(legacyCard).not.toBeNull();
    expect(within(portfolioCard as HTMLElement).getByText("1")).toBeInTheDocument();
    expect(within(legacyCard as HTMLElement).getByText("1")).toBeInTheDocument();
    expect(screen.getAllByText("历史 condition-hit 模型").length).toBeGreaterThan(0);
    expect(
      screen.getByText(/这些 runs 保留用于历史追溯，并显式带标签展示/),
    ).toBeInTheDocument();
  });
});
