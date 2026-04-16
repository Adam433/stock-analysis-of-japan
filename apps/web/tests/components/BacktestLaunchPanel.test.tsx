import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { BacktestLaunchPanel } from "@/components/backtests/BacktestLaunchPanel";
import type { BacktestRun } from "@/lib/types";

function buildBacktestRun(overrides: Partial<BacktestRun> = {}): BacktestRun {
  return {
    id: 9,
    strategy_configuration_id: 3,
    status: "queued",
    start_date: "2026-01-01",
    end_date: "2026-12-31",
    started_at: "2026-04-16T09:00:00Z",
    completed_at: null,
    rps_definition_version: "rps-v2",
    dataset_trade_date_start: "2026-01-05",
    dataset_trade_date_end: "2026-04-15",
    dataset_checksum: "dataset-123",
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
      min_rps_lines_required: 2,
    },
    ...overrides,
  };
}

describe("BacktestLaunchPanel", () => {
  function getDateInput(name: "start_date" | "end_date") {
    const input = document.querySelector(`input[name="${name}"]`);
    if (!(input instanceof HTMLInputElement)) {
      throw new Error(`Missing input ${name}`);
    }

    return input;
  }

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("blocks submission when the date range is invalid", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(null, { status: 200 }));

    render(
      <BacktestLaunchPanel
        apiBaseUrl="http://localhost:8000"
        initialRun={null}
        initialRuns={[]}
        initialError={null}
      />,
    );

    fireEvent.change(getDateInput("start_date"), { target: { value: "2026-12-31" } });
    fireEvent.change(getDateInput("end_date"), { target: { value: "2026-01-01" } });
    await user.click(screen.getByRole("button", { name: "启动回测" }));

    expect(screen.getByText("开始日期必须早于或等于结束日期。")).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("launches a new backtest run and updates the latest run card", async () => {
    const user = userEvent.setup();
    const launchedRun = buildBacktestRun({ id: 11, status: "queued" });
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ backtest_run: launchedRun }), { status: 200 }),
    );

    render(
      <BacktestLaunchPanel
        apiBaseUrl="http://localhost:8000"
        initialRun={null}
        initialRuns={[]}
        initialError={null}
      />,
    );

    fireEvent.change(getDateInput("start_date"), { target: { value: "2026-02-01" } });
    fireEvent.change(getDateInput("end_date"), { target: { value: "2026-03-31" } });
    await user.click(screen.getByRole("button", { name: "启动回测" }));

    await waitFor(() => {
      expect(screen.getByText("回测 #11 已持久化，当前状态为 queued。")).toBeInTheDocument();
    });

    expect(fetchMock).toHaveBeenCalledWith("http://localhost:8000/backtests/runs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        start_date: "2026-02-01",
        end_date: "2026-03-31",
      }),
    });
    expect(screen.getByRole("heading", { name: "#11" })).toBeInTheDocument();
    expect(screen.getByText("queued")).toBeInTheDocument();
  });

  it("executes the latest run and renders the completed summary", async () => {
    const user = userEvent.setup();
    const initialRun = buildBacktestRun();
    const completedRun = buildBacktestRun({
      status: "completed",
      completed_at: "2026-04-16T09:15:00Z",
      result_summary: {
        trade_dates_evaluated: 42,
        total_candidates_evaluated: 320,
        qualifying_observations: 18,
        unique_qualified_instruments: 7,
        first_qualified_trade_date: "2026-02-10",
        last_qualified_trade_date: "2026-03-28",
        result_checksum: "checksum-42",
      },
    });
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ backtest_run: completedRun }), { status: 200 }),
    );

    render(
      <BacktestLaunchPanel
        apiBaseUrl="http://localhost:8000"
        initialRun={initialRun}
        initialRuns={[initialRun]}
        initialError={null}
      />,
    );

    await user.click(screen.getByRole("button", { name: "执行最新任务" }));

    await waitFor(() => {
      expect(screen.getByText("回测 #9 已完成，校验和 checksum-42。")).toBeInTheDocument();
    });

    expect(fetchMock).toHaveBeenCalledWith("http://localhost:8000/backtests/runs/9/execute", {
      method: "POST",
    });
    expect(screen.getByText("回测 #9 已完成，校验和 checksum-42。")).toBeInTheDocument();
    expect(screen.getAllByText("checksum-42").length).toBeGreaterThan(0);
  });
});
