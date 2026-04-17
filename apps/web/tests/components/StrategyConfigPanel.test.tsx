import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { StrategyConfigPanel } from "@/components/screen/StrategyConfigPanel";
import type { ScreenRun, StrategyConfigurationResponse } from "@/lib/types";

vi.mock("next/link", () => ({
  default: ({ href, children, className }: { href: string; children: React.ReactNode; className?: string }) => (
    <a href={href} className={className}>
      {children}
    </a>
  ),
}));

vi.mock("@/components/watchlist/WatchlistToggleButton", () => ({
  WatchlistToggleButton: ({ symbol }: { symbol: string }) => <button type="button">观察 {symbol}</button>,
}));

function buildConfigurationResponse(
  overrides: Partial<StrategyConfigurationResponse["configuration"]> = {},
): StrategyConfigurationResponse {
  return {
    configuration: {
      id: 3,
      version: 4,
      rps_threshold: 90,
      high_proximity_threshold_pct: "5.00",
      selected_rps_windows: [50, 120, 250],
      ...overrides,
    },
    validation: {
      rps_threshold: { min: 0, max: 100, default: 90 },
      high_proximity_threshold_pct: { min: "0.00", max: "100.00", default: "5.00" },
      selected_rps_windows: { approved: [50, 120, 250], default: [50, 120, 250] },
    },
  };
}

function buildScreenRun(overrides: Partial<ScreenRun> = {}): ScreenRun {
  return {
    id: 8,
    strategy_configuration_id: 3,
    trade_date: "2026-04-15",
    executed_at: "2026-04-16T09:30:00Z",
    total_candidates: 120,
    qualified_count: 4,
    status: "completed",
    parameter_set: {
      id: 3,
      version: 4,
      rps_threshold: 90,
      high_proximity_threshold_pct: "5.00",
      selected_rps_windows: [50, 120, 250],
    },
    qualified_results: [
      {
        instrument_id: 61,
        symbol: "7203",
        exchange: "TSE",
        trade_date: "2026-04-15",
        best_rps_value: "97.1",
        rps_threshold: 90,
        high_proximity_ratio: "0.98",
        high_proximity_threshold_pct: "5.00",
        max_drawdown_from_high_pct: "2.10",
        rps_condition_passed: true,
        high_proximity_condition_passed: true,
      },
    ],
    ...overrides,
  };
}

describe("StrategyConfigPanel", () => {
  function getInput(name: string) {
    const input = document.querySelector(`[name="${name}"]`);
    if (
      !(input instanceof HTMLInputElement) &&
      !(input instanceof HTMLSelectElement)
    ) {
      throw new Error(`Missing input ${name}`);
    }

    return input;
  }

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("shows a validation error before saving when no RPS period is selected for screening", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ entries: [] }), { status: 200 }),
    );

    render(
      <StrategyConfigPanel
        apiBaseUrl="http://localhost:8000"
        initialData={buildConfigurationResponse()}
        initialError={null}
        initialRun={null}
        initialRunError={null}
        initialTradeDates={[{ trade_date: "2026-04-15" }]}
        initialTradeDateError={null}
      />,
    );

    await user.click(screen.getByRole("checkbox", { name: "50 日" }));
    await user.click(screen.getByRole("checkbox", { name: "120 日" }));
    await user.click(screen.getByRole("checkbox", { name: "250 日" }));
    await user.click(screen.getByRole("button", { name: "保存参数集" }));

    expect(screen.getByText("至少需要选择一个纳入筛选的 RPS 周期。")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("saves the strategy configuration and updates the active version", async () => {
    const user = userEvent.setup();
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(JSON.stringify({ entries: [{ instrument_id: 61 }] }), { status: 200 }))
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify(buildConfigurationResponse({ version: 5, rps_threshold: 95 })),
          { status: 200 },
        ),
      );

    render(
      <StrategyConfigPanel
        apiBaseUrl="http://localhost:8000"
        initialData={buildConfigurationResponse()}
        initialError={null}
        initialRun={null}
        initialRunError={null}
        initialTradeDates={[{ trade_date: "2026-04-15" }]}
        initialTradeDateError={null}
      />,
    );

    fireEvent.change(getInput("rps_threshold"), { target: { value: "95" } });
    await user.click(screen.getByRole("button", { name: "保存参数集" }));

    await waitFor(() => {
      expect(screen.getByText("配置已保存，新参数集将在下一次筛选中生效。")).toBeInTheDocument();
    });

    expect(fetchMock).toHaveBeenNthCalledWith(2, "http://localhost:8000/screen/configuration", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        rps_threshold: 95,
        high_proximity_threshold_pct: "5.00",
        selected_rps_windows: [50, 120, 250],
      }),
    });
    expect(screen.getByText("v5")).toBeInTheDocument();
  });

  it("launches a screen run and renders the qualified result list", async () => {
    const user = userEvent.setup();
    const screenRun = buildScreenRun();
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(JSON.stringify({ entries: [{ instrument_id: 61 }] }), { status: 200 }))
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ screen_run: screenRun }), { status: 200 }),
      );

    render(
      <StrategyConfigPanel
        apiBaseUrl="http://localhost:8000"
        initialData={buildConfigurationResponse()}
        initialError={null}
        initialRun={null}
        initialRunError={null}
        initialTradeDates={[{ trade_date: "2026-04-15" }]}
        initialTradeDateError={null}
      />,
    );

    fireEvent.change(getInput("trade_date"), { target: { value: "2026-04-15" } });
    await user.click(screen.getByRole("button", { name: "启动筛选" }));

    await waitFor(() => {
      expect(
        screen.getByText("筛选 #8 已完成，交易日 2026-04-15，候选 120 只，入选 4 只。"),
      ).toBeInTheDocument();
    });

    expect(fetchMock).toHaveBeenNthCalledWith(2, "http://localhost:8000/screen/runs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        trade_date: "2026-04-15",
      }),
    });
    expect(screen.getByRole("link", { name: "7203" })).toHaveAttribute(
      "href",
      "/stocks/61?screen_run_id=8",
    );
    expect(screen.getByText("观察 7203")).toBeInTheDocument();
  });

  it("blocks launching a screen run when there are unsaved parameter changes", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ entries: [{ instrument_id: 61 }] }), { status: 200 }),
    );

    render(
      <StrategyConfigPanel
        apiBaseUrl="http://localhost:8000"
        initialData={buildConfigurationResponse()}
        initialError={null}
        initialRun={null}
        initialRunError={null}
        initialTradeDates={[{ trade_date: "2026-04-15" }]}
        initialTradeDateError={null}
      />,
    );

    fireEvent.change(getInput("rps_threshold"), { target: { value: "95" } });

    expect(
      screen.getByText("当前有未保存的参数改动。保存后，新的参数集才会用于下一次筛选。"),
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "启动筛选" })).toBeDisabled();

    await user.click(screen.getByRole("button", { name: "启动筛选" }));

    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
