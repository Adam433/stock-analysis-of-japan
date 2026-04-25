import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
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

class MockIntersectionObserver {
  static instances: MockIntersectionObserver[] = [];

  callback: IntersectionObserverCallback;

  constructor(callback: IntersectionObserverCallback) {
    this.callback = callback;
    MockIntersectionObserver.instances.push(this);
  }

  observe = vi.fn();
  unobserve = vi.fn();
  disconnect = vi.fn();

  trigger(elements: Element[]) {
    this.callback(
      elements.map(
        (element) =>
          ({
            isIntersecting: true,
            target: element,
          }) as IntersectionObserverEntry,
      ),
      this as unknown as IntersectionObserver,
    );
  }
}

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

function buildQualifiedResult(index: number) {
  return {
    instrument_id: 60 + index,
    symbol: `${7202 + index}`,
    exchange: "TSE",
    trade_date: "2026-04-15",
    best_rps_value: "97.1",
    rps_threshold: 90,
    high_proximity_ratio: "0.98",
    high_proximity_threshold_pct: "5.00",
    max_drawdown_from_high_pct: "2.10",
    rps_condition_passed: true,
    high_proximity_condition_passed: true,
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
    qualified_results: [buildQualifiedResult(1)],
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
    vi.unstubAllGlobals();
    MockIntersectionObserver.instances = [];
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
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/watchlist")) {
        return Promise.resolve(new Response(JSON.stringify({ entries: [{ instrument_id: 61 }] }), { status: 200 }));
      }
      if (url.endsWith("/screen/runs")) {
        return Promise.resolve(new Response(JSON.stringify({ screen_run: screenRun }), { status: 200 }));
      }
      if (url.includes("/inline-analysis")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              inline_analysis: {
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
                candlesticks: [],
                candlestick_window_days_available: 0,
                valuation_by_fiscal_year: [],
                generated_at: "2026-04-17T12:00:00Z",
              },
            }),
            { status: 200 },
          ),
        );
      }
      return Promise.resolve(new Response(JSON.stringify({ entries: [] }), { status: 200 }));
    });

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

  it("eagerly loads inline analysis for small result lists", async () => {
    vi.stubGlobal("IntersectionObserver", MockIntersectionObserver);
    const screenRun = buildScreenRun();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/watchlist")) {
        return Promise.resolve(new Response(JSON.stringify({ entries: [] }), { status: 200 }));
      }
      if (url.includes("/inline-analysis")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              inline_analysis: {
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
                candlesticks: [],
                candlestick_window_days_available: 0,
                valuation_by_fiscal_year: [],
                generated_at: "2026-04-17T12:00:00Z",
              },
            }),
            { status: 200 },
          ),
        );
      }
      return Promise.resolve(new Response(JSON.stringify({ screen_run: screenRun }), { status: 200 }));
    });

    render(
      <StrategyConfigPanel
        apiBaseUrl="http://localhost:8000"
        initialData={buildConfigurationResponse()}
        initialError={null}
        initialRun={screenRun}
        initialRunError={null}
        initialTradeDates={[{ trade_date: "2026-04-15" }]}
        initialTradeDateError={null}
      />,
    );

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "http://localhost:8000/stocks/61/inline-analysis?screen_run_id=8",
        { cache: "no-store" },
      );
    });
  });

  it("waits for intersection before loading inline analysis for large result lists", async () => {
    vi.stubGlobal("IntersectionObserver", MockIntersectionObserver);
    const screenRun = buildScreenRun({
      qualified_count: 20,
      qualified_results: Array.from({ length: 20 }, (_, index) => buildQualifiedResult(index + 1)),
    });
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/watchlist")) {
        return Promise.resolve(new Response(JSON.stringify({ entries: [] }), { status: 200 }));
      }
      if (url.includes("/inline-analysis")) {
        const instrumentId = Number(url.match(/stocks\/(\d+)/)?.[1] ?? "0");
        return Promise.resolve(
          new Response(
            JSON.stringify({
              inline_analysis: {
                instrument: {
                  id: instrumentId,
                  symbol: String(instrumentId),
                  exchange: "TSE",
                  name: `Name ${instrumentId}`,
                  currency: "JPY",
                },
                screen_run_ref: {
                  id: 8,
                  trade_date: "2026-04-15",
                },
                candlesticks: [],
                candlestick_window_days_available: 0,
                valuation_by_fiscal_year: [],
                generated_at: "2026-04-17T12:00:00Z",
              },
            }),
            { status: 200 },
          ),
        );
      }
      return Promise.resolve(new Response(JSON.stringify({ screen_run: screenRun }), { status: 200 }));
    });

    render(
      <StrategyConfigPanel
        apiBaseUrl="http://localhost:8000"
        initialData={buildConfigurationResponse()}
        initialError={null}
        initialRun={screenRun}
        initialRunError={null}
        initialTradeDates={[{ trade_date: "2026-04-15" }]}
        initialTradeDateError={null}
      />,
    );

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledTimes(1);
    });

    const observer = MockIntersectionObserver.instances[0];
    await act(async () => {
      observer.trigger([screen.getByTestId("card-61"), screen.getByTestId("card-62")]);
    });

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith("http://localhost:8000/stocks/61/inline-analysis?screen_run_id=8", {
        cache: "no-store",
      });
      expect(fetchMock).toHaveBeenCalledWith("http://localhost:8000/stocks/62/inline-analysis?screen_run_id=8", {
        cache: "no-store",
      });
    });
  });
});
