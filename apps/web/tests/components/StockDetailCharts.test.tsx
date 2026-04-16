import { render, screen } from "@testing-library/react";

import { StockDetailCharts } from "@/components/stocks/StockDetailCharts";

const setDataMock = vi.fn();
const applyOptionsMock = vi.fn();
const fitContentMock = vi.fn();
const setVisibleLogicalRangeMock = vi.fn();
const subscribeVisibleLogicalRangeChangeMock = vi.fn();
const removeMock = vi.fn();

vi.mock("lightweight-charts", () => ({
  CandlestickSeries: "CandlestickSeries",
  LineSeries: "LineSeries",
  ColorType: { Solid: "solid" },
  CrosshairMode: { Normal: 0 },
  LineStyle: { Solid: 0, Dashed: 1, Dotted: 2 },
  createChart: vi.fn(() => ({
    applyOptions: applyOptionsMock,
    addSeries: vi.fn(() => ({ setData: setDataMock })),
    timeScale: vi.fn(() => ({
      fitContent: fitContentMock,
      setVisibleLogicalRange: setVisibleLogicalRangeMock,
      subscribeVisibleLogicalRangeChange: subscribeVisibleLogicalRangeChangeMock,
    })),
    remove: removeMock,
  })),
}));

describe("StockDetailCharts", () => {
  const observeMock = vi.fn();
  const disconnectMock = vi.fn();

  beforeEach(() => {
    setDataMock.mockClear();
    applyOptionsMock.mockClear();
    fitContentMock.mockClear();
    setVisibleLogicalRangeMock.mockClear();
    subscribeVisibleLogicalRangeChangeMock.mockClear();
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

  it("renders the empty-state copy when no indicator history exists", () => {
    render(
      <StockDetailCharts
        candlesticks={[
          {
            trade_date: "2026-04-15",
            open: "10",
            high: "12",
            low: "9",
            close: "11",
            adj_close: "11",
            volume: 100,
            data_status: "complete",
          },
        ]}
        indicatorHistory={[]}
        rpsThreshold={90}
      />,
    );

    expect(screen.getByLabelText("K 线图")).toBeInTheDocument();
    expect(screen.getByLabelText("RPS 面板")).toBeInTheDocument();
    expect(screen.getByText("暂无可绘制的 RPS 历史序列，页面已回退为安全占位状态。")).toBeInTheDocument();
    expect(screen.getByText("阈值 90")).toBeInTheDocument();
  });

  it("initializes charts and omits the empty copy when indicator history exists", () => {
    render(
      <StockDetailCharts
        candlesticks={[
          {
            trade_date: "2026-04-15",
            open: "10",
            high: "12",
            low: "9",
            close: "11",
            adj_close: "11",
            volume: 100,
            data_status: "complete",
          },
        ]}
        indicatorHistory={[
          {
            trade_date: "2026-04-15",
            rps_50: "95",
            rps_120: "90",
            rps_250: "85",
            high_proximity_ratio: "0.98",
          },
        ]}
        rpsThreshold={90}
      />,
    );

    expect(screen.queryByText("暂无可绘制的 RPS 历史序列，页面已回退为安全占位状态。")).not.toBeInTheDocument();
    expect(setDataMock).toHaveBeenCalled();
    expect(observeMock).toHaveBeenCalled();
  });
});
