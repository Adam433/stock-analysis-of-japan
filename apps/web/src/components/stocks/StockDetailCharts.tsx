"use client";

import { useEffect, useMemo, useRef } from "react";
import {
  CandlestickSeries,
  ColorType,
  createChart,
  CrosshairMode,
  LineSeries,
  LineStyle,
  type CandlestickData,
  type IChartApi,
  type LineData,
  type Time,
} from "lightweight-charts";
import type { Candlestick } from "@/lib/types";

type IndicatorHistoryRow = {
  trade_date: string;
  rps_50: string | null;
  rps_120: string | null;
  rps_250: string | null;
  high_proximity_ratio: string | null;
};

type StockDetailChartsProps = {
  candlesticks: Candlestick[];
  indicatorHistory: IndicatorHistoryRow[];
  rpsThreshold: number;
};

function toTime(value: string): Time {
  return value as Time;
}

function buildCandles(rows: Candlestick[]): CandlestickData<Time>[] {
  return rows
    .filter(
      (row) =>
        row.open !== null &&
        row.high !== null &&
        row.low !== null &&
        row.close !== null,
    )
    .map((row) => ({
      time: toTime(row.trade_date),
      open: Number(row.open),
      high: Number(row.high),
      low: Number(row.low),
      close: Number(row.close),
    }));
}

function buildLineData(
  rows: IndicatorHistoryRow[],
  key: "rps_50" | "rps_120" | "rps_250",
): LineData<Time>[] {
  return rows
    .filter((row) => row[key] !== null)
    .map((row) => ({
      time: toTime(row.trade_date),
      value: Number(row[key]),
    }));
}

function buildThresholdData(rows: IndicatorHistoryRow[], threshold: number): LineData<Time>[] {
  return rows.map((row) => ({
    time: toTime(row.trade_date),
    value: threshold,
  }));
}

function applySharedChartTheme(chart: IChartApi) {
  chart.applyOptions({
    layout: {
      background: { type: ColorType.Solid, color: "rgba(255,255,255,0)" },
      textColor: "#665f55",
      fontFamily: 'Georgia, "Times New Roman", serif',
    },
    grid: {
      vertLines: { color: "rgba(27, 24, 20, 0.06)" },
      horzLines: { color: "rgba(27, 24, 20, 0.06)" },
    },
    rightPriceScale: {
      borderColor: "rgba(27, 24, 20, 0.08)",
    },
    timeScale: {
      borderColor: "rgba(27, 24, 20, 0.08)",
      timeVisible: true,
      secondsVisible: false,
      rightOffset: 8,
      fixRightEdge: false,
    },
    crosshair: {
      mode: CrosshairMode.Normal,
      vertLine: {
        color: "rgba(27, 24, 20, 0.22)",
        width: 1,
        style: LineStyle.Solid,
      },
      horzLine: {
        color: "rgba(27, 24, 20, 0.22)",
        width: 1,
        style: LineStyle.Solid,
      },
    },
  });
}

function syncVisibleRange(sourceChart: IChartApi, targetChart: IChartApi) {
  let isSyncing = false;
  sourceChart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
    if (isSyncing) {
      return;
    }
    if (range) {
      isSyncing = true;
      targetChart.timeScale().setVisibleLogicalRange(range);
      queueMicrotask(() => {
        isSyncing = false;
      });
    }
  });
}

export function StockDetailCharts({
  candlesticks,
  indicatorHistory,
  rpsThreshold,
}: StockDetailChartsProps) {
  const priceContainerRef = useRef<HTMLDivElement | null>(null);
  const rpsContainerRef = useRef<HTMLDivElement | null>(null);

  const hasIndicatorHistory = indicatorHistory.length > 0;

  const candleData = useMemo(() => buildCandles(candlesticks), [candlesticks]);
  const rps50Data = useMemo(() => buildLineData(indicatorHistory, "rps_50"), [indicatorHistory]);
  const rps120Data = useMemo(() => buildLineData(indicatorHistory, "rps_120"), [indicatorHistory]);
  const rps250Data = useMemo(() => buildLineData(indicatorHistory, "rps_250"), [indicatorHistory]);
  const thresholdData = useMemo(() => buildThresholdData(indicatorHistory, rpsThreshold), [indicatorHistory, rpsThreshold]);

  useEffect(() => {
    const priceContainer = priceContainerRef.current;
    const rpsContainer = rpsContainerRef.current;
    if (!priceContainer || !rpsContainer) {
      return;
    }

    const priceChart = createChart(priceContainer, {
      width: priceContainer.clientWidth,
      height: 320,
      autoSize: true,
    });
    const rpsChart = createChart(rpsContainer, {
      width: rpsContainer.clientWidth,
      height: 220,
      autoSize: true,
    });

    applySharedChartTheme(priceChart);
    applySharedChartTheme(rpsChart);

    const priceSeries = priceChart.addSeries(CandlestickSeries, {
      upColor: "#0e5a52",
      downColor: "#8b2f24",
      borderVisible: false,
      wickUpColor: "#0e5a52",
      wickDownColor: "#8b2f24",
      priceLineVisible: true,
      lastValueVisible: true,
    });
    priceSeries.setData(candleData);

    const thresholdSeries = rpsChart.addSeries(LineSeries, {
      color: "#c96b2c",
      lineWidth: 2,
      lineStyle: LineStyle.Dashed,
      lastValueVisible: false,
      priceLineVisible: false,
    });
    thresholdSeries.setData(thresholdData);

    const rps50Series = rpsChart.addSeries(LineSeries, {
      color: "#0e5a52",
      lineWidth: 3,
      lastValueVisible: false,
    });
    const rps120Series = rpsChart.addSeries(LineSeries, {
      color: "#c96b2c",
      lineWidth: 2,
      lineStyle: LineStyle.Dashed,
      lastValueVisible: false,
    });
    const rps250Series = rpsChart.addSeries(LineSeries, {
      color: "#8b2f24",
      lineWidth: 2,
      lineStyle: LineStyle.Dotted,
      lastValueVisible: false,
    });

    rps50Series.setData(rps50Data);
    rps120Series.setData(rps120Data);
    rps250Series.setData(rps250Data);

    priceChart.timeScale().fitContent();
    rpsChart.timeScale().fitContent();
    syncVisibleRange(priceChart, rpsChart);
    syncVisibleRange(rpsChart, priceChart);

    const resizeObserver = new ResizeObserver(() => {
      priceChart.applyOptions({ width: priceContainer.clientWidth });
      rpsChart.applyOptions({ width: rpsContainer.clientWidth });
    });
    resizeObserver.observe(priceContainer);
    resizeObserver.observe(rpsContainer);

    return () => {
      resizeObserver.disconnect();
      priceChart.remove();
      rpsChart.remove();
    };
  }, [candleData, rps50Data, rps120Data, rps250Data, thresholdData]);

  return (
    <>
      <div className="chart-frame">
        <div ref={priceContainerRef} className="chart-canvas" aria-label="K 线图" />
      </div>
      <div className="chart-frame">
        <div ref={rpsContainerRef} className="chart-canvas" aria-label="RPS 面板" />
        {!hasIndicatorHistory ? (
          <p className="status-copy chart-empty-copy">
            暂无可绘制的 RPS 历史序列，页面已回退为安全占位状态。
          </p>
        ) : null}
      </div>
      <div className="rps-legend">
        <article className="legend-card">
          <p className="status-label">正式筛选信号</p>
          <h3>阈值 {rpsThreshold}</h3>
          <p className="status-copy">
            橙色虚线只是在图上映射正式筛选阈值；真正的通过/未通过判定来自
            已保存的筛选判定结果。
          </p>
        </article>
        <article className="legend-card">
          <p className="status-label">仅解释用途</p>
          <h3>真实历史序列</h3>
          <p className="status-copy">
            RPS 50 / 120 / 250 来自后端持久化的历史序列，用于回看走势，
            不会单独生成新的官方入选事件。
          </p>
        </article>
        <article className="legend-card legend-card--warning">
          <p className="status-label">未纳入正式规则</p>
          <h3>观察性标注</h3>
          <p className="status-copy">
            `翻红`、临时走势注释或任何未进入后端契约与测试的图形状态，当前都只是辅助理解，
            不影响筛选结论。
          </p>
        </article>
      </div>
    </>
  );
}
