"use client";

import { useEffect, useRef } from "react";
import {
  CandlestickSeries,
  ColorType,
  createChart,
  CrosshairMode,
  HistogramSeries,
  LineStyle,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type Time,
} from "lightweight-charts";

import { formatFiscalYearLabel } from "@/lib/formatters";
import type { Candlestick, FiscalYearValuation, InlineAnalysisPayload } from "@/lib/types";

type ResultAnalysisCardProps = {
  instrumentId: number;
  symbol: string;
  screenRunId: number;
  analysisPayload: InlineAnalysisPayload | "idle" | "loading" | "failed" | null;
  errorMessage?: string | null;
  onRetry?: () => void;
};

function toTime(value: string): Time {
  return value as Time;
}

function buildCandles(rows: Candlestick[]): CandlestickData<Time>[] {
  return rows
    .filter((row) => row.open !== null && row.high !== null && row.low !== null && row.close !== null)
    .map((row) => ({
      time: toTime(row.trade_date),
      open: Number(row.open),
      high: Number(row.high),
      low: Number(row.low),
      close: Number(row.close),
    }));
}

function buildFiscalBars(rows: FiscalYearValuation[]): HistogramData<Time>[] {
  return rows.map((row) => {
    const value = row.net_income === null ? 0 : Number(row.net_income);
    const time = `${row.fiscal_year_label}-${String(row.fiscal_year_end_month).padStart(2, "0")}` as Time;

    return {
      time,
      value,
      color:
        row.data_status === "missing" || row.net_income === null
          ? "rgba(102, 95, 85, 0.28)"
          : value < 0
            ? "#8b2f24"
            : "#0e5a52",
    };
  });
}

function applyChartTheme(chart: IChartApi) {
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
      rightOffset: 4,
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

function formatNetIncome(value: string | null, currency: string): string {
  if (value === null) {
    return "数据缺失";
  }

  return new Intl.NumberFormat("zh-CN", {
    maximumFractionDigits: 0,
  }).format(Number(value)) + ` ${currency}`;
}

// Product convention: loss-year PE is always rendered as N/A, never as a negative PE.
function formatPe(row: FiscalYearValuation): string {
  if (row.net_income !== null && Number(row.net_income) < 0) {
    return "N/A";
  }
  if (row.pe === null) {
    return "数据缺失";
  }
  return Number(row.pe).toFixed(1);
}

function formatPb(value: string | null): string {
  if (value === null) {
    return "数据缺失";
  }
  return Number(value).toFixed(2);
}

function renderSkeleton(state: "idle" | "loading", symbol: string) {
  return (
    <section className="result-analysis result-analysis--skeleton" role="status" aria-label="内联分析加载中">
      <div className="result-analysis__header">
        <p className="status-label">内联分析</p>
        <p className="status-copy">
          {state === "idle" ? `${symbol} 的内联分析将在接近视口时加载。` : `正在加载 ${symbol} 的 1 年 K 线与财务概览……`}
        </p>
      </div>
      <div className="analysis-skeleton-grid" aria-hidden="true">
        <div className="analysis-skeleton-block" />
        <div className="analysis-skeleton-block" />
      </div>
    </section>
  );
}

export function ResultAnalysisCard({
  instrumentId,
  symbol,
  screenRunId,
  analysisPayload,
  errorMessage,
  onRetry,
}: ResultAnalysisCardProps) {
  const priceContainerRef = useRef<HTMLDivElement | null>(null);
  const fundamentalsContainerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (
      analysisPayload === null ||
      analysisPayload === "idle" ||
      analysisPayload === "loading" ||
      analysisPayload === "failed"
    ) {
      return;
    }

    const priceContainer = priceContainerRef.current;
    const fundamentalsContainer = fundamentalsContainerRef.current;
    if (!priceContainer || !fundamentalsContainer) {
      return;
    }

    const priceChart = createChart(priceContainer, {
      width: priceContainer.clientWidth || 320,
      height: 260,
      autoSize: false,
    });
    const fundamentalsChart = createChart(fundamentalsContainer, {
      width: fundamentalsContainer.clientWidth || 320,
      height: 220,
      autoSize: false,
    });

    applyChartTheme(priceChart);
    applyChartTheme(fundamentalsChart);

    const candleSeries = priceChart.addSeries(CandlestickSeries, {
      upColor: "#0e5a52",
      downColor: "#8b2f24",
      borderVisible: false,
      wickUpColor: "#0e5a52",
      wickDownColor: "#8b2f24",
    });
    candleSeries.setData(buildCandles(analysisPayload.candlesticks));

    const fundamentalsSeries = fundamentalsChart.addSeries(HistogramSeries, {
      priceLineVisible: false,
      lastValueVisible: false,
    });
    fundamentalsSeries.setData(buildFiscalBars(analysisPayload.valuation_by_fiscal_year));

    priceChart.timeScale().fitContent();
    fundamentalsChart.timeScale().fitContent();

    let resizeObserver: ResizeObserver | null = null;
    if (typeof ResizeObserver !== "undefined") {
      resizeObserver = new ResizeObserver(() => {
        priceChart.applyOptions({ width: priceContainer.clientWidth || 320 });
        fundamentalsChart.applyOptions({ width: fundamentalsContainer.clientWidth || 320 });
      });
      resizeObserver.observe(priceContainer);
      resizeObserver.observe(fundamentalsContainer);
    }

    return () => {
      resizeObserver?.disconnect();
      priceChart.remove();
      fundamentalsChart.remove();
    };
  }, [analysisPayload]);

  if (analysisPayload === "idle" || analysisPayload === null) {
    return renderSkeleton("idle", symbol);
  }

  if (analysisPayload === "loading") {
    return renderSkeleton("loading", symbol);
  }

  if (analysisPayload === "failed") {
    return (
      <section className="result-analysis result-analysis--failed">
        <div className="result-analysis__header">
          <p className="status-label">内联分析</p>
          <p className="strategy-message strategy-message--error">
            {errorMessage ?? `无法加载 ${symbol} 的内联分析。`}
          </p>
        </div>
        <div className="workflow-banner workflow-banner--bad">
          <p>instrument #{instrumentId} / screen run #{screenRunId}</p>
        </div>
        <button type="button" className="strategy-button strategy-button--secondary" onClick={onRetry}>
          重试加载
        </button>
      </section>
    );
  }

  const hasShortWindow = analysisPayload.candlestick_window_days_available < 252;
  const valuationRows = analysisPayload.valuation_by_fiscal_year;

  return (
    <section className="result-analysis">
      <div className="result-analysis__header">
        <div>
          <p className="status-label">内联分析</p>
          <h4>{analysisPayload.instrument.symbol} 的价格走势与财务概览</h4>
        </div>
        <div className="result-analysis__meta">
          <p className="status-copy">来源筛选 #{analysisPayload.screen_run_ref.id}</p>
          <p className="status-copy">交易日 {analysisPayload.screen_run_ref.trade_date}</p>
        </div>
      </div>

      {hasShortWindow ? (
        <p className="status-copy">
          历史数据仅覆盖 {analysisPayload.candlestick_window_days_available} 个交易日。
        </p>
      ) : null}

      <div className="analysis-chart-grid">
        <div className="chart-frame">
          <div ref={priceContainerRef} className="chart-canvas" aria-label={`${symbol} 1 年 K 线图`} />
        </div>
        <div className="chart-frame">
          <div ref={fundamentalsContainerRef} className="chart-canvas" aria-label={`${symbol} 财年净利润柱状图`} />
          {!valuationRows.length ? (
            <p className="status-copy chart-empty-copy">最近 5 个财年数据缺失。</p>
          ) : null}
        </div>
      </div>

      <div className="analysis-fiscal-list">
        {valuationRows.length ? (
          valuationRows.map((row) => {
            const isLoss = row.net_income !== null && Number(row.net_income) < 0;
            const statusLabel =
              row.data_status === "missing" || row.net_income === null
                ? "数据缺失"
                : isLoss
                  ? "亏损"
                  : "盈利";

            return (
              <article key={`${row.fiscal_year_label}-${row.fiscal_year_end_month}`} className="analysis-fiscal-card">
                <p className="status-label">{formatFiscalYearLabel(row.fiscal_year_label, row.fiscal_year_end_month)}</p>
                <dl>
                  <div>
                    <dt>净利润</dt>
                    <dd aria-label={`${statusLabel}，净利润 ${formatNetIncome(row.net_income, row.net_income_currency)}`}>
                      {row.net_income === null ? "" : isLoss ? "-" : "+"}
                      {formatNetIncome(row.net_income, row.net_income_currency)}
                      <span className="analysis-fiscal-status">{statusLabel}</span>
                    </dd>
                  </div>
                  <div>
                    <dt>PE</dt>
                    <dd title={isLoss ? "净亏损，PE 不适用" : undefined}>{formatPe(row)}</dd>
                  </div>
                  <div>
                    <dt>PB</dt>
                    <dd>{formatPb(row.pb)}</dd>
                  </div>
                </dl>
              </article>
            );
          })
        ) : (
          <article className="analysis-fiscal-card analysis-fiscal-card--missing">
            <p className="status-label">财务概览</p>
            <p className="status-copy">数据缺失</p>
          </article>
        )}
      </div>
    </section>
  );
}
