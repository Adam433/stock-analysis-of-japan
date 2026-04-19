"use client";

import Link from "next/link";
import { useEffect, useRef } from "react";
import {
  ColorType,
  createChart,
  CrosshairMode,
  LineSeries,
  LineStyle,
  type IChartApi,
  type LineData,
  type Time,
} from "lightweight-charts";

import {
  SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE,
  isSourceScreenRunUnavailableMessage,
} from "@/lib/backtestErrors";
import { formatDateOnly, formatRatioAsPercent, formatTimestamp } from "@/lib/formatters";
import type { PortfolioReturnRunResult } from "@/lib/types";

type PortfolioReturnResultPanelProps = {
  initialResult: PortfolioReturnRunResult | null;
  initialError: string | null;
};

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

function EquityCurveChart({
  equityCurve,
}: {
  equityCurve: PortfolioReturnRunResult["equity_curve"];
}) {
  const chartContainerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const chartContainer = chartContainerRef.current;
    if (!chartContainer || equityCurve.length === 0) {
      return;
    }

    const chart = createChart(chartContainer, {
      width: chartContainer.clientWidth,
      height: 320,
      autoSize: false,
    });
    applyChartTheme(chart);

    const lineSeries = chart.addSeries(LineSeries, {
      color: "#0e5a52",
      lineWidth: 3,
      lastValueVisible: true,
      priceLineVisible: true,
    });
    const lineData: LineData<Time>[] = equityCurve.map((point) => ({
      time: point.trade_date as Time,
      value: Number(point.equity),
    }));
    lineSeries.setData(lineData);
    chart.timeScale().fitContent();

    const resizeObserver = new ResizeObserver(() => {
      chart.applyOptions({ width: chartContainer.clientWidth });
    });
    resizeObserver.observe(chartContainer);

    return () => {
      resizeObserver.disconnect();
      chart.remove();
    };
  }, [equityCurve]);

  if (equityCurve.length === 0) {
    return (
      <div className="chart-frame">
        <p className="status-copy chart-empty-copy">本次组合没有实际入场标的，暂无可绘制的组合权益曲线。</p>
      </div>
    );
  }

  return (
    <div className="chart-frame">
      <div ref={chartContainerRef} className="chart-canvas" aria-label="组合权益曲线" />
    </div>
  );
}

function exitReasonLabel(exitReason: string): string {
  return exitReason === "stop_loss" ? "止损平仓" : "持有期到期";
}

function buildBarWidth(realizedReturn: string): string {
  const magnitude = Math.min(Math.abs(Number(realizedReturn)) * 1000, 100);
  return `${Math.max(magnitude, 6)}%`;
}

export function PortfolioReturnResultPanel({
  initialResult,
  initialError,
}: PortfolioReturnResultPanelProps) {
  const isSourceUnavailable =
    isSourceScreenRunUnavailableMessage(initialError) ||
    (initialResult !== null && initialResult.source_screen_run === null);

  if (initialError) {
    return (
      <section className="screen-panel">
        <div className="screen-panel__header">
          <p className="eyebrow">结果详情</p>
          <h1>{isSourceUnavailable ? SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE : "portfolio-return 回测结果当前不可用。"}</h1>
          <p className="hero-text">{initialError}</p>
        </div>
        {isSourceUnavailable ? (
          <div className="run-metadata-grid">
            <article className="run-metadata-card run-metadata-card--warning">
              <p className="status-label">trace-back</p>
              <h3>
                <span className="result-link result-link--disabled" aria-disabled="true">
                  查看来源筛选（不可用）
                </span>
              </h3>
              <p className="status-copy">
                该 run 的执行结果仍已持久化，但来源 screen run 当前不可解析，因此策略定义无法再解释。
              </p>
            </article>
          </div>
        ) : null}
      </section>
    );
  }

  if (initialResult === null || initialResult.source_screen_run === null) {
    return (
      <section className="screen-panel">
        <div className="screen-panel__header">
          <p className="eyebrow">结果详情</p>
          <h1>{initialResult === null ? "暂无可展示的 portfolio-return 回测结果。" : SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE}</h1>
        </div>
        {initialResult !== null ? (
          <div className="run-metadata-grid">
            <article className="run-metadata-card run-metadata-card--warning">
              <p className="status-label">trace-back</p>
              <h3>
                <span className="result-link result-link--disabled" aria-disabled="true">
                  查看来源筛选（不可用）
                </span>
              </h3>
              <p className="status-copy">原筛选记录不可用，策略定义无法解析，因此不再显示 partial result。</p>
            </article>
          </div>
        ) : null}
      </section>
    );
  }

  const { run } = initialResult;

  return (
    <section className="screen-panel">
      <div className="screen-panel__header">
        <p className="eyebrow">结果详情</p>
        <h1>从投资视角复盘回测 #{run.id}。</h1>
        <p className="hero-text">
          这里直接展示 Story 5.2 已持久化的组合级结果，并按 FR45 计算 win rate 与 max drawdown，
          不再回退到旧的 condition-hit 统计语义。
        </p>
      </div>

      <div className="screen-summary-grid">
        <article className="screen-summary-card">
          <p className="status-label">累计收益</p>
          <h2>{formatRatioAsPercent(initialResult.cumulative_return)}</h2>
          <p className="status-copy">来源于已完成 run 的组合 cumulative return。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">胜率</p>
          <h2>{formatRatioAsPercent(initialResult.win_rate)}</h2>
          <p className="status-copy">只统计已平仓仓位中 realized return 严格大于 0 的占比。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">最大回撤</p>
          <h2>{formatRatioAsPercent(initialResult.max_drawdown)}</h2>
          <p className="status-copy">按组合权益曲线的 peak-to-trough 降幅计算。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">来源筛选</p>
          <h2>#{run.source_screen_run_id ?? "未知"}</h2>
          <p className="status-copy">
            {initialResult.source_screen_run
              ? `${formatDateOnly(initialResult.source_screen_run.trade_date)} / 参数集 v${initialResult.source_screen_run.strategy_configuration_version ?? "?"}`
              : "原筛选记录不可用"}
          </p>
        </article>
      </div>

      <section className="result-panel">
        <div className="result-panel__header">
          <p className="eyebrow">运行上下文</p>
          <h2>持久化的参数、来源与完成时刻。</h2>
        </div>
        <div className="run-metadata-grid">
          <article className="run-metadata-card">
            <p className="status-label">生效参数</p>
            <h3>
              {run.effective_holding_days ?? "-"} / {run.effective_stop_loss_pct ?? "-"} /{" "}
              {run.effective_portfolio_cap ?? "-"} / {run.effective_entry_deferral_window_days ?? "-"}
            </h3>
            <p className="status-copy">持有期 / 止损 / 组合上限 / 入场递延窗口。</p>
          </article>
          <article className="run-metadata-card">
            <p className="status-label">已入场标的</p>
            <h3>{run.position_count_after_exclusions ?? 0}</h3>
            <p className="status-copy">已排除证券数 {run.excluded_securities.length}，ranking policy 为 {run.ranking_policy_id ?? "未记录"}。</p>
          </article>
          <article className="run-metadata-card">
            <p className="status-label">开始 / 完成</p>
            <h3>{formatTimestamp(run.started_at)}</h3>
            <p className="status-copy">{formatTimestamp(run.completed_at, "尚未完成")}</p>
          </article>
          <article className="run-metadata-card">
            <p className="status-label">trace-back</p>
            {initialResult.source_screen_run ? (
              <>
                <h3>
                  <Link className="result-link" href={`/screen?run_id=${initialResult.source_screen_run.id}`}>
                    查看来源筛选 #{initialResult.source_screen_run.id}
                  </Link>
                </h3>
                <p className="status-copy">若后续 story 扩展 provenance 页，这个入口会继续沿用同一 source run。</p>
              </>
            ) : (
              <>
                <h3>原筛选记录不可用</h3>
                <p className="status-copy">该 run 仍可查看结果，但来源筛选记录当前无法读取。</p>
              </>
            )}
          </article>
        </div>
      </section>

      <section className="result-panel">
        <div className="result-panel__header">
          <p className="eyebrow">组合权益</p>
          <h2>按真实 trade_date 展示单 run 的组合曲线。</h2>
        </div>
        <EquityCurveChart equityCurve={initialResult.equity_curve} />
      </section>

      <section className="result-panel">
        <div className="result-panel__header">
          <p className="eyebrow">单证券分布</p>
          <h2>每笔已平仓仓位的 realized return 与退出原因。</h2>
        </div>
        {initialResult.per_security_returns.length ? (
          <div className="result-list">
            {initialResult.per_security_returns.map((item) => (
              <article key={`${item.instrument_id}-${item.exit_date}`} className="result-card">
                <div className="result-card__title">
                  <div>
                    <p className="status-label">{item.symbol}</p>
                    <h3>{formatRatioAsPercent(item.realized_return)}</h3>
                    <p className="status-copy">
                      {item.entry_date} 入场，{item.exit_date} 平仓
                    </p>
                  </div>
                  <div className="result-card__actions">
                    <p
                      className={`distribution-exit-flag ${
                        item.exit_reason === "stop_loss"
                          ? "distribution-exit-flag--stop"
                          : "distribution-exit-flag--expiry"
                      }`}
                    >
                      {exitReasonLabel(item.exit_reason)}
                    </p>
                  </div>
                </div>
                <div className="distribution-bar-track" aria-hidden="true">
                  <span
                    className={`distribution-bar-fill ${
                      Number(item.realized_return) >= 0
                        ? "distribution-bar-fill--positive"
                        : "distribution-bar-fill--negative"
                    }`}
                    style={{ width: buildBarWidth(item.realized_return) }}
                  />
                </div>
              </article>
            ))}
          </div>
        ) : (
          <p className="empty-state">本次组合在排除后没有实际入场标的，因此没有单证券收益分布。</p>
        )}
      </section>
    </section>
  );
}
