"use client";

import Link from "next/link";

import {
  SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE,
  isSourceScreenRunUnavailableMessage,
} from "@/lib/backtestErrors";
import { formatDateOnly, formatRatioAsPercent } from "@/lib/formatters";
import type { PortfolioReturnRunComparison } from "@/lib/types";

type PortfolioReturnComparePanelProps = {
  initialRuns: PortfolioReturnRunComparison[];
  initialError: string | null;
};

const LINE_COLORS = ["#0e5a52", "#c96b2c", "#8b2f24", "#365870"];

function buildChartPath(
  points: Array<{ days_since_entry: number; equity: string }>,
  {
    width,
    height,
    maxDays,
    minEquity,
    maxEquity,
  }: {
    width: number;
    height: number;
    maxDays: number;
    minEquity: number;
    maxEquity: number;
  },
): string {
  return points
    .map((point, index) => {
      const x = maxDays === 0 ? 0 : (point.days_since_entry / maxDays) * width;
      const value = Number(point.equity);
      const y =
        maxEquity === minEquity
          ? height / 2
          : height - ((value - minEquity) / (maxEquity - minEquity)) * height;
      return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(" ");
}

export function PortfolioReturnComparePanel({
  initialRuns,
  initialError,
}: PortfolioReturnComparePanelProps) {
  const isSourceUnavailable = isSourceScreenRunUnavailableMessage(initialError);

  if (initialError) {
    return (
      <section className="screen-panel">
        <div className="screen-panel__header">
          <p className="eyebrow">跨 run 对比</p>
          <h1>{isSourceUnavailable ? SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE : "portfolio-return 对比当前不可用。"}</h1>
          <p className="hero-text">{initialError}</p>
        </div>
        {isSourceUnavailable ? (
          <div className="run-metadata-grid">
            <article className="run-metadata-card run-metadata-card--warning">
              <p className="status-label">可解释性</p>
              <h3>source provenance 已断开</h3>
              <p className="status-copy">
                只要被选 runs 中任意一条失去 source screen run，这个对比页就不会再做 partial render。
              </p>
            </article>
          </div>
        ) : null}
      </section>
    );
  }

  if (initialRuns.length === 0) {
    return (
      <section className="screen-panel">
        <div className="screen-panel__header">
          <p className="eyebrow">跨 run 对比</p>
          <h1>暂无可展示的 portfolio-return 对比结果。</h1>
        </div>
      </section>
    );
  }

  const alignedPoints = initialRuns.flatMap((run) => run.aligned_equity_curve.map((point) => Number(point.equity)));
  const maxDays = Math.max(
    0,
    ...initialRuns.map((run) =>
      run.aligned_equity_curve.length ? run.aligned_equity_curve[run.aligned_equity_curve.length - 1].days_since_entry : 0,
    ),
  );
  const minEquity = alignedPoints.length ? Math.min(...alignedPoints) : 0;
  const maxEquity = alignedPoints.length ? Math.max(...alignedPoints) : 0;
  const chartWidth = 720;
  const chartHeight = 260;
  const distinctRpsDefinitionVersions = new Set(
    initialRuns.map((run) => run.compare_dimensions.rps_definition_version ?? "source_screen_run_unavailable"),
  );
  const hasRpsDefinitionMismatch = distinctRpsDefinitionVersions.size > 1;

  return (
    <section className="screen-panel">
      <div className="screen-panel__header">
        <p className="eyebrow">跨 run 对比</p>
        <h1>把策略调整放到同一坐标系里比较。</h1>
        <p className="hero-text">
          这里显式展示第一类对比维度，并把 equity curve 统一对齐到“自 T+1 入场起的交易日索引”，
          避免不同 source trade date 的 calendar-date 偶合干扰结论。
        </p>
      </div>

      <div className="screen-summary-grid">
        <article className="screen-summary-card">
          <p className="status-label">对比 runs</p>
          <h2>{initialRuns.length}</h2>
          <p className="status-copy">只允许 completed 的 portfolio-return lifecycle 进入这里。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">X 轴语义</p>
          <h2>交易日索引</h2>
          <p className="status-copy">0 = T+1 实际入场日；不按 calendar date 对齐。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">RPS 语义版本</p>
          <h2>{hasRpsDefinitionMismatch ? "存在差异" : "一致"}</h2>
          <p className="status-copy">compare 维度显式读取 source screen run 的 rps_definition_version。</p>
        </article>
      </div>

      <section className="result-panel">
        <div className="result-panel__header">
          <p className="eyebrow">对齐曲线</p>
          <h2>交易日（自 T+1 入场起）对齐后的组合权益。</h2>
        </div>
        <div className="chart-frame">
          <svg
            aria-label="对齐组合权益曲线图"
            className="compare-chart"
            viewBox={`0 0 ${chartWidth} ${chartHeight}`}
            role="img"
          >
            {initialRuns.map((run, index) => (
              <path
                key={`line-${run.run.id}`}
                d={buildChartPath(run.aligned_equity_curve, {
                  width: chartWidth,
                  height: chartHeight,
                  maxDays,
                  minEquity,
                  maxEquity,
                })}
                fill="none"
                stroke={LINE_COLORS[index % LINE_COLORS.length]}
                strokeWidth="3"
                strokeLinecap="round"
              />
            ))}
          </svg>
          <p className="status-copy chart-empty-copy">交易日（自 T+1 入场起）</p>
        </div>
      </section>

      <section className="result-panel">
        <div className="result-panel__header">
          <p className="eyebrow">第一类维度</p>
          <h2>holding / stop-loss / cap / source screen run 全部显式展开。</h2>
        </div>
        <div className="result-list">
          {initialRuns.map((run, index) => (
            <article key={run.run.id} className="result-card">
              <div className="result-card__title">
                <div>
                  <p className="status-label">回测 #{run.run.id}</p>
                  <h3>{formatRatioAsPercent(run.cumulative_return)}</h3>
                  <p className="status-copy">
                    胜率 {formatRatioAsPercent(run.win_rate)} / 最大回撤 {formatRatioAsPercent(run.max_drawdown)}
                  </p>
                </div>
                <div className="result-card__actions">
                  <p
                    className="distribution-exit-flag"
                    style={{ background: `${LINE_COLORS[index % LINE_COLORS.length]}20`, color: LINE_COLORS[index % LINE_COLORS.length] }}
                  >
                    曲线 {index + 1}
                  </p>
                </div>
              </div>
              <dl className="result-summary-grid">
                <div>
                  <dt>holding_days</dt>
                  <dd>{run.compare_dimensions.holding_days ?? "-"}</dd>
                </div>
                <div>
                  <dt>stop_loss_pct</dt>
                  <dd>{run.compare_dimensions.stop_loss_pct ?? "-"}</dd>
                </div>
                <div>
                  <dt>portfolio_cap</dt>
                  <dd>{run.compare_dimensions.portfolio_cap ?? "-"}</dd>
                </div>
                <div>
                  <dt>source screen run</dt>
                  <dd>
                    {run.source_screen_run ? (
                      <Link className="result-link" href={`/screen?run_id=${run.source_screen_run.id}`}>
                        #{run.source_screen_run.id} / {formatDateOnly(run.source_screen_run.trade_date)} / v
                        {run.source_screen_run.strategy_configuration_version ?? "?"}
                      </Link>
                    ) : (
                      "原筛选记录不可用"
                    )}
                  </dd>
                </div>
                <div>
                  <dt>rps_definition_version</dt>
                  <dd>
                    <span
                      className={`compare-semantic-flag ${
                        hasRpsDefinitionMismatch ? "compare-semantic-flag--warning" : ""
                      }`}
                      title={hasRpsDefinitionMismatch ? "RPS 定义版本不同" : "RPS 定义版本一致"}
                    >
                      {run.compare_dimensions.rps_definition_version ?? "source screen run 不可用"}
                    </span>
                  </dd>
                </div>
              </dl>
            </article>
          ))}
        </div>
      </section>
    </section>
  );
}
