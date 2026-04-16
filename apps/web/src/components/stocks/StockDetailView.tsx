"use client";

import { StockDetailCharts } from "@/components/stocks/StockDetailCharts";
import { WatchlistToggleButton } from "@/components/watchlist/WatchlistToggleButton";
import { formatNumber, formatPercent, formatTimestamp, formatDateOnly } from "@/lib/formatters";
import type { StockDetailPayload } from "@/lib/types";

type StockDetailViewProps = {
  apiBaseUrl: string;
  detail: StockDetailPayload;
};

export function StockDetailView({ apiBaseUrl, detail }: StockDetailViewProps) {
  const bestRpsValue = detail.rule_breakdown.rps_condition.best_rps_value;
  const maxDrawdown = detail.rule_breakdown.high_proximity_condition.max_drawdown_from_high_pct;
  const highProximityRatio = detail.rule_breakdown.high_proximity_condition.high_proximity_ratio;
  const officialRpsStatus = detail.rule_breakdown.rps_condition.passed ? "已通过" : "未通过";
  const visibleHistoryStartDate = detail.candlesticks[0]?.trade_date ?? detail.screen_run.trade_date;
  const visibleHistoryEndDate =
    detail.candlesticks.at(-1)?.trade_date ?? detail.screen_run.trade_date;
  const qualificationSummary = detail.rule_breakdown.passed
    ? `入选：最佳 RPS ${formatNumber(bestRpsValue)} 已突破 ${
        detail.rule_breakdown.rps_condition.threshold
      } 阈值，且价格距 52 周高点回撤在 ${formatPercent(maxDrawdown)} 以内。`
    : `未入选：${
        detail.rule_breakdown.rps_condition.passed ? "RPS 通过" : "RPS 未通过"
      }，${
        detail.rule_breakdown.high_proximity_condition.passed
          ? "距 52 周高点通过。"
          : "距 52 周高点未通过。"
      }`;

  return (
    <section className="stock-detail-shell">
      <div className="stock-detail-hero">
        <div>
          <p className="eyebrow">个股详情</p>
          <h1>
            {detail.instrument.symbol} <span>{detail.instrument.exchange}</span>
          </h1>
          <p className="hero-text">
            {detail.instrument.name ?? "未命名标的"} 使用了与入选判断完全相同的
            已存行情、派生指标与筛选结果。
          </p>
        </div>
        <div className="stock-detail-badges">
          <div className="screen-summary-card">
            <p className="status-label">筛选任务</p>
            <h2>#{detail.screen_run.id}</h2>
            <p className="status-copy">{formatTimestamp(detail.screen_run.executed_at)}</p>
          </div>
          <div className="screen-summary-card">
            <p className="status-label">参数集</p>
            <h2>v{detail.screen_run.strategy_configuration_version ?? "?"}</h2>
            <p className="status-copy">交易日 {formatDateOnly(detail.screen_run.trade_date)}</p>
          </div>
          <div className="screen-summary-card">
            <p className="status-label">观察列表</p>
            <h2>研究流</h2>
            <WatchlistToggleButton
              apiBaseUrl={apiBaseUrl}
              instrumentId={detail.instrument.id}
              symbol={detail.instrument.symbol}
            />
          </div>
        </div>
      </div>

      <div className="detail-snapshot-grid">
        <article className="run-metadata-card">
          <p className="status-label">最佳 RPS</p>
          <h3>{detail.rule_breakdown.rps_condition.best_rps_value ?? "不可用"}</h3>
          <p className="status-copy">
            阈值 {detail.rule_breakdown.rps_condition.threshold}，
            {detail.rule_breakdown.rps_condition.passed ? "已通过" : "未通过"}。
          </p>
        </article>
        <article className="run-metadata-card">
          <p className="status-label">距 52 周高点回撤</p>
          <h3>{formatPercent(detail.rule_breakdown.high_proximity_condition.max_drawdown_from_high_pct)}</h3>
          <p className="status-copy">
            允许回撤 {detail.rule_breakdown.high_proximity_condition.threshold_pct}%。
          </p>
        </article>
        <article className="run-metadata-card">
          <p className="status-label">入选结论</p>
          <h3>{detail.rule_breakdown.passed ? "已入选" : "未入选"}</h3>
          <p className="status-copy">
            RPS {detail.rule_breakdown.rps_condition.passed ? "通过" : "未通过"} / 距高点{" "}
            {detail.rule_breakdown.high_proximity_condition.passed ? "通过" : "未通过"}。
          </p>
        </article>
      </div>

      <section className="chart-panel">
        <div className="chart-panel__header">
          <p className="eyebrow">图表观察</p>
          <h2>真实 RPS 历史只承担验证与解释，不替代正式筛选判定。</h2>
          <p className="hero-text">
            图上的橙色阈值线与 RPS 历史序列用于帮助复核走势；当前 MVP 的正式 RPS
            规则仍只以 `rule_breakdown` 中的最佳 RPS、阈值和通过判定为准。
          </p>
        </div>
        <div className="detail-snapshot-grid chart-context-grid">
          <article className="run-metadata-card">
            <p className="status-label">筛选交易日</p>
            <h3>{formatDateOnly(detail.screen_run.trade_date)}</h3>
            <p className="status-copy">与本次筛选绑定的权威交易日。</p>
          </article>
          <article className="run-metadata-card">
            <p className="status-label">默认历史范围</p>
            <h3>
              {formatDateOnly(visibleHistoryStartDate)} 至 {formatDateOnly(visibleHistoryEndDate)}
            </h3>
            <p className="status-copy">首屏默认提供更长的历史窗口，便于多月走势复盘。</p>
          </article>
        </div>
        <div className="semantic-boundary-grid">
          <article className="semantic-boundary-card semantic-boundary-card--official">
            <p className="status-label">正式筛选信号</p>
            <h3>最佳 RPS {formatNumber(bestRpsValue)}</h3>
            <p className="status-copy">
              当前权威判定是“最佳 RPS {officialRpsStatus} 阈值 {detail.rule_breakdown.rps_condition.threshold}”。
              这部分直接来自已保存的筛选判定结果，会驱动入选或未入选结论。
            </p>
          </article>
          <article className="semantic-boundary-card semantic-boundary-card--explanatory">
            <p className="status-label">仅解释用途</p>
            <h3>RPS 历史与图表提示</h3>
            <p className="status-copy">
              图中的 RPS 历史只用于回看后端已存事实，帮助理解走势与阈值位置；
              它不会单独新增“官方”事件标签，也不会覆盖正式筛选结果。
            </p>
          </article>
        </div>
        <StockDetailCharts
          candlesticks={detail.candlesticks}
          indicatorHistory={detail.indicator_history}
          rpsThreshold={detail.rule_breakdown.rps_condition.threshold}
        />
      </section>

      <section className="chart-panel">
        <div className="chart-panel__header">
          <p className="eyebrow">规则拆解</p>
          <h2>来自原始筛选任务的精确入选值。</h2>
          <p className="hero-text">
            {qualificationSummary} 本区块镜像已存的规则判断，使入选原因在个股
            分析流程中始终可见。若图表上出现辅助说明，它们只能作为解释性观察，
            不能反向改写这里的正式结论。
          </p>
        </div>

        <div className="explainability-grid">
          <article className="explainability-card">
            <div className="explainability-card__header">
              <div>
                <p className="status-label">条件 1</p>
                <h3>RPS 强度</h3>
              </div>
              <p
                className={`explainability-flag ${
                  detail.rule_breakdown.rps_condition.passed
                    ? "explainability-flag--pass"
                    : "explainability-flag--fail"
                }`}
              >
                {detail.rule_breakdown.rps_condition.passed ? "通过" : "未通过"}
              </p>
            </div>

            <dl className="detail-list explainability-list">
              <div>
                <dt>阈值</dt>
                <dd>{detail.rule_breakdown.rps_condition.threshold}</dd>
              </div>
              <div>
                <dt>RPS 50</dt>
                <dd>{formatNumber(detail.rule_breakdown.rps_condition.rps_50)}</dd>
              </div>
              <div>
                <dt>RPS 120</dt>
                <dd>{formatNumber(detail.rule_breakdown.rps_condition.rps_120)}</dd>
              </div>
              <div>
                <dt>RPS 250</dt>
                <dd>{formatNumber(detail.rule_breakdown.rps_condition.rps_250)}</dd>
              </div>
              <div>
                <dt>用于判定的最佳 RPS</dt>
                <dd>{formatNumber(bestRpsValue)}</dd>
              </div>
              <div>
                <dt>判定</dt>
                <dd>{detail.rule_breakdown.rps_condition.passed ? "最佳 RPS 达到阈值" : "最佳 RPS 低于阈值"}</dd>
              </div>
            </dl>
          </article>

          <article className="explainability-card">
            <div className="explainability-card__header">
              <div>
                <p className="status-label">条件 2</p>
                <h3>距 52 周高点</h3>
              </div>
              <p
                className={`explainability-flag ${
                  detail.rule_breakdown.high_proximity_condition.passed
                    ? "explainability-flag--pass"
                    : "explainability-flag--fail"
                }`}
              >
                {detail.rule_breakdown.high_proximity_condition.passed ? "通过" : "未通过"}
              </p>
            </div>

            <dl className="detail-list explainability-list">
              <div>
                <dt>允许回撤</dt>
                <dd>{formatPercent(detail.rule_breakdown.high_proximity_condition.threshold_pct)}</dd>
              </div>
              <div>
                <dt>实际回撤</dt>
                <dd>{formatPercent(maxDrawdown)}</dd>
              </div>
              <div>
                <dt>距高点比率</dt>
                <dd>{formatNumber(highProximityRatio, 4)}</dd>
              </div>
              <div>
                <dt>52 周高点</dt>
                <dd>{formatNumber(detail.latest_indicator_snapshot.fifty_two_week_high)}</dd>
              </div>
              <div>
                <dt>最新复权收盘</dt>
                <dd>{formatNumber(detail.candlesticks.at(-1)?.adj_close ?? null)}</dd>
              </div>
              <div>
                <dt>判定</dt>
                <dd>
                  {detail.rule_breakdown.high_proximity_condition.passed
                    ? "价格距 52 周高点在允许范围内"
                    : "价格距 52 周高点已超出允许范围"}
                </dd>
              </div>
            </dl>
          </article>
        </div>
      </section>
    </section>
  );
}
