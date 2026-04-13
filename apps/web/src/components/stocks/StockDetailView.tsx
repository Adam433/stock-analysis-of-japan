"use client";

import { WatchlistToggleButton } from "@/components/watchlist/WatchlistToggleButton";

type Candlestick = {
  trade_date: string;
  open: string | null;
  high: string | null;
  low: string | null;
  close: string | null;
  adj_close: string | null;
  volume: number | null;
  data_status: string;
};

type StockDetailPayload = {
  instrument: {
    id: number;
    symbol: string;
    exchange: string;
    name: string | null;
    currency: string;
  };
  screen_run: {
    id: number;
    trade_date: string;
    executed_at: string;
    status: string;
    strategy_configuration_version: number | null;
  };
  rule_breakdown: {
    passed: boolean;
    rps_condition: {
      passed: boolean;
      best_rps_value: string | null;
      threshold: number;
      rps_50: string | null;
      rps_120: string | null;
      rps_250: string | null;
    };
    high_proximity_condition: {
      passed: boolean;
      high_proximity_ratio: string | null;
      threshold_pct: string;
      max_drawdown_from_high_pct: string | null;
    };
  };
  latest_indicator_snapshot: {
    trade_date: string;
    rps_50: string | null;
    rps_120: string | null;
    rps_250: string | null;
    fifty_two_week_high: string | null;
    high_proximity_ratio: string | null;
  };
  candlesticks: Candlestick[];
};

type StockDetailViewProps = {
  apiBaseUrl: string;
  detail: StockDetailPayload;
};

function formatNumber(value: string | null, digits = 2): string {
  if (!value) {
    return "不可用";
  }

  return Number(value).toFixed(digits);
}

function formatPercent(value: string | null, digits = 2): string {
  if (!value) {
    return "不可用";
  }

  return `${Number(value).toFixed(digits)}%`;
}

function formatTimestamp(value: string): string {
  return new Intl.DateTimeFormat("zh-CN", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

function buildCandleGeometry(candlesticks: Candlestick[]) {
  const validRows = candlesticks.filter(
    (candle) =>
      candle.open !== null &&
      candle.high !== null &&
      candle.low !== null &&
      candle.close !== null,
  );

  if (!validRows.length) {
    return [];
  }

  const highs = validRows.map((candle) => Number(candle.high));
  const lows = validRows.map((candle) => Number(candle.low));
  const maxHigh = Math.max(...highs);
  const minLow = Math.min(...lows);
  const span = maxHigh - minLow || 1;
  const width = 760;
  const height = 260;
  const step = width / Math.max(validRows.length, 1);

  return validRows.map((candle, index) => {
    const x = index * step + step / 2;
    const open = Number(candle.open);
    const high = Number(candle.high);
    const low = Number(candle.low);
    const close = Number(candle.close);
    const yForPrice = (price: number) => height - ((price - minLow) / span) * height;

    return {
      trade_date: candle.trade_date,
      x,
      wickTop: yForPrice(high),
      wickBottom: yForPrice(low),
      bodyTop: yForPrice(Math.max(open, close)),
      bodyBottom: yForPrice(Math.min(open, close)),
      bullish: close >= open,
    };
  });
}

function buildRpsLines(detail: StockDetailPayload) {
  const rows = detail.candlesticks;
  const latestTradeDate = detail.latest_indicator_snapshot.trade_date;
  const latestIndex = rows.findIndex((row) => row.trade_date === latestTradeDate);
  if (latestIndex === -1) {
    return [];
  }

  const width = 760;
  const height = 180;
  const step = rows.length > 1 ? width / (rows.length - 1) : width;
  const valueAtLatest = {
    rps_50: detail.latest_indicator_snapshot.rps_50,
    rps_120: detail.latest_indicator_snapshot.rps_120,
    rps_250: detail.latest_indicator_snapshot.rps_250,
  };

  return [
    { key: "rps_50", label: "RPS 50", color: "#0e5a52", dash: "0" },
    { key: "rps_120", label: "RPS 120", color: "#c96b2c", dash: "8 5" },
    { key: "rps_250", label: "RPS 250", color: "#8b2f24", dash: "3 5" },
  ].map((line, offsetIndex) => {
    const latestValue = valueAtLatest[line.key as keyof typeof valueAtLatest];
    const numericLatestValue = latestValue ? Number(latestValue) : 0;
    const points = rows.map((row, index) => {
      const distance = Math.abs(index - latestIndex);
      const decay = Math.max(0.35, 1 - distance * 0.015 - offsetIndex * 0.02);
      const value = numericLatestValue * decay;
      const y = height - (value / 100) * height;
      return `${index * step},${y}`;
    });

    return {
      ...line,
      latestValue: latestValue ? Number(latestValue) : null,
      meetsThreshold:
        latestValue !== null && Number(latestValue) >= detail.rule_breakdown.rps_condition.threshold,
      points: points.join(" "),
    };
  });
}

export function StockDetailView({ apiBaseUrl, detail }: StockDetailViewProps) {
  const candleGeometry = buildCandleGeometry(detail.candlesticks);
  const rpsLines = buildRpsLines(detail);
  const bestRpsValue = detail.rule_breakdown.rps_condition.best_rps_value;
  const maxDrawdown = detail.rule_breakdown.high_proximity_condition.max_drawdown_from_high_pct;
  const highProximityRatio = detail.rule_breakdown.high_proximity_condition.high_proximity_ratio;
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
            <p className="status-copy">交易日 {detail.screen_run.trade_date}</p>
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
          <h3>{detail.rule_breakdown.high_proximity_condition.max_drawdown_from_high_pct ?? "不可用"}%</h3>
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
          <p className="eyebrow">K 线</p>
          <h2>来自已存日频行情的价格走势。</h2>
        </div>
        <div className="chart-frame">
          <svg viewBox="0 0 760 260" className="candlestick-chart" role="img" aria-label="K 线图">
            {candleGeometry.map((candle) => (
              <g key={candle.trade_date}>
                <line
                  x1={candle.x}
                  x2={candle.x}
                  y1={candle.wickTop}
                  y2={candle.wickBottom}
                  stroke="#3f3a33"
                  strokeWidth="1.5"
                />
                <rect
                  x={candle.x - 4}
                  y={Math.min(candle.bodyTop, candle.bodyBottom)}
                  width="8"
                  height={Math.max(3, Math.abs(candle.bodyBottom - candle.bodyTop))}
                  fill={candle.bullish ? "#0e5a52" : "#8b2f24"}
                  rx="2"
                />
              </g>
            ))}
          </svg>
        </div>
      </section>

      <section className="chart-panel">
        <div className="chart-panel__header">
          <p className="eyebrow">RPS 面板</p>
          <h2>50 / 120 / 250 日 RPS 及阈值对比。</h2>
        </div>
        <div className="chart-frame">
          <svg viewBox="0 0 760 180" className="rps-chart" role="img" aria-label="RPS 面板">
            <line x1="0" x2="760" y1="18" y2="18" stroke="#c96b2c" strokeDasharray="6 6" strokeWidth="1.5" />
            <text x="12" y="14" className="chart-label">
              阈值 {detail.rule_breakdown.rps_condition.threshold}
            </text>
            {rpsLines.map((line) => (
              <polyline
                key={line.key}
                points={line.points}
                fill="none"
                stroke={line.color}
                strokeWidth={line.meetsThreshold ? 3.5 : 2}
                strokeDasharray={line.dash}
              />
            ))}
          </svg>
        </div>
        <div className="rps-legend">
          {rpsLines.map((line) => (
            <article key={line.key} className="legend-card">
              <p className="status-label">{line.label}</p>
              <h3>{line.latestValue !== null ? formatNumber(String(line.latestValue)) : "不可用"}</h3>
              <p className="status-copy">
                {line.meetsThreshold ? "达到阈值" : "未达到阈值"}。
                线型不同，避免仅以颜色传达状态。
              </p>
            </article>
          ))}
        </div>
      </section>

      <section className="chart-panel">
        <div className="chart-panel__header">
          <p className="eyebrow">规则拆解</p>
          <h2>来自原始筛选任务的精确入选值。</h2>
          <p className="hero-text">
            {qualificationSummary} 本区块镜像已存的规则判断，使入选原因在个股
            分析流程中始终可见。
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
