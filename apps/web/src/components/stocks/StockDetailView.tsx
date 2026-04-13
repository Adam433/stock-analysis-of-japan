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
    return "Unavailable";
  }

  return Number(value).toFixed(digits);
}

function formatPercent(value: string | null, digits = 2): string {
  if (!value) {
    return "Unavailable";
  }

  return `${Number(value).toFixed(digits)}%`;
}

function formatTimestamp(value: string): string {
  return new Intl.DateTimeFormat("en-US", {
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
    ? `Qualified because best RPS ${formatNumber(bestRpsValue)} cleared the ${
        detail.rule_breakdown.rps_condition.threshold
      } threshold and the stock stayed within ${formatPercent(maxDrawdown)} of its 52-week high.`
    : `Not qualified because ${
        detail.rule_breakdown.rps_condition.passed ? "RPS passed" : "RPS missed"
      } and ${
        detail.rule_breakdown.high_proximity_condition.passed
          ? "52-week-high proximity passed."
          : "52-week-high proximity missed."
      }`;

  return (
    <section className="stock-detail-shell">
      <div className="stock-detail-hero">
        <div>
          <p className="eyebrow">Stock Detail</p>
          <h1>
            {detail.instrument.symbol} <span>{detail.instrument.exchange}</span>
          </h1>
          <p className="hero-text">
            {detail.instrument.name ?? "Unnamed instrument"} is shown using the
            same stored market data, derived facts, and screen-run result that
            produced the qualification.
          </p>
        </div>
        <div className="stock-detail-badges">
          <div className="screen-summary-card">
            <p className="status-label">Run</p>
            <h2>#{detail.screen_run.id}</h2>
            <p className="status-copy">{formatTimestamp(detail.screen_run.executed_at)}</p>
          </div>
          <div className="screen-summary-card">
            <p className="status-label">Parameter Set</p>
            <h2>v{detail.screen_run.strategy_configuration_version ?? "?"}</h2>
            <p className="status-copy">Trade date {detail.screen_run.trade_date}</p>
          </div>
          <div className="screen-summary-card">
            <p className="status-label">Watchlist</p>
            <h2>Research Flow</h2>
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
          <p className="status-label">Best RPS</p>
          <h3>{detail.rule_breakdown.rps_condition.best_rps_value ?? "Unavailable"}</h3>
          <p className="status-copy">
            Threshold {detail.rule_breakdown.rps_condition.threshold} and{" "}
            {detail.rule_breakdown.rps_condition.passed ? "passed" : "did not pass"}.
          </p>
        </article>
        <article className="run-metadata-card">
          <p className="status-label">52-Week High Drawdown</p>
          <h3>{detail.rule_breakdown.high_proximity_condition.max_drawdown_from_high_pct ?? "Unavailable"}%</h3>
          <p className="status-copy">
            Allowed drawdown {detail.rule_breakdown.high_proximity_condition.threshold_pct}%.
          </p>
        </article>
        <article className="run-metadata-card">
          <p className="status-label">Qualification</p>
          <h3>{detail.rule_breakdown.passed ? "Qualified" : "Not qualified"}</h3>
          <p className="status-copy">
            RPS {detail.rule_breakdown.rps_condition.passed ? "passed" : "failed"} / proximity{" "}
            {detail.rule_breakdown.high_proximity_condition.passed ? "passed" : "failed"}.
          </p>
        </article>
      </div>

      <section className="chart-panel">
        <div className="chart-panel__header">
          <p className="eyebrow">Candlestick</p>
          <h2>Price action from stored daily bars.</h2>
        </div>
        <div className="chart-frame">
          <svg viewBox="0 0 760 260" className="candlestick-chart" role="img" aria-label="Candlestick chart">
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
          <p className="eyebrow">RPS Panel</p>
          <h2>50 / 120 / 250-day RPS context with threshold states.</h2>
        </div>
        <div className="chart-frame">
          <svg viewBox="0 0 760 180" className="rps-chart" role="img" aria-label="RPS panel">
            <line x1="0" x2="760" y1="18" y2="18" stroke="#c96b2c" strokeDasharray="6 6" strokeWidth="1.5" />
            <text x="12" y="14" className="chart-label">
              Threshold {detail.rule_breakdown.rps_condition.threshold}
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
              <h3>{line.latestValue !== null ? formatNumber(String(line.latestValue)) : "Unavailable"}</h3>
              <p className="status-copy">
                {line.meetsThreshold ? "Meets threshold" : "Below threshold"}.
                Line style is unique, so the state is not color-only.
              </p>
            </article>
          ))}
        </div>
      </section>

      <section className="chart-panel">
        <div className="chart-panel__header">
          <p className="eyebrow">Rule Breakdown</p>
          <h2>Exact qualifying values from the originating screen run.</h2>
          <p className="hero-text">
            {qualificationSummary} This section mirrors the stored rule outcome so the reason for
            qualification stays visible inside the stock analysis flow.
          </p>
        </div>

        <div className="explainability-grid">
          <article className="explainability-card">
            <div className="explainability-card__header">
              <div>
                <p className="status-label">Condition 1</p>
                <h3>RPS strength</h3>
              </div>
              <p
                className={`explainability-flag ${
                  detail.rule_breakdown.rps_condition.passed
                    ? "explainability-flag--pass"
                    : "explainability-flag--fail"
                }`}
              >
                {detail.rule_breakdown.rps_condition.passed ? "PASS" : "FAIL"}
              </p>
            </div>

            <dl className="detail-list explainability-list">
              <div>
                <dt>Threshold</dt>
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
                <dt>Best RPS used for qualification</dt>
                <dd>{formatNumber(bestRpsValue)}</dd>
              </div>
              <div>
                <dt>Decision</dt>
                <dd>{detail.rule_breakdown.rps_condition.passed ? "Best RPS met threshold" : "Best RPS below threshold"}</dd>
              </div>
            </dl>
          </article>

          <article className="explainability-card">
            <div className="explainability-card__header">
              <div>
                <p className="status-label">Condition 2</p>
                <h3>52-week-high proximity</h3>
              </div>
              <p
                className={`explainability-flag ${
                  detail.rule_breakdown.high_proximity_condition.passed
                    ? "explainability-flag--pass"
                    : "explainability-flag--fail"
                }`}
              >
                {detail.rule_breakdown.high_proximity_condition.passed ? "PASS" : "FAIL"}
              </p>
            </div>

            <dl className="detail-list explainability-list">
              <div>
                <dt>Allowed drawdown</dt>
                <dd>{formatPercent(detail.rule_breakdown.high_proximity_condition.threshold_pct)}</dd>
              </div>
              <div>
                <dt>Observed drawdown</dt>
                <dd>{formatPercent(maxDrawdown)}</dd>
              </div>
              <div>
                <dt>High proximity ratio</dt>
                <dd>{formatNumber(highProximityRatio, 4)}</dd>
              </div>
              <div>
                <dt>52-week high</dt>
                <dd>{formatNumber(detail.latest_indicator_snapshot.fifty_two_week_high)}</dd>
              </div>
              <div>
                <dt>Latest adjusted close</dt>
                <dd>{formatNumber(detail.candlesticks.at(-1)?.adj_close ?? null)}</dd>
              </div>
              <div>
                <dt>Decision</dt>
                <dd>
                  {detail.rule_breakdown.high_proximity_condition.passed
                    ? "Price stayed close enough to the 52-week high"
                    : "Price drifted too far below the 52-week high"}
                </dd>
              </div>
            </dl>
          </article>
        </div>
      </section>
    </section>
  );
}
