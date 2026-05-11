import Link from "next/link";

import { resolveApiBaseUrl } from "@/lib/apiBaseUrl";
import { fetchWithRetry } from "@/lib/fetchWithRetry";
import { formatRatioAsPercent, formatTimestamp } from "@/lib/formatters";

export const dynamic = "force-dynamic";

type OptimizationResultDetail = {
  optimization_run: {
    id: number;
    market: string;
    objective: string;
    train_start_date: string;
    train_end_date: string;
    validation_start_date: string | null;
    validation_end_date: string | null;
  };
  optimization_result: {
    id: number;
    rank: number | null;
    score: string | null;
    status: string;
    failure_reason: string | null;
    completed_at: string | null;
  };
  parameters: {
    rps_threshold?: number;
    selected_rps_windows?: number[];
    min_rps_windows_passing?: number;
    use_cup_handle?: boolean;
    holding_days?: number | null;
    stop_loss_pct?: string;
    rps_exit_threshold?: number | null;
    portfolio_cap?: number;
    fundamental_growth_params?: {
      max_pe?: string | null;
      max_pb?: string | null;
      min_growth_count?: number | null;
      effective_min_growth_count?: number | null;
      require_positive_operating_cash_flow?: boolean;
      require_positive_free_cash_flow?: boolean;
    };
  };
  train: {
    summary: Record<string, unknown>;
    trades: Array<Record<string, unknown>>;
  };
  validation: {
    summary: Record<string, unknown>;
    trades: Array<Record<string, unknown>>;
  } | null;
};

async function loadOptimizationDetail(
  apiBaseUrl: string,
  resultId: string,
): Promise<{ data: OptimizationResultDetail | null; error: string | null }> {
  if (!/^\d+$/.test(resultId)) {
    return { data: null, error: "resultId 必须是整数。" };
  }
  try {
    const response = await fetchWithRetry(
      `${apiBaseUrl}/backtests/optimization/results/${resultId}/detail?max_trades_returned=120`,
      { cache: "no-store" },
    );
    if (!response.ok) {
      const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
      return {
        data: null,
        error: payload?.detail ?? `无法加载优化结果详情（${response.status}）。`,
      };
    }
    const payload = (await response.json()) as { detail: OptimizationResultDetail };
    return { data: payload.detail, error: null };
  } catch {
    return {
      data: null,
      error: "优化结果详情接口不可达，请检查后端服务与 API 地址。",
    };
  }
}

function valueText(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "不可用";
  }
  return String(value);
}

function metric(summary: Record<string, unknown>, key: string): string {
  return formatRatioAsPercent(valueText(summary[key]) === "不可用" ? null : String(summary[key]));
}

function windowsLabel(windows: number[] | undefined): string {
  return windows?.length ? windows.join(" / ") : "未记录";
}

function renderTrades(trades: Array<Record<string, unknown>>) {
  return (
    <div className="run-metadata-grid backtest-summary-grid">
      {trades.slice(0, 12).map((trade, index) => (
        <article key={`${trade.symbol}-${trade.entry_date}-${index}`} className="run-metadata-card">
          <p className="status-label">{valueText(trade.period)} / {valueText(trade.exit_reason_label ?? trade.exit_reason)}</p>
          <h3>{valueText(trade.symbol)}</h3>
          <dl className="detail-list">
            <div>
              <dt>入场</dt>
              <dd>{valueText(trade.entry_date)} / {valueText(trade.entry_price)}</dd>
            </div>
            <div>
              <dt>退出</dt>
              <dd>{valueText(trade.exit_date)} / {valueText(trade.exit_price)}</dd>
            </div>
            <div>
              <dt>收益</dt>
              <dd>{formatRatioAsPercent(valueText(trade.realized_return) === "不可用" ? null : String(trade.realized_return))}</dd>
            </div>
          </dl>
        </article>
      ))}
      {trades.length === 0 ? <p className="status-copy">暂无交易明细。</p> : null}
    </div>
  );
}

export default async function OptimizationResultDetailPage({
  params,
}: {
  params: Promise<{ resultId: string }>;
}) {
  const apiBaseUrl = await resolveApiBaseUrl();
  const { resultId } = await params;
  const { data, error } = await loadOptimizationDetail(apiBaseUrl, resultId);

  return (
    <main id="main-content" className="dashboard-shell">
      <nav className="top-nav">
        <Link href="/">数据健康</Link>
        <span>/</span>
        <Link href="/experiments">实验</Link>
        <span>/</span>
        <span>优化结果详情</span>
      </nav>

      {data ? (
        <>
          <section className="screen-panel">
            <div className="screen-panel__header">
              <div>
                <p className="detail-label">Optimization Result</p>
                <h1>结果 #{data.optimization_result.id}</h1>
              </div>
              <p className="status-copy">
                run #{data.optimization_run.id} / rank {data.optimization_result.rank ?? "—"} / {data.optimization_result.status}
              </p>
            </div>
            <div className="status-grid">
              <article className="status-card status-card--neutral">
                <p className="status-label">Score</p>
                <h2>{data.optimization_result.score ?? "—"}</h2>
                <p className="status-meta">完成：{formatTimestamp(data.optimization_result.completed_at)}</p>
              </article>
              <article className="status-card status-card--neutral">
                <p className="status-label">RPS</p>
                <h2>{data.parameters.rps_threshold ?? "—"}</h2>
                <p className="status-meta">{windowsLabel(data.parameters.selected_rps_windows)}</p>
              </article>
              <article className="status-card status-card--neutral">
                <p className="status-label">Risk</p>
                <h2>{data.parameters.stop_loss_pct ?? "—"}</h2>
                <p className="status-meta">持仓 {data.parameters.portfolio_cap ?? "—"} / 持有 {data.parameters.holding_days ?? "不限"}</p>
              </article>
            </div>
          </section>

          <section className="detail-grid">
            <article className="detail-panel">
              <p className="detail-label">Train</p>
              <h3>训练期摘要</h3>
              <dl className="detail-list">
                <div>
                  <dt>总收益 / 回撤</dt>
                  <dd>{metric(data.train.summary, "total_return")} / {metric(data.train.summary, "max_drawdown")}</dd>
                </div>
                <div>
                  <dt>SPY 单笔 Alpha</dt>
                  <dd>{metric(data.train.summary, "spy_average_trade_excess_return")}</dd>
                </div>
                <div>
                  <dt>交易数 / 胜率</dt>
                  <dd>{valueText(data.train.summary.completed_trades)} / {metric(data.train.summary, "win_rate")}</dd>
                </div>
              </dl>
            </article>

            {data.validation ? (
              <article className="detail-panel">
                <p className="detail-label">Validation</p>
                <h3>验证期摘要</h3>
                <dl className="detail-list">
                  <div>
                    <dt>总收益 / 回撤</dt>
                    <dd>{metric(data.validation.summary, "total_return")} / {metric(data.validation.summary, "max_drawdown")}</dd>
                  </div>
                  <div>
                    <dt>SPY 单笔 Alpha</dt>
                    <dd>{metric(data.validation.summary, "spy_average_trade_excess_return")}</dd>
                  </div>
                  <div>
                    <dt>交易数 / 胜率</dt>
                    <dd>{valueText(data.validation.summary.completed_trades)} / {metric(data.validation.summary, "win_rate")}</dd>
                  </div>
                </dl>
              </article>
            ) : null}
          </section>

          <section className="detail-panel">
            <p className="detail-label">Trades</p>
            <h3>交易明细</h3>
            {renderTrades([
              ...data.train.trades,
              ...(data.validation?.trades ?? []),
            ])}
          </section>
        </>
      ) : (
        <section className="screen-panel">
          <p className="detail-label">Optimization Result</p>
          <h1>详情暂不可用。</h1>
          <p className="hero-text">{error}</p>
        </section>
      )}
    </main>
  );
}
