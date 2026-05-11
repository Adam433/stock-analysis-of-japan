import Link from "next/link";

import {
  describeApiBaseUrlResolution,
  resolveApiBaseUrl,
} from "@/lib/apiBaseUrl";
import { formatTimestamp } from "@/lib/formatters";
import { loadMarketDataHealth, type MarketDataHealthResponse } from "@/lib/marketDataHealth";

type MarketDataHealthResult =
  | { kind: "ok"; health: MarketDataHealthResponse }
  | { kind: "error"; message: string };

const workflowSteps = [
  "接入并标准化日股日频行情数据",
  "跟踪刷新结果、部分行与失败任务",
  "在筛选与回测前先依据已存信任信号判断",
];

export const dynamic = "force-dynamic";

async function getMarketDataHealth(apiBaseUrl: string): Promise<MarketDataHealthResult> {
  const { health, error } = await loadMarketDataHealth(apiBaseUrl);

  if (health) {
    return {
      kind: "ok",
      health,
    };
  }

  return {
    kind: "error",
    message: error ?? "健康检查接口调用失败。",
  };
}

function toneClassForStatus(status: string): string {
  if (status === "never-run") {
    return "status-card--neutral";
  }
  if (status === "fresh" || status === "succeeded" || status === "complete") {
    return "status-card--good";
  }
  if (status === "partial") {
    return "status-card--warn";
  }
  return "status-card--bad";
}

export default async function HomePage() {
  const apiBaseUrl = await resolveApiBaseUrl();
  const apiBaseResolution = describeApiBaseUrlResolution(apiBaseUrl);
  const healthResult = await getMarketDataHealth(apiBaseUrl);
  const health = healthResult.kind === "ok" ? healthResult.health : null;
  const refresh = health?.last_refresh;
  const universeManifest = health?.universe_manifest;
  const apiErrorMessage =
    healthResult.kind === "error"
      ? `${healthResult.message} ${apiBaseResolution}`
      : "健康检查接口调用成功。";
  const freshnessStatus = healthResult.kind === "ok" ? healthResult.health.freshness_state : "api-unreachable";
  const coverageStatus = healthResult.kind === "ok" ? healthResult.health.coverage_status : "connection-issue";
  const refreshStatus = healthResult.kind === "ok" ? refresh?.status ?? "never-run" : "connection-issue";
  const freshnessTone =
    healthResult.kind === "ok" ? toneClassForStatus(freshnessStatus) : "status-card--neutral";
  const coverageTone =
    healthResult.kind === "ok" ? toneClassForStatus(coverageStatus) : "status-card--neutral";
  const refreshTone =
    healthResult.kind === "ok" ? toneClassForStatus(refreshStatus) : "status-card--neutral";

  return (
    <main id="main-content" className="dashboard-shell">
      <nav className="top-nav">
        <span>数据健康</span>
        <span>/</span>
        <Link href="/screen">策略配置</Link>
        <span>/</span>
        <Link href="/watchlist">观察列表</Link>
        <span>/</span>
        <Link href="/backtests">回测</Link>
        <span>/</span>
        <Link href="/experiments">实验</Link>
      </nav>
      <section className="hero-panel">
        <div className="hero-copy">
          <p className="eyebrow">stockAnalyse</p>
          <h1>日股数据的运营可信视图。</h1>
          <p className="hero-text">
            Story 1.4 把外壳转化为实时健康面：数据新鲜度、部分覆盖与失败的刷新任务
            在任何筛选或回测结果被信任之前就已可见。
          </p>
          <p className="status-copy">API 基址：{apiBaseUrl}</p>
        </div>

        <div className="hero-orbit">
          {workflowSteps.map((step, index) => (
            <article key={step} className="orbit-card">
              <p className="orbit-step">环节 {index + 1}</p>
              <p>{step}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="status-grid">
        <article className={`status-card ${freshnessTone}`}>
          <p className="status-label">新鲜度</p>
          <h2>{freshnessStatus}</h2>
          <p className="status-copy">
            {health
              ? `最新交易日：${health.latest_trade_date ?? "暂无已存数据"}`
              : apiErrorMessage}
          </p>
          <p className="status-meta">
            数据龄期：{health?.age_in_days ?? "-"} 天
          </p>
        </article>

        <article className={`status-card ${coverageTone}`}>
          <p className="status-label">覆盖度</p>
          <h2>{coverageStatus}</h2>
          <p className="status-copy">
            {health
              ? `已存日频行情涵盖 ${health.total_instruments} 只标的`
              : "健康接口不可达，暂无法评估覆盖度。"}
          </p>
          <p className="status-meta">
            {universeManifest
              ? `普通股清单：${universeManifest.symbol_count} 只 ｜ 更新于 ${formatTimestamp(universeManifest.updated_at)}`
              : "普通股清单：缺失 ｜ 更新时间：缺失"}
          </p>
        </article>

        <article className={`status-card ${refreshTone}`}>
          <p className="status-label">最近一次刷新</p>
          <h2>{refreshStatus}</h2>
          <p className="status-copy">
            {health
              ? refresh
                ? `数据源：${refresh.provider} / ${
                    refresh.universe_scope === "full_universe"
                      ? `全量 universe（东京证券交易所普通股，${refresh.requested_symbol_count} 只）`
                      : `指定标的（${refresh.requested_symbol_count} 只）`
                  }`
                : "尚无刷新记录，当前仅能依据已存数据判断新鲜度与覆盖度。"
              : "接口不可达，刷新状态未知"}
          </p>
          <p className="status-meta">
            {health && !refresh
              ? "完成时间：尚未记录"
              : `完成时间：${formatTimestamp(refresh?.completed_at ?? null)}`}
          </p>
        </article>
      </section>

      <section className="detail-grid">
        <article className="detail-panel">
          <p className="detail-label">任务详情</p>
          <h3>刷新执行状态</h3>
          <dl className="detail-list">
            <div>
              <dt>开始时间</dt>
              <dd>{formatTimestamp(refresh?.started_at ?? null)}</dd>
            </div>
            <div>
              <dt>完成时间</dt>
              <dd>{formatTimestamp(refresh?.completed_at ?? null)}</dd>
            </div>
            <div>
              <dt>处理行数</dt>
              <dd>{refresh?.rows_processed ?? "-"}</dd>
            </div>
            <div>
              <dt>新增 / 更新行数</dt>
              <dd>
                {refresh ? `${refresh.rows_inserted} / ${refresh.rows_updated}` : "-"}
              </dd>
            </div>
          </dl>
        </article>

        <article className="detail-panel">
          <p className="detail-label">信任信号</p>
          <h3>会影响信心的情形</h3>
          <ul className="signal-list">
            <li>新鲜度超过 3 天将被标记为陈旧。</li>
            <li>任何部分或不可用行都会使覆盖度标记为部分。</li>
            <li>一次失败的刷新会持续显示，直到下一次刷新成功。</li>
          </ul>
        </article>

        <article className="detail-panel">
          <p className="detail-label">范围</p>
          <h3>请求的标的</h3>
          <p className="symbol-cloud">
            {refresh?.requested_symbols?.length
              ? refresh.requested_symbols.join(" · ")
              : "尚未记录标的列表"}
          </p>
          <p className="error-callout">
            {healthResult.kind === "ok"
              ? refresh?.error_message ?? (refresh ? "未记录刷新错误。" : "尚无刷新任务错误记录。")
              : apiErrorMessage}
          </p>
          <p className="status-copy">
            {universeManifest
              ? `普通股清单更新时间：${formatTimestamp(universeManifest.updated_at)}`
              : "尚未生成东京证券交易所普通股清单。"}
          </p>
        </article>
      </section>
    </main>
  );
}
