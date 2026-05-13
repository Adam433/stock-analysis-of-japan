import Link from "next/link";

import {
  OptimizationResultsPanel,
  type OptimizationResultSummary,
  type OptimizationRunSummary,
} from "@/components/experiments/OptimizationResultsPanel";
import { resolveApiBaseUrl } from "@/lib/apiBaseUrl";
import { fetchWithRetry } from "@/lib/fetchWithRetry";
import { formatDateOnly, formatRatioAsPercent, formatTimestamp } from "@/lib/formatters";

export const dynamic = "force-dynamic";

type GaRun = {
  id: number;
  market: string;
  objective: string;
  status: string;
  population_size: number;
  max_generations: number;
  completed_generations: number;
  total_generations: number;
  completed_individuals: number;
  total_individuals: number;
  failed_individuals: number;
  best_individual_id: number | null;
  train_start_date: string;
  train_end_date: string;
  holdout_start_date: string | null;
  holdout_end_date: string | null;
  started_at: string | null;
  completed_at: string | null;
};

type GaIndividual = {
  id: number;
  generation: number;
  individual_index: number;
  status: string;
  fitness: string | null;
  parameters: {
    rps_threshold?: number;
    selected_rps_windows?: number[];
    min_rps_windows_passing?: number;
    use_cup_handle?: boolean;
    portfolio_cap?: number;
    stop_loss_pct?: string;
  };
  metrics?: {
    aggregate?: {
      average_window_score?: string;
      minimum_window_score?: string;
      minimum_completed_trades?: number;
      max_drawdown?: string;
    };
  } | null;
};

type LoadResult<T> = {
  data: T;
  error: string | null;
};

async function loadJson<T>(url: string, fallback: T, label: string): Promise<LoadResult<T>> {
  try {
    const response = await fetchWithRetry(url, { cache: "no-store" });
    if (!response.ok) {
      return { data: fallback, error: `${label} 接口返回 ${response.status}。` };
    }
    return { data: (await response.json()) as T, error: null };
  } catch {
    return { data: fallback, error: `${label} 接口不可达。` };
  }
}

function formatProgress(done: number, total: number): string {
  if (!total) {
    return "0 / 0";
  }
  return `${done} / ${total}`;
}

function formatWindows(windows: number[] | undefined): string {
  return windows?.length ? windows.join(" / ") : "未记录";
}

function statusClass(status: string): string {
  if (status === "completed") {
    return "status-card--good";
  }
  if (status === "running" || status === "cancel_requested") {
    return "status-card--warn";
  }
  if (status === "failed" || status === "cancelled") {
    return "status-card--bad";
  }
  return "status-card--neutral";
}

async function loadLatestGaIndividuals(
  apiBaseUrl: string,
  runId: number | null,
): Promise<LoadResult<GaIndividual[]>> {
  if (runId === null) {
    return { data: [], error: null };
  }
  const payload = await loadJson<{ individuals: GaIndividual[] }>(
    `${apiBaseUrl}/backtests/ga/runs/${runId}/individuals?limit=5`,
    { individuals: [] },
    "GA 个体",
  );
  return { data: payload.data.individuals, error: payload.error };
}

async function loadLatestOptimizationResults(
  apiBaseUrl: string,
  runId: number | null,
): Promise<LoadResult<{ results: OptimizationResultSummary[]; total: number }>> {
  if (runId === null) {
    return { data: { results: [], total: 0 }, error: null };
  }
  const payload = await loadJson<{
    results: OptimizationResultSummary[];
    total: number;
  }>(
    `${apiBaseUrl}/backtests/optimization/runs/${runId}/results?limit=30&offset=0&summary_only=true`,
    { results: [], total: 0 },
    "参数优化结果",
  );
  return {
    data: {
      results: payload.data.results,
      total: payload.data.total,
    },
    error: payload.error,
  };
}

export default async function ExperimentsPage() {
  const apiBaseUrl = await resolveApiBaseUrl();
  const [gaRunsPayload, latestGaPayload, latestOptimizationPayload] = await Promise.all([
    loadJson<{ ga_runs: GaRun[] }>(
      `${apiBaseUrl}/backtests/ga/runs?limit=20`,
      { ga_runs: [] },
      "GA 实验列表",
    ),
    loadJson<{ ga_run: GaRun | null }>(
      `${apiBaseUrl}/backtests/ga/runs/latest`,
      { ga_run: null },
      "最近 GA 实验",
    ),
    loadJson<{ optimization_run: OptimizationRunSummary | null }>(
      `${apiBaseUrl}/backtests/optimization/runs/latest`,
      { optimization_run: null },
      "参数优化",
    ),
  ]);
  const latestGaRun = latestGaPayload.data.ga_run;
  const latestIndividualsPayload = await loadLatestGaIndividuals(
    apiBaseUrl,
    latestGaRun?.id ?? null,
  );
  const latestOptimizationRun = latestOptimizationPayload.data.optimization_run;
  const latestOptimizationResultsPayload = await loadLatestOptimizationResults(
    apiBaseUrl,
    latestOptimizationRun?.id ?? null,
  );
  const errors = [
    gaRunsPayload.error,
    latestGaPayload.error,
    latestOptimizationPayload.error,
    latestIndividualsPayload.error,
    latestOptimizationResultsPayload.error,
  ].filter(Boolean);

  return (
    <main id="main-content" className="dashboard-shell">
      <nav className="top-nav">
        <Link href="/">数据健康</Link>
        <span>/</span>
        <Link href="/screen">策略配置</Link>
        <span>/</span>
        <Link href="/backtests">回测</Link>
        <span>/</span>
        <span>实验</span>
        <span>/</span>
        <Link href="/x-signal-tracker">X 信号追踪</Link>
      </nav>

      <section className="screen-panel">
        <div className="screen-panel__header">
          <div>
            <p className="detail-label">Experiments</p>
            <h1>参数优化与 GA 实验</h1>
          </div>
          <p className="status-copy">API 基址：{apiBaseUrl}</p>
        </div>

        {errors.length ? (
          <p className="error-callout">{errors.join(" / ")}</p>
        ) : null}

        <div className="status-grid">
          <article className={`status-card ${statusClass(latestGaRun?.status ?? "none")}`}>
            <p className="status-label">最近 GA</p>
            <h2>{latestGaRun ? `#${latestGaRun.id}` : "暂无"}</h2>
            <p className="status-copy">{latestGaRun?.status ?? "未创建"}</p>
            <p className="status-meta">
              个体：{formatProgress(latestGaRun?.completed_individuals ?? 0, latestGaRun?.total_individuals ?? 0)}
            </p>
          </article>

          <article className={`status-card ${statusClass(latestOptimizationRun?.status ?? "none")}`}>
            <p className="status-label">最近参数优化</p>
            <h2>{latestOptimizationRun ? `#${latestOptimizationRun.id}` : "暂无"}</h2>
            <p className="status-copy">{latestOptimizationRun?.status ?? "未创建"}</p>
            <p className="status-meta">
              参数组：{formatProgress(
                latestOptimizationRun?.completed_parameter_sets ?? 0,
                latestOptimizationRun?.total_parameter_sets ?? 0,
              )}
            </p>
          </article>
        </div>
      </section>

      <section className="detail-grid">
        <article className="detail-panel">
          <p className="detail-label">GA Runs</p>
          <h3>最近记录</h3>
          <div className="run-metadata-grid backtest-summary-grid">
            {gaRunsPayload.data.ga_runs.map((run) => (
              <article key={run.id} className="run-metadata-card">
                <p className="status-label">#{run.id} / {run.status}</p>
                <h3>{run.objective}</h3>
                <dl className="detail-list">
                  <div>
                    <dt>训练窗口</dt>
                    <dd>{formatDateOnly(run.train_start_date)} - {formatDateOnly(run.train_end_date)}</dd>
                  </div>
                  <div>
                    <dt>代数</dt>
                    <dd>{formatProgress(run.completed_generations, run.total_generations)}</dd>
                  </div>
                  <div>
                    <dt>个体</dt>
                    <dd>{formatProgress(run.completed_individuals, run.total_individuals)}</dd>
                  </div>
                  <div>
                    <dt>最佳个体</dt>
                    <dd>{run.best_individual_id ? `#${run.best_individual_id}` : "暂无"}</dd>
                  </div>
                </dl>
              </article>
            ))}
            {gaRunsPayload.data.ga_runs.length === 0 ? (
              <p className="status-copy">暂无 GA run。</p>
            ) : null}
          </div>
        </article>

        <article className="detail-panel">
          <p className="detail-label">Best Individuals</p>
          <h3>最近 GA 个体</h3>
          <div className="run-metadata-grid backtest-summary-grid">
            {latestIndividualsPayload.data.map((individual) => (
              <article key={individual.id} className="run-metadata-card">
                <p className="status-label">
                  G{individual.generation} / #{individual.individual_index} / {individual.status}
                </p>
                <h3>{individual.fitness ?? "未评分"}</h3>
                <dl className="detail-list">
                  <div>
                    <dt>RPS</dt>
                    <dd>{individual.parameters.rps_threshold ?? "未记录"} / {formatWindows(individual.parameters.selected_rps_windows)}</dd>
                  </div>
                  <div>
                    <dt>杯柄</dt>
                    <dd>{individual.parameters.use_cup_handle ? "启用" : "关闭"}</dd>
                  </div>
                  <div>
                    <dt>持仓上限</dt>
                    <dd>{individual.parameters.portfolio_cap ?? "未记录"}</dd>
                  </div>
                  <div>
                    <dt>最大回撤</dt>
                    <dd>{formatRatioAsPercent(individual.metrics?.aggregate?.max_drawdown ?? null)}</dd>
                  </div>
                </dl>
              </article>
            ))}
            {latestIndividualsPayload.data.length === 0 ? (
              <p className="status-copy">暂无 GA 个体。</p>
            ) : null}
          </div>
          <p className="status-meta">最近更新时间：{formatTimestamp(latestGaRun?.completed_at ?? latestGaRun?.started_at ?? null)}</p>
        </article>
      </section>

      <section className="detail-grid">
        <OptimizationResultsPanel
          apiBaseUrl={apiBaseUrl}
          optimizationRun={latestOptimizationRun}
          initialResults={latestOptimizationResultsPayload.data.results}
          initialTotal={latestOptimizationResultsPayload.data.total}
          initialError={latestOptimizationResultsPayload.error}
        />
      </section>
    </main>
  );
}
