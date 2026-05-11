"use client";

import Link from "next/link";
import { useState } from "react";

import { formatRatioAsPercent, formatTimestamp } from "@/lib/formatters";

export type OptimizationRunSummary = {
  id: number;
  market: string;
  objective: string;
  status: string;
  completed_parameter_sets: number;
  total_parameter_sets: number;
  failed_parameter_sets: number;
  best_result_id: number | null;
  started_at: string | null;
  completed_at: string | null;
};

export type OptimizationResultSummary = {
  id: number;
  optimization_run_id: number;
  score: string | null;
  rank: number | null;
  status: string;
  failure_reason: string | null;
  completed_at: string | null;
  parameters: {
    rps_threshold?: number;
    selected_rps_windows?: number[];
    holding_days?: number | null;
    stop_loss_pct?: string;
    rps_exit_threshold?: number | null;
    portfolio_cap?: number;
    use_cup_handle?: boolean;
    fundamental_growth_params?: {
      max_pe?: string | null;
      max_pb?: string | null;
      min_growth_count?: number | null;
      effective_min_growth_count?: number | null;
      require_positive_operating_cash_flow?: boolean;
    };
  };
  train_metrics?: {
    total_return?: string | null;
    max_drawdown?: string | null;
    completed_trades?: number;
    win_rate?: string | null;
    spy_average_trade_excess_return?: string | null;
  } | null;
  validation_metrics?: {
    total_return?: string | null;
    max_drawdown?: string | null;
    completed_trades?: number;
    win_rate?: string | null;
    spy_average_trade_excess_return?: string | null;
  } | null;
};

type ResultsResponse = {
  results: OptimizationResultSummary[];
  total: number;
  limit: number;
  offset: number;
};

type Props = {
  apiBaseUrl: string;
  optimizationRun: OptimizationRunSummary | null;
  initialResults: OptimizationResultSummary[];
  initialTotal: number;
  initialError: string | null;
};

const PAGE_SIZE = 30;

function windowsLabel(windows: number[] | undefined): string {
  return windows?.length ? windows.join(" / ") : "未记录";
}

function metricValue(
  result: OptimizationResultSummary,
  key: "total_return" | "max_drawdown" | "win_rate" | "spy_average_trade_excess_return",
): string {
  const metrics = result.validation_metrics ?? result.train_metrics;
  return formatRatioAsPercent(metrics?.[key] ?? null);
}

export function OptimizationResultsPanel({
  apiBaseUrl,
  optimizationRun,
  initialResults,
  initialTotal,
  initialError,
}: Props) {
  const [results, setResults] = useState(initialResults);
  const [total, setTotal] = useState(initialTotal);
  const [offset, setOffset] = useState(0);
  const [message, setMessage] = useState(
    initialError ?? (optimizationRun ? `已加载优化任务 #${optimizationRun.id}。` : "暂无参数优化任务。"),
  );
  const [busyId, setBusyId] = useState<number | null>(null);

  async function loadPage(nextOffset: number) {
    if (!optimizationRun) {
      return;
    }
    setMessage("正在加载优化结果……");
    const response = await fetch(
      `${apiBaseUrl}/backtests/optimization/runs/${optimizationRun.id}/results?limit=${PAGE_SIZE}&offset=${nextOffset}&summary_only=true`,
    );
    if (!response.ok) {
      setMessage(`无法加载优化结果（${response.status}）。`);
      return;
    }
    const payload = (await response.json()) as ResultsResponse;
    setResults(payload.results);
    setTotal(payload.total);
    setOffset(payload.offset);
    setMessage(
      payload.total
        ? `已加载 ${payload.offset + 1} - ${Math.min(payload.offset + payload.results.length, payload.total)} / ${payload.total}。`
        : "暂无优化结果。",
    );
  }

  async function deleteResult(resultId: number) {
    if (!window.confirm(`删除优化结果 #${resultId}？`)) {
      return;
    }
    setBusyId(resultId);
    try {
      const response = await fetch(`${apiBaseUrl}/backtests/optimization/results/${resultId}`, {
        method: "DELETE",
      });
      if (!response.ok) {
        setMessage(`删除失败（${response.status}）。`);
        return;
      }
      setResults((current) => current.filter((result) => result.id !== resultId));
      setTotal((current) => Math.max(0, current - 1));
      setMessage(`优化结果 #${resultId} 已删除。`);
    } finally {
      setBusyId(null);
    }
  }

  async function savePreset(result: OptimizationResultSummary) {
    if (!optimizationRun) {
      return;
    }
    const name = window.prompt("预设名称", `Opt #${result.id} ${result.score ?? ""}`.trim());
    if (!name) {
      return;
    }
    setBusyId(result.id);
    try {
      const response = await fetch(`${apiBaseUrl}/strategy-presets`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          market: optimizationRun.market,
          name,
          description: `From optimization run #${optimizationRun.id}, result #${result.id}`,
          parameters: result.parameters,
          source_optimization_run_id: optimizationRun.id,
          source_optimization_result_id: result.id,
        }),
      });
      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
        setMessage(payload?.detail ?? `保存预设失败（${response.status}）。`);
        return;
      }
      setMessage(`优化结果 #${result.id} 已保存为策略预设。`);
    } finally {
      setBusyId(null);
    }
  }

  async function rerunResult(resultId: number) {
    setBusyId(resultId);
    try {
      const response = await fetch(`${apiBaseUrl}/backtests/optimization/results/${resultId}/rerun`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ execute_immediately: true, max_workers: 1 }),
      });
      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
        setMessage(payload?.detail ?? `重跑失败（${response.status}）。`);
        return;
      }
      const payload = (await response.json()) as { optimization_run: OptimizationRunSummary };
      setMessage(`已创建并启动单参数重跑任务 #${payload.optimization_run.id}。`);
    } finally {
      setBusyId(null);
    }
  }

  return (
    <article className="detail-panel">
      <p className="detail-label">Optimization Results</p>
      <h3>参数优化结果</h3>
      <p className="status-copy">{message}</p>

      <div className="run-metadata-grid backtest-summary-grid">
        {results.map((result) => (
          <article key={result.id} className="run-metadata-card">
            <p className="status-label">
              #{result.id} / rank {result.rank ?? "—"} / {result.status}
            </p>
            <h3>{result.score ?? "未评分"}</h3>
            <dl className="detail-list">
              <div>
                <dt>RPS</dt>
                <dd>{result.parameters.rps_threshold ?? "—"} / {windowsLabel(result.parameters.selected_rps_windows)}</dd>
              </div>
              <div>
                <dt>收益 / Alpha</dt>
                <dd>{metricValue(result, "total_return")} / {metricValue(result, "spy_average_trade_excess_return")}</dd>
              </div>
              <div>
                <dt>回撤 / 胜率</dt>
                <dd>{metricValue(result, "max_drawdown")} / {metricValue(result, "win_rate")}</dd>
              </div>
              <div>
                <dt>财务</dt>
                <dd>
                  PE {result.parameters.fundamental_growth_params?.max_pe ?? "不限"} / PB {result.parameters.fundamental_growth_params?.max_pb ?? "不限"}
                </dd>
              </div>
            </dl>
            <div className="inline-actions">
              <Link className="result-link" href={`/experiments/optimization-results/${result.id}`}>
                详情
              </Link>
              <button className="inline-action" type="button" disabled={busyId === result.id} onClick={() => savePreset(result)}>
                保存
              </button>
              <button className="inline-action" type="button" disabled={busyId === result.id} onClick={() => rerunResult(result.id)}>
                重跑
              </button>
              <button className="inline-action" type="button" disabled={busyId === result.id} onClick={() => deleteResult(result.id)}>
                删除
              </button>
            </div>
            <p className="status-meta">完成：{formatTimestamp(result.completed_at)}</p>
          </article>
        ))}
        {results.length === 0 ? <p className="status-copy">暂无优化结果。</p> : null}
      </div>

      <div className="pager">
        <button className="inline-action" type="button" disabled={!optimizationRun || offset === 0} onClick={() => loadPage(Math.max(0, offset - PAGE_SIZE))}>
          上一页
        </button>
        <span>{total ? `${offset + 1} - ${Math.min(offset + results.length, total)} / ${total}` : "0 / 0"}</span>
        <button className="inline-action" type="button" disabled={!optimizationRun || offset + PAGE_SIZE >= total} onClick={() => loadPage(offset + PAGE_SIZE)}>
          下一页
        </button>
      </div>
    </article>
  );
}
