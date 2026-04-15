"use client";

import { FormEvent, useState } from "react";

type BacktestRun = {
  id: number;
  strategy_configuration_id: number;
  status: string;
  start_date: string;
  end_date: string;
  started_at: string;
  completed_at: string | null;
  rps_definition_version: string | null;
  dataset_trade_date_start: string | null;
  dataset_trade_date_end: string | null;
  dataset_checksum: string | null;
  error_message: string | null;
  result_summary: {
    trade_dates_evaluated: number;
    total_candidates_evaluated: number;
    qualifying_observations: number;
    unique_qualified_instruments: number;
    first_qualified_trade_date: string | null;
    last_qualified_trade_date: string | null;
    result_checksum: string | null;
  };
  parameter_set: {
    id: number;
    version: number;
    rps_threshold: number;
    high_proximity_threshold_pct: string;
    selected_rps_windows: number[];
    min_rps_lines_required: number;
  };
};

type BacktestLaunchPanelProps = {
  apiBaseUrl: string;
  initialRun: BacktestRun | null;
  initialRuns: BacktestRun[];
  initialError: string | null;
};

function formatTimestamp(value: string | null): string {
  if (!value) {
    return "尚未完成";
  }

  return new Intl.DateTimeFormat("zh-CN", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

function formatRpsWindows(windows: number[]): string {
  return windows.map((window) => `${window} 日`).join(" / ");
}

export function BacktestLaunchPanel({
  apiBaseUrl,
  initialRun,
  initialRuns,
  initialError,
}: BacktestLaunchPanelProps) {
  const [startDate, setStartDate] = useState("2024-01-01");
  const [endDate, setEndDate] = useState("2024-12-31");
  const [latestRun, setLatestRun] = useState<BacktestRun | null>(initialRun);
  const [runs, setRuns] = useState<BacktestRun[]>(initialRuns);
  const [message, setMessage] = useState(
    initialError ?? "选择一个历史区间，启动持久化的回测任务。",
  );
  const [launchState, setLaunchState] = useState<"idle" | "launching" | "executing" | "ready" | "error">(
    initialError ? "error" : initialRun ? "ready" : "idle",
  );

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (startDate > endDate) {
      setLaunchState("error");
      setMessage("开始日期必须早于或等于结束日期。");
      return;
    }

    setLaunchState("launching");
    setMessage("正在启动回测并持久化运行上下文……");

    try {
      const response = await fetch(`${apiBaseUrl}/backtests/runs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          start_date: startDate,
          end_date: endDate,
        }),
      });

      if (!response.ok) {
        const payload = (await response.json()) as { detail?: string };
        throw new Error(payload.detail ?? `请求失败（${response.status}）`);
      }

      const payload = (await response.json()) as { backtest_run: BacktestRun };
      setLatestRun(payload.backtest_run);
      setRuns((current) => [payload.backtest_run, ...current.filter((run) => run.id !== payload.backtest_run.id)]);
      setLaunchState("ready");
      setMessage(
        `回测 #${payload.backtest_run.id} 已持久化，当前状态为 ${payload.backtest_run.status}。`,
      );
    } catch (error) {
      setLaunchState("error");
      setMessage(error instanceof Error ? error.message : "无法启动回测任务。");
    }
  }

  async function handleExecuteLatestRun() {
    if (!latestRun) {
      setLaunchState("error");
      setMessage("请先启动一个回测任务再执行。");
      return;
    }

    setLaunchState("executing");
    setMessage(`正在基于已存派生指标执行回测 #${latestRun.id}……`);

    try {
      const response = await fetch(`${apiBaseUrl}/backtests/runs/${latestRun.id}/execute`, {
        method: "POST",
      });

      if (!response.ok) {
        const payload = (await response.json()) as { detail?: string };
        throw new Error(payload.detail ?? `请求失败（${response.status}）`);
      }

      const payload = (await response.json()) as { backtest_run: BacktestRun };
      setLatestRun(payload.backtest_run);
      setRuns((current) => [payload.backtest_run, ...current.filter((run) => run.id !== payload.backtest_run.id)]);
      setLaunchState("ready");
      setMessage(
        `回测 #${payload.backtest_run.id} 已完成，校验和 ${payload.backtest_run.result_summary.result_checksum ?? "不可用"}。`,
      );
    } catch (error) {
      setLaunchState("error");
      setMessage(error instanceof Error ? error.message : "无法执行回测任务。");
    }
  }

  return (
    <section className="screen-panel">
      <div className="screen-panel__header">
        <p className="eyebrow">回测启动</p>
        <h1>执行前先持久化一次历史回测记录。</h1>
        <p className="hero-text">
          Story 5.1 建立回测运行记录，将其关联到当前参数集，并为耗时较长的任务
          明确展现“执行中”状态。
        </p>
      </div>

      <div className="screen-summary-grid">
        <article className="screen-summary-card">
          <p className="status-label">最新任务</p>
          <h2>{latestRun ? `#${latestRun.id}` : "不可用"}</h2>
          <p className="status-copy">已持久化的回测标识，便于后续结果检索。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">状态</p>
          <h2>{latestRun?.status ?? "无记录"}</h2>
          <p className="status-copy">回测启动状态显式呈现，避免暗示瞬时完成。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">参数集</p>
          <h2>{latestRun ? `v${latestRun.parameter_set.version}` : "不可用"}</h2>
          <p className="status-copy">每个回测任务都绑定到精确的策略配置版本。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">RPS 语义</p>
          <h2>{latestRun?.rps_definition_version ?? "历史未记录"}</h2>
          <p className="status-copy">用于核对回测是否与 screening 和图表解释保持同一语义。</p>
        </article>
      </div>

      <form className="strategy-form" onSubmit={handleSubmit}>
        <label className="strategy-field">
          <span>开始日期</span>
          <input
            name="start_date"
            type="date"
            value={startDate}
            aria-invalid={launchState === "error" && message.includes("开始日期")}
            onChange={(event) => setStartDate(event.target.value)}
          />
          <small>此次回测的历史区间起点。</small>
        </label>

        <label className="strategy-field">
          <span>结束日期</span>
          <input
            name="end_date"
            type="date"
            value={endDate}
            aria-invalid={launchState === "error" && message.includes("开始日期")}
            onChange={(event) => setEndDate(event.target.value)}
          />
          <small>此次回测的历史区间终点。</small>
        </label>

        <div className="strategy-actions">
          <div className="strategy-button-row">
            <button type="submit" className="strategy-button" disabled={launchState === "launching"}>
              {launchState === "launching" ? "启动中……" : "启动回测"}
            </button>
            <button
              type="button"
              className="strategy-button strategy-button--secondary"
              disabled={!latestRun || launchState === "executing" || launchState === "launching"}
              onClick={handleExecuteLatestRun}
            >
              {launchState === "executing" ? "执行中……" : "执行最新任务"}
            </button>
          </div>
          <p
            className={`strategy-message strategy-message--${launchState === "error" ? "error" : "ready"}`}
            role={launchState === "error" ? "alert" : "status"}
          >
            {message}
          </p>
        </div>
      </form>

      <section className="result-panel">
        <div className="result-panel__header">
          <p className="eyebrow">运行上下文</p>
          <h2>最近一次持久化的回测任务。</h2>
        </div>
        {latestRun ? (
          <div className="run-metadata-grid">
            <article className="run-metadata-card">
              <p className="status-label">区间</p>
              <h3>
                {latestRun.start_date} 至 {latestRun.end_date}
              </h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">开始时间</p>
              <h3>{formatTimestamp(latestRun.started_at)}</h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">完成时间</p>
              <h3>{formatTimestamp(latestRun.completed_at)}</h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">数据集范围</p>
              <h3>
                {latestRun.dataset_trade_date_start ?? "-"} 至 {latestRun.dataset_trade_date_end ?? "-"}
              </h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">数据集指纹</p>
              <h3>{latestRun.dataset_checksum ?? "历史未记录"}</h3>
            </article>
          </div>
        ) : (
          <p className="empty-state">暂无持久化的回测记录。</p>
        )}

        {latestRun?.status === "completed" ? (
          <div className="run-metadata-grid backtest-summary-grid">
            <article className="run-metadata-card">
              <p className="status-label">交易日数</p>
              <h3>{latestRun.result_summary.trade_dates_evaluated}</h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">入选快照数</p>
              <h3>{latestRun.result_summary.qualifying_observations}</h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">入选标的数</p>
              <h3>{latestRun.result_summary.unique_qualified_instruments}</h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">首个 / 最后入选日</p>
              <h3>
                {latestRun.result_summary.first_qualified_trade_date ?? "-"} /{" "}
                {latestRun.result_summary.last_qualified_trade_date ?? "-"}
              </h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">校验和</p>
              <h3>{latestRun.result_summary.result_checksum ?? "不可用"}</h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">评估候选数</p>
              <h3>{latestRun.result_summary.total_candidates_evaluated}</h3>
            </article>
          </div>
        ) : (
          <p className="empty-state">
            执行已持久化的任务以基于已存输入生成可复现的回测摘要。
          </p>
        )}
      </section>

      <section className="result-panel">
        <div className="result-panel__header">
          <p className="eyebrow">结果复盘</p>
          <h2>已完成任务及策略调整对比。</h2>
          <p className="status-copy">
            无需离开回测工作流，即可回顾已绑定到任务的产出并对比参数版本、区间
            与持久化摘要的差异。
          </p>
        </div>

        {runs.filter((run) => run.status === "completed").length ? (
          <>
            <div className="run-metadata-grid backtest-summary-grid">
              {runs
                .filter((run) => run.status === "completed")
                .slice(0, 2)
                .map((run) => (
                  <article key={`compare-${run.id}`} className="run-metadata-card">
                    <p className="status-label">任务 #{run.id}</p>
                    <h3>v{run.parameter_set.version}</h3>
                    <p className="status-copy">
                      区间 {run.start_date} 至 {run.end_date}
                    </p>
                    <p className="status-copy">RPS 语义 {run.rps_definition_version ?? "历史未记录"}</p>
                    <p className="status-copy">
                      入选快照 {run.result_summary.qualifying_observations} ｜ 标的{" "}
                      {run.result_summary.unique_qualified_instruments}
                    </p>
                    <p className="status-copy">
                      RPS {run.parameter_set.rps_threshold} / 距高点{" "}
                      {run.parameter_set.high_proximity_threshold_pct}%
                    </p>
                    <p className="status-copy">
                      窗口 {formatRpsWindows(run.parameter_set.selected_rps_windows)} ｜ 至少满足{" "}
                      {run.parameter_set.min_rps_lines_required} 条
                    </p>
                  </article>
                ))}
            </div>

            <div className="result-list">
              {runs
                .filter((run) => run.status === "completed")
                .map((run, index, completedRuns) => {
                  const previousRun = completedRuns[index + 1] ?? null;
                  const qualifyingDelta = previousRun
                    ? run.result_summary.qualifying_observations - previousRun.result_summary.qualifying_observations
                    : null;
                  const instrumentDelta = previousRun
                    ? run.result_summary.unique_qualified_instruments -
                      previousRun.result_summary.unique_qualified_instruments
                    : null;

                  return (
                    <article key={run.id} className="result-card">
                      <div className="result-card__title">
                        <div>
                          <p className="status-label">已完成回测</p>
                          <h3>任务 #{run.id}</h3>
                        </div>
                        <p className="result-pass-flag">v{run.parameter_set.version}</p>
                      </div>

                      <div className="result-summary-grid">
                        <div>
                          <dt>区间</dt>
                          <dd>
                            {run.start_date} 至 {run.end_date}
                          </dd>
                        </div>
                        <div>
                          <dt>入选快照数</dt>
                          <dd>{run.result_summary.qualifying_observations}</dd>
                        </div>
                        <div>
                          <dt>入选标的数</dt>
                          <dd>{run.result_summary.unique_qualified_instruments}</dd>
                        </div>
                        <div>
                          <dt>校验和</dt>
                          <dd>{run.result_summary.result_checksum ?? "不可用"}</dd>
                        </div>
                        <div>
                          <dt>数据集指纹</dt>
                          <dd>{run.dataset_checksum ?? "历史未记录"}</dd>
                        </div>
                      </div>

                      <ul className="signal-list">
                        <li>
                          参数集：RPS {run.parameter_set.rps_threshold} / 距高点{" "}
                          {run.parameter_set.high_proximity_threshold_pct}% / 窗口{" "}
                          {formatRpsWindows(run.parameter_set.selected_rps_windows)} / 至少满足{" "}
                          {run.parameter_set.min_rps_lines_required} 条
                        </li>
                        <li>
                          RPS 语义：{run.rps_definition_version ?? "历史未记录"} ｜ 数据集范围：
                          {" "}{run.dataset_trade_date_start ?? "-"} 至 {run.dataset_trade_date_end ?? "-"}
                        </li>
                        <li>
                          入选日期跨度：{run.result_summary.first_qualified_trade_date ?? "-"} 至{" "}
                          {run.result_summary.last_qualified_trade_date ?? "-"}
                        </li>
                        <li>
                          与上一次已完成任务相比：入选快照差{" "}
                          {qualifyingDelta === null ? "无可比" : qualifyingDelta >= 0 ? `+${qualifyingDelta}` : qualifyingDelta}
                          ，标的差{" "}
                          {instrumentDelta === null ? "无可比" : instrumentDelta >= 0 ? `+${instrumentDelta}` : instrumentDelta}
                        </li>
                      </ul>
                    </article>
                  );
                })}
            </div>
          </>
        ) : (
          <p className="empty-state">
            执行一个或多个回测任务后，即可查看已完成结果并进行跨任务对比。
          </p>
        )}
      </section>
    </section>
  );
}
