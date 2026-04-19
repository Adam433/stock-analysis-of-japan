"use client";

import Link from "next/link";
import { FormEvent, useEffect, useState } from "react";

import { SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE } from "@/lib/backtestErrors";
import { formatRatioAsPercent, formatTimestamp, formatRpsWindows } from "@/lib/formatters";
import type { PortfolioBacktestDefaultsResponse } from "@/lib/portfolio-backtest-defaults";
import type { BacktestRun, PortfolioBacktestDefaults, PortfolioBacktestFieldName } from "@/lib/types";

type BacktestLaunchPanelProps = {
  apiBaseUrl: string;
  screenRunId: number | null;
  initialRun: BacktestRun | null;
  initialRuns: BacktestRun[];
  initialError: string | null;
};

type PortfolioBacktestFormValues = Record<PortfolioBacktestFieldName, string>;
type PortfolioBacktestFieldErrors = Partial<Record<PortfolioBacktestFieldName, string>>;

const EMPTY_FORM_VALUES: PortfolioBacktestFormValues = {
  holding_days: "",
  stop_loss_pct: "",
  portfolio_cap: "",
  entry_deferral_window_days: "",
};

function getLifecycleLabel(run: BacktestRun): string {
  return run.backtest_lifecycle === "legacy_condition_hit"
    ? "历史 condition-hit 模型"
    : "portfolio-return 模型";
}

function isCompletedPortfolioReturnRun(run: BacktestRun): boolean {
  return run.status === "completed" && run.backtest_lifecycle === "portfolio_return";
}

function isSourceScreenRunAvailable(run: BacktestRun): boolean {
  return run.source_screen_run_available !== false && run.source_screen_run_id !== null;
}

function selectDefaultCompareIds(runs: BacktestRun[]): number[] {
  return runs.filter((run) => isCompletedPortfolioReturnRun(run) && isSourceScreenRunAvailable(run)).slice(0, 2).map((run) => run.id);
}

function formValuesFromDefaults(defaults: PortfolioBacktestDefaults): PortfolioBacktestFormValues {
  return {
    holding_days: String(defaults.holding_days),
    stop_loss_pct: String(defaults.stop_loss_pct),
    portfolio_cap: String(defaults.portfolio_cap),
    entry_deferral_window_days: String(defaults.entry_deferral_window_days),
  };
}

function validatePositiveInteger(
  value: string,
  message: string,
): { parsed: number | null; error?: string } {
  const trimmed = value.trim();
  if (!trimmed) {
    return { parsed: null, error: message };
  }

  const parsed = Number(trimmed);
  if (!Number.isInteger(parsed) || parsed < 1) {
    return { parsed: null, error: message };
  }

  return { parsed };
}

function validateForm(values: PortfolioBacktestFormValues): {
  errors: PortfolioBacktestFieldErrors;
  parsedValues: PortfolioBacktestDefaults | null;
} {
  const errors: PortfolioBacktestFieldErrors = {};
  const holdingDays = validatePositiveInteger(values.holding_days, "持有期必须是不小于 1 的整数。");
  const portfolioCap = validatePositiveInteger(values.portfolio_cap, "组合上限必须是不小于 1 的整数。");
  const entryDeferralWindowDays = validatePositiveInteger(
    values.entry_deferral_window_days,
    "入场顺延窗口必须是不小于 1 的整数。",
  );
  const stopLossTrimmed = values.stop_loss_pct.trim();
  const stopLossValue = Number(stopLossTrimmed);

  if (holdingDays.error) {
    errors.holding_days = holdingDays.error;
  }
  if (portfolioCap.error) {
    errors.portfolio_cap = portfolioCap.error;
  }
  if (entryDeferralWindowDays.error) {
    errors.entry_deferral_window_days = entryDeferralWindowDays.error;
  }
  if (!stopLossTrimmed || !Number.isFinite(stopLossValue) || stopLossValue <= -1 || stopLossValue >= 0) {
    errors.stop_loss_pct = "止损阈值必须大于 -1 且小于 0。";
  }

  if (Object.keys(errors).length > 0) {
    return { errors, parsedValues: null };
  }

  return {
    errors,
    parsedValues: {
      holding_days: holdingDays.parsed as number,
      stop_loss_pct: stopLossValue,
      portfolio_cap: portfolioCap.parsed as number,
      entry_deferral_window_days: entryDeferralWindowDays.parsed as number,
    },
  };
}

function getInitialMessage(initialError: string | null, screenRunId: number | null): string {
  if (initialError) {
    return initialError;
  }
  if (screenRunId === null) {
    return "暂无可用的 completed screen run，完成一次筛选后即可启动 portfolio-return 回测。";
  }
  return `准备基于 screen run #${screenRunId} 启动 portfolio-return 回测。`;
}

export function BacktestLaunchPanel({
  apiBaseUrl,
  screenRunId,
  initialRun,
  initialRuns,
  initialError,
}: BacktestLaunchPanelProps) {
  const [formValues, setFormValues] = useState<PortfolioBacktestFormValues>(EMPTY_FORM_VALUES);
  const [fieldErrors, setFieldErrors] = useState<PortfolioBacktestFieldErrors>({});
  const [defaultsLoaded, setDefaultsLoaded] = useState(false);
  const [latestRun, setLatestRun] = useState<BacktestRun | null>(initialRun);
  const [runs, setRuns] = useState<BacktestRun[]>(initialRuns);
  const [selectedCompareIds, setSelectedCompareIds] = useState<number[]>(selectDefaultCompareIds(initialRuns));
  const [message, setMessage] = useState(getInitialMessage(initialError, screenRunId));
  const [launchState, setLaunchState] = useState<"idle" | "loading-defaults" | "launching" | "ready" | "error">(
    initialError ? "error" : "idle",
  );
  const completedPortfolioReturnRuns = runs.filter(
    isCompletedPortfolioReturnRun,
  );
  const legacyRuns = runs.filter((run) => run.backtest_lifecycle === "legacy_condition_hit");
  const sourceUnavailableRuns = completedPortfolioReturnRuns.filter((run) => !isSourceScreenRunAvailable(run));
  const compareEligibleRuns = completedPortfolioReturnRuns.filter(isSourceScreenRunAvailable);

  useEffect(() => {
    const availableIds = compareEligibleRuns.map((run) => run.id);
    setSelectedCompareIds((current) => {
      const filtered = current.filter((id) => availableIds.includes(id));
      const nextSelection =
        filtered.length >= 2 || availableIds.length < 2
          ? filtered.length
            ? filtered
            : availableIds.slice(0, Math.min(2, availableIds.length))
          : availableIds.slice(0, 2);

      return nextSelection.length === current.length &&
        nextSelection.every((id, index) => id === current[index])
        ? current
        : nextSelection;
    });
  }, [compareEligibleRuns]);

  useEffect(() => {
    let cancelled = false;

    async function loadDefaults() {
      setLaunchState((current) => (current === "error" ? current : "loading-defaults"));

      try {
        const response = await fetch(`${apiBaseUrl}/backtests/defaults`);
        if (!response.ok) {
          const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
          throw new Error(payload?.detail ?? `无法加载回测默认值（${response.status}）。`);
        }

        const payload = (await response.json()) as PortfolioBacktestDefaultsResponse;
        if (cancelled) {
          return;
        }

        const nextValues = formValuesFromDefaults(payload.defaults);
        setFormValues(nextValues);
        setFieldErrors(validateForm(nextValues).errors);
        setDefaultsLoaded(true);
        setLaunchState((current) => (current === "error" ? current : initialRun ? "ready" : "idle"));
        if (!initialError) {
          setMessage(getInitialMessage(null, screenRunId));
        }
      } catch (error) {
        if (cancelled) {
          return;
        }

        setDefaultsLoaded(false);
        setLaunchState("error");
        setMessage(error instanceof Error ? error.message : "无法加载 portfolio-return 回测默认值。");
      }
    }

    void loadDefaults();

    return () => {
      cancelled = true;
    };
  }, [apiBaseUrl, initialError, initialRun, screenRunId]);

  function handleFieldChange(fieldName: PortfolioBacktestFieldName, value: string) {
    const nextValues = {
      ...formValues,
      [fieldName]: value,
    };
    setFormValues(nextValues);
    setFieldErrors(validateForm(nextValues).errors);
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (screenRunId === null) {
      setLaunchState("error");
      setMessage("缺少可用的 completed screen run，暂时无法启动回测。");
      return;
    }

    const validation = validateForm(formValues);
    setFieldErrors(validation.errors);
    if (validation.parsedValues === null) {
      setLaunchState("error");
      setMessage("请先修正表单中的参数错误。");
      return;
    }

    setLaunchState("launching");
    setMessage(`正在从 screen run #${screenRunId} 启动并开始执行 portfolio-return 回测……`);

    try {
      const response = await fetch(`${apiBaseUrl}/backtests/portfolio-return/runs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          screen_run_id: screenRunId,
          ...validation.parsedValues,
        }),
      });

      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
        throw new Error(payload?.detail ?? `请求失败（${response.status}）`);
      }

      const payload = (await response.json()) as { backtest_run: BacktestRun };
      setLatestRun(payload.backtest_run);
      setRuns((current) => [payload.backtest_run, ...current.filter((run) => run.id !== payload.backtest_run.id)]);
      setLaunchState("ready");
      setMessage(`portfolio-return 回测 #${payload.backtest_run.id} 已启动，当前状态为 ${payload.backtest_run.status}。`);
    } catch (error) {
      setLaunchState("error");
      setMessage(error instanceof Error ? error.message : "无法启动 portfolio-return 回测。");
    }
  }

  function handleCompareSelection(runId: number) {
    setSelectedCompareIds((current) =>
      current.includes(runId) ? current.filter((id) => id !== runId) : [...current, runId],
    );
  }

  const validation = validateForm(formValues);
  const isLaunchDisabled =
    !defaultsLoaded ||
    launchState === "launching" ||
    screenRunId === null ||
    validation.parsedValues === null;
  const compareHref =
    selectedCompareIds.length >= 2
      ? `/backtests/portfolio-return/compare?ids=${selectedCompareIds.join(",")}`
      : null;

  return (
    <section className="screen-panel">
      <div className="screen-panel__header">
        <p className="eyebrow">回测启动</p>
        <h1>从一个已完成的 screen run 单动作启动 portfolio-return 回测。</h1>
        <p className="hero-text">
          Story 5.1 将 launch 和 execute 合并为一个动作，并把本次 screen run
          的来源与生效参数一起持久化，避免再走旧的日期区间 condition-hit 流程。
        </p>
      </div>

      <div className="screen-summary-grid">
        <article className="screen-summary-card">
          <p className="status-label">来源 screen run</p>
          <h2>{screenRunId ?? "不可用"}</h2>
          <p className="status-copy">单一入口固定绑定到一个已完成的筛选结果。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">最新任务</p>
          <h2>{latestRun ? `#${latestRun.id}` : "不可用"}</h2>
          {latestRun ? (
            <p
              className={`lifecycle-badge ${
                latestRun.backtest_lifecycle === "legacy_condition_hit"
                  ? "lifecycle-badge--legacy"
                  : "lifecycle-badge--portfolio"
              }`}
            >
              {getLifecycleLabel(latestRun)}
            </p>
          ) : null}
          <p className="status-copy">新 run 会写入 `portfolio_return` lifecycle。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">状态</p>
          <h2>{latestRun?.status ?? "无记录"}</h2>
          <p className="status-copy">MVP 只暴露单一 in-progress 状态，不提供 cancel。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">参数集</p>
          <h2>{latestRun ? `v${latestRun.parameter_set.version}` : "不可用"}</h2>
          <p className="status-copy">继续保留策略配置版本，便于后续结果比对。</p>
        </article>
      </div>

      <form className="strategy-form" onSubmit={handleSubmit}>
        <label className="strategy-field">
          <span>持有期（交易日）</span>
          <input
            name="holding_days"
            type="number"
            min="1"
            step="1"
            value={formValues.holding_days}
            aria-invalid={fieldErrors.holding_days ? "true" : "false"}
            onChange={(event) => handleFieldChange("holding_days", event.target.value)}
          />
          <small>{fieldErrors.holding_days ?? "默认值来自 anchor 定义的 MVP 配置。"}</small>
        </label>

        <label className="strategy-field">
          <span>止损阈值</span>
          <input
            name="stop_loss_pct"
            type="number"
            min="-0.9999"
            max="-0.0001"
            step="0.0001"
            value={formValues.stop_loss_pct}
            aria-invalid={fieldErrors.stop_loss_pct ? "true" : "false"}
            onChange={(event) => handleFieldChange("stop_loss_pct", event.target.value)}
          />
          <small>{fieldErrors.stop_loss_pct ?? "输入负数比例，例如 -0.08 表示 -8%。"}</small>
        </label>

        <label className="strategy-field">
          <span>组合上限</span>
          <input
            name="portfolio_cap"
            type="number"
            min="1"
            step="1"
            value={formValues.portfolio_cap}
            aria-invalid={fieldErrors.portfolio_cap ? "true" : "false"}
            onChange={(event) => handleFieldChange("portfolio_cap", event.target.value)}
          />
          <small>{fieldErrors.portfolio_cap ?? "限制单次组合最多持有的标的数量。"}</small>
        </label>

        <label className="strategy-field">
          <span>入场顺延窗口（交易日）</span>
          <input
            name="entry_deferral_window_days"
            type="number"
            min="1"
            step="1"
            value={formValues.entry_deferral_window_days}
            aria-invalid={fieldErrors.entry_deferral_window_days ? "true" : "false"}
            onChange={(event) => handleFieldChange("entry_deferral_window_days", event.target.value)}
          />
          <small>
            {fieldErrors.entry_deferral_window_days ?? "用于后续 T+1 入场窗口模拟。"}
          </small>
        </label>

        <div className="strategy-actions">
          <div className="strategy-button-row">
            <button type="submit" className="strategy-button" disabled={isLaunchDisabled}>
              {launchState === "launching" ? "启动中……" : "启动并执行回测"}
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
        {latestRun?.backtest_lifecycle === "legacy_condition_hit" ? (
          <p className="status-copy lifecycle-copy">
            当前任务属于历史 condition-hit lifecycle；它会保留用于追溯，但不会计入
            portfolio-return 组合层汇总。
          </p>
        ) : null}
        {latestRun ? (
          <>
            <div className="run-metadata-grid">
              <article className="run-metadata-card">
                <p className="status-label">来源 screen run</p>
                <h3>{latestRun.source_screen_run_id ?? "历史未记录"}</h3>
              </article>
              <article className="run-metadata-card">
                <p className="status-label">开始时间</p>
                <h3>{formatTimestamp(latestRun.started_at, "尚未完成")}</h3>
              </article>
              <article className="run-metadata-card">
                <p className="status-label">完成时间</p>
                <h3>{formatTimestamp(latestRun.completed_at, "尚未完成")}</h3>
              </article>
              <article className="run-metadata-card">
                <p className="status-label">生效参数</p>
                <h3>
                  {latestRun.effective_holding_days ?? "-"} / {latestRun.effective_stop_loss_pct ?? "-"} /{" "}
                  {latestRun.effective_portfolio_cap ?? "-"} /{" "}
                  {latestRun.effective_entry_deferral_window_days ?? "-"}
                </h3>
              </article>
              <article className="run-metadata-card">
                <p className="status-label">数据集范围</p>
                <h3>
                  {latestRun.dataset_trade_date_start ?? "-"} 至 {latestRun.dataset_trade_date_end ?? "-"}
                </h3>
              </article>
              <article className="run-metadata-card">
                <p className="status-label">RPS 语义</p>
                <h3>
                  {!isSourceScreenRunAvailable(latestRun) && latestRun.backtest_lifecycle === "portfolio_return"
                    ? "策略定义不可解析"
                    : latestRun.rps_definition_version ??
                    (latestRun.backtest_lifecycle === "portfolio_return" && latestRun.source_screen_run_id !== null
                      ? "从 source screen run 解析"
                      : "历史未记录")}
                </h3>
              </article>
            </div>

            <div className="run-metadata-grid">
              <article className="run-metadata-card">
                <p className="status-label">参数集</p>
                {latestRun.backtest_lifecycle === "portfolio_return" && !isSourceScreenRunAvailable(latestRun) ? (
                  <>
                    <h3>不可解析</h3>
                    <p className="status-copy">{SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE}</p>
                  </>
                ) : (
                  <>
                    <h3>v{latestRun.parameter_set.version}</h3>
                    <p className="status-copy">
                      RPS {latestRun.parameter_set.rps_threshold} / 贴近年高 {latestRun.parameter_set.high_proximity_threshold_pct}%
                    </p>
                  </>
                )}
              </article>
              <article className="run-metadata-card">
                <p className="status-label">RPS 窗口</p>
                <h3>
                  {latestRun.backtest_lifecycle === "portfolio_return" && !isSourceScreenRunAvailable(latestRun)
                    ? "不可解析"
                    : formatRpsWindows(latestRun.parameter_set.selected_rps_windows)}
                </h3>
              </article>
              <article className="run-metadata-card">
                <p className="status-label">错误信息</p>
                <h3>{latestRun.error_message ?? "无"}</h3>
              </article>
            </div>
          </>
        ) : (
          <p className="empty-state">暂无持久化的回测记录。</p>
        )}

        {latestRun?.status === "completed" ? (
          <div className="run-metadata-grid backtest-summary-grid">
            <article className="run-metadata-card">
              <p className="status-label">累计收益</p>
              <h3>{formatRatioAsPercent(latestRun.cumulative_return)}</h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">已入场标的</p>
              <h3>{latestRun.position_count_after_exclusions ?? 0}</h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">排除标的</p>
              <h3>{latestRun.excluded_securities.length}</h3>
            </article>
            <article className="run-metadata-card">
              <p className="status-label">胜率视图</p>
              {latestRun.backtest_lifecycle === "portfolio_return" && isSourceScreenRunAvailable(latestRun) ? (
                <>
                  <h3>
                    <Link className="result-link" href={`/backtests/portfolio-return/${latestRun.id}`}>
                      打开结果详情
                    </Link>
                  </h3>
                  <p className="status-copy">查看 cumulative return、win rate、max drawdown 与权益曲线。</p>
                </>
              ) : latestRun.backtest_lifecycle === "portfolio_return" ? (
                <>
                  <h3>
                    <span className="result-link result-link--disabled" aria-disabled="true">
                      结果不可解释
                    </span>
                  </h3>
                  <p className="status-copy">{SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE}</p>
                </>
              ) : (
                <h3>仅历史追溯</h3>
              )}
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
            当前任务已启动；组合收益执行引擎会在后续 story 中继续接上。
          </p>
        )}
      </section>

      <section className="result-panel">
        <div className="result-panel__header">
          <p className="eyebrow">结果复盘</p>
          <h2>已完成任务及策略调整对比。</h2>
          <p className="status-copy">
            无需离开回测工作流，即可回顾已绑定到任务的产出并对比参数版本与持久化摘要的差异。
            这些 runs 保留用于历史追溯，并显式带标签展示。
          </p>
        </div>

        <div className="run-metadata-grid backtest-summary-grid">
          <article className="run-metadata-card">
            <p className="status-label">portfolio-return 已完成任务</p>
            <h3>{completedPortfolioReturnRuns.length}</h3>
            <p className="status-copy">组合层对比与汇总仅统计这一 lifecycle。</p>
          </article>
          <article className="run-metadata-card">
            <p className="status-label">历史 condition-hit 任务</p>
            <h3>{legacyRuns.length}</h3>
            <p className="status-copy">单独展示，不计入 portfolio-return 聚合。</p>
          </article>
        </div>

        {completedPortfolioReturnRuns.length ? (
          <>
            <div className="strategy-actions">
              <div className="strategy-button-row">
                <Link
                  href={compareHref ?? "/backtests"}
                  aria-disabled={compareHref ? "false" : "true"}
                  className={`strategy-button strategy-button--link ${compareHref ? "" : "strategy-button--disabled"}`}
                >
                  打开对比页
                </Link>
              </div>
              <p className="strategy-message strategy-message--ready">
                {compareHref
                  ? `已选择 ${selectedCompareIds.length} 条 portfolio-return runs 进入对比。`
                  : sourceUnavailableRuns.length
                    ? "至少选择两条来源筛选仍可解析的 portfolio-return runs 才能进入对比。"
                    : "至少选择两条已完成的 portfolio-return runs 才能进入对比。"}
              </p>
            </div>
            <div className="run-metadata-grid backtest-summary-grid">
              {completedPortfolioReturnRuns.map((run) => (
              <article
                key={`compare-${run.id}`}
                className={`run-metadata-card ${isSourceScreenRunAvailable(run) ? "" : "run-metadata-card--warning"}`}
              >
                <div className="compare-run-header">
                  <label
                    className={`compare-run-select ${isSourceScreenRunAvailable(run) ? "" : "compare-run-select--disabled"}`}
                    title={isSourceScreenRunAvailable(run) ? undefined : SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE}
                  >
                    <input
                      type="checkbox"
                      disabled={!isSourceScreenRunAvailable(run)}
                      checked={selectedCompareIds.includes(run.id)}
                      onChange={() => handleCompareSelection(run.id)}
                    />
                    <span>{isSourceScreenRunAvailable(run) ? "加入对比" : "来源不可解析"}</span>
                  </label>
                  <p className="lifecycle-badge lifecycle-badge--portfolio">portfolio-return 模型</p>
                </div>
                <p className="status-label">任务 #{run.id}</p>
                <h3>{formatRatioAsPercent(run.cumulative_return)}</h3>
                <p className="status-copy">
                  来源 screen run #{run.source_screen_run_id ?? "历史未记录"} / 参数集 v{run.parameter_set.version}
                </p>
                <p className="status-copy">
                  持有 {run.effective_holding_days ?? "-"} / 止损 {run.effective_stop_loss_pct ?? "-"} / cap {run.effective_portfolio_cap ?? "-"}
                </p>
                <p className="status-copy">
                  {isSourceScreenRunAvailable(run) ? (
                    <Link className="result-link" href={`/backtests/portfolio-return/${run.id}`}>
                      查看结果详情
                    </Link>
                  ) : (
                    <span className="result-link result-link--disabled" aria-disabled="true">
                      结果不可解释
                    </span>
                  )}
                </p>
                {!isSourceScreenRunAvailable(run) ? (
                  <p className="status-copy">{SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE}</p>
                ) : null}
              </article>
              ))}
            </div>
          </>
        ) : (
          <p className="empty-state">暂无已完成的 portfolio-return runs 可用于跨 run 对比。</p>
        )}

        {legacyRuns.length ? (
          <div className="run-metadata-grid backtest-summary-grid">
            {legacyRuns.slice(0, 2).map((run) => (
              <article key={`legacy-${run.id}`} className="run-metadata-card">
                <div className="compare-run-header">
                  <label className="compare-run-select" title="历史 condition-hit runs 不能加入 portfolio-return 对比。">
                    <input type="checkbox" disabled />
                    <span>不可加入对比</span>
                  </label>
                </div>
                <p className="lifecycle-badge lifecycle-badge--legacy">历史 condition-hit 模型</p>
                <h3>#{run.id}</h3>
                <p className="status-copy">
                  状态 {run.status}，参数集 v{run.parameter_set.version}
                </p>
              </article>
            ))}
          </div>
        ) : null}
      </section>
    </section>
  );
}
