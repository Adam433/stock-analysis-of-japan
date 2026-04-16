"use client";

import { FormEvent, useEffect, useState } from "react";
import Link from "next/link";

import { WatchlistToggleButton } from "@/components/watchlist/WatchlistToggleButton";
import { formatTimestamp, formatRpsWindows } from "@/lib/formatters";
import type { StrategyConfigurationResponse, ScreenRun, ScreenRunResult } from "@/lib/types";

type ScreeningTradeDateOption = {
  trade_date: string;
};

type StrategyConfigPanelProps = {
  apiBaseUrl: string;
  initialData: StrategyConfigurationResponse | null;
  initialError: string | null;
  initialRun: ScreenRun | null;
  initialRunError: string | null;
  initialTradeDates: ScreeningTradeDateOption[];
  initialTradeDateError: string | null;
};

type SaveState = "idle" | "saving" | "saved" | "error";
type RunState = "idle" | "running" | "ready" | "error";

export function StrategyConfigPanel({
  apiBaseUrl,
  initialData,
  initialError,
  initialRun,
  initialRunError,
  initialTradeDates,
  initialTradeDateError,
}: StrategyConfigPanelProps) {
  const [watchlistInstrumentIds, setWatchlistInstrumentIds] = useState<number[]>([]);
  const [rpsThreshold, setRpsThreshold] = useState(initialData?.configuration.rps_threshold ?? 90);
  const [highProximityThresholdPct, setHighProximityThresholdPct] = useState(
    initialData?.configuration.high_proximity_threshold_pct ?? "5.00",
  );
  const [selectedRpsWindows, setSelectedRpsWindows] = useState<number[]>(
    initialData?.configuration.selected_rps_windows ??
      initialData?.validation?.selected_rps_windows.default ??
      [50, 120, 250],
  );
  const [minRpsLinesRequired, setMinRpsLinesRequired] = useState(
    initialData?.configuration.min_rps_lines_required ??
      initialData?.validation?.min_rps_lines_required.default ??
      1,
  );
  const [activeVersion, setActiveVersion] = useState(initialData?.configuration.version ?? 0);
  const [message, setMessage] = useState(
    initialError ?? "调整阈值、保存参数集、然后启动筛选。",
  );
  const [saveState, setSaveState] = useState<SaveState>(initialError ? "error" : "idle");
  const [runState, setRunState] = useState<RunState>(initialRun ? "ready" : initialRunError ? "error" : "idle");
  const [runMessage, setRunMessage] = useState(
    initialRunError ?? "尚未从此工作流启动过筛选。",
  );
  const [latestRun, setLatestRun] = useState<ScreenRun | null>(initialRun);
  const [availableTradeDates, setAvailableTradeDates] = useState<ScreeningTradeDateOption[]>(initialTradeDates);
  const [selectedTradeDate, setSelectedTradeDate] = useState(initialTradeDates[0]?.trade_date ?? "");
  const approvedRpsWindows =
    initialData?.validation?.selected_rps_windows.approved ??
    initialData?.configuration.selected_rps_windows ??
    [50, 120, 250];
  const hasLoadedConfiguration = Boolean(initialData);
  const canRunWithoutTradeDateList = Boolean(initialTradeDateError) && !availableTradeDates.length;

  useEffect(() => {
    if (initialData) {
      setRpsThreshold(initialData.configuration.rps_threshold);
      setHighProximityThresholdPct(initialData.configuration.high_proximity_threshold_pct);
      setSelectedRpsWindows(initialData.configuration.selected_rps_windows);
      setMinRpsLinesRequired(initialData.configuration.min_rps_lines_required);
      setActiveVersion(initialData.configuration.version);
    }
  }, [initialData]);

  useEffect(() => {
    if (initialTradeDates.length) {
      setAvailableTradeDates(initialTradeDates);
      setSelectedTradeDate((current) => current || initialTradeDates[0].trade_date);
    }
  }, [initialTradeDates]);

  useEffect(() => {
    let cancelled = false;

    async function loadWatchlist() {
      try {
        const response = await fetch(`${apiBaseUrl}/watchlist`);
        if (!response.ok) {
          throw new Error();
        }

        const payload = (await response.json()) as {
          entries: Array<{ instrument_id: number }>;
        };
        if (!cancelled) {
          setWatchlistInstrumentIds(payload.entries.map((entry) => entry.instrument_id));
        }
      } catch {
        if (!cancelled) {
          setWatchlistInstrumentIds([]);
        }
      }
    }

    void loadWatchlist();

    return () => {
      cancelled = true;
    };
  }, [apiBaseUrl]);

  function validateInputs(): string | null {
    if (Number.isNaN(rpsThreshold) || rpsThreshold < 0 || rpsThreshold > 100) {
      return "RPS 阈值必须在 0 到 100 之间。";
    }

    const parsedHighThreshold = Number(highProximityThresholdPct);
    if (Number.isNaN(parsedHighThreshold) || parsedHighThreshold < 0 || parsedHighThreshold > 100) {
      return "距 52 周高点阈值必须在 0.00 到 100.00 之间。";
    }

    if (!selectedRpsWindows.length) {
      return "至少需要选择一个批准的 RPS 窗口。";
    }

    if (
      Number.isNaN(minRpsLinesRequired) ||
      minRpsLinesRequired < 1 ||
      minRpsLinesRequired > selectedRpsWindows.length
    ) {
      return "最少满足条数必须在 1 到已选 RPS 窗口数量之间。";
    }

    return null;
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const validationError = validateInputs();

    if (validationError) {
      setSaveState("error");
      setMessage(validationError);
      return;
    }

    setSaveState("saving");
    setMessage("正在保存策略配置……");

    try {
      const response = await fetch(`${apiBaseUrl}/screen/configuration`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          rps_threshold: rpsThreshold,
          high_proximity_threshold_pct: highProximityThresholdPct,
          selected_rps_windows: selectedRpsWindows,
          min_rps_lines_required: minRpsLinesRequired,
        }),
      });

      if (!response.ok) {
        const payload = (await response.json()) as { detail?: string };
        throw new Error(payload.detail ?? `请求失败（${response.status}）`);
      }

      const payload = (await response.json()) as StrategyConfigurationResponse;
      setActiveVersion(payload.configuration.version);
      setRpsThreshold(payload.configuration.rps_threshold);
      setHighProximityThresholdPct(payload.configuration.high_proximity_threshold_pct);
      setSelectedRpsWindows(payload.configuration.selected_rps_windows);
      setMinRpsLinesRequired(payload.configuration.min_rps_lines_required);
      setSaveState("saved");
      setMessage("配置已保存，新参数集将在下一次筛选中生效。");
    } catch (error) {
      setSaveState("error");
      setMessage(
        error instanceof Error
          ? error.message
          : "无法保存策略配置。",
      );
    }
  }

  async function handleRunScreen() {
    setRunState("running");
    setRunMessage(
      selectedTradeDate
        ? `正在基于 ${selectedTradeDate} 的派生指标启动筛选……`
        : "正在基于最新派生指标启动筛选……",
    );

    try {
      const response = await fetch(`${apiBaseUrl}/screen/runs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          trade_date: selectedTradeDate || null,
        }),
      });

      if (!response.ok) {
        const payload = (await response.json()) as { detail?: string };
        throw new Error(payload.detail ?? `筛选启动失败（${response.status}）`);
      }

      const payload = (await response.json()) as { screen_run: ScreenRun };
      setLatestRun(payload.screen_run);
      setRunState("ready");
      setRunMessage(
        `筛选 #${payload.screen_run.id} 已完成，交易日 ${payload.screen_run.trade_date}，候选 ${payload.screen_run.total_candidates} 只，入选 ${payload.screen_run.qualified_count} 只。`,
      );
    } catch (error) {
      setRunState("error");
      setRunMessage(
        error instanceof Error ? error.message : "无法启动筛选。",
      );
    }
  }

  function handleToggleRpsWindow(window: number) {
    setSelectedRpsWindows((current) => {
      const next = current.includes(window)
        ? current.filter((value) => value !== window)
        : [...current, window].sort((left, right) => left - right);

      setMinRpsLinesRequired((existing) => Math.min(existing, next.length || 1));
      return next;
    });
  }

  return (
    <section className="screen-panel">
      <div className="screen-panel__header">
        <p className="eyebrow">策略配置</p>
        <h1>运行 MVP 筛选并查看入选列表。</h1>
        <p className="hero-text">
          此工作流同时覆盖参数编辑与首个结果列表页。下方列表与产生它的
          参数集及运行日期严格绑定。
        </p>
      </div>

      <div className="screen-summary-grid">
        <article className="screen-summary-card">
          <p className="status-label">当前版本</p>
          <h2>{hasLoadedConfiguration ? `v${activeVersion}` : "不可用"}</h2>
          <p className="status-copy">已保存的参数集可追溯，便于对照历史运行。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">RPS 规则</p>
          <h2>{hasLoadedConfiguration ? rpsThreshold : "不可用"}</h2>
          <p className="status-copy">至少一条受支持的 RPS 曲线需达到该阈值。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">52 周高点规则</p>
          <h2>{hasLoadedConfiguration ? `${highProximityThresholdPct}%` : "不可用"}</h2>
          <p className="status-copy">相对 52 周高点允许的最大回撤幅度。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">RPS 窗口</p>
          <h2>{hasLoadedConfiguration ? formatRpsWindows(selectedRpsWindows) : "不可用"}</h2>
          <p className="status-copy">只允许从批准窗口集合里选择，便于后续统一扩展。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">最少满足条数</p>
          <h2>{hasLoadedConfiguration ? minRpsLinesRequired : "不可用"}</h2>
          <p className="status-copy">至少有这么多条已选 RPS 线达到阈值才算通过。</p>
        </article>
      </div>

      <form className="strategy-form" onSubmit={handleSubmit}>
        <label className="strategy-field">
          <span>筛选交易日</span>
          <select
            name="trade_date"
            value={selectedTradeDate}
            onChange={(event) => setSelectedTradeDate(event.target.value)}
            disabled={!availableTradeDates.length || runState === "running"}
          >
            {availableTradeDates.map((option) => (
              <option key={option.trade_date} value={option.trade_date}>
                {option.trade_date}
              </option>
            ))}
          </select>
          <small>
            {initialTradeDateError
              ? initialTradeDateError
              : availableTradeDates.length
                ? "只允许选择已存在于派生事实中的交易日。"
                : "暂无可用于筛选的派生事实交易日。"}
          </small>
        </label>

        <label className="strategy-field">
          <span>RPS 阈值</span>
          <input
            name="rps_threshold"
            type="number"
            min={0}
            max={100}
            value={rpsThreshold}
            aria-invalid={saveState === "error" && message.includes("RPS 阈值")}
            onChange={(event) => setRpsThreshold(Number(event.target.value))}
          />
          <small>取值为 0 到 100 的整数。</small>
        </label>

        <label className="strategy-field">
          <span>距 52 周高点阈值（%）</span>
          <input
            name="high_proximity_threshold_pct"
            type="number"
            min={0}
            max={100}
            step="0.01"
            value={highProximityThresholdPct}
            aria-invalid={saveState === "error" && message.includes("52 周高点阈值")}
            onChange={(event) => setHighProximityThresholdPct(event.target.value)}
          />
          <small>相对 52 周高点的距离百分比，0.00 到 100.00。</small>
        </label>

        <fieldset className="strategy-field">
          <span>批准的 RPS 窗口</span>
          <div className="strategy-checkbox-grid">
            {approvedRpsWindows.map((window) => (
              <label key={window} className="strategy-checkbox-option">
                <input
                  type="checkbox"
                  checked={selectedRpsWindows.includes(window)}
                  onChange={() => handleToggleRpsWindow(window)}
                />
                <span>{window} 日</span>
              </label>
            ))}
          </div>
          <small>当前只允许选择批准集合中的窗口，新增窗口由后端统一批准后即可出现。</small>
        </fieldset>

        <label className="strategy-field">
          <span>最少满足条数</span>
          <input
            name="min_rps_lines_required"
            type="number"
            min={1}
            max={Math.max(selectedRpsWindows.length, 1)}
            value={minRpsLinesRequired}
            aria-invalid={saveState === "error" && message.includes("最少满足条数")}
            onChange={(event) => setMinRpsLinesRequired(Number(event.target.value))}
          />
          <small>要求至少几条已选 RPS 线超过阈值，最大值等于已选窗口数。</small>
        </label>

        <div className="strategy-actions">
          <div className="strategy-button-row">
            <button type="submit" className="strategy-button" disabled={saveState === "saving"}>
              {saveState === "saving" ? "保存中……" : "保存参数集"}
            </button>
            <button
              type="button"
              className="strategy-button strategy-button--secondary"
              disabled={
                !hasLoadedConfiguration ||
                runState === "running" ||
                (!availableTradeDates.length && !canRunWithoutTradeDateList)
              }
              onClick={handleRunScreen}
            >
              {runState === "running" ? "筛选中……" : "启动筛选"}
            </button>
          </div>
          <p className={`strategy-message strategy-message--${saveState}`} role={saveState === "error" ? "alert" : "status"}>{message}</p>
          <p className={`strategy-message strategy-message--${runState}`} role={runState === "error" ? "alert" : "status"}>{runMessage}</p>
        </div>
      </form>

      <section className="result-panel">
        <div className="result-panel__header">
          <p className="eyebrow">结果列表</p>
          <h2>入选股票及其直接上下文。</h2>
          <p className="status-copy">
            {latestRun
              ? `筛选 #${latestRun.id} 于 ${formatTimestamp(latestRun.executed_at)} 执行，使用参数集 v${latestRun.parameter_set.version}，窗口 ${formatRpsWindows(latestRun.parameter_set.selected_rps_windows)}，至少满足 ${latestRun.parameter_set.min_rps_lines_required} 条。`
              : "启动一次筛选以填充结果列表。"}
          </p>
        </div>

        {latestRun ? (
          <>
            <div className="run-metadata-grid">
              <article className="run-metadata-card">
                <p className="status-label">运行日期</p>
                <h3>{latestRun.trade_date}</h3>
              </article>
              <article className="run-metadata-card">
                <p className="status-label">入选数</p>
                <h3>{latestRun.qualified_count}</h3>
              </article>
              <article className="run-metadata-card">
                <p className="status-label">候选数</p>
                <h3>{latestRun.total_candidates}</h3>
              </article>
              <article className="run-metadata-card">
                <p className="status-label">RPS 窗口</p>
                <h3>{formatRpsWindows(latestRun.parameter_set.selected_rps_windows)}</h3>
              </article>
              <article className="run-metadata-card">
                <p className="status-label">最少满足条数</p>
                <h3>{latestRun.parameter_set.min_rps_lines_required}</h3>
              </article>
            </div>

            {latestRun.qualified_results.length ? (
              <div className="result-list">
                {latestRun.qualified_results.map((result) => (
                  <article key={`${latestRun.id}-${result.instrument_id}`} className="result-card">
                    <div className="result-card__title">
                      <div>
                        <p className="status-label">{result.exchange}</p>
                        <h3>
                          <Link
                            href={`/stocks/${result.instrument_id}?screen_run_id=${latestRun.id}`}
                            className="result-link"
                          >
                            {result.symbol}
                          </Link>
                        </h3>
                      </div>
                      <div className="result-card__actions">
                        <p className="result-pass-flag">已入选</p>
                        <WatchlistToggleButton
                          apiBaseUrl={apiBaseUrl}
                          instrumentId={result.instrument_id}
                          symbol={result.symbol}
                          className="strategy-button strategy-button--secondary"
                          initialIsInWatchlist={watchlistInstrumentIds.includes(result.instrument_id)}
                          loadOnMount={false}
                          onToggleComplete={(nextValue) =>
                            setWatchlistInstrumentIds((current) =>
                              nextValue
                                ? [...current, result.instrument_id]
                                : current.filter((instrumentId) => instrumentId !== result.instrument_id),
                            )
                          }
                        />
                      </div>
                    </div>

                    <div className="result-summary-grid">
                      <div>
                        <dt>最佳 RPS</dt>
                        <dd>
                          {result.best_rps_value}，阈值 {result.rps_threshold}
                        </dd>
                      </div>
                      <div>
                        <dt>距高点回撤</dt>
                        <dd>
                          {result.max_drawdown_from_high_pct}%，上限 {result.high_proximity_threshold_pct}%
                        </dd>
                      </div>
                    </div>

                    <ul className="signal-list">
                      <li>
                        参数集：RPS {latestRun.parameter_set.rps_threshold} / 距高点{" "}
                        {latestRun.parameter_set.high_proximity_threshold_pct}% / 窗口{" "}
                        {formatRpsWindows(latestRun.parameter_set.selected_rps_windows)} / 至少满足{" "}
                        {latestRun.parameter_set.min_rps_lines_required} 条
                      </li>
                      <li>
                        RPS 条件：{result.rps_condition_passed ? "通过" : "未通过"}
                      </li>
                      <li>
                        距 52 周高点：{result.high_proximity_condition_passed ? "通过" : "未通过"}
                      </li>
                      <li>距高点比率：{result.high_proximity_ratio}</li>
                    </ul>
                  </article>
                ))}
              </div>
            ) : (
              <p className="empty-state">
                本次筛选在当前参数集下无入选股票。
              </p>
            )}
          </>
        ) : (
          <p className="empty-state">暂无已持久化的筛选记录。</p>
        )}
      </section>
    </section>
  );
}
