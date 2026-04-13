"use client";

import { FormEvent, useEffect, useState } from "react";
import Link from "next/link";

import { WatchlistToggleButton } from "@/components/watchlist/WatchlistToggleButton";

type StrategyConfiguration = {
  id: number;
  version: number;
  rps_threshold: number;
  high_proximity_threshold_pct: string;
};

type StrategyConfigurationResponse = {
  configuration: StrategyConfiguration;
  validation?: {
    rps_threshold: { min: number; max: number; default: number };
    high_proximity_threshold_pct: { min: string; max: string; default: string };
  };
};

type ScreenRunResult = {
  instrument_id: number;
  symbol: string;
  exchange: string;
  trade_date: string;
  best_rps_value: string | null;
  rps_threshold: number;
  high_proximity_ratio: string | null;
  high_proximity_threshold_pct: string;
  max_drawdown_from_high_pct: string | null;
  rps_condition_passed: boolean;
  high_proximity_condition_passed: boolean;
};

type ScreenRun = {
  id: number;
  strategy_configuration_id: number;
  trade_date: string;
  executed_at: string;
  total_candidates: number;
  qualified_count: number;
  status: string;
  parameter_set: {
    id: number;
    version: number;
    rps_threshold: number;
    high_proximity_threshold_pct: string;
  };
  qualified_results: ScreenRunResult[];
};

type StrategyConfigPanelProps = {
  apiBaseUrl: string;
  initialData: StrategyConfigurationResponse | null;
  initialError: string | null;
  initialRun: ScreenRun | null;
  initialRunError: string | null;
};

type SaveState = "idle" | "saving" | "saved" | "error";
type RunState = "idle" | "running" | "ready" | "error";

function formatTimestamp(value: string): string {
  return new Intl.DateTimeFormat("zh-CN", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

export function StrategyConfigPanel({
  apiBaseUrl,
  initialData,
  initialError,
  initialRun,
  initialRunError,
}: StrategyConfigPanelProps) {
  const [watchlistInstrumentIds, setWatchlistInstrumentIds] = useState<number[]>([]);
  const [rpsThreshold, setRpsThreshold] = useState(initialData?.configuration.rps_threshold ?? 90);
  const [highProximityThresholdPct, setHighProximityThresholdPct] = useState(
    initialData?.configuration.high_proximity_threshold_pct ?? "5.00",
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
  const hasLoadedConfiguration = Boolean(initialData);

  useEffect(() => {
    if (initialData) {
      setRpsThreshold(initialData.configuration.rps_threshold);
      setHighProximityThresholdPct(initialData.configuration.high_proximity_threshold_pct);
      setActiveVersion(initialData.configuration.version);
    }
  }, [initialData]);

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
    setRunMessage("正在基于最新派生指标启动筛选……");

    try {
      const response = await fetch(`${apiBaseUrl}/screen/runs`, {
        method: "POST",
      });

      if (!response.ok) {
        const payload = (await response.json()) as { detail?: string };
        throw new Error(payload.detail ?? `筛选启动失败（${response.status}）`);
      }

      const payload = (await response.json()) as { screen_run: ScreenRun };
      setLatestRun(payload.screen_run);
      setRunState("ready");
      setRunMessage(
        `筛选 #${payload.screen_run.id} 已完成，候选 ${payload.screen_run.total_candidates} 只，入选 ${payload.screen_run.qualified_count} 只。`,
      );
    } catch (error) {
      setRunState("error");
      setRunMessage(
        error instanceof Error ? error.message : "无法启动筛选。",
      );
    }
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
      </div>

      <form className="strategy-form" onSubmit={handleSubmit}>
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

        <div className="strategy-actions">
          <div className="strategy-button-row">
            <button type="submit" className="strategy-button" disabled={saveState === "saving"}>
              {saveState === "saving" ? "保存中……" : "保存参数集"}
            </button>
            <button
              type="button"
              className="strategy-button strategy-button--secondary"
              disabled={!hasLoadedConfiguration || runState === "running"}
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
              ? `筛选 #${latestRun.id} 于 ${formatTimestamp(latestRun.executed_at)} 执行，使用参数集 v${latestRun.parameter_set.version}。`
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
