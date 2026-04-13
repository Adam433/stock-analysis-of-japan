import type { MarketDataHealthResponse } from "@/lib/marketDataHealth";

type WorkflowTrustBannerProps = {
  workflowLabel: string;
  health: MarketDataHealthResponse | null;
  error: string | null;
};

function bannerTone(health: MarketDataHealthResponse | null, error: string | null): string {
  if (error || !health) {
    return "workflow-banner--bad";
  }
  if (
    health.freshness_state === "stale" ||
    health.coverage_status === "partial" ||
    health.coverage_status === "failed"
  ) {
    return "workflow-banner--warn";
  }
  return "workflow-banner--good";
}

export function WorkflowTrustBanner({
  workflowLabel,
  health,
  error,
}: WorkflowTrustBannerProps) {
  const toneClass = bannerTone(health, error);
  const explicitState = error
    ? "连接异常"
    : !health
      ? "不可用"
      : health.coverage_status === "failed"
        ? "刷新失败"
        : health.freshness_state === "stale"
          ? "数据陈旧"
          : health.coverage_status === "partial"
            ? "数据部分缺失"
            : "可用于日常研究";

  return (
    <section className={`workflow-banner ${toneClass}`} aria-live="polite">
      <div>
        <p className="status-label">信任状态</p>
        <h2>{workflowLabel}：{explicitState}</h2>
      </div>
      <div className="workflow-banner__copy">
        {error || !health ? (
          <p className="status-copy">
            {error ?? "数据健康信息暂不可用，此工作流不应按正常成功对待。"}
          </p>
        ) : (
          <>
            <p className="status-copy">
              新鲜度 {health.freshness_state}，覆盖度 {health.coverage_status}，最新交易日{" "}
              {health.latest_trade_date ?? "暂无"}。
            </p>
            <p className="status-copy">
              部分行 {health.partial_rows}，不可用行 {health.unavailable_rows}，最近一次刷新{" "}
              {health.last_refresh?.status ?? "缺失"}。
            </p>
          </>
        )}
      </div>
    </section>
  );
}
