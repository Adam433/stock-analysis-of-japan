import { formatDateOnly, formatRatioAsPercent, formatTimestamp } from "@/lib/formatters";
import type { XSignalAuthor, XSignalDashboard, XSignalMention } from "@/lib/types";

type XSignalTrackerPanelProps = {
  apiBaseUrl: string;
  initialDashboard: XSignalDashboard;
  initialError: string | null;
};

type XSignalGroup = {
  key: string;
  authorHandle: string;
  symbol: string;
  companyName: string | null;
  exchange: string | null;
  mentionKind: string;
  sectorLabel: string | null;
  totalMentionCount: number;
  observations: XSignalMention[];
};

function sentimentLabel(sentiment: string): string {
  if (sentiment === "bullish") {
    return "看多";
  }
  if (sentiment === "bearish") {
    return "看空";
  }
  if (sentiment === "neutral") {
    return "中性";
  }
  return "未知";
}

function mentionKindLabel(group: XSignalGroup): string {
  if (group.mentionKind === "stock") {
    return "个股";
  }
  return group.sectorLabel ? `${group.sectorLabel} 龙头` : "板块代理";
}

function returnClass(value: string | null): string {
  if (!value) {
    return "x-signal-return x-signal-return--neutral";
  }
  const numeric = Number(value);
  if (numeric > 0) {
    return "x-signal-return x-signal-return--positive";
  }
  if (numeric < 0) {
    return "x-signal-return x-signal-return--negative";
  }
  return "x-signal-return x-signal-return--neutral";
}

function sortedAuthors(authors: XSignalAuthor[]): XSignalAuthor[] {
  return [...authors].sort((left, right) => left.handle.localeCompare(right.handle));
}

function groupMentions(mentions: XSignalMention[]): XSignalGroup[] {
  const groups = new Map<string, XSignalGroup>();
  for (const mention of mentions) {
    const key = `${mention.author_id}:${mention.symbol}`;
    const current = groups.get(key);
    if (!current) {
      groups.set(key, {
        key,
        authorHandle: mention.author_handle,
        symbol: mention.symbol,
        companyName: mention.company_name,
        exchange: mention.exchange,
        mentionKind: mention.mention_kind,
        sectorLabel: mention.sector_label,
        totalMentionCount: mention.mention_count,
        observations: [mention],
      });
      continue;
    }
    current.totalMentionCount += mention.mention_count;
    current.observations.push(mention);
    if (mention.mention_kind === "stock") {
      current.mentionKind = "stock";
      current.sectorLabel = null;
    }
    current.companyName = current.companyName ?? mention.company_name;
    current.exchange = current.exchange ?? mention.exchange;
  }

  return [...groups.values()]
    .map((group) => ({
      ...group,
      observations: [...group.observations].sort((left, right) =>
        right.mention_date.localeCompare(left.mention_date),
      ),
    }))
    .sort((left, right) => right.observations[0].mention_date.localeCompare(left.observations[0].mention_date));
}

function formatPrice(dateValue: string | null, close: string | null): string {
  if (!dateValue || !close) {
    return "不可用";
  }
  return `${formatDateOnly(dateValue)} / ${Number(close).toFixed(2)}`;
}

function sentimentSummary(observations: XSignalMention[]): string {
  const counts = observations.reduce<Record<string, number>>((accumulator, mention) => {
    accumulator[mention.sentiment] = (accumulator[mention.sentiment] ?? 0) + 1;
    return accumulator;
  }, {});
  return [
    ["bullish", "看多"],
    ["bearish", "看空"],
    ["neutral", "中性"],
    ["unknown", "未知"],
  ]
    .filter(([key]) => counts[key])
    .map(([key, label]) => `${label} ${counts[key]}`)
    .join(" / ");
}

export function XSignalTrackerPanel({
  apiBaseUrl,
  initialDashboard,
  initialError,
}: XSignalTrackerPanelProps) {
  const dashboard = initialDashboard;
  const authors = sortedAuthors(dashboard.authors);
  const signalGroups = groupMentions(dashboard.mentions);

  return (
    <section className="screen-panel x-signal-panel">
      <div className="screen-panel__header x-signal-header">
        <div>
          <p className="eyebrow">X Signal Tracker</p>
          <h1>X 信号追踪台</h1>
          <p className="hero-text">只读看板：Codex 使用 Chrome 采集 X 发言，后端按股票和日期聚合信号。</p>
        </div>
        <p className="status-copy">API 基址：{apiBaseUrl}</p>
      </div>

      {initialError ? <p className="error-callout">{initialError}</p> : null}

      <div className="screen-summary-grid">
        <article className="screen-summary-card">
          <p className="status-label">X ID</p>
          <h2>{dashboard.authors.length}</h2>
          <p className="status-copy">当前跟踪账号数。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">发言</p>
          <h2>{dashboard.total_posts}</h2>
          <p className="status-copy">已进入本地库的内容条数。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">股票项目</p>
          <h2>{signalGroups.length}</h2>
          <p className="status-copy">同一股票集中展示，同一天只保留一个时间点。</p>
        </article>
      </div>

      <div className="x-signal-author-grid">
        {authors.map((author) => (
          <article key={author.id} className="x-signal-author-card">
            <div>
              <p className="status-label">@{author.handle}</p>
              <h3>{author.display_name ?? author.handle}</h3>
            </div>
            <dl className="detail-list">
              <div>
                <dt>发言</dt>
                <dd>{author.post_count}</dd>
              </div>
              <div>
                <dt>日级信号</dt>
                <dd>{author.mention_count}</dd>
              </div>
              <div>
                <dt>最近分析</dt>
                <dd>{formatTimestamp(author.last_analyzed_at)}</dd>
              </div>
            </dl>
          </article>
        ))}
      </div>

      <div className="result-panel">
        <div className="result-panel__header">
          <p className="detail-label">Signals</p>
          <h2>股票观察项目</h2>
        </div>

        {signalGroups.length ? (
          <div className="x-signal-table">
            {signalGroups.map((group) => {
              const latest = group.observations[0];
              return (
                <article key={group.key} className="x-signal-row x-signal-row--grouped">
                  <div className="x-signal-symbol-block">
                    <p className="status-label">@{group.authorHandle}</p>
                    <h3>{group.symbol}</h3>
                    <p className="status-copy">
                      {group.companyName ?? group.exchange ?? "未匹配本地标的"}
                    </p>
                  </div>
                  <div className="x-signal-badges">
                    <span className="lifecycle-badge lifecycle-badge--portfolio">
                      {mentionKindLabel(group)}
                    </span>
                    <span className="lifecycle-badge lifecycle-badge--legacy">
                      {sentimentSummary(group.observations) || "暂无情绪"}
                    </span>
                  </div>
                  <dl className="detail-list x-signal-price-list">
                    <div>
                      <dt>时间点</dt>
                      <dd>{group.observations.length}</dd>
                    </div>
                    <div>
                      <dt>提及次数</dt>
                      <dd>{group.totalMentionCount}</dd>
                    </div>
                    <div>
                      <dt>最新收盘</dt>
                      <dd>{formatPrice(latest.latest_price_date, latest.latest_close)}</dd>
                    </div>
                    <div>
                      <dt>最近节点涨跌</dt>
                      <dd className={returnClass(latest.cumulative_return)}>
                        {formatRatioAsPercent(latest.cumulative_return)}
                      </dd>
                    </div>
                  </dl>

                  <div className="x-signal-observation-list">
                    {group.observations.map((mention) => (
                      <div key={mention.id} className="x-signal-observation">
                        <div>
                          <p className="status-label">{formatDateOnly(mention.mention_date)}</p>
                          <p className="status-copy">
                            {sentimentLabel(mention.sentiment)} / 当天 {mention.mention_count} 次
                          </p>
                        </div>
                        <dl className="detail-list x-signal-observation-metrics">
                          <div>
                            <dt>当日收盘</dt>
                            <dd>{formatPrice(mention.mention_price_date, mention.mention_close)}</dd>
                          </div>
                          <div>
                            <dt>累计涨跌幅</dt>
                            <dd className={returnClass(mention.cumulative_return)}>
                              {formatRatioAsPercent(mention.cumulative_return)}
                            </dd>
                          </div>
                        </dl>
                        {mention.proxy_reason ? (
                          <p className="error-callout">{mention.proxy_reason}</p>
                        ) : null}
                        {mention.source_text_excerpt ? (
                          <p className="status-copy">{mention.source_text_excerpt}</p>
                        ) : null}
                      </div>
                    ))}
                  </div>
                </article>
              );
            })}
          </div>
        ) : (
          <p className="empty-state">尚未记录任何 X 信号。</p>
        )}
      </div>
    </section>
  );
}
