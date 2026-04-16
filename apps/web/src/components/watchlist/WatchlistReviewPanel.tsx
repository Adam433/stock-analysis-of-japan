"use client";

import Link from "next/link";
import { useState } from "react";

import { WatchlistToggleButton } from "@/components/watchlist/WatchlistToggleButton";
import type { WatchlistEntry } from "@/lib/types";

type WatchlistReviewPanelProps = {
  apiBaseUrl: string;
  initialEntries: WatchlistEntry[];
  initialError: string | null;
};

export function WatchlistReviewPanel({
  apiBaseUrl,
  initialEntries,
  initialError,
}: WatchlistReviewPanelProps) {
  const [entries, setEntries] = useState(initialEntries);

  return (
    <section className="screen-panel">
      <div className="screen-panel__header">
        <p className="eyebrow">观察列表复盘</p>
        <h1>带着研究备注回看已保存的候选。</h1>
        <p className="hero-text">
          此视图把观察列表变成每日复盘界面。已保存的代码、备注、观察原因与
          加入日期集中呈现，而不是散落在先前的筛选会话中。
        </p>
      </div>

      <div className="screen-summary-grid">
        <article className="screen-summary-card">
          <p className="status-label">条目数</p>
          <h2>{entries.length}</h2>
          <p className="status-copy">当前可复盘的持久化观察列表标的数。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">含研究备注</p>
          <h2>{entries.filter((entry) => entry.note || entry.observation_reason).length}</h2>
          <p className="status-copy">已填写备注或观察原因的条目数。</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">工作流</p>
          <h2>复盘</h2>
          <p className="status-copy">可在同一页面查看详情、编辑备注或移除条目。</p>
        </article>
      </div>

      {initialError ? <p className="error-callout">{initialError}</p> : null}

      {entries.length ? (
        <div className="watchlist-review-grid">
          {entries.map((entry) => (
            <article key={entry.id} className="watchlist-review-card">
              <div className="result-card__title">
                <div>
                  <p className="status-label">{entry.exchange}</p>
                  <h3>
                    <Link href={`/stocks/${entry.instrument_id}`} className="result-link">
                      {entry.symbol}
                    </Link>
                  </h3>
                  <p className="status-copy">{entry.name ?? "未命名标的"}</p>
                </div>
                <div className="result-card__actions">
                  <Link href={`/stocks/${entry.instrument_id}`} className="watchlist-link-button">
                    查看详情
                  </Link>
                  <Link href={`/screen`} className="watchlist-link-button">
                    返回筛选
                  </Link>
                </div>
              </div>

              <dl className="detail-list watchlist-detail-list">
                <div>
                  <dt>加入日期</dt>
                  <dd>{entry.added_date}</dd>
                </div>
                <div>
                  <dt>观察原因</dt>
                  <dd>{entry.observation_reason ?? "尚未填写"}</dd>
                </div>
                <div>
                  <dt>研究备注</dt>
                  <dd>{entry.note ?? "尚未保存备注"}</dd>
                </div>
              </dl>

              <WatchlistToggleButton
                apiBaseUrl={apiBaseUrl}
                instrumentId={entry.instrument_id}
                symbol={entry.symbol}
                initialIsInWatchlist
                initialNote={entry.note}
                initialObservationReason={entry.observation_reason}
                initialAddedDate={entry.added_date}
                loadOnMount={false}
                onToggleComplete={(nextValue) => {
                  if (!nextValue) {
                    setEntries((current) =>
                      current.filter((currentEntry) => currentEntry.instrument_id !== entry.instrument_id),
                    );
                  }
                }}
              />
            </article>
          ))}
        </div>
      ) : (
        <p className="empty-state">
          尚未保存任何观察列表条目。可在筛选结果列表或个股详情中添加候选。
        </p>
      )}
    </section>
  );
}
