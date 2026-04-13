"use client";

import Link from "next/link";
import { useState } from "react";

import { WatchlistToggleButton } from "@/components/watchlist/WatchlistToggleButton";

type WatchlistEntry = {
  id: number;
  instrument_id: number;
  symbol: string;
  exchange: string;
  name: string | null;
  note: string | null;
  observation_reason: string | null;
  added_date: string;
  added_at: string;
};

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
        <p className="eyebrow">Watchlist Review</p>
        <h1>Revisit saved candidates with their research context.</h1>
        <p className="hero-text">
          This view turns the watchlist into a daily review surface. Saved symbols, notes,
          observation reasons, and added dates stay visible together instead of being scattered
          across earlier screening sessions.
        </p>
      </div>

      <div className="screen-summary-grid">
        <article className="screen-summary-card">
          <p className="status-label">Entries</p>
          <h2>{entries.length}</h2>
          <p className="status-copy">Persisted watchlist securities currently available for review.</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">Research Context</p>
          <h2>{entries.filter((entry) => entry.note || entry.observation_reason).length}</h2>
          <p className="status-copy">Entries already carrying note or observation-reason context.</p>
        </article>
        <article className="screen-summary-card">
          <p className="status-label">Workflow</p>
          <h2>Review</h2>
          <p className="status-copy">Open details, edit context, or prune the list from one page.</p>
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
                  <h3>{entry.symbol}</h3>
                  <p className="status-copy">{entry.name ?? "Unnamed instrument"}</p>
                </div>
                <div className="result-card__actions">
                  <Link href={`/screen`} className="watchlist-link-button">
                    Back to screen
                  </Link>
                </div>
              </div>

              <dl className="detail-list watchlist-detail-list">
                <div>
                  <dt>Added date</dt>
                  <dd>{entry.added_date}</dd>
                </div>
                <div>
                  <dt>Observation reason</dt>
                  <dd>{entry.observation_reason ?? "Not captured yet"}</dd>
                </div>
                <div>
                  <dt>Research note</dt>
                  <dd>{entry.note ?? "No note saved yet"}</dd>
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
          No watchlist entries are stored yet. Add candidates from the screen result list or stock detail workflow.
        </p>
      )}
    </section>
  );
}
