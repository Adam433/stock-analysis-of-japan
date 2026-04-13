"use client";

import { FormEvent, useEffect, useState } from "react";

type WatchlistEntry = {
  instrument_id: number;
  note: string | null;
  observation_reason: string | null;
  added_date: string;
};

type WatchlistToggleButtonProps = {
  apiBaseUrl: string;
  instrumentId: number;
  symbol: string;
  className?: string;
  initialIsInWatchlist?: boolean;
  initialNote?: string | null;
  initialObservationReason?: string | null;
  initialAddedDate?: string | null;
  loadOnMount?: boolean;
  onToggleComplete?: (nextValue: boolean) => void;
};

type ToggleState = "idle" | "saving" | "error";

export function WatchlistToggleButton({
  apiBaseUrl,
  instrumentId,
  symbol,
  className,
  initialIsInWatchlist = false,
  initialNote = null,
  initialObservationReason = null,
  initialAddedDate = null,
  loadOnMount = true,
  onToggleComplete,
}: WatchlistToggleButtonProps) {
  const [isInWatchlist, setIsInWatchlist] = useState(initialIsInWatchlist);
  const [toggleState, setToggleState] = useState<ToggleState>("idle");
  const [message, setMessage] = useState<string | null>(null);
  const [note, setNote] = useState(initialNote ?? "");
  const [observationReason, setObservationReason] = useState(initialObservationReason ?? "");
  const [addedDate, setAddedDate] = useState<string | null>(initialAddedDate);
  const [isEditorOpen, setIsEditorOpen] = useState(false);

  useEffect(() => {
    setIsInWatchlist(initialIsInWatchlist);
  }, [initialIsInWatchlist]);

  useEffect(() => {
    setNote(initialNote ?? "");
  }, [initialNote]);

  useEffect(() => {
    setObservationReason(initialObservationReason ?? "");
  }, [initialObservationReason]);

  useEffect(() => {
    setAddedDate(initialAddedDate);
  }, [initialAddedDate]);

  useEffect(() => {
    if (!loadOnMount) {
      return;
    }

    let cancelled = false;

    async function loadWatchlistState() {
      try {
        const response = await fetch(`${apiBaseUrl}/watchlist`, { cache: "no-store" });
        if (!response.ok) {
          throw new Error(`Unable to load watchlist (${response.status}).`);
        }

        const payload = (await response.json()) as { entries: WatchlistEntry[] };
        const entry = payload.entries.find((item) => item.instrument_id === instrumentId);
        if (!cancelled) {
          setIsInWatchlist(Boolean(entry));
          setNote(entry?.note ?? "");
          setObservationReason(entry?.observation_reason ?? "");
          setAddedDate(entry?.added_date ?? null);
          setMessage(null);
        }
      } catch (error) {
        if (!cancelled) {
          setToggleState("error");
          setMessage(error instanceof Error ? error.message : "Unable to load watchlist state.");
        }
      }
    }

    void loadWatchlistState();

    return () => {
      cancelled = true;
    };
  }, [apiBaseUrl, instrumentId, loadOnMount]);

  useEffect(() => {
    if (!isEditorOpen || !isInWatchlist || addedDate !== null || loadOnMount) {
      return;
    }

    let cancelled = false;

    async function loadEntryDetails() {
      try {
        const response = await fetch(`${apiBaseUrl}/watchlist`, { cache: "no-store" });
        if (!response.ok) {
          throw new Error(`Unable to load watchlist (${response.status}).`);
        }

        const payload = (await response.json()) as { entries: WatchlistEntry[] };
        const entry = payload.entries.find((item) => item.instrument_id === instrumentId);
        if (!cancelled && entry) {
          setNote(entry.note ?? "");
          setObservationReason(entry.observation_reason ?? "");
          setAddedDate(entry.added_date);
        }
      } catch {
        // Keep the editor usable even if prefill fails.
      }
    }

    void loadEntryDetails();

    return () => {
      cancelled = true;
    };
  }, [addedDate, apiBaseUrl, instrumentId, isEditorOpen, isInWatchlist, loadOnMount]);

  async function handleToggle() {
    setToggleState("saving");
    setMessage(isInWatchlist ? `Removing ${symbol} from the watchlist...` : `Adding ${symbol} to the watchlist...`);

    try {
      const response = await fetch(
        isInWatchlist ? `${apiBaseUrl}/watchlist/${instrumentId}` : `${apiBaseUrl}/watchlist`,
        isInWatchlist
          ? { method: "DELETE" }
          : {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                instrument_id: instrumentId,
                note: note.trim() || null,
                observation_reason: observationReason.trim() || null,
              }),
            },
      );

      if (!response.ok) {
        const payload = (await response.json()) as { detail?: string };
        throw new Error(payload.detail ?? `Request failed with ${response.status}`);
      }

      const nextValue = !isInWatchlist;
      setIsInWatchlist(nextValue);
      setToggleState("idle");
      onToggleComplete?.(nextValue);

      if (nextValue) {
        const payload = (await response.json()) as { entry: WatchlistEntry };
        setAddedDate(payload.entry.added_date);
        setNote(payload.entry.note ?? "");
        setObservationReason(payload.entry.observation_reason ?? "");
      } else {
        setAddedDate(null);
      }

      setMessage(
        isInWatchlist
          ? `${symbol} removed from the watchlist.`
          : `${symbol} added to the watchlist with its saved research context.`,
      );
    } catch (error) {
      setToggleState("error");
      setMessage(error instanceof Error ? error.message : "Unable to update watchlist.");
    }
  }

  async function handleSaveContext(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setToggleState("saving");
    setMessage(`Saving watchlist context for ${symbol}...`);

    try {
      const response = await fetch(`${apiBaseUrl}/watchlist/${instrumentId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          note: note.trim() || null,
          observation_reason: observationReason.trim() || null,
        }),
      });
      if (!response.ok) {
        const payload = (await response.json()) as { detail?: string };
        throw new Error(payload.detail ?? `Request failed with ${response.status}`);
      }

      const payload = (await response.json()) as { entry: WatchlistEntry };
      setAddedDate(payload.entry.added_date);
      setNote(payload.entry.note ?? "");
      setObservationReason(payload.entry.observation_reason ?? "");
      setToggleState("idle");
      setMessage(`${symbol} watchlist note and observation reason saved.`);
    } catch (error) {
      setToggleState("error");
      setMessage(error instanceof Error ? error.message : "Unable to save watchlist context.");
    }
  }

  return (
    <div className="watchlist-toggle">
      <div className="watchlist-toggle__actions">
        <button
          type="button"
          className={className ?? "strategy-button"}
          disabled={toggleState === "saving"}
          onClick={handleToggle}
        >
          {toggleState === "saving"
            ? "Updating..."
            : isInWatchlist
              ? "Remove From Watchlist"
              : "Add To Watchlist"}
        </button>
        <button
          type="button"
          className="watchlist-link-button"
          onClick={() => setIsEditorOpen((current) => !current)}
        >
          {isEditorOpen ? "Hide Research Context" : "Edit Research Context"}
        </button>
      </div>

      {isEditorOpen ? (
        <form className="watchlist-editor" onSubmit={handleSaveContext}>
          <label className="strategy-field">
            <span>Observation reason</span>
            <input
              name="observation_reason"
              type="text"
              value={observationReason}
              onChange={(event) => setObservationReason(event.target.value)}
              placeholder="Why this stock matters"
            />
          </label>
          <label className="strategy-field">
            <span>Research note</span>
            <textarea
              name="note"
              value={note}
              onChange={(event) => setNote(event.target.value)}
              placeholder="Capture the setup, risk, or next check."
              rows={4}
            />
          </label>
          {addedDate ? <p className="status-copy">Added to watchlist on {addedDate}.</p> : null}
          <button
            type="submit"
            className="strategy-button strategy-button--secondary"
            disabled={!isInWatchlist || toggleState === "saving"}
          >
            Save Watchlist Context
          </button>
        </form>
      ) : null}

      {message ? (
        <p className={`strategy-message strategy-message--${toggleState === "error" ? "error" : "ready"}`}>
          {message}
        </p>
      ) : null}
    </div>
  );
}
