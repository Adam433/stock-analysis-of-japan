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
          throw new Error(`无法加载观察列表（${response.status}）。`);
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
          setMessage(error instanceof Error ? error.message : "无法加载观察列表状态。");
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
          throw new Error(`无法加载观察列表（${response.status}）。`);
        }

        const payload = (await response.json()) as { entries: WatchlistEntry[] };
        const entry = payload.entries.find((item) => item.instrument_id === instrumentId);
        if (!cancelled && entry) {
          setNote(entry.note ?? "");
          setObservationReason(entry.observation_reason ?? "");
          setAddedDate(entry.added_date);
        }
      } catch {
        // 即便预填失败也保持编辑器可用。
      }
    }

    void loadEntryDetails();

    return () => {
      cancelled = true;
    };
  }, [addedDate, apiBaseUrl, instrumentId, isEditorOpen, isInWatchlist, loadOnMount]);

  async function handleToggle() {
    setToggleState("saving");
    setMessage(isInWatchlist ? `正在将 ${symbol} 从观察列表移除……` : `正在将 ${symbol} 加入观察列表……`);

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
        throw new Error(payload.detail ?? `请求失败（${response.status}）`);
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
          ? `${symbol} 已从观察列表移除。`
          : `${symbol} 已加入观察列表，研究备注一并保留。`,
      );
    } catch (error) {
      setToggleState("error");
      setMessage(error instanceof Error ? error.message : "无法更新观察列表。");
    }
  }

  async function handleSaveContext(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setToggleState("saving");
    setMessage(`正在保存 ${symbol} 的观察列表备注……`);

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
        throw new Error(payload.detail ?? `请求失败（${response.status}）`);
      }

      const payload = (await response.json()) as { entry: WatchlistEntry };
      setAddedDate(payload.entry.added_date);
      setNote(payload.entry.note ?? "");
      setObservationReason(payload.entry.observation_reason ?? "");
      setToggleState("idle");
      setMessage(`${symbol} 的观察备注与观察原因已保存。`);
    } catch (error) {
      setToggleState("error");
      setMessage(error instanceof Error ? error.message : "无法保存观察列表备注。");
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
            ? "更新中……"
            : isInWatchlist
              ? "从观察列表移除"
              : "加入观察列表"}
        </button>
        <button
          type="button"
          className="watchlist-link-button"
          onClick={() => setIsEditorOpen((current) => !current)}
        >
          {isEditorOpen ? "隐藏研究备注" : "编辑研究备注"}
        </button>
      </div>

      {isEditorOpen ? (
        <form className="watchlist-editor" onSubmit={handleSaveContext}>
          <label className="strategy-field">
            <span>观察原因</span>
            <input
              name="observation_reason"
              type="text"
              value={observationReason}
              onChange={(event) => setObservationReason(event.target.value)}
              placeholder="为何关注这只股票"
            />
          </label>
          <label className="strategy-field">
            <span>研究备注</span>
            <textarea
              name="note"
              value={note}
              onChange={(event) => setNote(event.target.value)}
              placeholder="记录交易结构、风险点或下一步检查事项。"
              rows={4}
            />
          </label>
          {addedDate ? <p className="status-copy">加入观察列表日期：{addedDate}。</p> : null}
          <button
            type="submit"
            className="strategy-button strategy-button--secondary"
            disabled={!isInWatchlist || toggleState === "saving"}
          >
            保存观察备注
          </button>
        </form>
      ) : null}

      {message ? (
        <p
          className={`strategy-message strategy-message--${toggleState === "error" ? "error" : "ready"}`}
          role={toggleState === "error" ? "alert" : "status"}
        >
          {message}
        </p>
      ) : null}
    </div>
  );
}
