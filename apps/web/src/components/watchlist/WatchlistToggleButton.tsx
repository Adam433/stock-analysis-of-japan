"use client";

import { useEffect, useState } from "react";

type WatchlistToggleButtonProps = {
  apiBaseUrl: string;
  instrumentId: number;
  symbol: string;
  className?: string;
  initialIsInWatchlist?: boolean;
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
  loadOnMount = true,
  onToggleComplete,
}: WatchlistToggleButtonProps) {
  const [isInWatchlist, setIsInWatchlist] = useState(initialIsInWatchlist);
  const [toggleState, setToggleState] = useState<ToggleState>("idle");
  const [message, setMessage] = useState<string | null>(null);

  useEffect(() => {
    setIsInWatchlist(initialIsInWatchlist);
  }, [initialIsInWatchlist]);

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

        const payload = (await response.json()) as {
          entries: Array<{ instrument_id: number }>;
        };
        if (!cancelled) {
          setIsInWatchlist(payload.entries.some((entry) => entry.instrument_id === instrumentId));
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
              body: JSON.stringify({ instrument_id: instrumentId }),
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
      setMessage(
        isInWatchlist
          ? `${symbol} removed from the watchlist.`
          : `${symbol} added to the watchlist.`,
      );
    } catch (error) {
      setToggleState("error");
      setMessage(error instanceof Error ? error.message : "Unable to update watchlist.");
    }
  }

  return (
    <div className="watchlist-toggle">
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
      {message ? (
        <p className={`strategy-message strategy-message--${toggleState === "error" ? "error" : "ready"}`}>
          {message}
        </p>
      ) : null}
    </div>
  );
}
