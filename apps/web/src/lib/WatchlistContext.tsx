"use client";

import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from "react";

type WatchlistContextValue = {
  instrumentIds: number[];
  isLoaded: boolean;
  add: (instrumentId: number) => void;
  remove: (instrumentId: number) => void;
  has: (instrumentId: number) => boolean;
};

const WatchlistContext = createContext<WatchlistContextValue>({
  instrumentIds: [],
  isLoaded: false,
  add: () => {},
  remove: () => {},
  has: () => false,
});

export function useWatchlist() {
  return useContext(WatchlistContext);
}

export function WatchlistProvider({
  apiBaseUrl,
  children,
}: {
  apiBaseUrl: string;
  children: ReactNode;
}) {
  const [instrumentIds, setInstrumentIds] = useState<number[]>([]);
  const [isLoaded, setIsLoaded] = useState(false);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const response = await fetch(`${apiBaseUrl}/watchlist`);
        if (!response.ok) return;
        const payload = (await response.json()) as {
          entries: Array<{ instrument_id: number }>;
        };
        if (!cancelled) {
          setInstrumentIds(payload.entries.map((e) => e.instrument_id));
          setIsLoaded(true);
        }
      } catch {
        if (!cancelled) setIsLoaded(true);
      }
    }

    void load();
    return () => { cancelled = true; };
  }, [apiBaseUrl]);

  const add = useCallback((id: number) => {
    setInstrumentIds((current) =>
      current.includes(id) ? current : [...current, id],
    );
  }, []);

  const remove = useCallback((id: number) => {
    setInstrumentIds((current) => current.filter((v) => v !== id));
  }, []);

  const has = useCallback(
    (id: number) => instrumentIds.includes(id),
    [instrumentIds],
  );

  return (
    <WatchlistContext.Provider value={{ instrumentIds, isLoaded, add, remove, has }}>
      {children}
    </WatchlistContext.Provider>
  );
}
