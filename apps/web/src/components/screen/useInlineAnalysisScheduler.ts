"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { apiPaths } from "@/lib/apiPaths";
import { fetchWithRetry } from "@/lib/fetchWithRetry";
import {
  INCREMENTAL_LOAD_THRESHOLD,
  MAX_CONCURRENT_INLINE_LOADS,
  SCHEDULER_ROOT_MARGIN,
} from "@/lib/inlineAnalysisScheduler";
import type { InlineAnalysisPayload } from "@/lib/types";

type InstrumentTarget = {
  instrumentId: number;
  screenRunId: number;
};

type SchedulerOptions = {
  threshold?: number;
  rootMargin?: string;
  maxConcurrent?: number;
};

type InlineAnalysisLoadState =
  | { kind: "idle" }
  | { kind: "loading" }
  | { kind: "loaded"; data: InlineAnalysisPayload }
  | { kind: "failed"; error: string };

const IDLE_STATE: InlineAnalysisLoadState = { kind: "idle" };
const INLINE_ANALYSIS_FETCH_RETRIES = 2;
const INLINE_ANALYSIS_FETCH_RETRY_DELAY_MS = 100;

function buildTargetKey(instrumentId: number, screenRunId: number): string {
  return `${instrumentId}:${screenRunId}`;
}

function normalizeError(message: string | null, status: number | null): string {
  if (message === "Failed to fetch" || message === "Load failed") {
    return "内联分析接口不可达，请检查后端服务与 API 地址。";
  }
  if (message) {
    return message;
  }
  if (status !== null) {
    return `内联分析请求失败（${status}）。`;
  }
  return "内联分析请求失败，请稍后重试。";
}

export function useInlineAnalysisScheduler(
  apiBaseUrl: string,
  instruments: InstrumentTarget[],
  options: SchedulerOptions = {},
) {
  const threshold = options.threshold ?? INCREMENTAL_LOAD_THRESHOLD;
  const rootMargin = options.rootMargin ?? SCHEDULER_ROOT_MARGIN;
  const maxConcurrent = options.maxConcurrent ?? MAX_CONCURRENT_INLINE_LOADS;

  const [states, setStates] = useState<Record<number, InlineAnalysisLoadState>>({});
  const statesRef = useRef<Record<number, InlineAnalysisLoadState>>({});
  const queueRef = useRef<number[]>([]);
  const activeRef = useRef<Set<string>>(new Set());
  const currentTargetKeysRef = useRef<Record<number, string>>({});
  const nodesRef = useRef<Map<number, Element>>(new Map());
  const observerRef = useRef<IntersectionObserver | null>(null);
  const observedIdsRef = useRef<Set<number>>(new Set());
  const drainQueueRef = useRef<() => void>(() => {});

  const isIncremental = instruments.length >= threshold;
  const instrumentSignature = instruments
    .map((instrument) => `${instrument.instrumentId}:${instrument.screenRunId}`)
    .join("|");
  const knownIds = useMemo(
    () => instruments.map((instrument) => instrument.instrumentId),
    [instruments],
  );

  const updateState = useCallback((instrumentId: number, nextState: InlineAnalysisLoadState) => {
    statesRef.current = {
      ...statesRef.current,
      [instrumentId]: nextState,
    };
    setStates(statesRef.current);
  }, []);

  const loadInstrument = useCallback(async (instrumentId: number) => {
    const target = instruments.find((item) => item.instrumentId === instrumentId);
    if (!target) {
      return;
    }

    const targetKey = buildTargetKey(target.instrumentId, target.screenRunId);
    if (currentTargetKeysRef.current[instrumentId] !== targetKey) {
      return;
    }

    activeRef.current.add(targetKey);
    updateState(instrumentId, { kind: "loading" });

    try {
      const response = await fetchWithRetry(
        apiPaths(apiBaseUrl).stockInlineAnalysis(instrumentId, target.screenRunId),
        { cache: "no-store" },
        {
          retries: INLINE_ANALYSIS_FETCH_RETRIES,
          delay: INLINE_ANALYSIS_FETCH_RETRY_DELAY_MS,
        },
      );
      if (currentTargetKeysRef.current[instrumentId] !== targetKey) {
        return;
      }
      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
        updateState(instrumentId, {
          kind: "failed",
          error: normalizeError(payload?.detail ?? null, response.status),
        });
      } else {
        const payload = (await response.json()) as { inline_analysis: InlineAnalysisPayload };
        updateState(instrumentId, { kind: "loaded", data: payload.inline_analysis });
      }
    } catch (error) {
      if (currentTargetKeysRef.current[instrumentId] !== targetKey) {
        return;
      }
      updateState(instrumentId, {
        kind: "failed",
        error: normalizeError(error instanceof Error ? error.message : null, null),
      });
    } finally {
      activeRef.current.delete(targetKey);
      drainQueueRef.current();
    }
  }, [apiBaseUrl, instruments, updateState]);

  const drainQueue = useCallback(() => {
    while (activeRef.current.size < maxConcurrent && queueRef.current.length > 0) {
      const [nextInstrumentId, ...rest] = queueRef.current;
      queueRef.current = rest;
      void loadInstrument(nextInstrumentId);
    }
  }, [loadInstrument, maxConcurrent]);

  useEffect(() => {
    drainQueueRef.current = drainQueue;
  }, [drainQueue]);

  const enqueue = useCallback((instrumentId: number) => {
    const currentState = statesRef.current[instrumentId];
    if (currentState?.kind === "loading" || currentState?.kind === "loaded" || currentState?.kind === "failed") {
      return;
    }
    const targetKey = currentTargetKeysRef.current[instrumentId];
    if (!targetKey) {
      return;
    }
    if (activeRef.current.has(targetKey) || queueRef.current.includes(instrumentId)) {
      return;
    }
    queueRef.current = [...queueRef.current, instrumentId];
    drainQueue();
  }, [drainQueue]);

  const resetStates = useCallback((nextIds: number[]) => {
    const nextEntries = nextIds.map((instrumentId) => {
      const currentState = statesRef.current[instrumentId];
      if (currentState?.kind === "failed") {
        return [instrumentId, currentState] as const;
      }
      return [instrumentId, IDLE_STATE] as const;
    });

    statesRef.current = Object.fromEntries(nextEntries);
    setStates(statesRef.current);
    queueRef.current = [];
    activeRef.current.clear();
  }, []);

  useEffect(() => {
    currentTargetKeysRef.current = Object.fromEntries(
      instruments.map((instrument) => [
        instrument.instrumentId,
        buildTargetKey(instrument.instrumentId, instrument.screenRunId),
      ]),
    );
    resetStates(knownIds);

    if (!isIncremental) {
      knownIds.forEach((instrumentId) => {
        if (statesRef.current[instrumentId]?.kind !== "failed") {
          void loadInstrument(instrumentId);
        }
      });
      return;
    }

    if (observerRef.current) {
      observerRef.current.disconnect();
      observerRef.current = null;
    }

    const observedIds = observedIdsRef.current;
    observedIds.clear();
    observerRef.current =
      typeof IntersectionObserver === "undefined"
        ? null
        : new IntersectionObserver(
            (entries) => {
              entries.forEach((entry) => {
                if (!entry.isIntersecting) {
                  return;
                }
                const instrumentId = Number((entry.target as HTMLElement).dataset.instrumentId);
                if (!Number.isFinite(instrumentId)) {
                  return;
                }
                if (statesRef.current[instrumentId]?.kind === "failed") {
                  return;
                }
                enqueue(instrumentId);
              });
            },
            { rootMargin },
          );

    nodesRef.current.forEach((node, instrumentId) => {
      if (knownIds.includes(instrumentId) && observerRef.current) {
        observerRef.current.observe(node);
        observedIds.add(instrumentId);
      }
    });

    return () => {
      observerRef.current?.disconnect();
      observerRef.current = null;
      observedIds.clear();
    };
  }, [enqueue, instrumentSignature, instruments, isIncremental, knownIds, loadInstrument, resetStates, rootMargin]);

  useEffect(() => {
    const nextNodes = new Map<number, Element>();
    nodesRef.current.forEach((node, instrumentId) => {
      if (knownIds.includes(instrumentId)) {
        nextNodes.set(instrumentId, node);
      }
    });
    nodesRef.current = nextNodes;
  }, [instrumentSignature, knownIds]);

  function registerCardRef(instrumentId: number) {
    return (node: HTMLElement | null) => {
      const existingNode = nodesRef.current.get(instrumentId);
      if (existingNode && observerRef.current && observedIdsRef.current.has(instrumentId)) {
        observerRef.current.unobserve(existingNode);
        observedIdsRef.current.delete(instrumentId);
      }

      if (node === null) {
        nodesRef.current.delete(instrumentId);
        return;
      }

      node.dataset.instrumentId = String(instrumentId);
      nodesRef.current.set(instrumentId, node);
      if (isIncremental && observerRef.current) {
        observerRef.current.observe(node);
        observedIdsRef.current.add(instrumentId);
      }
    };
  }

  function retry(instrumentId: number) {
    queueRef.current = queueRef.current.filter((queuedId) => queuedId !== instrumentId);
    updateState(instrumentId, IDLE_STATE);
    if (isIncremental) {
      enqueue(instrumentId);
      return;
    }
    void loadInstrument(instrumentId);
  }

  return {
    states,
    registerCardRef,
    retry,
  };
}
