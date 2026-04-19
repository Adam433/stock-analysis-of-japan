import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { useInlineAnalysisScheduler } from "@/components/screen/useInlineAnalysisScheduler";
import type { InlineAnalysisPayload } from "@/lib/types";

type HarnessProps = {
  instruments: Array<{ instrumentId: number; screenRunId: number }>;
  threshold?: number;
  maxConcurrent?: number;
};

type DeferredResponse = {
  promise: Promise<Response>;
  resolve: (response: Response) => void;
};

class MockIntersectionObserver {
  static instances: MockIntersectionObserver[] = [];

  callback: IntersectionObserverCallback;
  observed = new Set<Element>();

  constructor(callback: IntersectionObserverCallback) {
    this.callback = callback;
    MockIntersectionObserver.instances.push(this);
  }

  observe = (element: Element) => {
    this.observed.add(element);
  };

  unobserve = (element: Element) => {
    this.observed.delete(element);
  };

  disconnect = () => {
    this.observed.clear();
  };

  trigger(elements: Element[]) {
    this.callback(
      elements.map(
        (element) =>
          ({
            isIntersecting: true,
            target: element,
          }) as IntersectionObserverEntry,
      ),
      this as unknown as IntersectionObserver,
    );
  }
}

function createDeferredResponse(): DeferredResponse {
  let resolve!: (response: Response) => void;
  const promise = new Promise<Response>((nextResolve) => {
    resolve = nextResolve;
  });
  return { promise, resolve };
}

function buildInlineAnalysisPayload(instrumentId: number): InlineAnalysisPayload {
  return {
    instrument: {
      id: instrumentId,
      symbol: String(instrumentId),
      exchange: "TSE",
      name: `Name ${instrumentId}`,
      currency: "JPY",
    },
    screen_run_ref: {
      id: 8,
      trade_date: "2026-04-15",
    },
    candlesticks: [],
    candlestick_window_days_available: 0,
    valuation_by_fiscal_year: [],
    generated_at: "2026-04-17T12:00:00Z",
  };
}

function SchedulerHarness({ instruments, threshold = 20, maxConcurrent = 4 }: HarnessProps) {
  const { states, registerCardRef, retry } = useInlineAnalysisScheduler("http://localhost:8000", instruments, {
    threshold,
    rootMargin: "0px",
    maxConcurrent,
  });

  return (
    <div>
      {instruments.map((instrument) => (
        <div key={instrument.instrumentId}>
          <div ref={registerCardRef(instrument.instrumentId)} data-testid={`card-${instrument.instrumentId}`} />
          <span data-testid={`state-${instrument.instrumentId}`}>{states[instrument.instrumentId]?.kind ?? "idle"}</span>
          <button type="button" onClick={() => retry(instrument.instrumentId)}>
            retry-{instrument.instrumentId}
          </button>
        </div>
      ))}
    </div>
  );
}

describe("useInlineAnalysisScheduler", () => {
  beforeEach(() => {
    MockIntersectionObserver.instances = [];
    vi.stubGlobal("IntersectionObserver", MockIntersectionObserver);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("loads all cards immediately for small lists", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const instrumentId = Number(String(input).match(/stocks\/(\d+)/)?.[1] ?? "0");
      return Promise.resolve(
        new Response(JSON.stringify({ inline_analysis: buildInlineAnalysisPayload(instrumentId) }), { status: 200 }),
      );
    });

    render(
      <SchedulerHarness
        instruments={[
          { instrumentId: 1, screenRunId: 8 },
          { instrumentId: 2, screenRunId: 8 },
          { instrumentId: 3, screenRunId: 8 },
          { instrumentId: 4, screenRunId: 8 },
          { instrumentId: 5, screenRunId: 8 },
        ]}
      />,
    );

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledTimes(5);
    });
    expect(screen.getByTestId("state-1")).toHaveTextContent("loaded");
  });

  it("only loads intersecting cards and respects the concurrency limit for large lists", async () => {
    const deferredByInstrument = new Map<number, DeferredResponse>();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const instrumentId = Number(String(input).match(/stocks\/(\d+)/)?.[1] ?? "0");
      const deferred = createDeferredResponse();
      deferredByInstrument.set(instrumentId, deferred);
      return deferred.promise;
    });

    render(
      <SchedulerHarness
        instruments={[
          { instrumentId: 1, screenRunId: 8 },
          { instrumentId: 2, screenRunId: 8 },
          { instrumentId: 3, screenRunId: 8 },
          { instrumentId: 4, screenRunId: 8 },
          { instrumentId: 5, screenRunId: 8 },
          { instrumentId: 6, screenRunId: 8 },
        ]}
        threshold={2}
        maxConcurrent={2}
      />,
    );

    const observer = MockIntersectionObserver.instances[0];
    observer.trigger([
      screen.getByTestId("card-1"),
      screen.getByTestId("card-2"),
      screen.getByTestId("card-3"),
      screen.getByTestId("card-4"),
    ]);

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledTimes(2);
    });

    deferredByInstrument
      .get(1)
      ?.resolve(new Response(JSON.stringify({ inline_analysis: buildInlineAnalysisPayload(1) }), { status: 200 }));
    deferredByInstrument
      .get(2)
      ?.resolve(new Response(JSON.stringify({ inline_analysis: buildInlineAnalysisPayload(2) }), { status: 200 }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledTimes(4);
    });

    deferredByInstrument
      .get(3)
      ?.resolve(new Response(JSON.stringify({ inline_analysis: buildInlineAnalysisPayload(3) }), { status: 200 }));
    deferredByInstrument
      .get(4)
      ?.resolve(new Response(JSON.stringify({ inline_analysis: buildInlineAnalysisPayload(4) }), { status: 200 }));

    await waitFor(() => {
      expect(screen.getByTestId("state-4")).toHaveTextContent("loaded");
    });
  });

  it("does not auto-retry failed cards but allows manual retry", async () => {
    const user = userEvent.setup();
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockRejectedValueOnce(new Error("network down"))
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ inline_analysis: buildInlineAnalysisPayload(1) }), { status: 200 }),
      );

    render(
      <SchedulerHarness
        instruments={[{ instrumentId: 1, screenRunId: 8 }]}
        threshold={1}
      />,
    );

    const observer = MockIntersectionObserver.instances[0];
    observer.trigger([screen.getByTestId("card-1")]);

    await waitFor(() => {
      expect(screen.getByTestId("state-1")).toHaveTextContent("failed");
    });

    observer.trigger([screen.getByTestId("card-1")]);
    expect(fetchMock).toHaveBeenCalledTimes(1);

    await user.click(screen.getByRole("button", { name: "retry-1" }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledTimes(2);
      expect(screen.getByTestId("state-1")).toHaveTextContent("loaded");
    });
  });

  it("ignores stale in-flight responses after the card is rebound to a new screen run", async () => {
    const deferredByUrl = new Map<string, DeferredResponse>();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      const deferred = createDeferredResponse();
      deferredByUrl.set(url, deferred);
      return deferred.promise;
    });

    const { rerender } = render(
      <SchedulerHarness
        instruments={[{ instrumentId: 1, screenRunId: 8 }]}
      />,
    );

    const oldUrl = "http://localhost:8000/stocks/1/inline-analysis?screen_run_id=8";
    const newUrl = "http://localhost:8000/stocks/1/inline-analysis?screen_run_id=9";

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(oldUrl);
    });

    rerender(
      <SchedulerHarness
        instruments={[{ instrumentId: 1, screenRunId: 9 }]}
      />,
    );

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(newUrl);
      expect(fetchMock).toHaveBeenCalledTimes(2);
    });

    deferredByUrl
      .get(oldUrl)
      ?.resolve(new Response(JSON.stringify({ inline_analysis: buildInlineAnalysisPayload(1) }), { status: 200 }));

    await waitFor(() => {
      expect(screen.getByTestId("state-1")).toHaveTextContent("loading");
    });

    deferredByUrl
      .get(newUrl)
      ?.resolve(new Response(JSON.stringify({ inline_analysis: buildInlineAnalysisPayload(1) }), { status: 200 }));

    await waitFor(() => {
      expect(screen.getByTestId("state-1")).toHaveTextContent("loaded");
    });
  });
});
