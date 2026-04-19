import { render, screen } from "@testing-library/react";

import PortfolioReturnResultPage from "@/app/backtests/portfolio-return/[runId]/page";
import PortfolioReturnComparePage from "@/app/backtests/portfolio-return/compare/page";

vi.mock("next/link", () => ({
  default: ({ href, children }: { href: string; children: React.ReactNode }) => (
    <a href={href}>{children}</a>
  ),
}));

vi.mock("@/lib/apiBaseUrl", () => ({
  resolveApiBaseUrl: vi.fn(),
}));

vi.mock("@/lib/fetchWithRetry", () => ({
  fetchWithRetry: vi.fn(),
}));

vi.mock("@/lib/marketDataHealth", () => ({
  loadMarketDataHealth: vi.fn(),
}));

vi.mock("@/components/shared/WorkflowTrustBanner", () => ({
  WorkflowTrustBanner: ({ workflowLabel }: { workflowLabel: string }) => <div>banner:{workflowLabel}</div>,
}));

vi.mock("@/components/backtests/PortfolioReturnResultPanel", () => ({
  PortfolioReturnResultPanel: ({
    initialResult,
    initialError,
  }: {
    initialResult: unknown;
    initialError: string | null;
  }) => <div>result-panel:{initialResult ? "data" : "none"}:{initialError ?? "ok"}</div>,
}));

vi.mock("@/components/backtests/PortfolioReturnComparePanel", () => ({
  PortfolioReturnComparePanel: ({
    initialRuns,
    initialError,
  }: {
    initialRuns: unknown[];
    initialError: string | null;
  }) => <div>compare-panel:{initialRuns.length}:{initialError ?? "ok"}</div>,
}));

describe("PortfolioReturn pages", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("maps 410 provenance errors on the result page to the canonical unavailable message", async () => {
    const { resolveApiBaseUrl } = await import("@/lib/apiBaseUrl");
    const { fetchWithRetry } = await import("@/lib/fetchWithRetry");
    const { loadMarketDataHealth } = await import("@/lib/marketDataHealth");

    vi.mocked(resolveApiBaseUrl).mockResolvedValue("http://localhost:8000");
    vi.mocked(fetchWithRetry).mockResolvedValue(
      new Response(
        JSON.stringify({
          error: "source_screen_run_unavailable",
          error_code: "source_screen_run_unavailable",
          backtest_run_id: 11,
        }),
        { status: 410 },
      ),
    );
    vi.mocked(loadMarketDataHealth).mockResolvedValue({ health: null, error: null });

    render(await PortfolioReturnResultPage({ params: Promise.resolve({ runId: "11" }) }));

    expect(screen.getByText("banner:回测工作流")).toBeInTheDocument();
    expect(screen.getByText("result-panel:none:原筛选记录不可用 — 策略定义无法解析")).toBeInTheDocument();
  });

  it("maps 410 provenance errors on the compare page to the canonical unavailable message", async () => {
    const { resolveApiBaseUrl } = await import("@/lib/apiBaseUrl");
    const { fetchWithRetry } = await import("@/lib/fetchWithRetry");
    const { loadMarketDataHealth } = await import("@/lib/marketDataHealth");

    vi.mocked(resolveApiBaseUrl).mockResolvedValue("http://localhost:8000");
    vi.mocked(fetchWithRetry).mockResolvedValue(
      new Response(
        JSON.stringify({
          error: "source_screen_run_unavailable",
          error_code: "source_screen_run_unavailable",
          backtest_run_id: 21,
        }),
        { status: 410 },
      ),
    );
    vi.mocked(loadMarketDataHealth).mockResolvedValue({ health: null, error: null });

    render(await PortfolioReturnComparePage({ searchParams: Promise.resolve({ ids: "21,22" }) }));

    expect(screen.getByText("banner:回测工作流")).toBeInTheDocument();
    expect(screen.getByText("compare-panel:0:原筛选记录不可用 — 策略定义无法解析")).toBeInTheDocument();
  });
});
