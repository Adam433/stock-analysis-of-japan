import { loadMarketDataHealth } from "@/lib/marketDataHealth";

vi.mock("@/lib/fetchWithRetry", () => ({
  fetchWithRetry: vi.fn(),
}));

describe("loadMarketDataHealth", () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it("returns parsed health data on success", async () => {
    const { fetchWithRetry } = await import("@/lib/fetchWithRetry");
    vi.mocked(fetchWithRetry).mockResolvedValue(
      new Response(
        JSON.stringify({
          freshness_state: "fresh",
          latest_trade_date: "2026-04-16",
          age_in_days: 0,
          coverage_status: "ok",
          total_instruments: 100,
          partial_rows: 0,
          unavailable_rows: 0,
          last_refresh: null,
          universe_manifest: null,
        }),
        { status: 200 },
      ),
    );

    const result = await loadMarketDataHealth("http://localhost:8000");

    expect(fetchWithRetry).toHaveBeenCalledWith("http://localhost:8000/health/market-data", {
      cache: "no-store",
    });
    expect(result.health?.freshness_state).toBe("fresh");
    expect(result.error).toBeNull();
  });

  it("returns an HTTP error message when the endpoint is not ok", async () => {
    const { fetchWithRetry } = await import("@/lib/fetchWithRetry");
    vi.mocked(fetchWithRetry).mockResolvedValue(new Response(null, { status: 503 }));

    const result = await loadMarketDataHealth("http://localhost:8000");

    expect(result.health).toBeNull();
    expect(result.error).toBe("健康检查接口返回 503。");
  });

  it("returns a connectivity error when fetchWithRetry throws", async () => {
    const { fetchWithRetry } = await import("@/lib/fetchWithRetry");
    vi.mocked(fetchWithRetry).mockRejectedValue(new Error("network down"));

    const result = await loadMarketDataHealth("http://localhost:8000");

    expect(result.health).toBeNull();
    expect(result.error).toContain("无法访问健康检查接口：http://localhost:8000/health/market-data");
  });
});
