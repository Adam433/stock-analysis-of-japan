describe("apiBaseUrl", () => {
  const originalEnv = process.env.STOCKANALYSE_API_BASE_URL;

  afterEach(() => {
    vi.restoreAllMocks();
    if (originalEnv === undefined) {
      delete process.env.STOCKANALYSE_API_BASE_URL;
    } else {
      process.env.STOCKANALYSE_API_BASE_URL = originalEnv;
    }
  });

  it("returns configured API base url candidates when env is set", async () => {
    process.env.STOCKANALYSE_API_BASE_URL = " http://example.com/api/ ";
    const { getApiBaseUrlCandidates } = await import("@/lib/apiBaseUrl");

    expect(getApiBaseUrlCandidates()).toEqual(["http://example.com/api"]);
  });

  it("resolves the first healthy default candidate", async () => {
    delete process.env.STOCKANALYSE_API_BASE_URL;
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockRejectedValueOnce(new Error("down"))
      .mockResolvedValueOnce(new Response(null, { status: 200 }));
    const { resolveApiBaseUrl } = await import("@/lib/apiBaseUrl");

    const result = await resolveApiBaseUrl();

    expect(result).toBe("http://127.0.0.1:8000");
    expect(fetchMock).toHaveBeenNthCalledWith(1, "http://localhost:8000/health/ready", {
      cache: "no-store",
    });
    expect(fetchMock).toHaveBeenNthCalledWith(2, "http://127.0.0.1:8000/health/ready", {
      cache: "no-store",
    });
  });

  it("falls back to the first candidate when every probe fails", async () => {
    delete process.env.STOCKANALYSE_API_BASE_URL;
    vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("all down"));
    const { resolveApiBaseUrl } = await import("@/lib/apiBaseUrl");

    const result = await resolveApiBaseUrl();

    expect(result).toBe("http://localhost:8000");
  });

  it("describes configured and default resolutions", async () => {
    const { describeApiBaseUrlResolution } = await import("@/lib/apiBaseUrl");

    process.env.STOCKANALYSE_API_BASE_URL = "http://example.com/api";
    expect(describeApiBaseUrlResolution("http://example.com/api")).toBe(
      "当前配置的 API 地址：http://example.com/api。",
    );

    delete process.env.STOCKANALYSE_API_BASE_URL;
    expect(describeApiBaseUrlResolution("http://localhost:8000")).toContain(
      "未配置 STOCKANALYSE_API_BASE_URL",
    );
  });
});
