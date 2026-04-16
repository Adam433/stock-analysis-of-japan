import { fetchWithRetry } from "@/lib/fetchWithRetry";

describe("fetchWithRetry", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("returns immediately when fetch succeeds", async () => {
    const response = new Response(JSON.stringify({ ok: true }), { status: 200 });
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(response);

    const result = await fetchWithRetry("https://example.com/api", undefined, {
      retries: 2,
      delay: 1,
    });

    expect(result).toBe(response);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("retries on server errors before succeeding", async () => {
    vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(null, { status: 503 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ ok: true }), { status: 200 }));

    const result = await fetchWithRetry("https://example.com/api", undefined, {
      retries: 2,
      delay: 1,
    });

    expect(result.status).toBe(200);
    expect(globalThis.fetch).toHaveBeenCalledTimes(2);
  });

  it("throws the last network error after exhausting retries", async () => {
    const networkError = new Error("network down");
    vi.spyOn(globalThis, "fetch").mockRejectedValue(networkError);

    await expect(
      fetchWithRetry("https://example.com/api", undefined, { retries: 2, delay: 1 }),
    ).rejects.toThrow("network down");

    expect(globalThis.fetch).toHaveBeenCalledTimes(3);
  });
});
