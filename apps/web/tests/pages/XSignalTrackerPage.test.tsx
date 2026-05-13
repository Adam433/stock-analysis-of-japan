import { render, screen } from "@testing-library/react";

import XSignalTrackerPage from "@/app/x-signal-tracker/page";
import { resolveApiBaseUrl } from "@/lib/apiBaseUrl";
import { fetchWithRetry } from "@/lib/fetchWithRetry";

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

vi.mock("@/components/x-signals/XSignalTrackerPanel", () => ({
  XSignalTrackerPanel: (props: {
    apiBaseUrl: string;
    initialDashboard: { total_posts: number; total_mentions: number };
    initialError: string | null;
  }) => (
    <div>
      {`x-signal-panel:${props.apiBaseUrl}:${props.initialDashboard.total_posts}:${props.initialDashboard.total_mentions}:${props.initialError ?? "ok"}`}
    </div>
  ),
}));

describe("XSignalTrackerPage", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("loads dashboard data into the tracker panel", async () => {
    vi.mocked(resolveApiBaseUrl).mockResolvedValue("http://localhost:8000");
    vi.mocked(fetchWithRetry).mockResolvedValue(
      new Response(
        JSON.stringify({
          dashboard: {
            authors: [],
            mentions: [],
            total_posts: 4,
            total_mentions: 2,
            latest_fetch_request: null,
          },
        }),
        { status: 200 },
      ),
    );

    render(await XSignalTrackerPage());

    expect(fetchWithRetry).toHaveBeenCalledWith("http://localhost:8000/x-signals/dashboard", {
      cache: "no-store",
    });
    expect(screen.getByText("x-signal-panel:http://localhost:8000:4:2:ok")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "数据健康" })).toHaveAttribute("href", "/");
  });
});
