import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { WatchlistReviewPanel } from "@/components/watchlist/WatchlistReviewPanel";
import type { WatchlistEntry } from "@/lib/types";

vi.mock("next/link", () => ({
  default: ({ href, children, className }: { href: string; children: React.ReactNode; className?: string }) => (
    <a href={href} className={className}>
      {children}
    </a>
  ),
}));

vi.mock("@/components/watchlist/WatchlistToggleButton", () => ({
  WatchlistToggleButton: ({
    symbol,
    onToggleComplete,
  }: {
    symbol: string;
    onToggleComplete?: (nextValue: boolean) => void;
  }) => (
    <button type="button" onClick={() => onToggleComplete?.(false)}>
      移除 {symbol}
    </button>
  ),
}));

function buildEntry(overrides: Partial<WatchlistEntry> = {}): WatchlistEntry {
  return {
    id: 1,
    instrument_id: 61,
    symbol: "7203",
    exchange: "TSE",
    name: "Toyota Motor",
    note: "关注突破后的量价关系",
    observation_reason: "平台整理完成",
    added_date: "2026-04-16",
    added_at: "2026-04-16T09:00:00Z",
    ...overrides,
  };
}

describe("WatchlistReviewPanel", () => {
  it("renders watchlist review cards with summary counts and links", () => {
    render(
      <WatchlistReviewPanel
        apiBaseUrl="http://localhost:8000"
        initialEntries={[
          buildEntry(),
          buildEntry({
            id: 2,
            instrument_id: 62,
            symbol: "6758",
            name: "Sony Group",
            note: null,
            observation_reason: null,
          }),
        ]}
        initialError={null}
      />,
    );

    expect(screen.getByText("观察列表复盘")).toBeInTheDocument();
    expect(screen.getByText("Toyota Motor")).toBeInTheDocument();
    expect(screen.getByText("Sony Group")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "7203" })).toHaveAttribute("href", "/stocks/61");
    expect(screen.getAllByText("2").length).toBeGreaterThan(0);
    expect(screen.getByText("平台整理完成")).toBeInTheDocument();
    expect(screen.getByText("尚未填写")).toBeInTheDocument();
    expect(screen.getAllByRole("link", { name: "查看详情" })[0]).toHaveAttribute("href", "/stocks/61");
  });

  it("shows an empty-state when no watchlist entry exists", () => {
    render(
      <WatchlistReviewPanel
        apiBaseUrl="http://localhost:8000"
        initialEntries={[]}
        initialError={null}
      />,
    );

    expect(
      screen.getByText("尚未保存任何观察列表条目。可在筛选结果列表或个股详情中添加候选。"),
    ).toBeInTheDocument();
  });

  it("removes an entry from the review grid when toggle callback reports removal", async () => {
    const user = userEvent.setup();

    render(
      <WatchlistReviewPanel
        apiBaseUrl="http://localhost:8000"
        initialEntries={[buildEntry()]}
        initialError="观察列表载入发生延迟。"
      />,
    );

    expect(screen.getByText("观察列表载入发生延迟。")).toBeInTheDocument();
    expect(screen.getByText("Toyota Motor")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "移除 7203" }));

    expect(screen.queryByText("Toyota Motor")).not.toBeInTheDocument();
    expect(
      screen.getByText("尚未保存任何观察列表条目。可在筛选结果列表或个股详情中添加候选。"),
    ).toBeInTheDocument();
  });
});
