import { render, screen } from "@testing-library/react";

import { XSignalTrackerPanel } from "@/components/x-signals/XSignalTrackerPanel";
import type { XSignalDashboard, XSignalMention } from "@/lib/types";

function buildMention(overrides: Partial<XSignalMention> = {}): XSignalMention {
  return {
    id: 9,
    author_id: 3,
    author_handle: "signaluser",
    post_id: 7,
    symbol: "NVDA",
    exchange: "NASDAQ",
    company_name: "NVIDIA",
    mention_kind: "stock",
    sector_label: null,
    sentiment: "bullish",
    confidence: "0.7000",
    mention_date: "2026-01-02",
    mention_count: 2,
    mentioned_at: "2026-01-02T15:00:00Z",
    is_sector_proxy: false,
    proxy_reason: null,
    source_text_excerpt: "$NVDA bullish after earnings.",
    source_post_ids: [7, 8],
    analysis_source: "heuristic-v1",
    mention_price_date: "2026-01-02",
    mention_close: "100.000000",
    latest_price_date: "2026-05-08",
    latest_close: "125.000000",
    cumulative_return: "0.25",
    ...overrides,
  };
}

function buildDashboard(overrides: Partial<XSignalDashboard> = {}): XSignalDashboard {
  return {
    authors: [
      {
        id: 3,
        handle: "signaluser",
        display_name: "Signal User",
        notes: null,
        tracking_status: "active",
        post_count: 4,
        mention_count: 3,
        last_fetch_requested_at: "2026-05-01T09:00:00Z",
        last_analyzed_at: "2026-05-02T09:00:00Z",
      },
    ],
    mentions: [
      buildMention(),
      buildMention({
        id: 10,
        mention_date: "2026-03-04",
        mention_count: 1,
        mentioned_at: "2026-03-04T15:00:00Z",
        sentiment: "bearish",
        mention_price_date: "2026-03-04",
        mention_close: "110.000000",
        cumulative_return: "0.1363636363",
        source_text_excerpt: "$NVDA downside risk.",
        source_post_ids: [11],
      }),
      buildMention({
        id: 11,
        symbol: "MSFT",
        company_name: "Microsoft",
        mention_kind: "sector_proxy",
        sector_label: "Cloud Software",
        sentiment: "unknown",
        mention_count: 1,
        proxy_reason: "提到了 Cloud Software 板块，但未识别到明确个股。",
        source_text_excerpt: "Cloud demand is still durable.",
      }),
    ],
    total_posts: 4,
    total_mentions: 3,
    latest_fetch_request: null,
    ...overrides,
  };
}

describe("XSignalTrackerPanel", () => {
  it("renders a read-only dashboard grouped by symbol", () => {
    render(
      <XSignalTrackerPanel
        apiBaseUrl="http://localhost:8000"
        initialDashboard={buildDashboard()}
        initialError={null}
      />,
    );

    expect(screen.getByText("X 信号追踪台")).toBeInTheDocument();
    expect(screen.getByText("只读看板：Codex 使用 Chrome 采集 X 发言，后端按股票和日期聚合信号。")).toBeInTheDocument();
    expect(screen.getByText("Signal User")).toBeInTheDocument();
    expect(screen.getAllByText("NVDA")).toHaveLength(1);
    expect(screen.getByText("MSFT")).toBeInTheDocument();
    expect(screen.getByText("看多 1 / 看空 1")).toBeInTheDocument();
    expect(screen.getAllByText("25.00%").length).toBeGreaterThan(0);
    expect(screen.getByText("当天 2 次", { exact: false })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "运行分析" })).not.toBeInTheDocument();
    expect(screen.queryByLabelText("发言内容")).not.toBeInTheDocument();
  });

  it("shows the initial loading error without hiding existing dashboard data", () => {
    render(
      <XSignalTrackerPanel
        apiBaseUrl="http://localhost:8000"
        initialDashboard={buildDashboard()}
        initialError="X Signal Tracker 接口不可达。"
      />,
    );

    expect(screen.getByText("X Signal Tracker 接口不可达。")).toBeInTheDocument();
    expect(screen.getByText("NVDA")).toBeInTheDocument();
  });
});
