import { render, screen } from "@testing-library/react";

import { WorkflowTrustBanner } from "@/components/shared/WorkflowTrustBanner";
import type { MarketDataHealthResponse } from "@/lib/marketDataHealth";

function buildHealth(overrides: Partial<MarketDataHealthResponse> = {}): MarketDataHealthResponse {
  return {
    freshness_state: "fresh",
    latest_trade_date: "2026-04-16",
    age_in_days: 0,
    coverage_status: "complete",
    total_instruments: 1234,
    partial_rows: 1,
    unavailable_rows: 0,
    last_refresh: {
      status: "succeeded",
      provider: "local_csv_directory",
      universe_scope: "full_universe",
      universe_filter: "tse_common_stock",
      requested_symbol_count: 1234,
      started_at: "2026-04-16T00:00:00Z",
      completed_at: "2026-04-16T00:10:00Z",
      rows_processed: 1000,
      rows_inserted: 800,
      rows_updated: 200,
      partial_rows: 1,
      unavailable_rows: 0,
      latest_trade_date: "2026-04-16",
      error_message: null,
      requested_symbols: ["7203"],
    },
    universe_manifest: {
      universe_filter: "tse_common_stock",
      symbol_count: 1234,
      updated_at: "2026-04-16T00:00:00Z",
    },
    ...overrides,
  };
}

describe("WorkflowTrustBanner", () => {
  it("renders a good-state banner when health is usable", () => {
    const { container } = render(
      <WorkflowTrustBanner
        workflowLabel="筛选工作流"
        health={buildHealth()}
        error={null}
      />,
    );

    expect(screen.getByRole("heading", { name: "筛选工作流：可用于日常研究" })).toBeInTheDocument();
    expect(screen.getByText(/新鲜度 fresh，覆盖度 complete/)).toBeInTheDocument();
    expect(screen.getByText(/东京证券交易所普通股清单 1234 只/)).toBeInTheDocument();
    expect(container.querySelector(".workflow-banner--good")).not.toBeNull();
  });

  it("renders a warning-state banner for stale or partial data", () => {
    const { container } = render(
      <WorkflowTrustBanner
        workflowLabel="回测工作流"
        health={buildHealth({ freshness_state: "stale", coverage_status: "partial" })}
        error={null}
      />,
    );

    expect(screen.getByRole("heading", { name: "回测工作流：数据陈旧" })).toBeInTheDocument();
    expect(container.querySelector(".workflow-banner--warn")).not.toBeNull();
  });

  it("renders a bad-state banner when an error exists", () => {
    const { container } = render(
      <WorkflowTrustBanner
        workflowLabel="观察列表工作流"
        health={null}
        error="健康接口不可达"
      />,
    );

    expect(screen.getByRole("heading", { name: "观察列表工作流：连接异常" })).toBeInTheDocument();
    expect(screen.getByText("健康接口不可达")).toBeInTheDocument();
    expect(container.querySelector(".workflow-banner--bad")).not.toBeNull();
  });
});
