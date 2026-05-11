import { render, screen } from "@testing-library/react";

import ExperimentsPage from "@/app/experiments/page";

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

describe("ExperimentsPage", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders GA and optimization experiment summaries", async () => {
    const { resolveApiBaseUrl } = await import("@/lib/apiBaseUrl");
    const { fetchWithRetry } = await import("@/lib/fetchWithRetry");

    vi.mocked(resolveApiBaseUrl).mockResolvedValue("http://localhost:8000");
    vi.mocked(fetchWithRetry)
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            ga_runs: [
              {
                id: 5,
                market: "us",
                objective: "spy_alpha",
                status: "completed",
                population_size: 2,
                max_generations: 2,
                completed_generations: 2,
                total_generations: 2,
                completed_individuals: 4,
                total_individuals: 4,
                failed_individuals: 0,
                best_individual_id: 12,
                train_start_date: "2025-07-01",
                train_end_date: "2026-02-28",
                holdout_start_date: "2026-03-01",
                holdout_end_date: "2026-04-30",
                started_at: "2026-05-11T13:06:36Z",
                completed_at: "2026-05-11T13:07:57Z",
              },
            ],
          }),
          { status: 200 },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            ga_run: {
              id: 5,
              market: "us",
              objective: "spy_alpha",
              status: "completed",
              population_size: 2,
              max_generations: 2,
              completed_generations: 2,
              total_generations: 2,
              completed_individuals: 4,
              total_individuals: 4,
              failed_individuals: 0,
              best_individual_id: 12,
              train_start_date: "2025-07-01",
              train_end_date: "2026-02-28",
              holdout_start_date: "2026-03-01",
              holdout_end_date: "2026-04-30",
              started_at: "2026-05-11T13:06:36Z",
              completed_at: "2026-05-11T13:07:57Z",
            },
          }),
          { status: 200 },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            optimization_run: {
              id: 41,
              market: "us",
              objective: "spy_alpha",
              status: "cancelled",
              completed_parameter_sets: 8,
              total_parameter_sets: 18,
              failed_parameter_sets: 0,
              best_result_id: 100,
              started_at: "2026-05-11T10:00:00Z",
              completed_at: "2026-05-11T11:00:00Z",
            },
          }),
          { status: 200 },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            individuals: [
              {
                id: 12,
                generation: 0,
                individual_index: 1,
                status: "completed",
                fitness: "0.031983",
                parameters: {
                  rps_threshold: 80,
                  selected_rps_windows: [120, 250],
                  use_cup_handle: false,
                  portfolio_cap: 20,
                },
                metrics: {
                  aggregate: {
                    max_drawdown: "0.078419",
                  },
                },
              },
            ],
          }),
          { status: 200 },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            results: [
              {
                id: 100,
                optimization_run_id: 41,
                score: "0.128000",
                rank: 1,
                status: "completed",
                failure_reason: null,
                completed_at: "2026-05-11T10:30:00Z",
                parameters: {
                  rps_threshold: 80,
                  selected_rps_windows: [120, 250],
                  use_cup_handle: false,
                  portfolio_cap: 20,
                  fundamental_growth_params: { max_pe: "60", max_pb: "15" },
                },
                train_metrics: { total_return: "0.100000", max_drawdown: "-0.050000" },
                validation_metrics: { total_return: "0.080000", max_drawdown: "-0.040000" },
              },
            ],
            total: 1,
            limit: 30,
            offset: 0,
          }),
          { status: 200 },
        ),
      );

    render(await ExperimentsPage());

    expect(screen.getByText("参数优化与 GA 实验")).toBeInTheDocument();
    expect(screen.getByText("#5")).toBeInTheDocument();
    expect(screen.getByText("#41")).toBeInTheDocument();
    expect(screen.getByText("0.031983")).toBeInTheDocument();
    expect(screen.getByText("0.128000")).toBeInTheDocument();
    expect(screen.getAllByText("80 / 120 / 250").length).toBeGreaterThanOrEqual(1);
    expect(screen.getByRole("link", { name: "详情" })).toHaveAttribute(
      "href",
      "/experiments/optimization-results/100",
    );
    expect(screen.getByRole("link", { name: "回测" })).toHaveAttribute("href", "/backtests");
  });
});
