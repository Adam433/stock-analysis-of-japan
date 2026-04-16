import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { WatchlistToggleButton } from "@/components/watchlist/WatchlistToggleButton";

describe("WatchlistToggleButton", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("adds an instrument to the watchlist with trimmed research context", async () => {
    const user = userEvent.setup();
    const onToggleComplete = vi.fn();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          entry: {
            instrument_id: 61,
            note: "观察成交量",
            observation_reason: "突破平台",
            added_date: "2026-04-16",
          },
        }),
        { status: 200 },
      ),
    );

    render(
      <WatchlistToggleButton
        apiBaseUrl="http://localhost:8000"
        instrumentId={61}
        symbol="7203"
        loadOnMount={false}
        onToggleComplete={onToggleComplete}
      />,
    );

    await user.click(screen.getByRole("button", { name: "编辑研究备注" }));
    await user.type(screen.getByRole("textbox", { name: "观察原因" }), "  突破平台  ");
    await user.type(screen.getByRole("textbox", { name: "研究备注" }), "  观察成交量  ");
    await user.click(screen.getByRole("button", { name: "加入观察列表" }));

    await waitFor(() => {
      expect(screen.getByText("7203 已加入观察列表，研究备注一并保留。")).toBeInTheDocument();
    });

    expect(fetchMock).toHaveBeenCalledWith("http://localhost:8000/watchlist", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        instrument_id: 61,
        note: "观察成交量",
        observation_reason: "突破平台",
      }),
    });
    expect(onToggleComplete).toHaveBeenCalledWith(true);
    expect(screen.getByRole("button", { name: "从观察列表移除" })).toBeInTheDocument();
    expect(screen.getByText("加入观察列表日期：2026-04-16。")).toBeInTheDocument();
  });

  it("does not call delete when removal confirmation is cancelled", async () => {
    const user = userEvent.setup();
    const confirmMock = vi.spyOn(window, "confirm").mockReturnValue(false);
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(null, { status: 200 }));

    render(
      <WatchlistToggleButton
        apiBaseUrl="http://localhost:8000"
        instrumentId={61}
        symbol="7203"
        loadOnMount={false}
        initialIsInWatchlist
      />,
    );

    await user.click(screen.getByRole("button", { name: "从观察列表移除" }));

    expect(confirmMock).toHaveBeenCalledWith("确定要将 7203 从观察列表移除吗？");
    expect(fetchMock).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "从观察列表移除" })).toBeInTheDocument();
  });

  it("saves updated research context for an existing watchlist entry", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          entry: {
            instrument_id: 61,
            note: "更新后的备注",
            observation_reason: "趋势确认",
            added_date: "2026-04-10",
          },
        }),
        { status: 200 },
      ),
    );

    render(
      <WatchlistToggleButton
        apiBaseUrl="http://localhost:8000"
        instrumentId={61}
        symbol="7203"
        loadOnMount={false}
        initialIsInWatchlist
        initialNote="旧备注"
        initialObservationReason="旧原因"
        initialAddedDate="2026-04-10"
      />,
    );

    await user.click(screen.getByRole("button", { name: "编辑研究备注" }));

    const reasonInput = screen.getByRole("textbox", { name: "观察原因" });
    const noteInput = screen.getByRole("textbox", { name: "研究备注" });
    fireEvent.change(reasonInput, { target: { value: "趋势确认" } });
    fireEvent.change(noteInput, { target: { value: "更新后的备注" } });
    await user.click(screen.getByRole("button", { name: "保存观察备注" }));

    await waitFor(() => {
      expect(screen.getByText("7203 的观察备注与观察原因已保存。")).toBeInTheDocument();
    });

    expect(fetchMock).toHaveBeenCalledWith("http://localhost:8000/watchlist/61", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        note: "更新后的备注",
        observation_reason: "趋势确认",
      }),
    });
    expect(screen.getByDisplayValue("更新后的备注")).toBeInTheDocument();
    expect(screen.getByDisplayValue("趋势确认")).toBeInTheDocument();
  });
});
