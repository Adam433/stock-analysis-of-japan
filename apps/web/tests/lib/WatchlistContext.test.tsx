import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { WatchlistProvider, useWatchlist } from "@/lib/WatchlistContext";

function Consumer() {
  const watchlist = useWatchlist();

  return (
    <div>
      <p>loaded:{String(watchlist.isLoaded)}</p>
      <p>ids:{watchlist.instrumentIds.join(",")}</p>
      <p>has-61:{String(watchlist.has(61))}</p>
      <button type="button" onClick={() => watchlist.add(99)}>
        add-99
      </button>
      <button type="button" onClick={() => watchlist.remove(61)}>
        remove-61
      </button>
    </div>
  );
}

describe("WatchlistContext", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("loads watchlist ids from the API and exposes has()", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ entries: [{ instrument_id: 61 }, { instrument_id: 62 }] }), {
        status: 200,
      }),
    );

    render(
      <WatchlistProvider apiBaseUrl="http://localhost:8000">
        <Consumer />
      </WatchlistProvider>,
    );

    await waitFor(() => {
      expect(screen.getByText("loaded:true")).toBeInTheDocument();
    });

    expect(screen.getByText("ids:61,62")).toBeInTheDocument();
    expect(screen.getByText("has-61:true")).toBeInTheDocument();
  });

  it("supports add/remove mutations after initial load", async () => {
    const user = userEvent.setup();
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ entries: [{ instrument_id: 61 }] }), { status: 200 }),
    );

    render(
      <WatchlistProvider apiBaseUrl="http://localhost:8000">
        <Consumer />
      </WatchlistProvider>,
    );

    await waitFor(() => {
      expect(screen.getByText("ids:61")).toBeInTheDocument();
    });

    await user.click(screen.getByRole("button", { name: "add-99" }));
    expect(screen.getByText("ids:61,99")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "remove-61" }));
    expect(screen.getByText("ids:99")).toBeInTheDocument();
    expect(screen.getByText("has-61:false")).toBeInTheDocument();
  });

  it("marks loading complete even when the fetch fails", async () => {
    vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("network down"));

    render(
      <WatchlistProvider apiBaseUrl="http://localhost:8000">
        <Consumer />
      </WatchlistProvider>,
    );

    await waitFor(() => {
      expect(screen.getByText("loaded:true")).toBeInTheDocument();
    });

    expect(screen.getByText("ids:")).toBeInTheDocument();
  });
});
