import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { ErrorBoundary } from "@/components/shared/ErrorBoundary";

function CrashingChild({ shouldThrow = true }: { shouldThrow?: boolean }) {
  if (shouldThrow) {
    throw new Error("渲染失败");
  }

  return <div>正常内容</div>;
}

describe("ErrorBoundary", () => {
  const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

  afterAll(() => {
    consoleErrorSpy.mockRestore();
  });

  afterEach(() => {
    consoleErrorSpy.mockClear();
  });

  it("renders the default fallback when a child throws", () => {
    render(
      <ErrorBoundary>
        <CrashingChild />
      </ErrorBoundary>,
    );

    expect(screen.getByRole("alert")).toBeInTheDocument();
    expect(screen.getByText("页面渲染出错")).toBeInTheDocument();
    expect(screen.getByText("渲染失败")).toBeInTheDocument();
  });

  it("resets after retry and renders healthy content again", async () => {
    const user = userEvent.setup();
    let shouldThrow = true;

    const { rerender } = render(
      <ErrorBoundary>
        <CrashingChild shouldThrow={shouldThrow} />
      </ErrorBoundary>,
    );

    shouldThrow = false;
    rerender(
      <ErrorBoundary>
        <CrashingChild shouldThrow={shouldThrow} />
      </ErrorBoundary>,
    );

    await user.click(screen.getByRole("button", { name: "重试" }));

    expect(screen.getByText("正常内容")).toBeInTheDocument();
  });

  it("uses a custom fallback when provided", () => {
    render(
      <ErrorBoundary fallback={<div>自定义回退</div>}>
        <CrashingChild />
      </ErrorBoundary>,
    );

    expect(screen.getByText("自定义回退")).toBeInTheDocument();
    expect(screen.queryByText("页面渲染出错")).not.toBeInTheDocument();
  });
});
