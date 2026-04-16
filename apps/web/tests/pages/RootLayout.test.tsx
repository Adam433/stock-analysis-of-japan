import { render, screen } from "@testing-library/react";

import RootLayout, { metadata } from "@/app/layout";

vi.mock("@/components/shared/ErrorBoundary", () => ({
  ErrorBoundary: ({ children }: { children: React.ReactNode }) => <div data-testid="error-boundary">{children}</div>,
}));

describe("RootLayout", () => {
  it("exports the expected metadata", () => {
    expect(metadata.title).toBe("stockAnalyse");
    expect(metadata.description).toBe("日股筛选与回测工作台");
  });

  it("renders skip link and wraps children with the error boundary", () => {
    render(
      RootLayout({
        children: <main id="main-content">页面内容</main>,
      }),
    );

    expect(screen.getByRole("link", { name: "跳转到主要内容" })).toHaveAttribute("href", "#main-content");
    expect(screen.getByTestId("error-boundary")).toBeInTheDocument();
    expect(screen.getByText("页面内容")).toBeInTheDocument();
  });
});
