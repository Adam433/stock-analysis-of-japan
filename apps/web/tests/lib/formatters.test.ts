import {
  formatDateOnly,
  formatNumber,
  formatPercent,
  formatRpsWindows,
  formatTimestamp,
} from "@/lib/formatters";

describe("formatters", () => {
  it("returns fallbacks for empty values", () => {
    expect(formatTimestamp(null)).toBe("暂无");
    expect(formatNumber(null)).toBe("不可用");
    expect(formatPercent(null)).toBe("不可用");
  });

  it("formats numeric and percentage values with precision", () => {
    expect(formatNumber("12.3456")).toBe("12.35");
    expect(formatNumber("12.3456", 1)).toBe("12.3");
    expect(formatPercent("87.654", 1)).toBe("87.7%");
  });

  it("formats RPS windows and date-only strings", () => {
    expect(formatRpsWindows([20, 50, 120])).toBe("20 日 / 50 日 / 120 日");
    expect(formatDateOnly("2026-04-16")).toBe("2026年4月16日");
    expect(formatDateOnly("invalid")).toBe("invalid");
  });
});
