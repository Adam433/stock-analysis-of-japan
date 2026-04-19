export function formatTimestamp(value: string | null, fallback = "暂无"): string {
  if (!value) {
    return fallback;
  }

  return new Intl.DateTimeFormat("zh-CN", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

export function formatRpsWindows(windows: number[]): string {
  return windows.map((window) => `${window} 日`).join(" / ");
}

export function formatNumber(value: string | null, digits = 2): string {
  if (!value) {
    return "不可用";
  }

  return Number(value).toFixed(digits);
}

export function formatPercent(value: string | null, digits = 2): string {
  if (!value) {
    return "不可用";
  }

  return `${Number(value).toFixed(digits)}%`;
}

export function formatRatioAsPercent(value: string | null, digits = 2): string {
  if (!value) {
    return "不可用";
  }

  return `${(Number(value) * 100).toFixed(digits)}%`;
}

export function formatDateOnly(value: string): string {
  const [year, month, day] = value.split("-").map(Number);
  if (!year || !month || !day) {
    return value;
  }

  return new Intl.DateTimeFormat("zh-CN", {
    dateStyle: "long",
    timeZone: "UTC",
  }).format(new Date(Date.UTC(year, month - 1, day)));
}

export function formatFiscalYearLabel(fiscalYearLabel: string, fiscalYearEndMonth: number): string {
  const month = String(fiscalYearEndMonth).padStart(2, "0");
  return `${fiscalYearLabel}（${month} 月结束）`;
}
