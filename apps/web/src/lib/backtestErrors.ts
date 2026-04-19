export const SOURCE_SCREEN_RUN_UNAVAILABLE_ERROR_CODE = "source_screen_run_unavailable";
export const SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE = "原筛选记录不可用 — 策略定义无法解析";

type PortfolioReturnErrorPayload = {
  detail?: string;
  error?: string;
  error_code?: string;
  backtest_run_id?: number;
} | null;

export function isSourceScreenRunUnavailablePayload(payload: PortfolioReturnErrorPayload): boolean {
  return (
    payload?.error === SOURCE_SCREEN_RUN_UNAVAILABLE_ERROR_CODE ||
    payload?.error_code === SOURCE_SCREEN_RUN_UNAVAILABLE_ERROR_CODE
  );
}

export function isSourceScreenRunUnavailableMessage(message: string | null): boolean {
  return (
    message === SOURCE_SCREEN_RUN_UNAVAILABLE_MESSAGE ||
    message === SOURCE_SCREEN_RUN_UNAVAILABLE_ERROR_CODE
  );
}
