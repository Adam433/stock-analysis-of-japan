const DEFAULT_API_BASE_URLS = [
  "http://localhost:8000",
  "http://127.0.0.1:8000",
] as const;
const API_READY_PATH = "/health/ready";

function normalizeApiBaseUrl(value: string): string {
  return value.trim().replace(/\/+$/, "");
}

function unique(values: string[]): string[] {
  return [...new Set(values)];
}

export function getApiBaseUrlCandidates(): string[] {
  const configuredApiBaseUrl = process.env.STOCKANALYSE_API_BASE_URL?.trim();

  if (configuredApiBaseUrl) {
    return [normalizeApiBaseUrl(configuredApiBaseUrl)];
  }

  return unique(DEFAULT_API_BASE_URLS.map(normalizeApiBaseUrl));
}

export async function resolveApiBaseUrl(): Promise<string> {
  const candidates = getApiBaseUrlCandidates();

  for (const candidate of candidates) {
    try {
      const response = await fetch(`${candidate}${API_READY_PATH}`, {
        cache: "no-store",
      });

      if (response.ok) {
        return candidate;
      }
    } catch {
      continue;
    }
  }

  return candidates[0];
}

export function describeApiBaseUrlResolution(apiBaseUrl: string): string {
  const candidates = getApiBaseUrlCandidates();
  const attempted = candidates.join("、");

  if (process.env.STOCKANALYSE_API_BASE_URL?.trim()) {
    return `当前配置的 API 地址：${apiBaseUrl}。`;
  }

  return `未配置 STOCKANALYSE_API_BASE_URL，已尝试默认地址：${attempted}。当前采用 ${apiBaseUrl}。`;
}
