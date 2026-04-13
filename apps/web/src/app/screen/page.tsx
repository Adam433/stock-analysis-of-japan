import Link from "next/link";

import { StrategyConfigPanel } from "@/components/screen/StrategyConfigPanel";

type StrategyConfigurationResponse = {
  configuration: {
    id: number;
    version: number;
    rps_threshold: number;
    high_proximity_threshold_pct: string;
  };
  validation?: {
    rps_threshold: { min: number; max: number; default: number };
    high_proximity_threshold_pct: { min: string; max: string; default: string };
  };
};

type LoadStrategyConfigurationResult = {
  data: StrategyConfigurationResponse | null;
  error: string | null;
};

const apiBaseUrl =
  process.env.STOCKANALYSE_API_BASE_URL ??
  process.env.NEXT_PUBLIC_STOCKANALYSE_API_BASE_URL ??
  "http://127.0.0.1:8000";

export const dynamic = "force-dynamic";

async function loadStrategyConfiguration(): Promise<LoadStrategyConfigurationResult> {
  try {
    const response = await fetch(`${apiBaseUrl}/screen/configuration`, {
      cache: "no-store",
    });

    if (!response.ok) {
      return {
        data: null,
        error: `Unable to load strategy configuration (${response.status}).`,
      };
    }

    return {
      data: (await response.json()) as StrategyConfigurationResponse,
      error: null,
    };
  } catch {
    return {
      data: null,
      error: "Strategy configuration API is unreachable. Check backend availability and API base URL.",
    };
  }
}

export default async function ScreenConfigurationPage() {
  const { data, error } = await loadStrategyConfiguration();

  return (
    <main className="dashboard-shell">
      <nav className="top-nav">
        <Link href="/">Data Health</Link>
        <span>/</span>
        <span>Screen Configuration</span>
      </nav>
      <StrategyConfigPanel
        apiBaseUrl={apiBaseUrl}
        initialData={data}
        initialError={error}
      />
    </main>
  );
}
