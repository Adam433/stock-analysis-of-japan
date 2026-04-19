export type StrategyConfiguration = {
  id: number;
  version: number;
  rps_threshold: number;
  high_proximity_threshold_pct: string;
  selected_rps_windows: number[];
};

export type StrategyConfigurationResponse = {
  configuration: StrategyConfiguration;
  validation?: {
    rps_threshold: { min: number; max: number; default: number };
    high_proximity_threshold_pct: { min: string; max: string; default: string };
    selected_rps_windows: { approved: number[]; default: number[] };
  };
};

export type ScreenRunResult = {
  instrument_id: number;
  symbol: string;
  exchange: string;
  trade_date: string;
  best_rps_value: string | null;
  rps_threshold: number;
  high_proximity_ratio: string | null;
  high_proximity_threshold_pct: string;
  max_drawdown_from_high_pct: string | null;
  rps_condition_passed: boolean;
  high_proximity_condition_passed: boolean;
};

export type ScreenRun = {
  id: number;
  strategy_configuration_id: number;
  trade_date: string;
  executed_at: string;
  total_candidates: number;
  qualified_count: number;
  status: string;
  parameter_set: {
    id: number;
    version: number;
    rps_threshold: number;
    high_proximity_threshold_pct: string;
    selected_rps_windows: number[];
  };
  qualified_results: ScreenRunResult[];
};

export type BacktestLifecycle = "portfolio_return" | "legacy_condition_hit";
export type PortfolioBacktestFieldName =
  | "holding_days"
  | "stop_loss_pct"
  | "portfolio_cap"
  | "entry_deferral_window_days";

export type PortfolioBacktestDefaults = {
  holding_days: number;
  stop_loss_pct: number;
  portfolio_cap: number;
  entry_deferral_window_days: number;
};

export type BacktestRun = {
  id: number;
  source_screen_run_id: number | null;
  source_screen_run_available?: boolean | null;
  strategy_configuration_id: number;
  status: string;
  backtest_lifecycle: BacktestLifecycle;
  start_date: string;
  end_date: string;
  started_at: string;
  completed_at: string | null;
  rps_definition_version: string | null;
  dataset_trade_date_start: string | null;
  dataset_trade_date_end: string | null;
  dataset_checksum: string | null;
  effective_holding_days: number | null;
  effective_stop_loss_pct: string | null;
  effective_portfolio_cap: number | null;
  effective_entry_deferral_window_days: number | null;
  ranking_policy_id: string | null;
  excluded_securities: Array<{
    instrument_id: number;
    symbol: string;
    exclusion_reason: string;
  }>;
  portfolio_value: string | null;
  position_count_after_exclusions: number | null;
  cumulative_return: string | null;
  equity_curve: Array<{
    trade_date: string;
    equity: string;
  }>;
  per_security_returns: Array<{
    instrument_id: number;
    symbol: string;
    entry_date: string;
    exit_date: string;
    exit_reason: string;
    realized_return: string;
  }>;
  error_message: string | null;
  result_summary: {
    trade_dates_evaluated: number;
    total_candidates_evaluated: number;
    qualifying_observations: number;
    unique_qualified_instruments: number;
    first_qualified_trade_date: string | null;
    last_qualified_trade_date: string | null;
    result_checksum: string | null;
  };
  parameter_set: {
    id: number;
    version: number;
    rps_threshold: number;
    high_proximity_threshold_pct: string;
    selected_rps_windows: number[];
  };
};

export type PortfolioReturnSourceScreenRun = {
  id: number;
  trade_date: string;
  strategy_configuration_version: number | null;
  status: string;
};

export type PortfolioReturnRunResult = {
  run: BacktestRun;
  cumulative_return: string | null;
  win_rate: string;
  max_drawdown: string;
  equity_curve: Array<{
    trade_date: string;
    equity: string;
  }>;
  per_security_returns: BacktestRun["per_security_returns"];
  source_screen_run: PortfolioReturnSourceScreenRun | null;
};

export type PortfolioReturnRunComparison = PortfolioReturnRunResult & {
  compare_dimensions: {
    holding_days: number | null;
    stop_loss_pct: string | null;
    portfolio_cap: number | null;
    source_screen_run_id: number | null;
    source_trade_date: string | null;
    strategy_configuration_version: number | null;
    rps_definition_version: string | null;
  };
  aligned_equity_curve: Array<{
    days_since_entry: number;
    equity: string;
  }>;
};

export type WatchlistEntry = {
  id: number;
  instrument_id: number;
  symbol: string;
  exchange: string;
  name: string | null;
  note: string | null;
  observation_reason: string | null;
  added_date: string;
  added_at: string;
};

export type Candlestick = {
  trade_date: string;
  open: string | null;
  high: string | null;
  low: string | null;
  close: string | null;
  adj_close: string | null;
  volume: number | null;
  data_status: string;
};

export type StockDetailPayload = {
  instrument: {
    id: number;
    symbol: string;
    exchange: string;
    name: string | null;
    currency: string;
  };
  screen_run: {
    id: number;
    trade_date: string;
    executed_at: string;
    status: string;
    strategy_configuration_version: number | null;
  };
  rule_breakdown: {
    passed: boolean;
    rps_condition: {
      passed: boolean;
      best_rps_value: string | null;
      threshold: number;
      rps_50: string | null;
      rps_120: string | null;
      rps_250: string | null;
    };
    high_proximity_condition: {
      passed: boolean;
      high_proximity_ratio: string | null;
      threshold_pct: string;
      max_drawdown_from_high_pct: string | null;
    };
  };
  latest_indicator_snapshot: {
    trade_date: string;
    rps_50: string | null;
    rps_120: string | null;
    rps_250: string | null;
    fifty_two_week_high: string | null;
    high_proximity_ratio: string | null;
  };
  candlesticks: Candlestick[];
  indicator_history: {
    trade_date: string;
    rps_50: string | null;
    rps_120: string | null;
    rps_250: string | null;
    high_proximity_ratio: string | null;
  }[];
};

export type FiscalYearValuation = {
  fiscal_year_label: string;
  fiscal_year_end_month: number;
  net_income: string | null;
  net_income_currency: string;
  pe: string | null;
  pb: string | null;
  data_status: string;
};

export type InlineAnalysisPayload = {
  instrument: {
    id: number;
    symbol: string;
    exchange: string;
    name: string | null;
    currency: string;
  };
  screen_run_ref: {
    id: number;
    trade_date: string;
  };
  candlesticks: Candlestick[];
  candlestick_window_days_available: number;
  valuation_by_fiscal_year: FiscalYearValuation[];
  generated_at: string;
};
