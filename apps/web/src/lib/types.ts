export type StrategyConfiguration = {
  id: number;
  version: number;
  rps_threshold: number;
  high_proximity_threshold_pct: string;
  selected_rps_windows: number[];
  min_rps_lines_required: number;
};

export type StrategyConfigurationResponse = {
  configuration: StrategyConfiguration;
  validation?: {
    rps_threshold: { min: number; max: number; default: number };
    high_proximity_threshold_pct: { min: string; max: string; default: string };
    selected_rps_windows: { approved: number[]; default: number[] };
    min_rps_lines_required: { min: number; max: number; default: number };
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
    min_rps_lines_required: number;
  };
  qualified_results: ScreenRunResult[];
};

export type BacktestRun = {
  id: number;
  strategy_configuration_id: number;
  status: string;
  start_date: string;
  end_date: string;
  started_at: string;
  completed_at: string | null;
  rps_definition_version: string | null;
  dataset_trade_date_start: string | null;
  dataset_trade_date_end: string | null;
  dataset_checksum: string | null;
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
    min_rps_lines_required: number;
  };
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
