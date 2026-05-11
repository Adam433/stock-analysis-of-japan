from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.dashboard import (
    APPROVED_RPS_WINDOWS,
    DEFAULT_CUP_HANDLE_PARAMS,
    DEFAULT_FUNDAMENTAL_GROWTH_PARAMS,
    DEFAULT_CHART_WINDOW_DAYS,
    DEFAULT_RPS_THRESHOLD,
    CupHandleParams,
    FundamentalGrowthParams,
    get_chart_with_markers,
    get_overview,
    screen_universe,
)
from stockanalyse_api.services.dashboard_ingest import (
    DEFAULT_DASHBOARD_MATERIALIZE_SINCE_DAYS,
    get_job_state,
    trigger_fundamentals_refresh,
    trigger_update_and_materialize,
)
from stockanalyse_api.services.dashboard_strategy_backtest import (
    run_cup_handle_rps_backtest,
)

router = APIRouter(prefix="/dashboard", tags=["dashboard"])

_INDEX_HTML_PATH = Path(__file__).resolve().parent.parent / "templates" / "dashboard.html"
_CHART_HTML_PATH = Path(__file__).resolve().parent.parent / "templates" / "chart_view.html"


class CupHandleParamsRequest(BaseModel):
    min_cup_duration: int = Field(
        default=DEFAULT_CUP_HANDLE_PARAMS.min_cup_duration, ge=5, le=500
    )
    max_cup_duration: int = Field(
        default=DEFAULT_CUP_HANDLE_PARAMS.max_cup_duration, ge=5, le=500
    )
    min_handle_duration: int = Field(
        default=DEFAULT_CUP_HANDLE_PARAMS.min_handle_duration, ge=1, le=120
    )
    max_handle_duration: int = Field(
        default=DEFAULT_CUP_HANDLE_PARAMS.max_handle_duration, ge=1, le=120
    )
    min_total_duration: int = Field(
        default=DEFAULT_CUP_HANDLE_PARAMS.min_total_duration, ge=10, le=600
    )
    max_total_duration: int = Field(
        default=DEFAULT_CUP_HANDLE_PARAMS.max_total_duration, ge=10, le=600
    )
    min_cup_depth_pct: float = Field(
        default=float(DEFAULT_CUP_HANDLE_PARAMS.min_cup_depth_pct), ge=0, le=100
    )
    max_cup_depth_pct: float = Field(
        default=float(DEFAULT_CUP_HANDLE_PARAMS.max_cup_depth_pct), ge=0, le=100
    )
    min_handle_pullback_pct: float = Field(
        default=float(DEFAULT_CUP_HANDLE_PARAMS.min_handle_pullback_pct), ge=0, le=100
    )
    max_handle_pullback_pct: float = Field(
        default=float(DEFAULT_CUP_HANDLE_PARAMS.max_handle_pullback_pct), ge=0, le=100
    )
    max_right_lip_delta_pct: float = Field(
        default=float(DEFAULT_CUP_HANDLE_PARAMS.max_right_lip_delta_pct), ge=0, le=50
    )
    require_prior_uptrend: bool = DEFAULT_CUP_HANDLE_PARAMS.require_prior_uptrend
    prior_uptrend_lookback_days: int = Field(
        default=DEFAULT_CUP_HANDLE_PARAMS.prior_uptrend_lookback_days, ge=1, le=500
    )
    min_prior_uptrend_pct: float = Field(
        default=float(DEFAULT_CUP_HANDLE_PARAMS.min_prior_uptrend_pct), ge=0, le=300
    )
    min_handle_low_position_pct: float = Field(
        default=float(DEFAULT_CUP_HANDLE_PARAMS.min_handle_low_position_pct), ge=0, le=100
    )
    max_handle_depth_to_cup_depth_pct: float = Field(
        default=float(DEFAULT_CUP_HANDLE_PARAMS.max_handle_depth_to_cup_depth_pct),
        ge=0,
        le=100,
    )
    max_handle_high_above_lip_pct: float = Field(
        default=float(DEFAULT_CUP_HANDLE_PARAMS.max_handle_high_above_lip_pct),
        ge=0,
        le=50,
    )
    min_bottom_dwell_days: int = Field(
        default=DEFAULT_CUP_HANDLE_PARAMS.min_bottom_dwell_days, ge=1, le=120
    )
    bottom_zone_pct: float = Field(
        default=float(DEFAULT_CUP_HANDLE_PARAMS.bottom_zone_pct), ge=0, le=100
    )
    min_bottom_span_pct: float = Field(
        default=float(DEFAULT_CUP_HANDLE_PARAMS.min_bottom_span_pct), ge=0, le=100
    )
    min_cup_side_duration_pct: float = Field(
        default=float(DEFAULT_CUP_HANDLE_PARAMS.min_cup_side_duration_pct),
        ge=0,
        le=50,
    )
    require_breakout_volume: bool = DEFAULT_CUP_HANDLE_PARAMS.require_breakout_volume
    breakout_volume_avg_days: int = Field(
        default=DEFAULT_CUP_HANDLE_PARAMS.breakout_volume_avg_days, ge=1, le=250
    )
    min_breakout_volume_multiplier: float = Field(
        default=float(DEFAULT_CUP_HANDLE_PARAMS.min_breakout_volume_multiplier),
        ge=0,
        le=10,
    )
    breakout_lookback_days: int = Field(
        default=DEFAULT_CUP_HANDLE_PARAMS.breakout_lookback_days, ge=1, le=120
    )
    lookback_days: int = Field(
        default=DEFAULT_CUP_HANDLE_PARAMS.lookback_days, ge=30, le=750
    )

    def to_service_params(self) -> CupHandleParams:
        return CupHandleParams(
            min_cup_duration=self.min_cup_duration,
            max_cup_duration=self.max_cup_duration,
            min_handle_duration=self.min_handle_duration,
            max_handle_duration=self.max_handle_duration,
            min_total_duration=self.min_total_duration,
            max_total_duration=self.max_total_duration,
            min_cup_depth_pct=Decimal(str(self.min_cup_depth_pct)),
            max_cup_depth_pct=Decimal(str(self.max_cup_depth_pct)),
            min_handle_pullback_pct=Decimal(str(self.min_handle_pullback_pct)),
            max_handle_pullback_pct=Decimal(str(self.max_handle_pullback_pct)),
            max_right_lip_delta_pct=Decimal(str(self.max_right_lip_delta_pct)),
            require_prior_uptrend=self.require_prior_uptrend,
            prior_uptrend_lookback_days=self.prior_uptrend_lookback_days,
            min_prior_uptrend_pct=Decimal(str(self.min_prior_uptrend_pct)),
            min_handle_low_position_pct=Decimal(str(self.min_handle_low_position_pct)),
            max_handle_depth_to_cup_depth_pct=Decimal(
                str(self.max_handle_depth_to_cup_depth_pct)
            ),
            max_handle_high_above_lip_pct=Decimal(str(self.max_handle_high_above_lip_pct)),
            min_bottom_dwell_days=self.min_bottom_dwell_days,
            bottom_zone_pct=Decimal(str(self.bottom_zone_pct)),
            min_bottom_span_pct=Decimal(str(self.min_bottom_span_pct)),
            min_cup_side_duration_pct=Decimal(str(self.min_cup_side_duration_pct)),
            require_breakout_volume=self.require_breakout_volume,
            breakout_volume_avg_days=self.breakout_volume_avg_days,
            min_breakout_volume_multiplier=Decimal(str(self.min_breakout_volume_multiplier)),
            breakout_lookback_days=self.breakout_lookback_days,
            lookback_days=self.lookback_days,
        )


class FundamentalGrowthParamsRequest(BaseModel):
    enabled: bool = DEFAULT_FUNDAMENTAL_GROWTH_PARAMS.enabled
    min_years: int = Field(default=DEFAULT_FUNDAMENTAL_GROWTH_PARAMS.min_years, ge=2, le=10)
    min_growth_count: int | None = Field(default=None, ge=1, le=9)
    min_yoy_growth_pct: float = Field(
        default=float(DEFAULT_FUNDAMENTAL_GROWTH_PARAMS.min_yoy_growth_pct),
        ge=-100,
        le=500,
    )
    require_positive_net_income: bool = (
        DEFAULT_FUNDAMENTAL_GROWTH_PARAMS.require_positive_net_income
    )
    reporting_lag_days: int = Field(
        default=DEFAULT_FUNDAMENTAL_GROWTH_PARAMS.reporting_lag_days,
        ge=0,
        le=365,
    )
    max_pe: float | None = Field(default=None, gt=0, le=500)
    max_pb: float | None = Field(default=None, gt=0, le=100)
    require_positive_operating_cash_flow: bool = (
        DEFAULT_FUNDAMENTAL_GROWTH_PARAMS.require_positive_operating_cash_flow
    )
    require_positive_free_cash_flow: bool = (
        DEFAULT_FUNDAMENTAL_GROWTH_PARAMS.require_positive_free_cash_flow
    )
    min_operating_cash_flow_growth_count: int | None = Field(default=None, ge=1, le=9)
    min_operating_cash_flow_yoy_growth_pct: float = Field(
        default=float(
            DEFAULT_FUNDAMENTAL_GROWTH_PARAMS.min_operating_cash_flow_yoy_growth_pct
        ),
        ge=-100,
        le=500,
    )

    def to_service_params(self) -> FundamentalGrowthParams:
        return FundamentalGrowthParams(
            enabled=self.enabled,
            min_years=self.min_years,
            min_growth_count=self.min_growth_count,
            min_yoy_growth_pct=Decimal(str(self.min_yoy_growth_pct)),
            require_positive_net_income=self.require_positive_net_income,
            reporting_lag_days=self.reporting_lag_days,
            max_pe=None if self.max_pe is None else Decimal(str(self.max_pe)),
            max_pb=None if self.max_pb is None else Decimal(str(self.max_pb)),
            require_positive_operating_cash_flow=self.require_positive_operating_cash_flow,
            require_positive_free_cash_flow=self.require_positive_free_cash_flow,
            min_operating_cash_flow_growth_count=self.min_operating_cash_flow_growth_count,
            min_operating_cash_flow_yoy_growth_pct=Decimal(
                str(self.min_operating_cash_flow_yoy_growth_pct)
            ),
        )


class ScreenRequest(BaseModel):
    market: str = Field(default="jp", pattern="^(jp|us)$")
    use_rps: bool = False
    rps_threshold: int = Field(default=DEFAULT_RPS_THRESHOLD, ge=0, le=100)
    selected_rps_windows: list[int] = Field(default_factory=lambda: list(APPROVED_RPS_WINDOWS))
    min_rps_windows_passing: int = Field(default=2, ge=1, le=len(APPROVED_RPS_WINDOWS))
    use_cup_handle: bool = False
    cup_handle_params: CupHandleParamsRequest = Field(default_factory=CupHandleParamsRequest)
    fundamental_growth_params: FundamentalGrowthParamsRequest = Field(
        default_factory=FundamentalGrowthParamsRequest
    )
    trade_date: date | None = None


class ChartRequest(BaseModel):
    market: str = Field(default="jp", pattern="^(jp|us)$")
    use_rps: bool = False
    rps_threshold: int = Field(default=DEFAULT_RPS_THRESHOLD, ge=0, le=100)
    selected_rps_windows: list[int] = Field(default_factory=lambda: list(APPROVED_RPS_WINDOWS))
    min_rps_windows_passing: int = Field(default=2, ge=1, le=len(APPROVED_RPS_WINDOWS))
    use_cup_handle: bool = False
    cup_handle_params: CupHandleParamsRequest = Field(default_factory=CupHandleParamsRequest)
    fundamental_growth_params: FundamentalGrowthParamsRequest = Field(
        default_factory=FundamentalGrowthParamsRequest
    )
    trade_date: date | None = None
    window_days: int = Field(default=DEFAULT_CHART_WINDOW_DAYS, ge=30, le=750)


class CupHandleRpsBacktestRequest(BaseModel):
    market: str = Field(default="jp", pattern="^(jp|us)$")
    start_date: date
    end_date: date
    use_rps: bool = True
    rps_threshold: int = Field(default=DEFAULT_RPS_THRESHOLD, ge=0, le=100)
    selected_rps_windows: list[int] = Field(default_factory=lambda: list(APPROVED_RPS_WINDOWS))
    min_rps_windows_passing: int = Field(default=2, ge=1, le=len(APPROVED_RPS_WINDOWS))
    use_cup_handle: bool = True
    cup_handle_params: CupHandleParamsRequest = Field(default_factory=CupHandleParamsRequest)
    fundamental_growth_params: FundamentalGrowthParamsRequest = Field(
        default_factory=FundamentalGrowthParamsRequest
    )
    holding_days: int | None = Field(default=130, ge=1, le=500)
    stop_loss_pct: float = Field(default=-0.08, gt=-1, lt=0)
    take_profit_pct: float | None = Field(default=None, gt=0, le=10)
    rps_exit_threshold: int | None = Field(default=None, ge=0, le=100)
    portfolio_cap: int = Field(default=10, ge=1, le=200)
    position_weight_pct: float = Field(default=0.10, gt=0, le=1)
    initial_capital: float = Field(default=100000, gt=0, le=1_000_000_000)
    position_size_amount: float | None = Field(default=None, gt=0, le=1_000_000_000)
    allow_reentry_while_open: bool = False
    entry_delay_days: int = Field(default=0, ge=0, le=60)
    entry_deferral_window_days: int = Field(default=5, ge=1, le=60)
    max_trades_returned: int = Field(default=300, ge=0, le=2000)


@router.get("", response_class=HTMLResponse, include_in_schema=False)
@router.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard_index() -> HTMLResponse:
    html = _INDEX_HTML_PATH.read_text(encoding="utf-8")
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/chart-view", response_class=HTMLResponse, include_in_schema=False)
def dashboard_chart_view() -> HTMLResponse:
    html = _CHART_HTML_PATH.read_text(encoding="utf-8")
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/optimization-results/{result_id}", response_class=HTMLResponse, include_in_schema=False)
def dashboard_optimization_result_detail(result_id: int) -> HTMLResponse:
    html = """
<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>策略交易明细 · stockAnalyse</title>
<style>
  :root {
    --bg: #111315; --panel: #191c20; --panel-subtle: #14171a;
    --panel-border: #30363d; --text: #eef2f6; --muted: #9aa4b2;
    --accent: #2dd4bf; --bad: #f43f5e; --good: #22c55e;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--bg); color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC",
      "Microsoft YaHei", sans-serif; font-size: 14px;
  }
  header {
    padding: 16px 24px; border-bottom: 1px solid var(--panel-border);
    display: flex; align-items: center; justify-content: space-between; gap: 12px;
    background: var(--panel);
  }
  h1 { margin: 0; font-size: 18px; }
  main { padding: 16px 24px 28px; }
  .card {
    background: var(--panel); border: 1px solid var(--panel-border);
    border-radius: 8px; padding: 14px 16px; margin-bottom: 12px;
  }
  .muted { color: var(--muted); }
  .grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }
  .stat { background: var(--panel-subtle); border-radius: 8px; padding: 10px; }
  .stat .label { color: var(--muted); font-size: 12px; }
  .stat .value { font-weight: 700; margin-top: 4px; overflow-wrap: anywhere; }
  .table-wrap { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { text-align: left; padding: 8px; border-bottom: 1px solid var(--panel-border); }
  th { color: var(--muted); font-weight: 600; white-space: nowrap; }
  td { vertical-align: top; }
  .reason { min-width: 180px; }
  .gain { color: var(--good); }
  .loss { color: var(--bad); }
  button {
    border: 1px solid var(--panel-border); border-radius: 8px; padding: 8px 12px;
    background: transparent; color: var(--text); cursor: pointer; font-weight: 650;
  }
  .tabs { display: flex; gap: 8px; margin-bottom: 10px; flex-wrap: wrap; }
  .tabs button.active { background: var(--accent); color: #06201d; border-color: transparent; }
  .empty { color: var(--muted); padding: 20px 0; text-align: center; }
  @media (max-width: 760px) {
    header { align-items: flex-start; flex-direction: column; }
    main { padding: 12px; }
    .grid { grid-template-columns: 1fr 1fr; }
  }
</style>
</head>
<body>
<header>
  <div>
    <h1>策略交易明细</h1>
    <div class="muted" id="subtitle">加载中...</div>
  </div>
  <button type="button" onclick="window.location.href='/dashboard'">返回 Dashboard</button>
</header>
<main>
  <div class="card">
    <div class="grid" id="summaryGrid"></div>
  </div>
  <div class="card">
    <div class="tabs">
      <button type="button" class="active" data-period="validation">验证期</button>
      <button type="button" data-period="train">训练期</button>
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>股票代码</th>
            <th>信号日</th>
            <th>买入时间</th>
            <th>买入价</th>
            <th>买入原因</th>
            <th>卖出时间</th>
            <th>卖出价</th>
            <th>卖出原因</th>
            <th>收益</th>
            <th>资金</th>
            <th>RPS</th>
          </tr>
        </thead>
        <tbody id="tradeBody"></tbody>
      </table>
    </div>
    <div class="empty" id="emptyState" style="display:none;">没有可显示的交易明细</div>
  </div>
</main>
<script>
  const resultId = __RESULT_ID__;
  let detail = null;
  let activePeriod = 'validation';

  function escapeHtml(value) {
    return String(value ?? '—').replace(/[&<>"']/g, (char) => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[char]));
  }

  function pct(value) {
    if (value === null || value === undefined || value === '—') return '—';
    const number = Number(value);
    if (!Number.isFinite(number)) return escapeHtml(value);
    return `${(number * 100).toFixed(2)}%`;
  }

  function money(value) {
    if (value === null || value === undefined || value === '') return '—';
    const number = Number(value);
    if (!Number.isFinite(number)) return escapeHtml(value);
    return number.toLocaleString(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }

  async function api(path) {
    const res = await fetch(path);
    if (!res.ok) {
      let detailText = res.statusText;
      try { detailText = (await res.json()).detail || detailText; } catch (_) {}
      throw new Error(detailText);
    }
    return res.json();
  }

  function stat(label, value) {
    return `<div class="stat"><div class="label">${escapeHtml(label)}</div><div class="value">${escapeHtml(value)}</div></div>`;
  }

  function formatHoldingDays(value) {
    return value === null || value === undefined || value === '' ? '不限定' : value;
  }

  function renderSummary() {
    const result = detail.optimization_result || {};
    const params = detail.parameters || {};
    const metrics = (result.validation_metrics || result.train_metrics || {});
    const trainMetrics = result.train_metrics || {};
    const validationMetrics = result.validation_metrics || {};
    document.getElementById('subtitle').textContent =
      `结果 #${result.id} · 任务 #${result.optimization_run_id} · 排名 ${result.rank ?? '—'}`;
    document.getElementById('summaryGrid').innerHTML = [
      stat('参数', `RPS ${params.rps_threshold ?? '—'} · ${(params.selected_rps_windows || []).join('+') || '—'}`),
      stat('卖点', `止损 ${params.stop_loss_pct ?? '—'} · RPS退 ${params.rps_exit_threshold ?? '—'} · 持有 ${formatHoldingDays(params.holding_days)}`),
      stat('交易', `${metrics.completed_trades ?? '—'} 笔 · 胜率 ${pct(metrics.win_rate)}`),
      stat('收益/风险', `总 ${pct(metrics.total_return)} · 回撤 ${pct(metrics.max_drawdown)}`),
      stat('资金', `初始 ${money(params.initial_capital || metrics.initial_capital)} · 每笔 ${money(params.position_size_amount || metrics.position_size_amount)}`),
      stat('训练终值', `${money(trainMetrics.final_capital)} · 盈亏 ${money(trainMetrics.total_profit)}`),
      stat('验证终值', `${money(validationMetrics.final_capital)} · 盈亏 ${money(validationMetrics.total_profit)}`),
    ].join('');
  }

  function activeTrades() {
    const section = detail[activePeriod];
    return section && Array.isArray(section.trades) ? section.trades : [];
  }

  function renderTrades() {
    const trades = activeTrades();
    const body = document.getElementById('tradeBody');
    document.getElementById('emptyState').style.display = trades.length ? 'none' : 'block';
    body.innerHTML = trades.map((trade) => {
      const ret = Number(trade.realized_return);
      const returnClass = Number.isFinite(ret) && ret >= 0 ? 'gain' : 'loss';
      return `
        <tr>
          <td>${escapeHtml(trade.symbol)}</td>
          <td>${escapeHtml(trade.signal_date)}</td>
          <td>${escapeHtml(trade.entry_date)}</td>
          <td>${escapeHtml(trade.entry_price)}</td>
          <td class="reason">${escapeHtml(trade.entry_reason)}</td>
          <td>${escapeHtml(trade.exit_date)}</td>
          <td>${escapeHtml(trade.exit_price)}</td>
          <td class="reason">${escapeHtml(trade.exit_reason_label || trade.exit_reason)}</td>
          <td class="${returnClass}">${pct(trade.realized_return)}</td>
          <td>${money(trade.realized_profit)}<div class="muted">${money(trade.invested_cash)} → ${money(trade.exit_cash)}</div></td>
          <td>${escapeHtml(trade.rps_score)}</td>
        </tr>
      `;
    }).join('');
  }

  document.querySelectorAll('[data-period]').forEach((button) => {
    button.addEventListener('click', () => {
      activePeriod = button.dataset.period;
      document.querySelectorAll('[data-period]').forEach((item) =>
        item.classList.toggle('active', item === button)
      );
      renderTrades();
    });
  });

  api(`/backtests/optimization/results/${resultId}/detail?max_trades_returned=300`)
    .then((payload) => {
      detail = payload.detail;
      renderSummary();
      if (!detail.validation) {
        activePeriod = 'train';
        document.querySelector('[data-period="validation"]').style.display = 'none';
        document.querySelector('[data-period="train"]').classList.add('active');
      }
      renderTrades();
    })
    .catch((err) => {
      document.getElementById('subtitle').textContent = `加载失败：${err.message}`;
      document.getElementById('emptyState').style.display = 'block';
    });
</script>
</body>
</html>
"""
    return HTMLResponse(
        content=html.replace("__RESULT_ID__", str(result_id)),
        headers={"Cache-Control": "no-store"},
    )


@router.get("/api/overview")
def read_overview(market: str = "jp") -> dict[str, object]:
    try:
        with SessionLocal() as session:
            return {"overview": get_overview(session, market=market).to_dict()}
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/api/screen")
def post_screen(payload: ScreenRequest) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            return screen_universe(
                session,
                use_rps=payload.use_rps,
                rps_threshold=payload.rps_threshold,
                selected_rps_windows=payload.selected_rps_windows,
                min_rps_windows_passing=payload.min_rps_windows_passing,
                use_cup_handle=payload.use_cup_handle,
                cup_handle_params=payload.cup_handle_params.to_service_params(),
                fundamental_growth_params=payload.fundamental_growth_params.to_service_params(),
                trade_date=payload.trade_date,
                market=payload.market,
            )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


class IngestRequest(BaseModel):
    market: str = Field(default="jp", pattern="^(jp|us)$")
    materialize_since_days: int | None = DEFAULT_DASHBOARD_MATERIALIZE_SINCE_DAYS
    skip_refresh: bool = False
    skip_materialize: bool = False


@router.post("/api/update")
def post_update_and_materialize(payload: IngestRequest | None = None) -> dict[str, object]:
    payload = payload or IngestRequest()
    return trigger_update_and_materialize(
        materialize_since_days=payload.materialize_since_days,
        skip_refresh=payload.skip_refresh,
        skip_materialize=payload.skip_materialize,
        market=payload.market,
    )


@router.post("/api/update/refresh")
def post_update_market_data(payload: IngestRequest | None = None) -> dict[str, object]:
    payload = payload or IngestRequest()
    return trigger_update_and_materialize(
        materialize_since_days=None,
        skip_refresh=False,
        skip_materialize=True,
        market=payload.market,
    )


@router.post("/api/update/materialize")
def post_materialize_indicators(payload: IngestRequest | None = None) -> dict[str, object]:
    payload = payload or IngestRequest(skip_refresh=True)
    return trigger_update_and_materialize(
        materialize_since_days=payload.materialize_since_days,
        skip_refresh=True,
        skip_materialize=False,
        market=payload.market,
    )


@router.post("/api/update/fundamentals")
def post_refresh_fundamentals(payload: IngestRequest | None = None) -> dict[str, object]:
    payload = payload or IngestRequest(market="us")
    return trigger_fundamentals_refresh(market=payload.market)


@router.get("/api/update/status")
def read_update_status() -> dict[str, object]:
    return {"state": get_job_state()}


@router.post("/api/backtest/cup-handle-rps")
def post_cup_handle_rps_backtest(payload: CupHandleRpsBacktestRequest) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            result = run_cup_handle_rps_backtest(
                session,
                start_date=payload.start_date,
                end_date=payload.end_date,
                use_rps=payload.use_rps,
                rps_threshold=payload.rps_threshold,
                selected_rps_windows=payload.selected_rps_windows,
                min_rps_windows_passing=payload.min_rps_windows_passing,
                use_cup_handle=payload.use_cup_handle,
                cup_handle_params=payload.cup_handle_params.to_service_params(),
                fundamental_growth_params=payload.fundamental_growth_params.to_service_params(),
                market=payload.market,
                holding_days=payload.holding_days,
                stop_loss_pct=Decimal(str(payload.stop_loss_pct)),
                take_profit_pct=(
                    Decimal(str(payload.take_profit_pct))
                    if payload.take_profit_pct is not None
                    else None
                ),
                rps_exit_threshold=payload.rps_exit_threshold,
                portfolio_cap=payload.portfolio_cap,
                position_weight_pct=Decimal(str(payload.position_weight_pct)),
                initial_capital=Decimal(str(payload.initial_capital)),
                position_size_amount=(
                    Decimal(str(payload.position_size_amount))
                    if payload.position_size_amount is not None
                    else None
                ),
                allow_reentry_while_open=payload.allow_reentry_while_open,
                entry_delay_days=payload.entry_delay_days,
                entry_deferral_window_days=payload.entry_deferral_window_days,
                max_trades_returned=payload.max_trades_returned,
            )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return {"result": result.to_dict()}


@router.post("/api/chart/{instrument_id}")
def post_chart(instrument_id: int, payload: ChartRequest) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            result = get_chart_with_markers(
                session,
                instrument_id=instrument_id,
                use_rps=payload.use_rps,
                rps_threshold=payload.rps_threshold,
                selected_rps_windows=payload.selected_rps_windows,
                min_rps_windows_passing=payload.min_rps_windows_passing,
                use_cup_handle=payload.use_cup_handle,
                cup_handle_params=payload.cup_handle_params.to_service_params(),
                trade_date=payload.trade_date,
                window_days=payload.window_days,
                market=payload.market,
            )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(status_code=404, detail="Instrument or candles not found.")
    return result
