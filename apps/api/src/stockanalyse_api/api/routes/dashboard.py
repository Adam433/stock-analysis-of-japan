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
    trigger_update_and_materialize,
)
from stockanalyse_api.services.dashboard_strategy_backtest import (
    run_cup_handle_rps_backtest,
)

router = APIRouter(prefix="/dashboard", tags=["dashboard"])

_INDEX_HTML_PATH = Path(__file__).resolve().parent.parent / "templates" / "dashboard.html"


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

    def to_service_params(self) -> FundamentalGrowthParams:
        return FundamentalGrowthParams(
            enabled=self.enabled,
            min_years=self.min_years,
            min_growth_count=self.min_growth_count,
            min_yoy_growth_pct=Decimal(str(self.min_yoy_growth_pct)),
            require_positive_net_income=self.require_positive_net_income,
            reporting_lag_days=self.reporting_lag_days,
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
    rps_threshold: int = Field(default=DEFAULT_RPS_THRESHOLD, ge=0, le=100)
    selected_rps_windows: list[int] = Field(default_factory=lambda: list(APPROVED_RPS_WINDOWS))
    min_rps_windows_passing: int = Field(default=2, ge=1, le=len(APPROVED_RPS_WINDOWS))
    cup_handle_params: CupHandleParamsRequest = Field(default_factory=CupHandleParamsRequest)
    fundamental_growth_params: FundamentalGrowthParamsRequest = Field(
        default_factory=FundamentalGrowthParamsRequest
    )
    holding_days: int = Field(default=130, ge=1, le=500)
    stop_loss_pct: float = Field(default=-0.08, gt=-1, lt=0)
    portfolio_cap: int = Field(default=20, ge=1, le=200)
    entry_deferral_window_days: int = Field(default=5, ge=1, le=60)
    max_trades_returned: int = Field(default=300, ge=0, le=2000)


@router.get("", response_class=HTMLResponse, include_in_schema=False)
@router.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard_index() -> HTMLResponse:
    html = _INDEX_HTML_PATH.read_text(encoding="utf-8")
    return HTMLResponse(content=html)


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
                rps_threshold=payload.rps_threshold,
                selected_rps_windows=payload.selected_rps_windows,
                min_rps_windows_passing=payload.min_rps_windows_passing,
                cup_handle_params=payload.cup_handle_params.to_service_params(),
                fundamental_growth_params=payload.fundamental_growth_params.to_service_params(),
                market=payload.market,
                holding_days=payload.holding_days,
                stop_loss_pct=Decimal(str(payload.stop_loss_pct)),
                portfolio_cap=payload.portfolio_cap,
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
