from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from hashlib import sha256

from sqlalchemy import desc, select

from stockanalyse_api.domain.backtests.models import BacktestRun
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.domain.screens.models import ScreenRun, ScreenRunResult, StrategyConfiguration
from stockanalyse_api.services.backtesting import BacktestRunSummary, serialize_backtest_run
from stockanalyse_api.services.portfolio_backtest_defaults import (
    MVP_ENTRY_DEFERRAL_WINDOW_DAYS,
    MVP_HOLDING_DAYS,
    MVP_PORTFOLIO_CAP,
    MVP_PORTFOLIO_VALUE,
    MVP_STOP_LOSS_PCT,
)
DEBOUNCE_WINDOW_SECONDS = 5
RANKING_POLICY_ID = "rps_desc_ticker_asc_v1"
ERROR_MESSAGE_DATA_INSUFFICIENT = "数据不足以完成持有期"
RATIO_PATTERN = Decimal("0.000001")


class DataInsufficientError(ValueError):
    """Raised when market data does not extend far enough to finish the simulation."""


def dispatch_portfolio_return_backtest_execution(session, run: BacktestRun) -> None:
    execute_portfolio_return_backtest(session, run.id)


def _coerce_decimal(value: Decimal | float | int | str | None, *, field_name: str) -> Decimal:
    if value is None:
        raise ValueError(f"{field_name} is required.")

    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be a decimal number.") from exc


def _quantize_ratio(value: Decimal) -> Decimal:
    return value.quantize(RATIO_PATTERN, rounding=ROUND_HALF_UP)


def _dump_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash_payload(value: object) -> str:
    return sha256(_dump_json(value).encode("utf-8")).hexdigest()


def _append_dataset_tuple(
    dataset_tuples: set[str],
    *,
    instrument_id: int,
    symbol: str,
    trade_date: date,
    row: MarketDataDaily | None,
    consulted_trade_dates: set[date] | None = None,
) -> None:
    if consulted_trade_dates is not None:
        consulted_trade_dates.add(trade_date)

    if row is None:
        dataset_tuples.add(f"{trade_date.isoformat()}:{instrument_id}:{symbol}:missing")
        return

    dataset_tuples.add(
        ":".join(
            [
                trade_date.isoformat(),
                str(instrument_id),
                symbol,
                row.data_status,
                f"{row.open:.6f}" if row.open is not None else "null",
                f"{row.close:.6f}" if row.close is not None else "null",
                f"{row.adj_close:.6f}" if row.adj_close is not None else "null",
            ]
        )
    )


def _is_valid_open(row: MarketDataDaily | None) -> bool:
    return row is not None and row.data_status != "unavailable" and row.open is not None


def _normalize_launch_parameters(
    *,
    holding_days: int | None,
    stop_loss_pct: Decimal | float | int | str | None,
    portfolio_cap: int | None,
    entry_deferral_window_days: int | None,
) -> tuple[int, Decimal, int, int]:
    normalized_holding_days = MVP_HOLDING_DAYS if holding_days is None else int(holding_days)
    normalized_stop_loss_pct = MVP_STOP_LOSS_PCT if stop_loss_pct is None else _coerce_decimal(
        stop_loss_pct,
        field_name="stop_loss_pct",
    )
    normalized_portfolio_cap = MVP_PORTFOLIO_CAP if portfolio_cap is None else int(portfolio_cap)
    normalized_entry_deferral_window_days = (
        MVP_ENTRY_DEFERRAL_WINDOW_DAYS
        if entry_deferral_window_days is None
        else int(entry_deferral_window_days)
    )

    if normalized_holding_days < 1:
        raise ValueError("holding_days must be an integer greater than or equal to 1.")
    if normalized_stop_loss_pct <= Decimal("-1") or normalized_stop_loss_pct >= Decimal("0"):
        raise ValueError("stop_loss_pct must be greater than -1 and less than 0.")
    if normalized_portfolio_cap < 1:
        raise ValueError("portfolio_cap must be an integer greater than or equal to 1.")
    if normalized_entry_deferral_window_days < 1:
        raise ValueError("entry_deferral_window_days must be an integer greater than or equal to 1.")

    return (
        normalized_holding_days,
        normalized_stop_loss_pct.quantize(Decimal("0.0001")),
        normalized_portfolio_cap,
        normalized_entry_deferral_window_days,
    )


def _load_screen_run(session, *, screen_run_id: int) -> ScreenRun:
    screen_run = session.get(ScreenRun, screen_run_id)
    if screen_run is None:
        raise LookupError("Screen run not found.")
    if screen_run.status != "completed":
        raise ValueError("Only completed screen runs can launch a portfolio-return backtest.")
    return screen_run


def _load_strategy_configuration(session, *, strategy_configuration_id: int) -> StrategyConfiguration:
    configuration = session.get(StrategyConfiguration, strategy_configuration_id)
    if configuration is None:
        raise ValueError("Strategy configuration not found for screen run.")
    return configuration


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _find_latest_matching_run(
    session,
    *,
    screen_run_id: int,
    holding_days: int,
    stop_loss_pct: Decimal,
    portfolio_cap: int,
    entry_deferral_window_days: int,
) -> BacktestRun | None:
    return session.execute(
        select(BacktestRun)
        .where(
            BacktestRun.backtest_lifecycle == "portfolio_return",
            BacktestRun.source_screen_run_id == screen_run_id,
            BacktestRun.effective_holding_days == holding_days,
            BacktestRun.effective_stop_loss_pct == stop_loss_pct,
            BacktestRun.effective_portfolio_cap == portfolio_cap,
            BacktestRun.effective_entry_deferral_window_days == entry_deferral_window_days,
        )
        .order_by(desc(BacktestRun.started_at), desc(BacktestRun.id))
        .limit(1)
    ).scalar_one_or_none()


def _prepare_run_for_launch(
    run: BacktestRun,
    *,
    screen_run: ScreenRun,
    holding_days: int,
    stop_loss_pct: Decimal,
    portfolio_cap: int,
    entry_deferral_window_days: int,
) -> None:
    now = datetime.now(UTC)
    run.strategy_configuration_id = screen_run.strategy_configuration_id
    run.source_screen_run_id = screen_run.id
    run.start_date = screen_run.trade_date
    run.end_date = screen_run.trade_date
    run.started_at = now
    run.completed_at = None
    run.rps_definition_version = None
    run.backtest_lifecycle = "portfolio_return"
    run.status = "running"
    run.dataset_trade_date_start = None
    run.dataset_trade_date_end = None
    run.dataset_checksum = None
    run.trade_dates_evaluated = 0
    run.total_candidates_evaluated = 0
    run.qualifying_observations = 0
    run.unique_qualified_instruments = 0
    run.first_qualified_trade_date = None
    run.last_qualified_trade_date = None
    run.result_checksum = None
    run.error_message = None
    run.effective_holding_days = holding_days
    run.effective_stop_loss_pct = stop_loss_pct
    run.effective_portfolio_cap = portfolio_cap
    run.effective_entry_deferral_window_days = entry_deferral_window_days
    run.ranking_policy_id = RANKING_POLICY_ID
    run.excluded_securities_json = _dump_json([])
    run.portfolio_value = MVP_PORTFOLIO_VALUE
    run.position_count_after_exclusions = 0
    run.cumulative_return = Decimal("0")
    run.equity_curve_json = _dump_json([])
    run.per_security_returns_json = _dump_json([])


def _persist_failed_recoverable(session, run: BacktestRun, *, message: str) -> None:
    run.status = "failed-recoverable"
    run.error_message = message
    run.completed_at = datetime.now(UTC)
    session.commit()
    session.refresh(run)


def _persist_data_insufficient(session, run: BacktestRun) -> None:
    run.status = "failed-data-insufficient"
    run.error_message = ERROR_MESSAGE_DATA_INSUFFICIENT
    run.completed_at = datetime.now(UTC)
    session.commit()
    session.refresh(run)


def _apply_dataset_context(
    run: BacktestRun,
    *,
    consulted_trade_dates: set[date],
    dataset_tuples: set[str],
    excluded_securities: list[dict[str, object]],
) -> None:
    run.ranking_policy_id = RANKING_POLICY_ID
    run.excluded_securities_json = _dump_json(excluded_securities)
    run.dataset_trade_date_start = min(consulted_trade_dates) if consulted_trade_dates else None
    run.dataset_trade_date_end = max(consulted_trade_dates) if consulted_trade_dates else None
    run.dataset_checksum = sha256("|".join(sorted(dataset_tuples)).encode("utf-8")).hexdigest()


def _load_qualified_candidates(session, *, screen_run_id: int) -> list[dict[str, object]]:
    rows = session.execute(
        select(ScreenRunResult, Instrument)
        .join(Instrument, Instrument.id == ScreenRunResult.instrument_id)
        .where(ScreenRunResult.screen_run_id == screen_run_id, ScreenRunResult.passed.is_(True))
    ).all()

    candidates = [
        {
            "instrument_id": instrument.id,
            "symbol": instrument.symbol,
            "best_rps_value": result.best_rps_value or Decimal("0"),
        }
        for result, instrument in rows
    ]
    candidates.sort(key=lambda item: (-Decimal(item["best_rps_value"]), str(item["symbol"])))
    return candidates


def _load_market_data_rows(
    session,
    *,
    instrument_ids: list[int],
    screen_trade_date: date,
) -> tuple[dict[int, dict[date, MarketDataDaily]], list[date]]:
    if not instrument_ids:
        return {}, []

    rows = session.execute(
        select(MarketDataDaily)
        .where(
            MarketDataDaily.instrument_id.in_(instrument_ids),
            MarketDataDaily.trade_date > screen_trade_date,
        )
        .order_by(MarketDataDaily.trade_date.asc(), MarketDataDaily.instrument_id.asc())
    ).scalars().all()

    market_rows_by_instrument: dict[int, dict[date, MarketDataDaily]] = {}
    trade_dates: list[date] = []
    seen_trade_dates: set[date] = set()
    for row in rows:
        market_rows_by_instrument.setdefault(row.instrument_id, {})[row.trade_date] = row
        if row.trade_date not in seen_trade_dates:
            seen_trade_dates.add(row.trade_date)
            trade_dates.append(row.trade_date)

    return market_rows_by_instrument, trade_dates


def _resolve_entry_plan(
    *,
    candidate: dict[str, object],
    instrument_rows: dict[date, MarketDataDaily],
    global_trade_dates: list[date],
    screen_trade_date: date,
    entry_deferral_window_days: int,
    dataset_tuples: set[str],
    consulted_trade_dates: set[date],
) -> tuple[date, Decimal] | tuple[None, None, str]:
    window_trade_dates = [trade_date for trade_date in global_trade_dates if trade_date > screen_trade_date][
        :entry_deferral_window_days
    ]
    saw_non_unavailable_row = False

    for trade_date in window_trade_dates:
        row = instrument_rows.get(trade_date)
        _append_dataset_tuple(
            dataset_tuples,
            instrument_id=int(candidate["instrument_id"]),
            symbol=str(candidate["symbol"]),
            trade_date=trade_date,
            row=row,
            consulted_trade_dates=consulted_trade_dates,
        )
        if row is not None and row.data_status != "unavailable":
            saw_non_unavailable_row = True
        if _is_valid_open(row):
            assert row is not None and row.open is not None
            return trade_date, Decimal(row.open)

    if len(window_trade_dates) < entry_deferral_window_days:
        raise DataInsufficientError(ERROR_MESSAGE_DATA_INSUFFICIENT)
    if saw_non_unavailable_row:
        return None, None, "no_valid_open_in_deferral_window"
    return None, None, "suspended_delisted_or_corp_action_in_deferral_window"


def _find_next_valid_exit_open(
    *,
    candidate: dict[str, object],
    instrument_rows: dict[date, MarketDataDaily],
    global_trade_dates: list[date],
    after_trade_date: date,
    dataset_tuples: set[str],
    consulted_trade_dates: set[date],
) -> tuple[date, Decimal]:
    found_future_trade_date = False

    for trade_date in global_trade_dates:
        if trade_date <= after_trade_date:
            continue
        found_future_trade_date = True
        row = instrument_rows.get(trade_date)
        _append_dataset_tuple(
            dataset_tuples,
            instrument_id=int(candidate["instrument_id"]),
            symbol=str(candidate["symbol"]),
            trade_date=trade_date,
            row=row,
            consulted_trade_dates=consulted_trade_dates,
        )
        if _is_valid_open(row):
            assert row is not None and row.open is not None
            return trade_date, Decimal(row.open)

    if found_future_trade_date:
        raise DataInsufficientError(ERROR_MESSAGE_DATA_INSUFFICIENT)
    raise DataInsufficientError(ERROR_MESSAGE_DATA_INSUFFICIENT)


def _resolve_position_result(
    *,
    candidate: dict[str, object],
    instrument_rows: dict[date, MarketDataDaily],
    global_trade_dates: list[date],
    entry_date: date,
    entry_price: Decimal,
    holding_days: int,
    stop_loss_pct: Decimal,
    dataset_tuples: set[str],
    consulted_trade_dates: set[date],
) -> dict[str, object]:
    entry_index = global_trade_dates.index(entry_date)
    last_mark_price = entry_price
    breach_date: date | None = None
    held_trading_days = 0
    expiry_signal_date: date | None = None

    for trade_date in global_trade_dates[entry_index:]:
        row = instrument_rows.get(trade_date)
        _append_dataset_tuple(
            dataset_tuples,
            instrument_id=int(candidate["instrument_id"]),
            symbol=str(candidate["symbol"]),
            trade_date=trade_date,
            row=row,
            consulted_trade_dates=consulted_trade_dates,
        )
        if row is not None and row.data_status != "unavailable" and row.adj_close is not None:
            last_mark_price = Decimal(row.adj_close)

        held_trading_days += 1
        if _quantize_ratio((last_mark_price / entry_price) - Decimal("1")) <= stop_loss_pct:
            breach_date = trade_date
            break
        if held_trading_days >= holding_days:
            expiry_signal_date = trade_date
            break

    if breach_date is not None:
        exit_reason = "stop_loss"
        trigger_trade_date = breach_date
    else:
        if expiry_signal_date is None:
            raise DataInsufficientError(ERROR_MESSAGE_DATA_INSUFFICIENT)
        exit_reason = "holding_period_elapsed"
        trigger_trade_date = expiry_signal_date

    exit_date, exit_price = _find_next_valid_exit_open(
        candidate=candidate,
        instrument_rows=instrument_rows,
        global_trade_dates=global_trade_dates,
        after_trade_date=trigger_trade_date,
        dataset_tuples=dataset_tuples,
        consulted_trade_dates=consulted_trade_dates,
    )
    realized_return = _quantize_ratio((exit_price / entry_price) - Decimal("1"))

    return {
        "instrument_id": int(candidate["instrument_id"]),
        "symbol": str(candidate["symbol"]),
        "entry_date": entry_date,
        "entry_price": entry_price,
        "exit_date": exit_date,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "realized_return": realized_return,
    }


def _build_equity_curve(
    *,
    positions: list[dict[str, object]],
    market_rows_by_instrument: dict[int, dict[date, MarketDataDaily]],
    global_trade_dates: list[date],
) -> list[dict[str, str]]:
    if not positions:
        return []

    first_entry_date = min(position["entry_date"] for position in positions)
    last_exit_date = max(position["exit_date"] for position in positions)
    relevant_trade_dates = [
        trade_date for trade_date in global_trade_dates if first_entry_date <= trade_date <= last_exit_date
    ]
    equity_curve: list[dict[str, str]] = []
    weight = _quantize_ratio(MVP_PORTFOLIO_VALUE / Decimal(len(positions)))
    last_mark_price_by_instrument = {
        int(position["instrument_id"]): Decimal(position["entry_price"]) for position in positions
    }

    for trade_date in relevant_trade_dates:
        equity = Decimal("0")
        for position in positions:
            entry_date = position["entry_date"]
            exit_date = position["exit_date"]
            entry_price = position["entry_price"]
            realized_return = position["realized_return"]
            instrument_rows = market_rows_by_instrument[int(position["instrument_id"])]

            if trade_date < entry_date:
                contribution = weight
            elif trade_date >= exit_date:
                contribution = weight * (Decimal("1") + realized_return)
            else:
                row = instrument_rows.get(trade_date)
                if row is not None and row.data_status != "unavailable" and row.adj_close is not None:
                    mark_price = Decimal(row.adj_close)
                    last_mark_price_by_instrument[int(position["instrument_id"])] = mark_price
                else:
                    mark_price = last_mark_price_by_instrument[int(position["instrument_id"])]
                contribution = weight * (mark_price / entry_price)

            equity += contribution

        equity_curve.append(
            {
                "trade_date": trade_date.isoformat(),
                "equity": f"{_quantize_ratio(equity):.6f}",
            }
        )

    return equity_curve

def execute_portfolio_return_backtest(session, run_id: int) -> BacktestRunSummary:
    run = session.get(BacktestRun, run_id)
    if run is None:
        raise LookupError("Backtest run not found.")
    if run.backtest_lifecycle != "portfolio_return":
        raise ValueError("Only portfolio_return runs can use the portfolio execution engine.")
    if run.source_screen_run_id is None:
        raise ValueError("Portfolio-return backtest run is missing source_screen_run_id.")

    configuration = _load_strategy_configuration(session, strategy_configuration_id=run.strategy_configuration_id)
    screen_run = _load_screen_run(session, screen_run_id=run.source_screen_run_id)
    candidates = _load_qualified_candidates(session, screen_run_id=screen_run.id)
    selected_candidates = candidates[: int(run.effective_portfolio_cap or 0)]
    excluded_securities = [
        {
            "instrument_id": int(candidate["instrument_id"]),
            "symbol": str(candidate["symbol"]),
            "exclusion_reason": "cap_overflow",
        }
        for candidate in candidates[int(run.effective_portfolio_cap or 0) :]
    ]

    market_rows_by_instrument, global_trade_dates = _load_market_data_rows(
        session,
        instrument_ids=[int(candidate["instrument_id"]) for candidate in selected_candidates],
        screen_trade_date=screen_run.trade_date,
    )
    dataset_tuples: set[str] = set()
    consulted_trade_dates: set[date] = set()
    positions: list[dict[str, object]] = []

    try:
        for candidate in selected_candidates:
            instrument_id = int(candidate["instrument_id"])
            instrument_rows = market_rows_by_instrument.get(instrument_id, {})
            entry_resolution = _resolve_entry_plan(
                candidate=candidate,
                instrument_rows=instrument_rows,
                global_trade_dates=global_trade_dates,
                screen_trade_date=screen_run.trade_date,
                entry_deferral_window_days=int(run.effective_entry_deferral_window_days or 0),
                dataset_tuples=dataset_tuples,
                consulted_trade_dates=consulted_trade_dates,
            )

            if len(entry_resolution) == 3:
                _, _, reason = entry_resolution
                excluded_securities.append(
                    {
                        "instrument_id": instrument_id,
                        "symbol": str(candidate["symbol"]),
                        "exclusion_reason": reason,
                    }
                )
                continue

            entry_date, entry_price = entry_resolution
            assert entry_date is not None and entry_price is not None
            position = _resolve_position_result(
                candidate=candidate,
                instrument_rows=instrument_rows,
                global_trade_dates=global_trade_dates,
                entry_date=entry_date,
                entry_price=entry_price,
                holding_days=int(run.effective_holding_days or 0),
                stop_loss_pct=Decimal(run.effective_stop_loss_pct or Decimal("0")),
                dataset_tuples=dataset_tuples,
                consulted_trade_dates=consulted_trade_dates,
            )
            positions.append(position)

    except DataInsufficientError:
        _apply_dataset_context(
            run,
            consulted_trade_dates=consulted_trade_dates,
            dataset_tuples=dataset_tuples,
            excluded_securities=excluded_securities,
        )
        _persist_data_insufficient(session, run)
        return serialize_backtest_run(session, run, configuration)
    except ValueError as exc:
        _apply_dataset_context(
            run,
            consulted_trade_dates=consulted_trade_dates,
            dataset_tuples=dataset_tuples,
            excluded_securities=excluded_securities,
        )
        _persist_failed_recoverable(session, run, message=str(exc))
        return serialize_backtest_run(session, run, configuration)

    equity_curve = _build_equity_curve(
        positions=positions,
        market_rows_by_instrument=market_rows_by_instrument,
        global_trade_dates=global_trade_dates,
    )
    portfolio_value = MVP_PORTFOLIO_VALUE
    cumulative_return = (
        _quantize_ratio(
            sum((position["realized_return"] for position in positions), Decimal("0")) / Decimal(len(positions))
        )
        if positions
        else Decimal("0")
    )

    _apply_dataset_context(
        run,
        consulted_trade_dates=consulted_trade_dates,
        dataset_tuples=dataset_tuples,
        excluded_securities=excluded_securities,
    )
    run.portfolio_value = portfolio_value
    run.position_count_after_exclusions = len(positions)
    run.cumulative_return = cumulative_return
    run.equity_curve_json = _dump_json(equity_curve)
    run.per_security_returns_json = _dump_json(
        [
            {
                "instrument_id": position["instrument_id"],
                "symbol": position["symbol"],
                "entry_date": position["entry_date"].isoformat(),
                "exit_date": position["exit_date"].isoformat(),
                "exit_reason": position["exit_reason"],
                "realized_return": f"{position['realized_return']:.6f}",
            }
            for position in positions
        ]
    )
    run.trade_dates_evaluated = len(equity_curve)
    run.total_candidates_evaluated = len(candidates)
    run.qualifying_observations = len(positions)
    run.unique_qualified_instruments = len(positions)
    run.first_qualified_trade_date = min((position["entry_date"] for position in positions), default=None)
    run.last_qualified_trade_date = max((position["entry_date"] for position in positions), default=None)
    run.result_checksum = _hash_payload(
        {
            "cumulative_return": f"{cumulative_return:.6f}",
            "equity_curve": json.loads(run.equity_curve_json),
            "per_security_returns": json.loads(run.per_security_returns_json),
            "excluded_securities": excluded_securities,
        }
    )
    run.status = "completed"
    run.error_message = None
    run.completed_at = datetime.now(UTC)
    session.commit()
    session.refresh(run)
    return serialize_backtest_run(session, run, configuration)


def launch_portfolio_return_backtest(
    session,
    *,
    screen_run_id: int,
    holding_days: int | None = None,
    stop_loss_pct: Decimal | float | int | str | None = None,
    portfolio_cap: int | None = None,
    entry_deferral_window_days: int | None = None,
    rps_definition_version: str | None = None,
) -> BacktestRunSummary:
    if rps_definition_version is not None:
        raise ValueError("portfolio_return launch does not accept rps_definition_version; resolve semantics via source screen run.")

    (
        normalized_holding_days,
        normalized_stop_loss_pct,
        normalized_portfolio_cap,
        normalized_entry_deferral_window_days,
    ) = _normalize_launch_parameters(
        holding_days=holding_days,
        stop_loss_pct=stop_loss_pct,
        portfolio_cap=portfolio_cap,
        entry_deferral_window_days=entry_deferral_window_days,
    )
    screen_run = _load_screen_run(session, screen_run_id=screen_run_id)
    configuration = _load_strategy_configuration(
        session,
        strategy_configuration_id=screen_run.strategy_configuration_id,
    )
    latest_matching_run = _find_latest_matching_run(
        session,
        screen_run_id=screen_run_id,
        holding_days=normalized_holding_days,
        stop_loss_pct=normalized_stop_loss_pct,
        portfolio_cap=normalized_portfolio_cap,
        entry_deferral_window_days=normalized_entry_deferral_window_days,
    )
    now = datetime.now(UTC)
    debounce_threshold = now - timedelta(seconds=DEBOUNCE_WINDOW_SECONDS)

    if latest_matching_run is not None and latest_matching_run.status == "failed-recoverable":
        run = latest_matching_run
    elif (
        latest_matching_run is not None
        and latest_matching_run.status == "running"
        and _as_utc(latest_matching_run.started_at) >= debounce_threshold
    ):
        return serialize_backtest_run(session, latest_matching_run, configuration)
    else:
        run = BacktestRun(
            strategy_configuration_id=screen_run.strategy_configuration_id,
            source_screen_run_id=screen_run.id,
            start_date=screen_run.trade_date,
            end_date=screen_run.trade_date,
            started_at=now,
            completed_at=None,
            backtest_lifecycle="portfolio_return",
            status="running",
            error_message=None,
            effective_holding_days=normalized_holding_days,
            effective_stop_loss_pct=normalized_stop_loss_pct,
            effective_portfolio_cap=normalized_portfolio_cap,
            effective_entry_deferral_window_days=normalized_entry_deferral_window_days,
        )
        session.add(run)

    _prepare_run_for_launch(
        run,
        screen_run=screen_run,
        holding_days=normalized_holding_days,
        stop_loss_pct=normalized_stop_loss_pct,
        portfolio_cap=normalized_portfolio_cap,
        entry_deferral_window_days=normalized_entry_deferral_window_days,
    )
    if run.rps_definition_version is not None:
        raise ValueError("portfolio_return runs must persist rps_definition_version as NULL.")
    session.commit()
    session.refresh(run)

    try:
        dispatch_portfolio_return_backtest_execution(session, run)
    except Exception as exc:
        _persist_failed_recoverable(session, run, message=str(exc))
        return serialize_backtest_run(session, run, configuration)

    session.refresh(run)
    return serialize_backtest_run(session, run, configuration)
