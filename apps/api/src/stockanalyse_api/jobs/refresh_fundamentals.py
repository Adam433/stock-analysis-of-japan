from __future__ import annotations

import argparse
from collections.abc import Sequence

from sqlalchemy import and_, func, or_, select

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.domain.fundamentals.models import FundamentalsAnnual
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.services.fundamentals_refresh import DEFAULT_FUNDAMENTALS_PROVIDER
from stockanalyse_api.services.fundamentals_refresh import refresh_instrument_fundamentals
from stockanalyse_api.services.ingestion.providers.registry import build_ingestion_provider


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh annual fundamentals for instruments.")
    parser.add_argument("--symbols", nargs="*", help="Optional symbols to refresh, e.g. 6758.T 7203.T.")
    parser.add_argument("--limit", type=int, default=None, help="Optional maximum instruments to refresh.")
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Optional number of sorted instruments to skip before refreshing.",
    )
    parser.add_argument("--provider", default=DEFAULT_FUNDAMENTALS_PROVIDER)
    parser.add_argument(
        "--exchange",
        choices=["TSE", "US"],
        default=None,
        help="Optional exchange filter. Defaults to US for sec_companyfacts.",
    )
    parser.add_argument("--progress-every", type=int, default=25)
    parser.add_argument(
        "--missing-cash-flow-only",
        action="store_true",
        help="Only refresh instruments whose latest annual fundamentals lack operating/free cash flow.",
    )
    parser.add_argument(
        "--missing-valuation-inputs-only",
        action="store_true",
        help="Only refresh instruments whose latest annual fundamentals lack SEC valuation inputs.",
    )
    return parser.parse_args(argv)


def _load_instruments(
    session,
    *,
    symbols: list[str] | None,
    exchange: str | None,
    limit: int | None,
    offset: int = 0,
    missing_cash_flow_only: bool = False,
    missing_valuation_inputs_only: bool = False,
) -> list[Instrument]:
    query = (
        select(Instrument)
        .join(MarketDataDaily, MarketDataDaily.instrument_id == Instrument.id)
        .group_by(Instrument.id)
        .order_by(Instrument.symbol.asc())
    )
    if symbols:
        query = query.where(Instrument.symbol.in_(symbols))
    if exchange:
        query = query.where(Instrument.exchange == exchange)
    if missing_cash_flow_only or missing_valuation_inputs_only:
        latest_fiscal_year = (
            select(
                FundamentalsAnnual.instrument_id.label("instrument_id"),
                func.max(FundamentalsAnnual.fiscal_year_end_date).label("latest_end"),
            )
            .group_by(FundamentalsAnnual.instrument_id)
            .subquery()
        )
        query = (
            query.outerjoin(
                latest_fiscal_year,
                latest_fiscal_year.c.instrument_id == Instrument.id,
            )
            .outerjoin(
                FundamentalsAnnual,
                and_(
                    FundamentalsAnnual.instrument_id == Instrument.id,
                    FundamentalsAnnual.fiscal_year_end_date
                    == latest_fiscal_year.c.latest_end,
                ),
            )
        )
        missing_conditions = [latest_fiscal_year.c.latest_end.is_(None)]
        if missing_cash_flow_only:
            missing_conditions.extend(
                [
                    FundamentalsAnnual.operating_cash_flow.is_(None),
                    FundamentalsAnnual.free_cash_flow.is_(None),
                ]
            )
        if missing_valuation_inputs_only:
            missing_conditions.extend(
                [
                    FundamentalsAnnual.diluted_eps.is_(None),
                    FundamentalsAnnual.stockholders_equity.is_(None),
                    FundamentalsAnnual.weighted_average_diluted_shares.is_(None),
                ]
            )
        query = query.where(or_(*missing_conditions))
    if offset:
        query = query.offset(offset)
    if limit is not None:
        query = query.limit(limit)
    return list(session.execute(query).scalars())


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    symbols = args.symbols or None
    exchange = args.exchange
    if exchange is None and args.provider in {"sec_companyfacts", "sec_companyfacts_yahoo_fallback"}:
        exchange = "US"
    provider = build_ingestion_provider(args.provider)
    refreshed = 0
    failed = 0

    with SessionLocal() as session:
        instruments = _load_instruments(
            session,
            symbols=symbols,
            exchange=exchange,
            limit=args.limit,
            offset=args.offset,
            missing_cash_flow_only=args.missing_cash_flow_only,
            missing_valuation_inputs_only=args.missing_valuation_inputs_only,
        )
        for index, instrument in enumerate(instruments, start=1):
            ok = refresh_instrument_fundamentals(
                session,
                instrument_id=instrument.id,
                provider=provider,
            )
            if ok:
                refreshed += 1
            else:
                failed += 1
            if args.progress_every and index % args.progress_every == 0:
                print(
                    f"[fundamentals] {index}/{len(instruments)} instruments "
                    f"refreshed={refreshed} failed={failed}",
                    flush=True,
                )

    print(
        {
            "processed": refreshed + failed,
            "refreshed": refreshed,
            "failed": failed,
        }
    )


if __name__ == "__main__":
    main()
