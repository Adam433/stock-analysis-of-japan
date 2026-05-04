from __future__ import annotations

import json
import os
import ssl
import time
from datetime import UTC, date, datetime
from decimal import Decimal
from urllib.parse import quote
from urllib.request import Request, urlopen

try:
    import certifi
except ImportError:  # pragma: no cover - certifi is optional at runtime.
    certifi = None

from stockanalyse_api.services.ingestion.provider_models import ProviderFundamentalsAnnual

NET_INCOME_TAGS = (
    "NetIncomeLoss",
    "ProfitLoss",
    "NetIncomeLossAvailableToCommonStockholdersBasic",
)
IFRS_NET_INCOME_TAGS = (
    "ProfitLossAttributableToOwnersOfParent",
    "ProfitLossAttributableToOrdinaryEquityHoldersOfParentEntity",
    "ProfitLoss",
)
OPERATING_CASH_FLOW_TAGS = (
    "NetCashProvidedByUsedInOperatingActivities",
    "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
)
IFRS_OPERATING_CASH_FLOW_TAGS = (
    "CashFlowsFromUsedInOperatingActivities",
)
CAPITAL_EXPENDITURE_TAGS = (
    "PaymentsToAcquirePropertyPlantAndEquipment",
    "PaymentsToAcquireProductiveAssets",
)
IFRS_CAPITAL_EXPENDITURE_TAGS = (
    "PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities",
    "PaymentsToAcquirePropertyPlantAndEquipment",
)
DILUTED_EPS_TAGS = (
    "EarningsPerShareDiluted",
    "EarningsPerShareBasicAndDiluted",
    "EarningsPerShareBasic",
)
IFRS_DILUTED_EPS_TAGS = (
    "DilutedEarningsLossPerShare",
    "BasicEarningsLossPerShare",
)
STOCKHOLDERS_EQUITY_TAGS = (
    "StockholdersEquity",
    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
)
IFRS_STOCKHOLDERS_EQUITY_TAGS = (
    "EquityAttributableToOwnersOfParent",
    "Equity",
)
DILUTED_SHARES_TAGS = (
    "WeightedAverageNumberOfDilutedSharesOutstanding",
    "WeightedAverageNumberOfSharesOutstandingBasic",
)
ENTITY_SHARES_TAGS = (
    "EntityCommonStockSharesOutstanding",
)
ANNUAL_FORMS = {"10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"}
SUPPLEMENTAL_ANNUAL_FORMS = {"6-K", "6-K/A"}
REGISTRATION_ANNUAL_FORMS = {"S-1", "S-1/A", "F-1", "F-1/A"}
MIN_ANNUAL_DURATION_DAYS = 300


class SecCompanyFactsFundamentalsProvider:
    provider_name = "sec_companyfacts"
    market_scope = "us_equities_fundamentals"
    credential_boundary = "backend_only"

    def __init__(
        self,
        *,
        timeout_seconds: int = 30,
        user_agent: str | None = None,
        min_request_interval_seconds: float | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent or os.environ.get(
            "STOCKANALYSE_SEC_USER_AGENT",
            "stockAnalyse/0.1 contact=local@example.com",
        )
        self.min_request_interval_seconds = (
            min_request_interval_seconds
            if min_request_interval_seconds is not None
            else float(os.environ.get("STOCKANALYSE_SEC_MIN_REQUEST_INTERVAL_SECONDS", "0.12"))
        )
        self._last_request_at: float | None = None
        self._ticker_to_cik: dict[str, int] | None = None

    def fetch_annual_fundamentals(
        self,
        symbol: str,
        *,
        exchange: str = "US",
    ) -> list[ProviderFundamentalsAnnual]:
        cik = self._resolve_cik(symbol)
        if cik is None:
            return []
        payload = self._fetch_companyfacts(cik)
        return self._parse_companyfacts(symbol, exchange, payload)

    def _resolve_cik(self, symbol: str) -> int | None:
        if self._ticker_to_cik is None:
            mapping: dict[str, int] = {}
            self._add_company_ticker_mapping(
                mapping,
                self._fetch_json("https://www.sec.gov/files/company_tickers.json"),
            )
            self._add_exchange_ticker_mapping(
                mapping,
                self._fetch_json("https://www.sec.gov/files/company_tickers_exchange.json"),
            )
            self._ticker_to_cik = mapping
        return self._ticker_to_cik.get(symbol.replace(".", "-").upper())

    @staticmethod
    def _add_company_ticker_mapping(mapping: dict[str, int], payload: dict[str, object]) -> None:
        if not isinstance(payload, dict):
            return
        for row in payload.values():
            if not isinstance(row, dict):
                continue
            ticker = row.get("ticker")
            cik = row.get("cik_str")
            if ticker and cik is not None:
                mapping[str(ticker).upper()] = int(cik)

    @staticmethod
    def _add_exchange_ticker_mapping(mapping: dict[str, int], payload: dict[str, object]) -> None:
        if not isinstance(payload, dict):
            return
        fields = payload.get("fields")
        data = payload.get("data")
        if not isinstance(fields, list) or not isinstance(data, list):
            return
        try:
            cik_index = fields.index("cik")
            ticker_index = fields.index("ticker")
        except ValueError:
            return
        for row in data:
            if not isinstance(row, list) or len(row) <= max(cik_index, ticker_index):
                continue
            ticker = row[ticker_index]
            cik = row[cik_index]
            if ticker and cik is not None:
                mapping.setdefault(str(ticker).upper(), int(cik))

    def _fetch_companyfacts(self, cik: int) -> dict[str, object]:
        return self._fetch_json(
            f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
        )

    def _fetch_json(self, url: str) -> dict[str, object]:
        self._throttle_request()
        request = Request(
            quote(url, safe=":/?=&."),
            headers={
                "User-Agent": self.user_agent,
                "Accept": "application/json",
            },
        )
        ssl_context = (
            ssl.create_default_context(cafile=certifi.where())
            if certifi is not None
            else ssl.create_default_context()
        )
        with urlopen(request, timeout=self.timeout_seconds, context=ssl_context) as response:
            return json.loads(response.read().decode("utf-8"))

    def _throttle_request(self) -> None:
        if self.min_request_interval_seconds <= 0:
            return
        now = time.monotonic()
        if self._last_request_at is not None:
            elapsed = now - self._last_request_at
            if elapsed < self.min_request_interval_seconds:
                time.sleep(self.min_request_interval_seconds - elapsed)
        self._last_request_at = time.monotonic()

    def _parse_companyfacts(
        self,
        symbol: str,
        exchange: str,
        payload: dict[str, object],
    ) -> list[ProviderFundamentalsAnnual]:
        facts = payload.get("facts")
        if not isinstance(facts, dict):
            return []

        for accepted_forms, allow_missing_fp, require_start_date in (
            (ANNUAL_FORMS, False, False),
            (ANNUAL_FORMS, True, True),
            (SUPPLEMENTAL_ANNUAL_FORMS, True, True),
            (REGISTRATION_ANNUAL_FORMS, True, True),
        ):
            candidates = self._collect_net_income_candidates(
                symbol,
                exchange,
                facts,
                accepted_forms=accepted_forms,
                allow_missing_fp=allow_missing_fp,
                require_start_date=require_start_date,
            )
            if candidates:
                best_rows = max(
                    candidates,
                    key=lambda rows: (rows[-1].fiscal_year_end_date, len(rows)),
                )
                self._merge_cash_flow_fields(
                    best_rows,
                    facts,
                    accepted_forms=accepted_forms,
                    allow_missing_fp=allow_missing_fp,
                    require_start_date=require_start_date,
                )
                self._merge_valuation_input_fields(
                    best_rows,
                    facts,
                    accepted_forms=accepted_forms,
                    allow_missing_fp=allow_missing_fp,
                    require_start_date=require_start_date,
                )
                return best_rows
        return []

    def _collect_net_income_candidates(
        self,
        symbol: str,
        exchange: str,
        facts: dict[str, object],
        *,
        accepted_forms: set[str],
        allow_missing_fp: bool,
        require_start_date: bool,
    ) -> list[list[ProviderFundamentalsAnnual]]:
        candidates: list[list[ProviderFundamentalsAnnual]] = []
        for taxonomy_name, tags in (
            ("us-gaap", NET_INCOME_TAGS),
            ("ifrs-full", IFRS_NET_INCOME_TAGS),
        ):
            taxonomy = facts.get(taxonomy_name)
            if not isinstance(taxonomy, dict):
                continue
            for tag in tags:
                fact = taxonomy.get(tag)
                units = fact.get("units") if isinstance(fact, dict) else None
                if not isinstance(units, dict):
                    continue
                for currency, unit_rows in self._iter_currency_unit_rows(units):
                    parsed = self._parse_net_income_rows(
                        symbol,
                        exchange,
                        unit_rows,
                        currency=currency,
                        accepted_forms=accepted_forms,
                        allow_missing_fp=allow_missing_fp,
                        require_start_date=require_start_date,
                    )
                    if parsed:
                        candidates.append(parsed[-10:])
        return candidates

    def _parse_net_income_rows(
        self,
        symbol: str,
        exchange: str,
        rows: list[object],
        *,
        currency: str = "USD",
        accepted_forms: set[str] = ANNUAL_FORMS,
        allow_missing_fp: bool = False,
        require_start_date: bool = False,
    ) -> list[ProviderFundamentalsAnnual]:
        by_fiscal_year_end: dict[date, tuple[ProviderFundamentalsAnnual, tuple[int, date]]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            form = row.get("form")
            fiscal_period = row.get("fp")
            if form not in accepted_forms:
                continue
            if fiscal_period != "FY" and not (allow_missing_fp and fiscal_period is None):
                continue
            fiscal_year_end = self._read_date(row.get("end"))
            fiscal_year_start = self._read_date(row.get("start"))
            filed_date = self._read_date(row.get("filed")) or date.today()
            value = row.get("val")
            if fiscal_year_end is None or value is None:
                continue
            if require_start_date and fiscal_year_start is None:
                continue
            if not self._is_annual_duration(fiscal_year_start, fiscal_year_end):
                continue
            row_rank = _annual_fact_rank(row, fiscal_year_end, filed_date)
            existing = by_fiscal_year_end.get(fiscal_year_end)
            if existing is not None and existing[1] >= row_rank:
                continue
            by_fiscal_year_end[fiscal_year_end] = (
                ProviderFundamentalsAnnual(
                    symbol=symbol,
                    exchange=exchange,
                    fiscal_year_end_date=fiscal_year_end,
                    fiscal_year_label=f"FY{fiscal_year_end.year}",
                    net_income=Decimal(str(value)),
                    net_income_currency=currency,
                    source=self.provider_name,
                    source_as_of_date=filed_date,
                    data_status="partial",
                ),
                row_rank,
            )

        return sorted(
            (item[0] for item in by_fiscal_year_end.values()),
            key=lambda item: item.fiscal_year_end_date,
        )

    def _merge_cash_flow_fields(
        self,
        rows: list[ProviderFundamentalsAnnual],
        facts: dict[str, object],
        *,
        accepted_forms: set[str],
        allow_missing_fp: bool,
        require_start_date: bool,
    ) -> None:
        operating_by_end = self._collect_annual_fact_values(
            facts,
            taxonomy_tags=(
                ("us-gaap", OPERATING_CASH_FLOW_TAGS),
                ("ifrs-full", IFRS_OPERATING_CASH_FLOW_TAGS),
            ),
            accepted_forms=accepted_forms,
            allow_missing_fp=allow_missing_fp,
            require_start_date=require_start_date,
        )
        capex_by_end = self._collect_annual_fact_values(
            facts,
            taxonomy_tags=(
                ("us-gaap", CAPITAL_EXPENDITURE_TAGS),
                ("ifrs-full", IFRS_CAPITAL_EXPENDITURE_TAGS),
            ),
            accepted_forms=accepted_forms,
            allow_missing_fp=allow_missing_fp,
            require_start_date=require_start_date,
        )
        for row in rows:
            operating_value = operating_by_end.get(row.fiscal_year_end_date)
            if operating_value is None:
                continue
            row.operating_cash_flow = operating_value[0]
            capex_value = capex_by_end.get(row.fiscal_year_end_date)
            row.free_cash_flow = (
                _free_cash_flow(row.operating_cash_flow, capex_value[0])
                if capex_value is not None
                else None
            )

    def _merge_valuation_input_fields(
        self,
        rows: list[ProviderFundamentalsAnnual],
        facts: dict[str, object],
        *,
        accepted_forms: set[str],
        allow_missing_fp: bool,
        require_start_date: bool,
    ) -> None:
        diluted_eps_by_end = self._collect_annual_fact_values(
            facts,
            taxonomy_tags=(
                ("us-gaap", DILUTED_EPS_TAGS),
                ("ifrs-full", IFRS_DILUTED_EPS_TAGS),
            ),
            accepted_forms=accepted_forms,
            allow_missing_fp=allow_missing_fp,
            require_start_date=require_start_date,
            unit_names=None,
        )
        equity_by_end = self._collect_annual_fact_values(
            facts,
            taxonomy_tags=(
                ("us-gaap", STOCKHOLDERS_EQUITY_TAGS),
                ("ifrs-full", IFRS_STOCKHOLDERS_EQUITY_TAGS),
            ),
            accepted_forms=accepted_forms,
            allow_missing_fp=allow_missing_fp,
            require_start_date=False,
            unit_names=None,
        )
        diluted_shares_by_end = self._collect_annual_fact_values(
            facts,
            taxonomy_tags=(
                ("us-gaap", DILUTED_SHARES_TAGS),
                ("dei", ENTITY_SHARES_TAGS),
            ),
            accepted_forms=accepted_forms,
            allow_missing_fp=allow_missing_fp,
            require_start_date=False,
            unit_names={"shares"},
        )
        for row in rows:
            diluted_eps_value = diluted_eps_by_end.get(row.fiscal_year_end_date)
            if diluted_eps_value is not None:
                row.diluted_eps = diluted_eps_value[0]
            equity_value = equity_by_end.get(row.fiscal_year_end_date)
            if equity_value is not None:
                row.stockholders_equity = equity_value[0]
            diluted_shares_value = diluted_shares_by_end.get(row.fiscal_year_end_date)
            if diluted_shares_value is not None:
                row.weighted_average_diluted_shares = diluted_shares_value[0]

    def _collect_annual_fact_values(
        self,
        facts: dict[str, object],
        *,
        taxonomy_tags: tuple[tuple[str, tuple[str, ...]], ...],
        accepted_forms: set[str],
        allow_missing_fp: bool,
        require_start_date: bool,
        unit_names: set[str] | None = None,
    ) -> dict[date, tuple[Decimal, date]]:
        by_fiscal_year_end: dict[date, tuple[Decimal, date, tuple[int, date]]] = {}
        for taxonomy_name, tags in taxonomy_tags:
            taxonomy = facts.get(taxonomy_name)
            if not isinstance(taxonomy, dict):
                continue
            for tag in tags:
                fact = taxonomy.get(tag)
                units = fact.get("units") if isinstance(fact, dict) else None
                if not isinstance(units, dict):
                    continue
                for _unit_name, unit_rows in self._iter_unit_rows(units, unit_names=unit_names):
                    for row in unit_rows:
                        if not isinstance(row, dict):
                            continue
                        form = row.get("form")
                        fiscal_period = row.get("fp")
                        if form not in accepted_forms:
                            continue
                        if fiscal_period != "FY" and not (
                            allow_missing_fp and fiscal_period is None
                        ):
                            continue
                        fiscal_year_end = self._read_date(row.get("end"))
                        fiscal_year_start = self._read_date(row.get("start"))
                        filed_date = self._read_date(row.get("filed")) or date.today()
                        value = row.get("val")
                        if fiscal_year_end is None or value is None:
                            continue
                        if require_start_date and fiscal_year_start is None:
                            continue
                        if not self._is_annual_duration(fiscal_year_start, fiscal_year_end):
                            continue
                        row_rank = _annual_fact_rank(row, fiscal_year_end, filed_date)
                        existing = by_fiscal_year_end.get(fiscal_year_end)
                        if existing is not None and existing[2] >= row_rank:
                            continue
                        by_fiscal_year_end[fiscal_year_end] = (
                            Decimal(str(value)),
                            filed_date,
                            row_rank,
                        )
        return {
            fiscal_year_end: (value, filed_date)
            for fiscal_year_end, (value, filed_date, _rank) in by_fiscal_year_end.items()
        }

    @staticmethod
    def _iter_unit_rows(
        units: dict[str, object],
        *,
        unit_names: set[str] | None,
    ) -> list[tuple[str, list[object]]]:
        rows_by_unit = [
            (unit_name, rows)
            for unit_name, rows in units.items()
            if isinstance(unit_name, str)
            and isinstance(rows, list)
            and (unit_names is None or unit_name in unit_names)
        ]
        return sorted(rows_by_unit, key=lambda item: _unit_sort_key(item[0]))

    @staticmethod
    def _iter_currency_unit_rows(units: dict[str, object]) -> list[tuple[str, list[object]]]:
        return [
            (unit_name, rows)
            for unit_name, rows in SecCompanyFactsFundamentalsProvider._iter_unit_rows(
                units,
                unit_names=None,
            )
            if len(unit_name) == 3 and unit_name.isalpha()
        ]

    @staticmethod
    def _is_annual_duration(start: date | None, end: date) -> bool:
        if start is None:
            return True
        return (end - start).days >= MIN_ANNUAL_DURATION_DAYS

    @staticmethod
    def _read_date(value: object) -> date | None:
        if not value:
            return None
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, UTC).date()
        return date.fromisoformat(str(value))


def _free_cash_flow(
    operating_cash_flow: Decimal | None,
    capital_expenditures: Decimal | None,
) -> Decimal | None:
    if operating_cash_flow is None or capital_expenditures is None:
        return None
    if capital_expenditures < 0:
        return operating_cash_flow + capital_expenditures
    return operating_cash_flow - capital_expenditures


def _unit_sort_key(unit_name: str) -> tuple[int, str]:
    preferred = {"USD": 0, "USD/shares": 0, "shares": 0}
    return (preferred.get(unit_name, 1), unit_name)


def _annual_fact_rank(row: dict[str, object], fiscal_year_end: date, filed_date: date) -> tuple[int, date]:
    fiscal_year = row.get("fy")
    try:
        fiscal_year_value = int(fiscal_year) if fiscal_year is not None else None
    except (TypeError, ValueError):
        fiscal_year_value = None
    is_primary_fiscal_year = (
        fiscal_year_value is None or fiscal_year_value == fiscal_year_end.year
    )
    return (1 if is_primary_fiscal_year else 0, filed_date)
