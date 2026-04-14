from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse
from urllib.error import HTTPError

from stockanalyse_api.services.ingestion.providers.yahoo_finance_chart_provider import (
    DEFAULT_HISTORY_START,
    YahooFinanceChartProvider,
)


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class YahooFinanceChartProviderTests(unittest.TestCase):
    def test_provider_parses_chart_payload_into_daily_bars(self) -> None:
        payload = {
            "chart": {
                "result": [
                    {
                        "meta": {"currency": "JPY", "longName": "Toyota Motor"},
                        "timestamp": [1710028800, 1710115200],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [1000.0, 1010.0],
                                    "high": [1010.0, 1020.0],
                                    "low": [995.0, 1005.0],
                                    "close": [1005.0, 1015.0],
                                    "volume": [100, 120],
                                }
                            ],
                            "adjclose": [{"adjclose": [1005.0, 1015.0]}],
                        },
                    }
                ],
                "error": None,
            }
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            symbols_file = Path(temp_dir) / "symbols.txt"
            symbols_file.write_text("7203.T\n", encoding="utf-8")
            provider = YahooFinanceChartProvider(symbols_file=symbols_file)

            with patch.object(provider, "_fetch_chart_payload", return_value=payload):
                bars = list(provider.fetch_daily_bars(["7203.T"]))

        self.assertEqual(len(bars), 2)
        self.assertEqual(bars[0].symbol, "7203.T")
        self.assertEqual(bars[0].instrument_name, "Toyota Motor")
        self.assertEqual(bars[1].trade_date.isoformat(), "2024-03-11")

    def test_provider_requests_only_dates_after_latest_stored_trade_date(self) -> None:
        captured_urls: list[str] = []
        payload = {"chart": {"result": [], "error": None}}

        def fake_urlopen(request, timeout=0, context=None):
            captured_urls.append(request.full_url)
            return _FakeResponse(payload)

        with tempfile.TemporaryDirectory() as temp_dir:
            symbols_file = Path(temp_dir) / "symbols.txt"
            symbols_file.write_text("7203.T\n", encoding="utf-8")
            provider = YahooFinanceChartProvider(symbols_file=symbols_file)

            with patch(
                "stockanalyse_api.services.ingestion.providers.yahoo_finance_chart_provider.urlopen",
                side_effect=fake_urlopen,
            ):
                list(
                    provider.fetch_daily_bars(
                        ["7203.T"],
                        start_after_by_symbol={"7203.T": date(2024, 3, 10)},
                    )
                )

        parsed = urlparse(captured_urls[0])
        query = parse_qs(parsed.query)
        requested_start = datetime.fromtimestamp(int(query["period1"][0]), UTC).date()
        self.assertEqual(requested_start, date(2024, 3, 11))

    def test_provider_uses_default_history_start_for_cold_symbol(self) -> None:
        captured_urls: list[str] = []
        payload = {"chart": {"result": [], "error": None}}

        def fake_urlopen(request, timeout=0, context=None):
            captured_urls.append(request.full_url)
            return _FakeResponse(payload)

        with tempfile.TemporaryDirectory() as temp_dir:
            symbols_file = Path(temp_dir) / "symbols.txt"
            symbols_file.write_text("7203.T\n", encoding="utf-8")
            provider = YahooFinanceChartProvider(symbols_file=symbols_file)

            with patch(
                "stockanalyse_api.services.ingestion.providers.yahoo_finance_chart_provider.urlopen",
                side_effect=fake_urlopen,
            ):
                list(provider.fetch_daily_bars(["7203.T"]))

        parsed = urlparse(captured_urls[0])
        query = parse_qs(parsed.query)
        requested_start = datetime.fromtimestamp(int(query["period1"][0]), UTC).date()
        self.assertEqual(requested_start, DEFAULT_HISTORY_START)

    def test_provider_skips_symbols_that_return_404(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            symbols_file = Path(temp_dir) / "symbols.txt"
            symbols_file.write_text("7203.T\n", encoding="utf-8")
            provider = YahooFinanceChartProvider(symbols_file=symbols_file)

            with patch.object(
                provider,
                "_fetch_chart_payload",
                side_effect=HTTPError(
                    url="https://query1.finance.yahoo.com/v8/finance/chart/7203.T",
                    code=404,
                    msg="Not Found",
                    hdrs=None,
                    fp=None,
                ),
            ):
                bars = list(provider.fetch_daily_bars(["7203.T"]))

        self.assertEqual(bars, [])

    def test_provider_skips_symbols_that_return_400(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            symbols_file = Path(temp_dir) / "symbols.txt"
            symbols_file.write_text("7203.T\n", encoding="utf-8")
            provider = YahooFinanceChartProvider(symbols_file=symbols_file)

            with patch.object(
                provider,
                "_fetch_chart_payload",
                side_effect=HTTPError(
                    url="https://query1.finance.yahoo.com/v8/finance/chart/7203.T",
                    code=400,
                    msg="Bad Request",
                    hdrs=None,
                    fp=None,
                ),
            ):
                bars = list(provider.fetch_daily_bars(["7203.T"]))

        self.assertEqual(bars, [])


if __name__ == "__main__":
    unittest.main()
