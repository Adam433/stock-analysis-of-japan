from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path

from stockanalyse_api.services.ingestion.providers.local_csv_directory_provider import (
    LocalCsvDirectoryProvider,
)


class LocalCsvDirectoryProviderTests(unittest.TestCase):
    def test_provider_reads_supported_symbols_and_daily_bars(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            csv_dir = root / "raw"
            csv_dir.mkdir()
            symbols_file = root / "symbols.txt"
            symbols_file.write_text("7203.T\n6758.T\n", encoding="utf-8")
            (csv_dir / "7203.T.csv").write_text(
                "Date,Open,High,Low,Close,Adj Close,Volume\n"
                "2026-04-10,2785.0,2810.5,2774.0,2801.0,2801.0,25100400\n",
                encoding="utf-8",
            )

            provider = LocalCsvDirectoryProvider(csv_dir, symbols_file=symbols_file)

            instruments = provider.list_supported_instruments()
            bars = list(provider.fetch_daily_bars(["7203.T"]))

        self.assertEqual([instrument.symbol for instrument in instruments], ["7203.T", "6758.T"])
        self.assertEqual(instruments[0].instrument_type, "common_stock")
        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0].symbol, "7203.T")
        self.assertEqual(str(bars[0].close), "2801.0")

    def test_provider_requires_symbols_file_for_full_universe_enumeration(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            provider = LocalCsvDirectoryProvider(Path(temp_dir))

            with self.assertRaises(ValueError):
                provider.list_supported_instruments()

    def test_provider_emits_only_rows_after_latest_stored_date(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            csv_dir = root / "raw"
            csv_dir.mkdir()
            symbols_file = root / "symbols.txt"
            symbols_file.write_text("7203.T\n", encoding="utf-8")
            (csv_dir / "7203.T.csv").write_text(
                "Date,Open,High,Low,Close,Adj Close,Volume\n"
                "2026-04-10,2785.0,2810.5,2774.0,2801.0,2801.0,25100400\n"
                "2026-04-11,2800.0,2820.0,2790.0,2815.0,2815.0,23000000\n"
                "2026-04-12,2820.0,2835.0,2810.0,2831.0,2831.0,22000000\n",
                encoding="utf-8",
            )

            provider = LocalCsvDirectoryProvider(csv_dir, symbols_file=symbols_file)

            bars = list(
                provider.fetch_daily_bars(
                    ["7203.T"],
                    start_after_by_symbol={"7203.T": date.fromisoformat("2026-04-11")},
                )
            )

        self.assertEqual([bar.trade_date.isoformat() for bar in bars], ["2026-04-12"])


if __name__ == "__main__":
    unittest.main()
