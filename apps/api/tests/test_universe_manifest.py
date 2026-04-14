from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from stockanalyse_api.services.ingestion.universe_manifest import (
    build_tse_common_stock_symbols,
    load_rows_from_table,
    write_symbol_manifest,
)


class UniverseManifestTests(unittest.TestCase):
    def test_build_tse_common_stock_symbols_filters_out_non_common_stock_products(self) -> None:
        rows = [
            {"コード": "7203.0", "市場・商品区分": "プライム（内国株式）"},
            {"コード": "6758.0", "市場・商品区分": "スタンダード（内国株式）"},
            {"コード": "8951", "市場・商品区分": "REIT"},
            {"コード": "1343", "市場・商品区分": "ETF・ETN"},
            {"コード": "559A", "市場・商品区分": "グロース（内国株式）"},
        ]

        symbols = build_tse_common_stock_symbols(rows)

        self.assertEqual(symbols, ["559A.T", "6758.T", "7203.T"])

    def test_load_rows_from_csv_and_write_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            csv_path = root / "listed.csv"
            csv_path.write_text(
                "コード,市場・商品区分\n7203,プライム（内国株式）\n8951,REIT\n",
                encoding="utf-8",
            )

            rows = load_rows_from_table(csv_path)
            symbols = build_tse_common_stock_symbols(rows)
            output = write_symbol_manifest(symbols, root / "symbols.txt")
            output_content = output.read_text(encoding="utf-8")

        self.assertEqual(rows[0]["コード"], "7203")
        self.assertEqual(symbols, ["7203.T"])
        self.assertEqual(output_content, "7203.T\n")


if __name__ == "__main__":
    unittest.main()
