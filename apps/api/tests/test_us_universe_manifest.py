from __future__ import annotations

import unittest

from stockanalyse_api.services.ingestion.us_universe_manifest import (
    parse_us_common_stock_symbols,
)


NASDAQ_LISTED = """Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
AAPL|Apple Inc. Common Stock|Q|N|N|100|N|N
QQQ|Invesco QQQ Trust, Series 1|Q|N|N|100|Y|N
ADRU|Example PLC American Depositary Shares|Q|N|N|100|N|N
UNIT|Uniti Group Inc. Common Stock|Q|N|N|100|N|N
TEST|Test Issuer Common Stock|Q|Y|N|100|N|N
File Creation Time: 0428202620:00
"""

OTHER_LISTED = """ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol
IBM|International Business Machines Corporation Common Stock|N|IBM|N|100|N|IBM
BRK/B|Berkshire Hathaway Inc. Class B Common Stock|N|BRK/B|N|100|N|BRK/B
BABA|Alibaba Group Holding Limited American Depositary Shares each representing eight Ordinary share|N|BABA|N|100|N|BABA
XYZ/U|Example Acquisition Corp. Units|N|XYZ/U|N|100|N|XYZ/U
ARKK|ARK Innovation ETF|P|ARKK|Y|100|N|ARKK
File Creation Time: 0428202620:00
"""


class UsUniverseManifestTests(unittest.TestCase):
    def test_parse_us_common_stock_symbols_keeps_common_stocks_only(self) -> None:
        symbols = parse_us_common_stock_symbols(NASDAQ_LISTED, OTHER_LISTED)

        self.assertEqual(symbols, ["AAPL", "BRK.B", "IBM", "UNIT"])


if __name__ == "__main__":
    unittest.main()
