from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import re
import ssl
from urllib.request import Request, urlopen

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
DEFAULT_USER_AGENT = "stockAnalyse/0.1 us-universe-sync"
DEFAULT_OTHER_EXCHANGE_CODES = ("N",)

_COMMON_STOCK_MARKERS = (
    "common stock",
    "common shares",
    "ordinary shares",
)
_EXCLUDED_SECURITY_NAME_MARKERS = (
    "american depositary",
    "adr",
    "ads",
    "depositary shares",
    "preferred",
    "preference",
    "warrant",
    "notes due",
    "senior notes",
    "subordinated notes",
    "debenture",
    "bond",
    "etf",
    "etn",
    "fund",
)


@dataclass(frozen=True, slots=True)
class UsUniverseManifest:
    symbol_count: int
    updated_at: str
    output_path: str
    nasdaq_listed_url: str
    other_listed_url: str
    other_exchange_codes: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol_count": self.symbol_count,
            "updated_at": self.updated_at,
            "output_path": self.output_path,
            "nasdaq_listed_url": self.nasdaq_listed_url,
            "other_listed_url": self.other_listed_url,
            "other_exchange_codes": list(self.other_exchange_codes),
        }


def fetch_us_common_stock_symbols(
    *,
    nasdaq_listed_url: str = NASDAQ_LISTED_URL,
    other_listed_url: str = OTHER_LISTED_URL,
    other_exchange_codes: tuple[str, ...] | list[str] = DEFAULT_OTHER_EXCHANGE_CODES,
    timeout_seconds: int = 30,
) -> list[str]:
    nasdaq_text = _fetch_text(nasdaq_listed_url, timeout_seconds=timeout_seconds)
    other_text = _fetch_text(other_listed_url, timeout_seconds=timeout_seconds)
    return parse_us_common_stock_symbols(
        nasdaq_text,
        other_text,
        other_exchange_codes=tuple(other_exchange_codes),
    )


def parse_us_common_stock_symbols(
    nasdaq_listed_text: str,
    other_listed_text: str,
    *,
    other_exchange_codes: tuple[str, ...] | list[str] = DEFAULT_OTHER_EXCHANGE_CODES,
) -> list[str]:
    symbols: set[str] = set()
    allowed_other_exchanges = {code.upper() for code in other_exchange_codes}

    for row in _parse_pipe_rows(nasdaq_listed_text):
        symbol = _normalize_symbol(row.get("Symbol"))
        security_name = row.get("Security Name", "")
        if (
            symbol
            and row.get("ETF", "").upper() == "N"
            and row.get("Test Issue", "").upper() == "N"
            and _looks_like_common_stock(security_name)
        ):
            symbols.add(symbol)

    for row in _parse_pipe_rows(other_listed_text):
        symbol = _normalize_symbol(row.get("ACT Symbol"))
        security_name = row.get("Security Name", "")
        exchange_code = row.get("Exchange", "").upper()
        if (
            symbol
            and exchange_code in allowed_other_exchanges
            and row.get("ETF", "").upper() == "N"
            and row.get("Test Issue", "").upper() == "N"
            and _looks_like_common_stock(security_name)
        ):
            symbols.add(symbol)

    return sorted(symbols)


def sync_us_common_stock_universe(
    output_path: Path,
    *,
    nasdaq_listed_url: str = NASDAQ_LISTED_URL,
    other_listed_url: str = OTHER_LISTED_URL,
    other_exchange_codes: tuple[str, ...] | list[str] = DEFAULT_OTHER_EXCHANGE_CODES,
    timeout_seconds: int = 30,
) -> UsUniverseManifest:
    symbols = fetch_us_common_stock_symbols(
        nasdaq_listed_url=nasdaq_listed_url,
        other_listed_url=other_listed_url,
        other_exchange_codes=tuple(other_exchange_codes),
        timeout_seconds=timeout_seconds,
    )
    updated_at = datetime.now(UTC).isoformat()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        "\n".join(
            [
                "# US common-stock universe generated from NASDAQ Trader symbol directories.",
                f"# nasdaq_listed_url={nasdaq_listed_url}",
                f"# other_listed_url={other_listed_url}",
                f"# other_exchange_codes={','.join(other_exchange_codes)}",
                f"# updated_at={updated_at}",
                f"# symbol_count={len(symbols)}",
                *symbols,
                "",
            ]
        ),
        encoding="utf-8",
    )
    return UsUniverseManifest(
        symbol_count=len(symbols),
        updated_at=updated_at,
        output_path=str(output_path),
        nasdaq_listed_url=nasdaq_listed_url,
        other_listed_url=other_listed_url,
        other_exchange_codes=tuple(other_exchange_codes),
    )


def _fetch_text(url: str, *, timeout_seconds: int) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "text/plain",
        },
    )
    with urlopen(request, timeout=timeout_seconds, context=_ssl_context()) as response:
        return response.read().decode("utf-8")


def _ssl_context() -> ssl.SSLContext:
    try:
        import certifi

        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        return ssl.create_default_context()


def _parse_pipe_rows(text: str) -> list[dict[str, str]]:
    lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.startswith("File Creation Time")
    ]
    if not lines:
        return []
    headers = [header.strip() for header in lines[0].split("|")]
    rows: list[dict[str, str]] = []
    for line in lines[1:]:
        values = [value.strip() for value in line.split("|")]
        if len(values) != len(headers):
            continue
        rows.append(dict(zip(headers, values, strict=True)))
    return rows


def _normalize_symbol(value: str | None) -> str | None:
    if value is None:
        return None
    symbol = value.strip().upper().replace("/", ".")
    if not symbol:
        return None
    if any(marker in symbol for marker in ("$", "^", " ")):
        return None
    return symbol


def _looks_like_common_stock(security_name: str) -> bool:
    normalized = security_name.lower()
    if any(marker in normalized for marker in _EXCLUDED_SECURITY_NAME_MARKERS):
        return False
    if re.search(r"\b(rights?|units?|warrants?)\b", normalized):
        return False
    return any(marker in normalized for marker in _COMMON_STOCK_MARKERS)
