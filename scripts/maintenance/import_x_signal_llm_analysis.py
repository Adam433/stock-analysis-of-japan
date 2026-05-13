from __future__ import annotations

import argparse
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import stockanalyse_api.domain.backtests.models  # noqa: F401
import stockanalyse_api.domain.fundamentals.models  # noqa: F401
import stockanalyse_api.domain.indicators.models  # noqa: F401
import stockanalyse_api.domain.instruments.models  # noqa: F401
import stockanalyse_api.domain.market_data.models  # noqa: F401
import stockanalyse_api.domain.operations.models  # noqa: F401
import stockanalyse_api.domain.screens.models  # noqa: F401
import stockanalyse_api.domain.watchlists.models  # noqa: F401
import stockanalyse_api.domain.x_signals.models  # noqa: F401

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.x_signal_tracker import (
    XSignalLLMAnalysisItem,
    XSignalLLMPostAnalysis,
    apply_x_signal_llm_analysis_results,
)


def _iter_json_files(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for path in paths:
        if path.is_dir():
            files.extend(sorted(candidate for candidate in path.rglob("*.json") if candidate.is_file()))
        elif path.is_file():
            files.append(path)
        else:
            raise SystemExit(f"Path not found: {path}")
    return files


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _parse_item(row: dict[str, Any]) -> XSignalLLMAnalysisItem | None:
    symbol = str(row.get("symbol") or "").strip().upper().lstrip("$")
    if not symbol:
        return None
    mention_kind = str(row.get("mention_kind") or row.get("type") or "stock").strip().lower()
    is_sector_proxy = _bool_value(row.get("is_sector_proxy")) or mention_kind in {
        "sector",
        "sector_proxy",
        "theme_proxy",
    }
    return XSignalLLMAnalysisItem(
        symbol=symbol,
        sentiment=str(row.get("sentiment") or "unknown"),
        mention_kind=mention_kind,
        sector_label=str(row.get("sector_label") or "") or None,
        confidence=_decimal_or_none(row.get("confidence")),
        reason=str(row.get("reason") or row.get("analysis_note") or row.get("rationale") or "") or None,
        is_sector_proxy=is_sector_proxy,
        proxy_reason=str(row.get("proxy_reason") or "") or None,
    )


def _parse_results_file(path: Path, source_override: str | None) -> tuple[str, list[XSignalLLMPostAnalysis]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "results" not in payload:
        return "", []

    analysis_source = source_override or str(payload.get("analysis_source") or "").strip()
    if not analysis_source:
        raise SystemExit(f"{path} is missing analysis_source.")

    analyses: list[XSignalLLMPostAnalysis] = []
    results = payload.get("results")
    if not isinstance(results, list):
        raise SystemExit(f"{path} results must be a list.")
    for row in results:
        if not isinstance(row, dict):
            continue
        try:
            post_id = int(row.get("post_id"))
        except (TypeError, ValueError):
            raise SystemExit(f"{path} has a result without a valid post_id.")
        raw_items = row.get("items") or []
        if not isinstance(raw_items, list):
            raise SystemExit(f"{path} post_id={post_id} items must be a list.")
        items = [_parse_item(item) for item in raw_items if isinstance(item, dict)]
        analyses.append(
            XSignalLLMPostAnalysis(
                post_id=post_id,
                items=[item for item in items if item is not None],
            )
        )
    return analysis_source, analyses


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import LLM X signal analysis JSON results into the tracker database."
    )
    parser.add_argument("paths", nargs="+", type=Path, help="Result JSON file or directory.")
    parser.add_argument("--analysis-source", help="Override analysis_source in result files.")
    parser.add_argument(
        "--partial",
        action="store_true",
        help="Only replace mentions tied to imported post ids instead of replacing author analysis.",
    )
    args = parser.parse_args()

    grouped: dict[str, list[XSignalLLMPostAnalysis]] = {}
    skipped_files = 0
    for path in _iter_json_files(args.paths):
        analysis_source, analyses = _parse_results_file(path, args.analysis_source)
        if not analyses:
            skipped_files += 1
            continue
        grouped.setdefault(analysis_source, []).extend(analyses)

    if not grouped:
        raise SystemExit("No result JSON files with a results array were found.")

    summaries = []
    with SessionLocal() as session:
        for analysis_source, analyses in grouped.items():
            result = apply_x_signal_llm_analysis_results(
                session,
                analyses,
                analysis_source=analysis_source,
                replace_author_mentions=not args.partial,
            )
            summaries.append(result.to_dict())

    print(
        json.dumps(
            {
                "imported": summaries,
                "skipped_files": skipped_files,
                "replace_author_mentions": not args.partial,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
