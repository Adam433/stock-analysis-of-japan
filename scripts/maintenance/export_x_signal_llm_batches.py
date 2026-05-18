from __future__ import annotations

import argparse
import json
import re
from datetime import UTC, datetime
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
from sqlalchemy import select

from stockanalyse_api.config.settings import get_data_dir
from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.domain.x_signals.models import XSignalAuthor, XSignalPost
from stockanalyse_api.services.x_signal_tracker import normalize_x_handle


SECTOR_LEADERS: dict[str, list[dict[str, str]]] = {
    "ai_infrastructure": [
        {"symbol": "NVDA", "name": "Nvidia"},
        {"symbol": "AVGO", "name": "Broadcom"},
    ],
    "semiconductors": [
        {"symbol": "NVDA", "name": "Nvidia"},
        {"symbol": "AVGO", "name": "Broadcom"},
    ],
    "cloud": [
        {"symbol": "MSFT", "name": "Microsoft"},
        {"symbol": "AMZN", "name": "Amazon"},
    ],
    "cybersecurity": [
        {"symbol": "CRWD", "name": "CrowdStrike"},
        {"symbol": "PANW", "name": "Palo Alto Networks"},
    ],
    "electric_vehicles": [
        {"symbol": "TSLA", "name": "Tesla"},
        {"symbol": "GM", "name": "General Motors"},
    ],
    "crypto": [
        {"symbol": "COIN", "name": "Coinbase"},
        {"symbol": "MSTR", "name": "Strategy"},
    ],
    "banks": [
        {"symbol": "JPM", "name": "JPMorgan Chase"},
        {"symbol": "BAC", "name": "Bank of America"},
    ],
    "energy": [
        {"symbol": "XOM", "name": "Exxon Mobil"},
        {"symbol": "CVX", "name": "Chevron"},
    ],
    "biotech": [
        {"symbol": "AMGN", "name": "Amgen"},
        {"symbol": "GILD", "name": "Gilead Sciences"},
    ],
    "space": [
        {"symbol": "RKLB", "name": "Rocket Lab"},
        {"symbol": "LMT", "name": "Lockheed Martin"},
    ],
    "software": [
        {"symbol": "MSFT", "name": "Microsoft"},
        {"symbol": "ORCL", "name": "Oracle"},
    ],
    "internet_platforms": [
        {"symbol": "GOOG", "name": "Alphabet"},
        {"symbol": "META", "name": "Meta Platforms"},
    ],
}


def _analysis_marker(raw_payload_json: str | None) -> str | None:
    if not raw_payload_json:
        return None
    try:
        payload = json.loads(raw_payload_json)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    marker = payload.get("x_signal_llm_analysis")
    if not isinstance(marker, dict):
        return None
    source = marker.get("analysis_source")
    return str(source) if source else None


def _safe_slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip())
    return slug.strip("-") or "analysis"


def _batch_payload(
    *,
    analysis_source: str,
    batch_index: int,
    batch_count: int,
    posts: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "analysis_source": analysis_source,
        "batch_index": batch_index,
        "batch_count": batch_count,
        "instructions": {
            "sentiment_values": ["bullish", "bearish", "无法判断"],
            "rules": [
                "Read the full post content. Do not infer direction from fixed keyword matching.",
                "Record only direct investable targets that the author evaluates, buys, sells, shorts, recommends, warns about, or otherwise expresses a market view on.",
                "Do not record companies that appear only as customers, suppliers, peers, analogy examples, benchmark comparisons, index constituents, or background context unless they have their own market view.",
                "When a post compares one target against peer tickers to justify the target, record the target and any explicit sector/theme proxy, but do not record the peer tickers as stock items unless they have their own view.",
                "If the post names one or more individual stocks/companies with a direct market view, record those stocks.",
                "If the post explicitly names a sector/theme, also record the top two leaders for that sector/theme as sector_proxy items.",
                "If the post names no individual stock/company and only discusses a sector/theme, record the top two leaders for that sector/theme as sector_proxy items.",
                "If direction is not clear from tone and wording, use 无法判断.",
                "Use the tracker's listed ticker when a cashtag or company alias differs, for example GOOG for Google/Alphabet and ETOR for eToro.",
            ],
            "sector_proxy_note": "When using a sector leader, set mention_kind=sector_proxy, is_sector_proxy=true, sector_label, and proxy_reason.",
        },
        "sector_leaders": SECTOR_LEADERS,
        "result_schema": {
            "analysis_source": analysis_source,
            "results": [
                {
                    "post_id": 123,
                    "items": [
                        {
                            "symbol": "NVDA",
                            "mention_kind": "stock",
                            "sector_label": None,
                            "sentiment": "bullish",
                            "confidence": 0.8,
                            "reason": "Short rationale in Chinese or English.",
                            "is_sector_proxy": False,
                            "proxy_reason": None,
                        }
                    ],
                }
            ],
        },
        "posts": posts,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export saved X signal posts into JSON batches for LLM analysis."
    )
    parser.add_argument("--analysis-source", default="5.5ExtraHigh")
    parser.add_argument("--author", action="append", default=[], help="X handle to export. Can repeat.")
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--include-analyzed", action="store_true")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()

    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be positive.")

    handles = [normalize_x_handle(handle) for handle in args.author]
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = args.output_dir or (
        get_data_dir() / "x_signal_llm_batches" / f"{timestamp}_{_safe_slug(args.analysis_source)}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    with SessionLocal() as session:
        statement = (
            select(XSignalPost, XSignalAuthor)
            .join(XSignalAuthor, XSignalAuthor.id == XSignalPost.author_id)
            .order_by(XSignalAuthor.handle.asc(), XSignalPost.posted_at.asc(), XSignalPost.id.asc())
        )
        if handles:
            statement = statement.where(XSignalAuthor.handle.in_(handles))
        rows = session.execute(statement).all()

    posts: list[dict[str, Any]] = []
    skipped_analyzed = 0
    for post, author in rows:
        existing_source = _analysis_marker(post.raw_payload_json)
        if not args.include_analyzed and existing_source == args.analysis_source:
            skipped_analyzed += 1
            continue
        posts.append(
            {
                "post_id": post.id,
                "author_handle": author.handle,
                "posted_at": post.posted_at.isoformat(),
                "source_url": post.source_url,
                "already_analyzed_source": existing_source,
                "content": post.content,
            }
        )

    batch_count = (len(posts) + args.batch_size - 1) // args.batch_size
    for index in range(batch_count):
        batch_posts = posts[index * args.batch_size : (index + 1) * args.batch_size]
        payload = _batch_payload(
            analysis_source=args.analysis_source,
            batch_index=index + 1,
            batch_count=batch_count,
            posts=batch_posts,
        )
        path = output_dir / f"batch_{index + 1:04d}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    print(
        json.dumps(
            {
                "output_dir": str(output_dir),
                "analysis_source": args.analysis_source,
                "post_count": len(posts),
                "batch_count": batch_count,
                "skipped_analyzed": skipped_analyzed,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
