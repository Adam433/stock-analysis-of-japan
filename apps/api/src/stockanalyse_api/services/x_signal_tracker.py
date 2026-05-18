from __future__ import annotations

import calendar
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import delete, func, or_, select

from stockanalyse_api.config.settings import get_data_dir
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.domain.x_signals.models import (
    XSignalAuthor,
    XSignalFetchRequest,
    XSignalMention,
    XSignalPost,
)

ANALYSIS_SOURCE = "extraction-v1"
LEGACY_ANALYSIS_SOURCES = ("heuristic-v1",)
PREFERRED_US_EXCHANGES = ("NASDAQ", "NYSE", "AMEX", "ARCA", "US")
INSTRUMENT_SYMBOL_ALIASES = {
    "GOOG": ("GOOGL",),
    "GOOGL": ("GOOG",),
    "ETOR": ("ETORO",),
    "ETORO": ("ETOR",),
}
TRACKER_SYMBOL_ALIASES = {
    "GOOGL": "GOOG",
    "ETORO": "ETOR",
}
TOKEN_STOPWORDS = {
    "A",
    "AI",
    "ALL",
    "AM",
    "AND",
    "ATH",
    "CEO",
    "CFO",
    "CNBC",
    "CPI",
    "ETF",
    "EPS",
    "EV",
    "FED",
    "FOMC",
    "GDP",
    "IPO",
    "IR",
    "LLM",
    "LOL",
    "MACD",
    "NASDAQ",
    "NYSE",
    "OR",
    "PE",
    "QE",
    "QT",
    "RIP",
    "ROI",
    "RSI",
    "SEC",
    "THE",
    "US",
    "USA",
    "USD",
    "X",
}
COMMON_TICKERS = {
    "AAPL",
    "ABBV",
    "AMD",
    "AMGN",
    "AMZN",
    "AVGO",
    "BA",
    "BABA",
    "BAC",
    "COIN",
    "COST",
    "CRM",
    "CRWD",
    "DIS",
    "GOOG",
    "GOOGL",
    "INTC",
    "JPM",
    "LLY",
    "META",
    "MRNA",
    "MSFT",
    "NFLX",
    "NKE",
    "NVDA",
    "ORCL",
    "PLTR",
    "QQQ",
    "RKLB",
    "SHOP",
    "SMCI",
    "SPY",
    "TSLA",
    "TSM",
    "UNH",
    "V",
    "XOM",
}
EXPLICIT_STOCK_ALIASES: dict[str, tuple[str, ...]] = {
    "AAPL": ("Apple", "苹果"),
    "AMD": ("Advanced Micro Devices", "超威半导体"),
    "AMZN": ("Amazon", "亚马逊"),
    "ARM": ("Arm Holdings", "ARM"),
    "ASML": ("ASML", "阿斯麦"),
    "AVGO": ("Broadcom", "博通"),
    "GOOG": ("Google", "Alphabet", "谷歌"),
    "INTC": ("Intel", "英特尔"),
    "META": ("Meta", "Facebook", "脸书"),
    "MSFT": ("Microsoft", "微软"),
    "NVDA": ("Nvidia", "英伟达"),
    "ORCL": ("Oracle", "甲骨文"),
    "PLTR": ("Palantir", "帕兰提尔"),
    "QCOM": ("Qualcomm", "高通"),
    "TSLA": ("Tesla", "特斯拉"),
    "TSM": ("TSMC", "Taiwan Semiconductor", "台积电"),
}
SYMBOL_PATTERN = re.compile(r"(?<![A-Z0-9])(\$?)([A-Z]{1,5})(?![A-Z0-9])")


@dataclass(slots=True)
class XSignalAuthorSummary:
    id: int
    handle: str
    display_name: str | None
    notes: str | None
    tracking_status: str
    post_count: int
    analyzed_post_count: int
    mention_count: int
    latest_posted_at: str | None
    last_fetch_requested_at: str | None
    last_analyzed_at: str | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class XSignalFetchRequestSummary:
    id: int
    author_id: int
    lookback_value: int
    lookback_unit: str
    requested_from: str
    requested_to: str
    source_url: str
    status: str
    created_at: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class XSignalImportResult:
    created_count: int
    updated_count: int
    total_posts: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class XSignalAnalysisResult:
    analyzed_posts: int
    mention_count: int
    analysis_source: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class XSignalLLMAnalysisItem:
    symbol: str
    sentiment: str
    mention_kind: str = "stock"
    sector_label: str | None = None
    confidence: Decimal | None = None
    reason: str | None = None
    is_sector_proxy: bool = False
    proxy_reason: str | None = None


@dataclass(frozen=True, slots=True)
class XSignalLLMPostAnalysis:
    post_id: int
    items: list[XSignalLLMAnalysisItem]


@dataclass(slots=True)
class XSignalMentionSummary:
    id: int
    author_id: int
    author_handle: str
    post_id: int | None
    symbol: str
    exchange: str | None
    company_name: str | None
    mention_kind: str
    sector_label: str | None
    sentiment: str
    llm_sentiment: str | None
    manual_sentiment: str | None
    sentiment_source: str
    confidence: str | None
    mention_date: str
    mention_count: int
    mentioned_at: str
    is_sector_proxy: bool
    proxy_reason: str | None
    source_text_excerpt: str | None
    source_post_ids: list[int]
    analysis_source: str
    mention_price_date: str | None
    mention_close: str | None
    latest_price_date: str | None
    latest_close: str | None
    cumulative_return: str | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class XSignalDashboardSummary:
    authors: list[XSignalAuthorSummary]
    mentions: list[XSignalMentionSummary]
    total_posts: int
    total_mentions: int
    latest_fetch_request: XSignalFetchRequestSummary | None

    def to_dict(self) -> dict[str, object]:
        return {
            "authors": [author.to_dict() for author in self.authors],
            "mentions": [mention.to_dict() for mention in self.mentions],
            "total_posts": self.total_posts,
            "total_mentions": self.total_mentions,
            "latest_fetch_request": (
                self.latest_fetch_request.to_dict() if self.latest_fetch_request else None
            ),
        }


@dataclass(frozen=True, slots=True)
class ImportedXPost:
    posted_at: datetime
    content: str
    external_post_id: str | None = None
    source_url: str | None = None
    raw_payload: dict[str, Any] | None = None


def normalize_x_handle(handle: str) -> str:
    normalized = handle.strip().lstrip("@").strip().lower()
    if not normalized:
        raise ValueError("X handle is required.")
    if not re.fullmatch(r"[a-z0-9_]{1,64}", normalized):
        raise ValueError("X handle may contain only letters, numbers, and underscores.")
    return normalized


def _is_llm_analysis_source(analysis_source: str | None) -> bool:
    return bool(analysis_source) and analysis_source not in (ANALYSIS_SOURCE, *LEGACY_ANALYSIS_SOURCES)


def _iso_datetime(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    return value.isoformat() if isinstance(value, datetime) else value


def _decimal_to_string(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None


def _content_hash(author_id: int, posted_at: datetime, content: str, external_post_id: str | None) -> str:
    normalized = "\n".join(
        [
            str(author_id),
            external_post_id or "",
            "" if external_post_id else posted_at.date().isoformat(),
            " ".join(content.split()),
        ]
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _subtract_months(day: date, months: int) -> date:
    month_index = day.month - 1 - months
    year = day.year + month_index // 12
    month = month_index % 12 + 1
    max_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(day.day, max_day))


def _to_fetch_summary(fetch_request: XSignalFetchRequest) -> XSignalFetchRequestSummary:
    return XSignalFetchRequestSummary(
        id=fetch_request.id,
        author_id=fetch_request.author_id,
        lookback_value=fetch_request.lookback_value,
        lookback_unit=fetch_request.lookback_unit,
        requested_from=fetch_request.requested_from.isoformat(),
        requested_to=fetch_request.requested_to.isoformat(),
        source_url=fetch_request.source_url,
        status=fetch_request.status,
        created_at=fetch_request.created_at.isoformat(),
    )


def add_x_signal_author(
    session,
    handle: str,
    *,
    display_name: str | None = None,
    notes: str | None = None,
) -> XSignalAuthorSummary:
    normalized_handle = normalize_x_handle(handle)
    existing = session.execute(
        select(XSignalAuthor).where(XSignalAuthor.handle == normalized_handle).limit(1)
    ).scalar_one_or_none()
    if existing is not None:
        if display_name is not None:
            existing.display_name = display_name.strip() or None
        if notes is not None:
            existing.notes = notes.strip() or None
        session.commit()
        return list_x_signal_authors(session, author_id=existing.id)[0]

    author = XSignalAuthor(
        handle=normalized_handle,
        display_name=display_name.strip() if display_name else None,
        notes=notes.strip() if notes else None,
    )
    session.add(author)
    session.commit()
    return list_x_signal_authors(session, author_id=author.id)[0]


def list_x_signal_authors(session, *, author_id: int | None = None) -> list[XSignalAuthorSummary]:
    post_counts = (
        select(
            XSignalPost.author_id,
            func.count(XSignalPost.id).label("post_count"),
            func.max(XSignalPost.posted_at).label("latest_posted_at"),
        )
        .group_by(XSignalPost.author_id)
        .subquery()
    )
    analyzed_post_counts = (
        select(XSignalPost.author_id, func.count(XSignalPost.id).label("analyzed_post_count"))
        .where(XSignalPost.raw_payload_json.like('%"x_signal_llm_analysis"%'))
        .group_by(XSignalPost.author_id)
        .subquery()
    )
    mention_counts = (
        select(XSignalMention.author_id, func.count(XSignalMention.id).label("mention_count"))
        .group_by(XSignalMention.author_id)
        .subquery()
    )
    statement = (
        select(
            XSignalAuthor,
            func.coalesce(post_counts.c.post_count, 0),
            func.coalesce(analyzed_post_counts.c.analyzed_post_count, 0),
            func.coalesce(mention_counts.c.mention_count, 0),
            post_counts.c.latest_posted_at,
        )
        .outerjoin(post_counts, post_counts.c.author_id == XSignalAuthor.id)
        .outerjoin(analyzed_post_counts, analyzed_post_counts.c.author_id == XSignalAuthor.id)
        .outerjoin(mention_counts, mention_counts.c.author_id == XSignalAuthor.id)
        .order_by(XSignalAuthor.handle.asc())
    )
    if author_id is not None:
        statement = statement.where(XSignalAuthor.id == author_id)

    rows = session.execute(statement).all()
    return [
        XSignalAuthorSummary(
            id=author.id,
            handle=author.handle,
            display_name=author.display_name,
            notes=author.notes,
            tracking_status=author.tracking_status,
            post_count=int(post_count),
            analyzed_post_count=int(analyzed_post_count),
            mention_count=int(mention_count),
            latest_posted_at=_iso_datetime(latest_posted_at),
            last_fetch_requested_at=_iso_datetime(author.last_fetch_requested_at),
            last_analyzed_at=_iso_datetime(author.last_analyzed_at),
        )
        for author, post_count, analyzed_post_count, mention_count, latest_posted_at in rows
    ]


def create_x_signal_fetch_request(
    session,
    author_id: int,
    *,
    lookback_months: int,
) -> XSignalFetchRequestSummary:
    author = session.get(XSignalAuthor, author_id)
    if author is None:
        raise LookupError("X signal author not found.")
    if lookback_months <= 0:
        raise ValueError("lookback_months must be positive.")

    requested_to = datetime.now(UTC).date()
    requested_from = _subtract_months(requested_to, lookback_months)
    source_url = f"https://x.com/{author.handle}"
    fetch_request = XSignalFetchRequest(
        author_id=author.id,
        lookback_value=lookback_months,
        lookback_unit="months",
        requested_from=requested_from,
        requested_to=requested_to,
        source_url=source_url,
        status="pending_chrome_capture",
    )
    author.last_fetch_requested_at = datetime.now(UTC)
    session.add(fetch_request)
    session.commit()
    session.refresh(fetch_request)
    return _to_fetch_summary(fetch_request)


def import_x_signal_posts(
    session,
    author_id: int,
    posts: list[ImportedXPost],
) -> XSignalImportResult:
    author = session.get(XSignalAuthor, author_id)
    if author is None:
        raise LookupError("X signal author not found.")

    created_count = 0
    updated_count = 0
    for post in posts:
        content = post.content.strip()
        if not content:
            continue
        content_hash = _content_hash(author.id, post.posted_at, content, post.external_post_id)
        existing = session.execute(
            select(XSignalPost)
            .where(
                XSignalPost.author_id == author.id,
                or_(
                    XSignalPost.content_hash == content_hash,
                    XSignalPost.external_post_id == post.external_post_id
                    if post.external_post_id
                    else XSignalPost.content_hash == content_hash,
                ),
            )
            .limit(1)
        ).scalar_one_or_none()
        raw_payload_json = (
            json.dumps(post.raw_payload, ensure_ascii=False, sort_keys=True)
            if post.raw_payload is not None
            else None
        )
        if existing is None:
            session.add(
                XSignalPost(
                    author_id=author.id,
                    external_post_id=post.external_post_id,
                    posted_at=post.posted_at,
                    content=content,
                    content_hash=content_hash,
                    source_url=post.source_url,
                    raw_payload_json=raw_payload_json,
                )
            )
            created_count += 1
        else:
            existing.posted_at = post.posted_at
            existing.content = content
            existing.content_hash = content_hash
            existing.source_url = post.source_url
            existing.raw_payload_json = raw_payload_json
            updated_count += 1

    latest_fetch_request = session.execute(
        select(XSignalFetchRequest)
        .where(
            XSignalFetchRequest.author_id == author.id,
            XSignalFetchRequest.status == "pending_chrome_capture",
        )
        .order_by(XSignalFetchRequest.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    if latest_fetch_request is not None and created_count + updated_count > 0:
        latest_fetch_request.status = "imported"

    session.commit()
    total_posts = session.execute(
        select(func.count(XSignalPost.id)).where(XSignalPost.author_id == author.id)
    ).scalar_one()
    return XSignalImportResult(
        created_count=created_count,
        updated_count=updated_count,
        total_posts=int(total_posts),
    )


def _find_instrument(session, symbol: str) -> Instrument | None:
    normalized_symbol = _normalize_tracker_symbol(symbol)
    candidate_symbols = (
        normalized_symbol,
        *INSTRUMENT_SYMBOL_ALIASES.get(normalized_symbol, ()),
        *INSTRUMENT_SYMBOL_ALIASES.get(symbol.upper(), ()),
    )
    for candidate_symbol in candidate_symbols:
        matches = session.execute(
            select(Instrument).where(func.upper(Instrument.symbol) == candidate_symbol)
        ).scalars().all()
        for exchange in PREFERRED_US_EXCHANGES:
            for instrument in matches:
                if instrument.exchange.upper() == exchange:
                    return instrument
        if matches:
            return matches[0]
    return None


def _normalize_tracker_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper().lstrip("$")
    return TRACKER_SYMBOL_ALIASES.get(normalized, normalized)


def _extract_symbols(session, content: str) -> list[tuple[str, Instrument | None]]:
    seen: set[str] = set()
    symbols: list[tuple[str, Instrument | None]] = []
    for marker, raw_symbol in SYMBOL_PATTERN.findall(content.upper()):
        symbol = raw_symbol.upper()
        if symbol in seen or symbol in TOKEN_STOPWORDS:
            continue
        instrument = _find_instrument(session, symbol)
        if marker == "$":
            seen.add(symbol)
            symbols.append((symbol, instrument))
    for symbol, aliases in EXPLICIT_STOCK_ALIASES.items():
        if symbol in seen:
            continue
        if not any(_contains_explicit_stock_alias(content, alias) for alias in aliases):
            continue
        seen.add(symbol)
        symbols.append((symbol, _find_instrument(session, symbol)))
    return symbols


def _contains_explicit_stock_alias(content: str, alias: str) -> bool:
    if any(ord(char) > 127 for char in alias):
        return alias in content
    return re.search(rf"(?<![A-Za-z0-9]){re.escape(alias)}(?![A-Za-z0-9])", content, re.IGNORECASE) is not None


def _infer_sentiment(content: str) -> tuple[str, Decimal]:
    _ = content
    return "unknown", Decimal("0.0000")


def _merge_sentiment(current: str, incoming: str) -> str:
    if current == incoming:
        return current
    if current == "unknown":
        return incoming
    if incoming == "unknown":
        return current
    return "unknown"


def _normalize_llm_sentiment(sentiment: str) -> str:
    normalized = sentiment.strip().lower()
    mapping = {
        "bullish": "bullish",
        "positive": "bullish",
        "long": "bullish",
        "看涨": "bullish",
        "看多": "bullish",
        "利多": "bullish",
        "bearish": "bearish",
        "negative": "bearish",
        "short": "bearish",
        "看跌": "bearish",
        "看空": "bearish",
        "利空": "bearish",
        "unknown": "unknown",
        "undetermined": "unknown",
        "unable_to_determine": "unknown",
        "cannot_determine": "unknown",
        "neutral": "unknown",
        "unclear": "unknown",
        "mixed": "unknown",
        "无法判断": "unknown",
        "不明": "unknown",
        "中性": "unknown",
        "混合": "unknown",
    }
    return mapping.get(normalized, "unknown")


def _normalize_manual_sentiment(sentiment: str) -> str:
    normalized_input = sentiment.strip().lower()
    valid_inputs = {
        "bullish",
        "positive",
        "long",
        "看涨",
        "看多",
        "利多",
        "bearish",
        "negative",
        "short",
        "看跌",
        "看空",
        "利空",
        "unknown",
        "undetermined",
        "unable_to_determine",
        "cannot_determine",
        "unclear",
        "mixed",
        "无法判断",
        "不明",
        "混合",
    }
    if normalized_input not in valid_inputs:
        raise ValueError("sentiment must be bullish, bearish, or unable to determine.")
    normalized = _normalize_llm_sentiment(sentiment)
    if sentiment.strip().lower() in {"neutral", "中性"}:
        raise ValueError("manual sentiment does not support neutral; use 无法判断 when direction is unclear.")
    return normalized


def _normalize_mention_kind(mention_kind: str, is_sector_proxy: bool) -> str:
    if is_sector_proxy or mention_kind.strip().lower() in {"sector", "sector_proxy", "theme_proxy"}:
        return "sector_proxy"
    return "stock"


def _load_raw_payload(raw_payload_json: str | None) -> dict[str, object]:
    if not raw_payload_json:
        return {}
    try:
        payload = json.loads(raw_payload_json)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _mark_post_llm_analyzed(
    post: XSignalPost,
    *,
    analysis_source: str,
    analyzed_at: datetime,
    item_count: int,
) -> None:
    payload = _load_raw_payload(post.raw_payload_json)
    payload["x_signal_llm_analysis"] = {
        "analysis_source": analysis_source,
        "analyzed_at": analyzed_at.isoformat(),
        "item_count": item_count,
    }
    post.raw_payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _sentiment_override_key(symbol: str, mention_date: date, source_post_ids: list[int]) -> str:
    post_ids = ",".join(str(post_id) for post_id in sorted(source_post_ids))
    return f"{symbol}|{mention_date.isoformat()}|{post_ids}"


def _load_sentiment_overrides(handle: str) -> dict[str, dict[str, object]]:
    normalized_handle = normalize_x_handle(handle)
    path = get_data_dir() / f"x_signal_llm_sentiment_{normalized_handle}.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    rows = payload.get("overrides", []) if isinstance(payload, dict) else []
    overrides = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").upper()
        mention_date = str(row.get("mention_date") or "")
        sentiment = str(row.get("sentiment") or "")
        source_post_ids = row.get("source_post_ids") or []
        if sentiment not in {"bullish", "bearish", "neutral", "unknown"}:
            continue
        if not symbol or not mention_date or not isinstance(source_post_ids, list):
            continue
        try:
            post_ids = [int(value) for value in source_post_ids]
            key = _sentiment_override_key(symbol, date.fromisoformat(mention_date), post_ids)
        except (TypeError, ValueError):
            continue
        overrides[key] = row
    return overrides


def _build_excerpt(content: str) -> str:
    return content.strip()


def _add_excerpt(current: str | None, content: str) -> str:
    next_excerpt = _build_excerpt(content)
    if not current:
        return next_excerpt
    if next_excerpt in current:
        return current
    return f"{current}\n\n---\n\n{next_excerpt}"


def _register_aggregated_mention(
    mentions_by_day: dict[tuple[str, date], dict[str, object]],
    *,
    author_id: int,
    post: XSignalPost,
    symbol: str,
    instrument: Instrument | None,
    mention_kind: str,
    sector_label: str | None,
    sentiment: str,
    confidence: Decimal,
    is_sector_proxy: bool,
    proxy_reason: str | None = None,
    analysis_note: str | None = None,
) -> None:
    mention_date = post.posted_at.date()
    key = (symbol, mention_date)
    existing = mentions_by_day.get(key)
    if existing is None:
        mentions_by_day[key] = {
            "author_id": author_id,
            "post_id": post.id,
            "source_post_ids": {post.id},
            "instrument_id": instrument.id if instrument else None,
            "symbol": symbol,
            "exchange": instrument.exchange if instrument else None,
            "company_name": instrument.name if instrument else None,
            "mention_kind": mention_kind,
            "sector_label": sector_label,
            "sentiment": sentiment,
            "confidence": confidence,
            "mention_date": mention_date,
            "mentioned_at": post.posted_at,
            "mention_count": 1,
            "is_sector_proxy": is_sector_proxy,
            "proxy_reason": proxy_reason,
            "source_text_excerpt": _build_excerpt(post.content),
            "analysis_note": analysis_note,
        }
        return

    source_post_ids = existing["source_post_ids"]
    already_counted = False
    if isinstance(source_post_ids, set):
        already_counted = post.id in source_post_ids
        source_post_ids.add(post.id)
    if not already_counted:
        existing["mention_count"] = int(existing["mention_count"]) + 1
    existing["sentiment"] = _merge_sentiment(str(existing["sentiment"]), sentiment)
    existing["confidence"] = max(existing["confidence"], confidence)  # type: ignore[arg-type]
    if post.posted_at < existing["mentioned_at"]:  # type: ignore[operator]
        existing["mentioned_at"] = post.posted_at
        existing["post_id"] = post.id
    if mention_kind == "stock" and existing["mention_kind"] != "stock":
        existing["mention_kind"] = "stock"
        existing["sector_label"] = None
        existing["is_sector_proxy"] = False
        existing["proxy_reason"] = None
    existing["source_text_excerpt"] = _add_excerpt(
        existing["source_text_excerpt"] if isinstance(existing["source_text_excerpt"], str) else None,
        post.content,
    )
    if analysis_note:
        existing_note = existing.get("analysis_note")
        if not isinstance(existing_note, str) or not existing_note:
            existing["analysis_note"] = analysis_note
        elif analysis_note not in existing_note:
            existing["analysis_note"] = f"{existing_note}\n\n{analysis_note}"


def apply_x_signal_llm_analysis_results(
    session,
    post_analyses: list[XSignalLLMPostAnalysis],
    *,
    analysis_source: str,
    replace_author_mentions: bool = True,
) -> XSignalAnalysisResult:
    if not analysis_source.strip():
        raise ValueError("analysis_source is required.")
    normalized_source = analysis_source.strip()
    if len(normalized_source) > 64:
        raise ValueError("analysis_source must be 64 characters or fewer.")
    if not post_analyses:
        return XSignalAnalysisResult(
            analyzed_posts=0,
            mention_count=0,
            analysis_source=normalized_source,
        )

    post_ids = [analysis.post_id for analysis in post_analyses]
    posts = session.execute(
        select(XSignalPost).where(XSignalPost.id.in_(post_ids))
    ).scalars().all()
    posts_by_id = {post.id: post for post in posts}
    missing_post_ids = sorted(set(post_ids) - set(posts_by_id))
    if missing_post_ids:
        raise LookupError(f"X signal posts not found: {missing_post_ids}")

    analyses_by_author: dict[int, list[XSignalLLMPostAnalysis]] = {}
    for analysis in post_analyses:
        post = posts_by_id[analysis.post_id]
        analyses_by_author.setdefault(post.author_id, []).append(analysis)

    mention_total = 0
    analyzed_at = datetime.now(UTC)
    sources_to_replace = (ANALYSIS_SOURCE, *LEGACY_ANALYSIS_SOURCES, normalized_source)
    for author_id, author_analyses in analyses_by_author.items():
        if replace_author_mentions:
            session.execute(
                delete(XSignalMention).where(
                    XSignalMention.author_id == author_id,
                    XSignalMention.analysis_source.in_(sources_to_replace),
                )
            )
        else:
            source_post_id_strings = {str(analysis.post_id) for analysis in author_analyses}
            existing_mentions = session.execute(
                select(XSignalMention).where(
                    XSignalMention.author_id == author_id,
                    XSignalMention.analysis_source.in_(sources_to_replace),
                )
            ).scalars().all()
            for mention in existing_mentions:
                try:
                    source_ids = {
                        str(value) for value in json.loads(mention.source_post_ids_json or "[]")
                    }
                except (TypeError, json.JSONDecodeError):
                    source_ids = set()
                if source_ids & source_post_id_strings:
                    session.delete(mention)

        mentions_by_day: dict[tuple[str, date], dict[str, object]] = {}
        for analysis in sorted(
            author_analyses,
            key=lambda item: (posts_by_id[item.post_id].posted_at, item.post_id),
        ):
            post = posts_by_id[analysis.post_id]
            _mark_post_llm_analyzed(
                post,
                analysis_source=normalized_source,
                analyzed_at=analyzed_at,
                item_count=len(analysis.items),
            )
            for item in analysis.items:
                symbol = _normalize_tracker_symbol(item.symbol)
                if not symbol:
                    continue
                mention_kind = _normalize_mention_kind(item.mention_kind, item.is_sector_proxy)
                confidence = item.confidence if item.confidence is not None else Decimal("0.0000")
                if confidence < 0:
                    confidence = Decimal("0.0000")
                if confidence > 1:
                    confidence = Decimal("1.0000")
                _register_aggregated_mention(
                    mentions_by_day,
                    author_id=author_id,
                    post=post,
                    symbol=symbol,
                    instrument=_find_instrument(session, symbol),
                    mention_kind=mention_kind,
                    sector_label=item.sector_label,
                    sentiment=_normalize_llm_sentiment(item.sentiment),
                    confidence=confidence.quantize(Decimal("0.0001")),
                    is_sector_proxy=mention_kind == "sector_proxy",
                    proxy_reason=item.proxy_reason,
                    analysis_note=item.reason,
                )

        for mention_data in mentions_by_day.values():
            source_post_ids = sorted(mention_data["source_post_ids"])
            session.add(
                XSignalMention(
                    author_id=mention_data["author_id"],
                    post_id=mention_data["post_id"],
                    instrument_id=mention_data["instrument_id"],
                    symbol=mention_data["symbol"],
                    exchange=mention_data["exchange"],
                    company_name=mention_data["company_name"],
                    mention_kind=mention_data["mention_kind"],
                    sector_label=mention_data["sector_label"],
                    sentiment=mention_data["sentiment"],
                    confidence=mention_data["confidence"],
                    mention_date=mention_data["mention_date"],
                    mention_count=mention_data["mention_count"],
                    mentioned_at=mention_data["mentioned_at"],
                    is_sector_proxy=mention_data["is_sector_proxy"],
                    proxy_reason=mention_data["proxy_reason"],
                    source_text_excerpt=mention_data["source_text_excerpt"],
                    source_post_ids_json=json.dumps(source_post_ids, sort_keys=True),
                    analysis_source=normalized_source,
                    analysis_note=mention_data.get("analysis_note"),
                    llm_sentiment=mention_data["sentiment"],
                    manual_sentiment=None,
                    sentiment_source="llm",
                )
            )
        mention_total += len(mentions_by_day)

        author = session.get(XSignalAuthor, author_id)
        if author is not None:
            author.last_analyzed_at = analyzed_at

    session.commit()
    return XSignalAnalysisResult(
        analyzed_posts=len(post_analyses),
        mention_count=mention_total,
        analysis_source=normalized_source,
    )


def analyze_x_signal_author_posts(session, author_id: int) -> XSignalAnalysisResult:
    author = session.get(XSignalAuthor, author_id)
    if author is None:
        raise LookupError("X signal author not found.")

    sentiment_overrides = _load_sentiment_overrides(author.handle)
    posts = session.execute(
        select(XSignalPost)
        .where(XSignalPost.author_id == author.id)
        .order_by(XSignalPost.posted_at.asc(), XSignalPost.id.asc())
    ).scalars().all()

    session.execute(
        delete(XSignalMention).where(
            XSignalMention.author_id == author.id,
            XSignalMention.analysis_source.in_((ANALYSIS_SOURCE, *LEGACY_ANALYSIS_SOURCES)),
        )
    )
    mentions_by_day: dict[tuple[str, date], dict[str, object]] = {}
    for post in posts:
        sentiment, confidence = _infer_sentiment(post.content)
        symbols = _extract_symbols(session, post.content)
        if symbols:
            for symbol, instrument in symbols:
                _register_aggregated_mention(
                    mentions_by_day,
                    author_id=author.id,
                    post=post,
                    symbol=symbol,
                    instrument=instrument,
                    mention_kind="stock",
                    sector_label=None,
                    sentiment=sentiment,
                    confidence=confidence,
                    is_sector_proxy=False,
                )
            continue

        continue

    for mention_data in mentions_by_day.values():
        source_post_ids = sorted(mention_data["source_post_ids"])
        override = sentiment_overrides.get(
            _sentiment_override_key(
                str(mention_data["symbol"]),
                mention_data["mention_date"],  # type: ignore[arg-type]
                source_post_ids,
            )
        )
        if override is not None:
            mention_data["sentiment"] = override["sentiment"]
            if override.get("confidence") is not None:
                try:
                    mention_data["confidence"] = Decimal(str(override["confidence"]))
                except Exception:
                    pass
        session.add(
            XSignalMention(
                author_id=mention_data["author_id"],
                post_id=mention_data["post_id"],
                instrument_id=mention_data["instrument_id"],
                symbol=mention_data["symbol"],
                exchange=mention_data["exchange"],
                company_name=mention_data["company_name"],
                mention_kind=mention_data["mention_kind"],
                sector_label=mention_data["sector_label"],
                sentiment=mention_data["sentiment"],
                confidence=mention_data["confidence"],
                mention_date=mention_data["mention_date"],
                mention_count=mention_data["mention_count"],
                mentioned_at=mention_data["mentioned_at"],
                is_sector_proxy=mention_data["is_sector_proxy"],
                proxy_reason=mention_data["proxy_reason"],
                source_text_excerpt=mention_data["source_text_excerpt"],
                source_post_ids_json=json.dumps(source_post_ids, sort_keys=True),
                analysis_source=ANALYSIS_SOURCE,
                llm_sentiment=None,
                manual_sentiment=None,
                sentiment_source="extraction",
            )
        )

    author.last_analyzed_at = datetime.now(UTC)
    session.commit()
    return XSignalAnalysisResult(
        analyzed_posts=len(posts),
        mention_count=len(mentions_by_day),
        analysis_source=ANALYSIS_SOURCE,
    )


def _close_on_or_before(session, instrument_id: int, target_date: date) -> MarketDataDaily | None:
    return session.execute(
        select(MarketDataDaily)
        .where(
            MarketDataDaily.instrument_id == instrument_id,
            MarketDataDaily.trade_date <= target_date,
            MarketDataDaily.close.is_not(None),
        )
        .order_by(MarketDataDaily.trade_date.desc())
        .limit(1)
    ).scalar_one_or_none()


def _latest_close(session, instrument_id: int) -> MarketDataDaily | None:
    return session.execute(
        select(MarketDataDaily)
        .where(
            MarketDataDaily.instrument_id == instrument_id,
            MarketDataDaily.close.is_not(None),
        )
        .order_by(MarketDataDaily.trade_date.desc())
        .limit(1)
    ).scalar_one_or_none()


def _mention_to_summary(
    session,
    mention: XSignalMention,
    author: XSignalAuthor,
) -> XSignalMentionSummary:
    mention_price = None
    latest_price = None
    cumulative_return = None
    if mention.instrument_id is not None:
        mention_price = _close_on_or_before(session, mention.instrument_id, mention.mentioned_at.date())
        latest_price = _latest_close(session, mention.instrument_id)
        if (
            mention_price is not None
            and mention_price.close is not None
            and latest_price is not None
            and latest_price.close is not None
            and mention_price.close != 0
        ):
            cumulative_return = (latest_price.close - mention_price.close) / mention_price.close
    source_post_ids = []
    if mention.source_post_ids_json:
        try:
            source_post_ids = [int(value) for value in json.loads(mention.source_post_ids_json)]
        except (TypeError, ValueError, json.JSONDecodeError):
            source_post_ids = []

    return XSignalMentionSummary(
        id=mention.id,
        author_id=author.id,
        author_handle=author.handle,
        post_id=mention.post_id,
        symbol=mention.symbol,
        exchange=mention.exchange,
        company_name=mention.company_name,
        mention_kind=mention.mention_kind,
        sector_label=mention.sector_label,
        sentiment=mention.sentiment,
        llm_sentiment=mention.llm_sentiment,
        manual_sentiment=mention.manual_sentiment,
        sentiment_source=mention.sentiment_source,
        confidence=_decimal_to_string(mention.confidence),
        mention_date=mention.mention_date.isoformat(),
        mention_count=mention.mention_count,
        mentioned_at=mention.mentioned_at.isoformat(),
        is_sector_proxy=mention.is_sector_proxy,
        proxy_reason=mention.proxy_reason,
        source_text_excerpt=mention.source_text_excerpt,
        source_post_ids=source_post_ids,
        analysis_source=mention.analysis_source,
        mention_price_date=mention_price.trade_date.isoformat() if mention_price else None,
        mention_close=_decimal_to_string(mention_price.close) if mention_price else None,
        latest_price_date=latest_price.trade_date.isoformat() if latest_price else None,
        latest_close=_decimal_to_string(latest_price.close) if latest_price else None,
        cumulative_return=_decimal_to_string(cumulative_return),
    )


def list_x_signal_mentions(session, *, limit: int = 100) -> list[XSignalMentionSummary]:
    rows = session.execute(
        select(XSignalMention, XSignalAuthor)
        .join(XSignalAuthor, XSignalAuthor.id == XSignalMention.author_id)
        .order_by(XSignalMention.mentioned_at.desc(), XSignalMention.id.desc())
        .limit(limit)
    ).all()
    return [_mention_to_summary(session, mention, author) for mention, author in rows]


def update_x_signal_mention_sentiment(
    session,
    mention_id: int,
    sentiment: str,
) -> XSignalMentionSummary:
    mention = session.get(XSignalMention, mention_id)
    if mention is None:
        raise LookupError("X signal mention not found.")
    manual_sentiment = _normalize_manual_sentiment(sentiment)
    mention.manual_sentiment = manual_sentiment
    mention.sentiment = manual_sentiment
    mention.sentiment_source = "manual"
    session.commit()
    author = session.get(XSignalAuthor, mention.author_id)
    if author is None:
        raise LookupError("X signal author not found.")
    return _mention_to_summary(session, mention, author)


def restore_x_signal_mention_llm_sentiment(
    session,
    mention_id: int,
) -> XSignalMentionSummary:
    mention = session.get(XSignalMention, mention_id)
    if mention is None:
        raise LookupError("X signal mention not found.")
    mention.manual_sentiment = None
    if mention.llm_sentiment is not None or _is_llm_analysis_source(mention.analysis_source):
        mention.sentiment = mention.llm_sentiment or "unknown"
        mention.sentiment_source = "llm"
    else:
        mention.sentiment = "unknown"
        mention.sentiment_source = "extraction"
    session.commit()
    author = session.get(XSignalAuthor, mention.author_id)
    if author is None:
        raise LookupError("X signal author not found.")
    return _mention_to_summary(session, mention, author)


def get_x_signal_dashboard(session, *, mention_limit: int = 120) -> XSignalDashboardSummary:
    authors = list_x_signal_authors(session)
    mentions = list_x_signal_mentions(session, limit=mention_limit)
    total_posts = session.execute(select(func.count(XSignalPost.id))).scalar_one()
    total_mentions = session.execute(select(func.count(XSignalMention.id))).scalar_one()
    latest_fetch = session.execute(
        select(XSignalFetchRequest)
        .order_by(XSignalFetchRequest.created_at.desc(), XSignalFetchRequest.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    return XSignalDashboardSummary(
        authors=authors,
        mentions=mentions,
        total_posts=int(total_posts),
        total_mentions=int(total_mentions),
        latest_fetch_request=_to_fetch_summary(latest_fetch) if latest_fetch else None,
    )


def sample_import_posts_from_text(text: str) -> list[ImportedXPost]:
    now = datetime.now(UTC)
    posts = []
    for index, block in enumerate(part.strip() for part in text.split("\n\n") if part.strip()):
        posts.append(
            ImportedXPost(
                posted_at=now - timedelta(minutes=index),
                content=block,
            )
        )
    return posts
