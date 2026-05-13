from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.x_signal_tracker import (
    ImportedXPost,
    add_x_signal_author,
    analyze_x_signal_author_posts,
    create_x_signal_fetch_request,
    get_x_signal_dashboard,
    import_x_signal_posts,
    restore_x_signal_mention_llm_sentiment,
    sample_import_posts_from_text,
    update_x_signal_mention_sentiment,
)

router = APIRouter(prefix="/x-signals", tags=["x-signals"])
_TRACKER_HTML_PATH = Path(__file__).resolve().parent.parent / "templates" / "x_signal_tracker.html"


class XSignalAuthorCreateRequest(BaseModel):
    handle: str
    display_name: str | None = None
    notes: str | None = None


class XSignalFetchRequestCreateRequest(BaseModel):
    lookback_months: int = Field(default=20, ge=1, le=120)


class XSignalPostImportItem(BaseModel):
    posted_at: datetime
    content: str
    external_post_id: str | None = None
    source_url: str | None = None
    raw_payload: dict[str, Any] | None = None


class XSignalPostImportRequest(BaseModel):
    posts: list[XSignalPostImportItem] = Field(default_factory=list)
    plain_text: str | None = None


class XSignalMentionSentimentUpdateRequest(BaseModel):
    sentiment: str


@router.get("/tracker", response_class=HTMLResponse, include_in_schema=False)
def x_signal_tracker_page() -> HTMLResponse:
    html = _TRACKER_HTML_PATH.read_text(encoding="utf-8")
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/dashboard")
def read_x_signal_dashboard(
    limit: int = Query(default=500, ge=1, le=2000),
) -> dict[str, object]:
    with SessionLocal() as session:
        dashboard = get_x_signal_dashboard(session, mention_limit=limit)
    return {"dashboard": dashboard.to_dict()}


@router.post("/authors")
def create_x_signal_author(payload: XSignalAuthorCreateRequest) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            author = add_x_signal_author(
                session,
                payload.handle,
                display_name=payload.display_name,
                notes=payload.notes,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"author": author.to_dict()}


@router.post("/authors/{author_id}/fetch-requests")
def create_fetch_request(
    author_id: int,
    payload: XSignalFetchRequestCreateRequest,
) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            fetch_request = create_x_signal_fetch_request(
                session,
                author_id,
                lookback_months=payload.lookback_months,
            )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"fetch_request": fetch_request.to_dict()}


@router.post("/authors/{author_id}/posts/import")
def import_posts(author_id: int, payload: XSignalPostImportRequest) -> dict[str, object]:
    posts = [
        ImportedXPost(
            posted_at=post.posted_at,
            content=post.content,
            external_post_id=post.external_post_id,
            source_url=post.source_url,
            raw_payload=post.raw_payload,
        )
        for post in payload.posts
    ]
    if payload.plain_text:
        posts.extend(sample_import_posts_from_text(payload.plain_text))

    try:
        with SessionLocal() as session:
            result = import_x_signal_posts(session, author_id, posts)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return {"import_result": result.to_dict()}


@router.post("/authors/{author_id}/analyze")
def analyze_author(author_id: int) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            result = analyze_x_signal_author_posts(session, author_id)
            dashboard = get_x_signal_dashboard(session)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return {
        "analysis_result": result.to_dict(),
        "dashboard": dashboard.to_dict(),
    }


@router.patch("/mentions/{mention_id}/sentiment")
def update_mention_sentiment(
    mention_id: int,
    payload: XSignalMentionSentimentUpdateRequest,
) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            mention = update_x_signal_mention_sentiment(session, mention_id, payload.sentiment)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"mention": mention.to_dict()}


@router.post("/mentions/{mention_id}/sentiment/restore")
def restore_mention_sentiment(mention_id: int) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            mention = restore_x_signal_mention_llm_sentiment(session, mention_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return {"mention": mention.to_dict()}
