from __future__ import annotations

import argparse
import html
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.maintenance.import_x_signal_status_ids import best_tweet_text, load_main_js, make_opener, unwrap_tweet
from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.x_signal_tracker import (
    ImportedXPost,
    add_x_signal_author,
    create_x_signal_fetch_request,
    import_x_signal_posts,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handle", required=True)
    parser.add_argument("--cutoff", required=True, help="Inclusive UTC date, e.g. 2024-09-17")
    parser.add_argument("--until", required=True, help="Exclusive UTC date, e.g. 2026-05-18")
    parser.add_argument("--status-json", required=True)
    parser.add_argument("--posts-json", required=True)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=300)
    parser.add_argument("--sleep", type=float, default=0.35)
    parser.add_argument("--include-replies", action="store_true")
    parser.add_argument("--import-db", action="store_true")
    parser.add_argument("--lookback-months", type=int, default=20)
    return parser.parse_args()


def extract_operation_metadata(js_text: str, operation_name: str) -> tuple[str, dict[str, bool], dict[str, bool]]:
    operation = re.search(
        r"queryId:\"([^\"]+)\",operationName:\""
        + re.escape(operation_name)
        + r"\".*?metadata:\{featureSwitches:\[(.*?)\],fieldToggles:\[(.*?)\]",
        js_text,
    )
    if operation is None:
        raise RuntimeError(f"Could not locate X GraphQL metadata for {operation_name}.")
    features = {name: True for name in re.findall(r"\"([^\"]+)\"", operation.group(2))}
    fields = {name: True for name in re.findall(r"\"([^\"]+)\"", operation.group(3))}
    return operation.group(1), features, fields


def extract_bearer(js_text: str) -> str:
    bearer = re.search(r"AAAAAAAA[A-Za-z0-9%]+", js_text)
    if bearer is None:
        raise RuntimeError("Could not locate X bearer token in main JavaScript bundle.")
    return bearer.group(0)


def graphql_get(
    opener: urllib.request.OpenerDirector,
    headers: dict[str, str],
    *,
    query_id: str,
    operation_name: str,
    variables: dict[str, Any],
    features: dict[str, bool],
    fields: dict[str, bool],
) -> dict[str, Any]:
    params = {
        "variables": json.dumps(variables, separators=(",", ":")),
        "features": json.dumps(features, separators=(",", ":")),
        "fieldToggles": json.dumps(fields, separators=(",", ":")),
    }
    url = f"https://x.com/i/api/graphql/{query_id}/{operation_name}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers=headers)
    with opener.open(request, timeout=25) as response:
        return json.loads(response.read().decode("utf-8"))


def user_rest_id(
    opener: urllib.request.OpenerDirector,
    headers: dict[str, str],
    *,
    handle: str,
    query_id: str,
    features: dict[str, bool],
    fields: dict[str, bool],
) -> tuple[str, str | None]:
    document = graphql_get(
        opener,
        headers,
        query_id=query_id,
        operation_name="UserByScreenName",
        variables={"screen_name": handle},
        features=features,
        fields=fields,
    )
    result = document.get("data", {}).get("user", {}).get("result", {})
    if result.get("__typename") != "User" or not result.get("rest_id"):
        raise RuntimeError(f"Could not resolve X user @{handle}.")
    return str(result["rest_id"]), (result.get("core") or {}).get("name")


def timeline_instructions(document: dict[str, Any]) -> list[dict[str, Any]]:
    return (
        document.get("data", {})
        .get("user", {})
        .get("result", {})
        .get("timeline", {})
        .get("timeline", {})
        .get("instructions", [])
    )


def instruction_entries(instructions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for instruction in instructions:
        if isinstance(instruction.get("entries"), list):
            entries.extend(instruction["entries"])
        if isinstance(instruction.get("entry"), dict):
            entries.append(instruction["entry"])
    return entries


def bottom_cursor(entries: list[dict[str, Any]]) -> str | None:
    for entry in entries:
        content = entry.get("content") or {}
        if (
            content.get("entryType") == "TimelineTimelineCursor"
            and content.get("cursorType") == "Bottom"
            and content.get("value")
        ):
            return str(content["value"])
    return None


def timeline_tweet_results(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for entry in entries:
        content = entry.get("content") or {}
        item_content = content.get("itemContent") or {}
        result = (item_content.get("tweet_results") or {}).get("result")
        if isinstance(result, dict):
            results.append(result)

        for module_item in content.get("items") or []:
            module_content = (module_item.get("item") or {}).get("itemContent") or {}
            result = (module_content.get("tweet_results") or {}).get("result")
            if isinstance(result, dict):
                results.append(result)
    return results


def parse_tweet(
    tweet_result: dict[str, Any],
    *,
    handle: str,
    cutoff: datetime,
    until: datetime,
    include_replies: bool,
) -> tuple[dict[str, Any] | None, datetime | None]:
    tweet = unwrap_tweet(tweet_result)
    if tweet is None:
        return None, None

    legacy = tweet.get("legacy") or {}
    user = (tweet.get("core") or {}).get("user_results", {}).get("result", {})
    screen_name = (
        (user.get("core") or {}).get("screen_name")
        or (user.get("legacy") or {}).get("screen_name")
        or ""
    ).lower()
    if screen_name != handle:
        return None, None

    created_raw = legacy.get("created_at")
    if not created_raw:
        return None, None
    created_at = parsedate_to_datetime(created_raw).astimezone(UTC)

    if not include_replies and legacy.get("in_reply_to_status_id_str"):
        return None, created_at
    if legacy.get("retweeted_status_result"):
        return None, created_at
    if not (cutoff <= created_at < until):
        return None, created_at

    tweet_id = str(tweet.get("rest_id") or legacy.get("id_str") or "")
    content = html.unescape(best_tweet_text(tweet)).strip()
    if not tweet_id or not content:
        return None, created_at

    return (
        {
            "id": tweet_id,
            "posted_at": created_at.isoformat(),
            "content": content,
            "source_url": f"https://x.com/{handle}/status/{tweet_id}",
            "lang": legacy.get("lang"),
            "favorite_count": legacy.get("favorite_count"),
            "reply_count": legacy.get("reply_count"),
            "retweet_count": legacy.get("retweet_count"),
            "quote_count": legacy.get("quote_count"),
            "is_reply": bool(legacy.get("in_reply_to_status_id_str")),
            "in_reply_to_status_id": legacy.get("in_reply_to_status_id_str"),
            "quoted_status_id": legacy.get("quoted_status_id_str"),
        },
        created_at,
    )


def write_outputs(
    *,
    handle: str,
    cutoff: str,
    until: str,
    status_path: Path,
    posts_path: Path,
    ids: dict[str, dict[str, Any]],
    posts: dict[str, dict[str, Any]],
    pages: list[dict[str, Any]],
    display_name: str | None,
    user_id: str | None,
    complete: bool,
) -> None:
    captured_at = datetime.now(UTC).isoformat()
    status_path.write_text(
        json.dumps(
            {
                "handle": handle,
                "display_name": display_name,
                "user_id": user_id,
                "cutoff": cutoff,
                "until": until,
                "captured_at": captured_at,
                "complete": complete,
                "ids": ids,
                "pages": pages,
                "count": len(ids),
                "source": "UserTweetsGuest",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    posts_path.write_text(
        json.dumps(
            {
                "handle": handle,
                "display_name": display_name,
                "user_id": user_id,
                "cutoff": cutoff,
                "until": until,
                "captured_at": captured_at,
                "complete": complete,
                "posts": sorted(posts.values(), key=lambda post: post["posted_at"], reverse=True),
                "count": len(posts),
                "source": "UserTweetsGuest",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def import_posts_to_db(
    *,
    handle: str,
    display_name: str | None,
    posts: dict[str, dict[str, Any]],
    lookback_months: int,
) -> dict[str, int]:
    imported_posts = [
        ImportedXPost(
            posted_at=datetime.fromisoformat(post["posted_at"]),
            content=post["content"],
            external_post_id=post["id"],
            source_url=post["source_url"],
            raw_payload={
                "capture_source": "user_tweets_guest",
                "lang": post.get("lang"),
                "favorite_count": post.get("favorite_count"),
                "reply_count": post.get("reply_count"),
                "retweet_count": post.get("retweet_count"),
                "quote_count": post.get("quote_count"),
                "is_reply": post.get("is_reply"),
                "in_reply_to_status_id": post.get("in_reply_to_status_id"),
                "quoted_status_id": post.get("quoted_status_id"),
            },
        )
        for post in posts.values()
    ]

    with SessionLocal() as session:
        author_summary = add_x_signal_author(session, handle, display_name=display_name)
        create_x_signal_fetch_request(session, author_summary.id, lookback_months=lookback_months)
        import_result = import_x_signal_posts(session, author_summary.id, imported_posts)
    return import_result.to_dict()


def main() -> None:
    args = parse_args()
    handle = args.handle.lower().lstrip("@")
    cutoff = datetime.fromisoformat(args.cutoff).replace(tzinfo=UTC)
    until = datetime.fromisoformat(args.until).replace(tzinfo=UTC)
    status_path = Path(args.status_json)
    posts_path = Path(args.posts_json)
    status_path.parent.mkdir(parents=True, exist_ok=True)
    posts_path.parent.mkdir(parents=True, exist_ok=True)

    js_text = load_main_js()
    bearer = extract_bearer(js_text)
    user_query_id, user_features, user_fields = extract_operation_metadata(js_text, "UserByScreenName")
    tweets_query_id, tweets_features, tweets_fields = extract_operation_metadata(js_text, "UserTweets")
    opener, headers = make_opener(bearer)

    rest_id, display_name = user_rest_id(
        opener,
        headers,
        handle=handle,
        query_id=user_query_id,
        features=user_features,
        fields=user_fields,
    )
    print(f"resolved @{handle} user_id={rest_id} display_name={display_name or ''}", flush=True)

    ids: dict[str, dict[str, Any]] = {}
    posts: dict[str, dict[str, Any]] = {}
    pages: list[dict[str, Any]] = []
    cursor: str | None = None
    seen_cursors: set[str] = set()
    complete = False

    for page_number in range(1, args.max_pages + 1):
        variables: dict[str, Any] = {
            "userId": rest_id,
            "count": args.count,
            "includePromotedContent": False,
            "withQuickPromoteEligibilityTweetFields": False,
            "withVoice": True,
            "withV2Timeline": True,
        }
        if cursor:
            variables["cursor"] = cursor

        try:
            document = graphql_get(
                opener,
                headers,
                query_id=tweets_query_id,
                operation_name="UserTweets",
                variables=variables,
                features=tweets_features,
                fields=tweets_fields,
            )
        except urllib.error.HTTPError as error:
            if error.code not in {401, 403, 429}:
                raise
            time.sleep(max(args.sleep, 1.0))
            opener, headers = make_opener(bearer)
            document = graphql_get(
                opener,
                headers,
                query_id=tweets_query_id,
                operation_name="UserTweets",
                variables=variables,
                features=tweets_features,
                fields=tweets_fields,
            )

        entries = instruction_entries(timeline_instructions(document))
        tweet_results = timeline_tweet_results(entries)
        page_new = 0
        page_kept = 0
        page_oldest: datetime | None = None
        page_newest: datetime | None = None

        for result in tweet_results:
            post, created_at = parse_tweet(
                result,
                handle=handle,
                cutoff=cutoff,
                until=until,
                include_replies=args.include_replies,
            )
            if created_at is not None:
                page_oldest = created_at if page_oldest is None else min(page_oldest, created_at)
                page_newest = created_at if page_newest is None else max(page_newest, created_at)
            if post is None:
                continue
            page_kept += 1
            if post["id"] not in posts:
                page_new += 1
                posts[post["id"]] = post
                ids[post["id"]] = {"id": post["id"], "source": "UserTweets", "posted_at": post["posted_at"]}

        next_cursor = bottom_cursor(entries)
        pages.append(
            {
                "page": page_number,
                "cursor": cursor,
                "next_cursor": next_cursor,
                "tweet_results": len(tweet_results),
                "kept_in_window": page_kept,
                "new_count": page_new,
                "total_after": len(posts),
                "oldest_seen": page_oldest.isoformat() if page_oldest else None,
                "newest_seen": page_newest.isoformat() if page_newest else None,
            }
        )
        print(
            f"page={page_number} results={len(tweet_results)} new={page_new} total={len(posts)} "
            f"oldest={page_oldest.isoformat() if page_oldest else '-'}",
            flush=True,
        )
        write_outputs(
            handle=handle,
            cutoff=args.cutoff,
            until=args.until,
            status_path=status_path,
            posts_path=posts_path,
            ids=ids,
            posts=posts,
            pages=pages,
            display_name=display_name,
            user_id=rest_id,
            complete=False,
        )

        if page_oldest is not None and page_oldest < cutoff:
            complete = True
            break
        if not next_cursor or next_cursor in seen_cursors:
            complete = True
            break
        if page_new == 0 and page_number > 2:
            complete = True
            break

        seen_cursors.add(next_cursor)
        cursor = next_cursor
        time.sleep(args.sleep)

    write_outputs(
        handle=handle,
        cutoff=args.cutoff,
        until=args.until,
        status_path=status_path,
        posts_path=posts_path,
        ids=ids,
        posts=posts,
        pages=pages,
        display_name=display_name,
        user_id=rest_id,
        complete=complete,
    )

    db_result: dict[str, int] | None = None
    if args.import_db:
        db_result = import_posts_to_db(
            handle=handle,
            display_name=display_name,
            posts=posts,
            lookback_months=args.lookback_months,
        )

    print(
        json.dumps(
            {
                "handle": handle,
                "display_name": display_name,
                "user_id": rest_id,
                "posts": len(posts),
                "pages": len(pages),
                "complete": complete,
                "status_json": str(status_path),
                "posts_json": str(posts_path),
                "db_import": db_result,
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
