from __future__ import annotations

import argparse
import html
import http.cookiejar
import json
import re
import ssl
import time
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

from sqlalchemy import func, select

from stockanalyse_api.domain.backtests import models as backtest_models  # noqa: F401
from stockanalyse_api.domain.fundamentals import models as fundamentals_models  # noqa: F401
from stockanalyse_api.domain.indicators import models as indicator_models  # noqa: F401
from stockanalyse_api.domain.instruments import models as instrument_models  # noqa: F401
from stockanalyse_api.domain.market_data import models as market_data_models  # noqa: F401
from stockanalyse_api.domain.operations import models as operations_models  # noqa: F401
from stockanalyse_api.domain.screens import models as screen_models  # noqa: F401
from stockanalyse_api.domain.watchlists import models as watchlist_models  # noqa: F401
from stockanalyse_api.domain.x_signals import models as x_signal_models  # noqa: F401
from stockanalyse_api.domain.x_signals.models import XSignalAuthor, XSignalMention, XSignalPost
from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.x_signal_tracker import (
    ImportedXPost,
    add_x_signal_author,
    analyze_x_signal_author_posts,
    import_x_signal_posts,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handle", default="tig88411109")
    parser.add_argument(
        "--status-json",
        default="data/x_signal_status_ids_tig88411109_20m_posts_only.json",
    )
    parser.add_argument(
        "--cache-json",
        default="data/x_signal_posts_tig88411109_posts_only_incremental.json",
    )
    parser.add_argument("--cutoff", default="2024-09-12")
    parser.add_argument("--until", default="2026-05-13")
    parser.add_argument("--sleep", type=float, default=0.01)
    parser.add_argument(
        "--skip-analysis",
        action="store_true",
        help="Only import captured posts; do not run the legacy extraction analysis.",
    )
    return parser.parse_args()


def load_main_js() -> str:
    cached = Path("/tmp/x_main.js")
    if cached.exists():
        return cached.read_text(encoding="utf-8")
    home = http_get_text("https://x.com/home")
    match = re.search(
        r"https://abs\.twimg\.com/responsive-web/client-web/main\.[^\" ]+\.js",
        home,
    )
    if match is None:
        raise RuntimeError("Could not locate X main JavaScript bundle.")
    js_text = http_get_text(match.group(0))
    cached.write_text(js_text, encoding="utf-8")
    return js_text


def http_get_text(url: str) -> str:
    request = urllib.request.Request(url, headers={"user-agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=30, context=ssl._create_unverified_context()) as response:
        return response.read().decode("utf-8", errors="replace")


def extract_graphql_metadata(js_text: str) -> tuple[str, dict[str, bool], dict[str, bool], str]:
    bearer = re.search(r"AAAAAAAA[A-Za-z0-9%]+", js_text)
    operation = re.search(
        r"queryId:\"([^\"]+)\",operationName:\"TweetResultByRestId\".*?"
        r"metadata:\{featureSwitches:\[(.*?)\],fieldToggles:\[(.*?)\]",
        js_text,
    )
    if bearer is None or operation is None:
        raise RuntimeError("Could not locate X GraphQL metadata.")
    features = {name: True for name in re.findall(r"\"([^\"]+)\"", operation.group(2))}
    fields = {name: True for name in re.findall(r"\"([^\"]+)\"", operation.group(3))}
    return bearer.group(0), features, fields, operation.group(1)


def make_opener(bearer: str) -> tuple[urllib.request.OpenerDirector, dict[str, str]]:
    context = ssl._create_unverified_context()
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(jar),
        urllib.request.HTTPSHandler(context=context),
    )
    headers = {
        "authorization": f"Bearer {bearer}",
        "user-agent": "Mozilla/5.0",
        "content-type": "application/json",
        "x-twitter-active-user": "yes",
        "x-twitter-client-language": "en",
    }
    request = urllib.request.Request(
        "https://api.x.com/1.1/guest/activate.json",
        data=b"{}",
        headers=headers,
        method="POST",
    )
    with opener.open(request, timeout=8) as response:
        headers["x-guest-token"] = json.loads(response.read().decode("utf-8"))["guest_token"]
    return opener, headers


def unwrap_tweet(result: dict[str, Any] | None) -> dict[str, Any] | None:
    if result and result.get("__typename") == "TweetWithVisibilityResults":
        result = result.get("tweet")
    if result and result.get("__typename") == "Tweet":
        return result
    return None


def best_tweet_text(tweet: dict[str, Any]) -> str:
    legacy_text = (tweet.get("legacy") or {}).get("full_text") or ""
    note = tweet.get("note_tweet", {}).get("note_tweet_results", {}).get("result", {})
    note_text = note.get("text") or ""
    return note_text if len(note_text) > len(legacy_text) else legacy_text


def fetch_tweet(
    opener: urllib.request.OpenerDirector,
    headers: dict[str, str],
    *,
    query_id: str,
    features: dict[str, bool],
    fields: dict[str, bool],
    tweet_id: str,
) -> dict[str, Any]:
    variables = {
        "tweetId": tweet_id,
        "withCommunity": False,
        "includePromotedContent": False,
        "withVoice": True,
    }
    params = {
        "variables": json.dumps(variables, separators=(",", ":")),
        "features": json.dumps(features, separators=(",", ":")),
        "fieldToggles": json.dumps(fields, separators=(",", ":")),
    }
    url = f"https://x.com/i/api/graphql/{query_id}/TweetResultByRestId?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers=headers)
    with opener.open(request, timeout=8) as response:
        return json.loads(response.read().decode("utf-8"))


def load_status_ids(path: Path) -> list[str]:
    document = json.loads(path.read_text(encoding="utf-8"))
    return sorted(
        [tweet_id for tweet_id in document.get("ids", {}) if re.fullmatch(r"\d+", tweet_id)],
        key=int,
        reverse=True,
    )


def load_cached_posts(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return {post["id"]: post for post in document.get("posts", []) if post.get("id")}


def main() -> None:
    args = parse_args()
    handle = args.handle.lower().lstrip("@")
    cutoff = datetime.fromisoformat(args.cutoff).replace(tzinfo=UTC)
    until = datetime.fromisoformat(args.until).replace(tzinfo=UTC)
    status_path = Path(args.status_json)
    cache_path = Path(args.cache_json)

    tweet_ids = load_status_ids(status_path)
    cached_posts = load_cached_posts(cache_path)

    with SessionLocal() as session:
        author = session.execute(select(XSignalAuthor).where(XSignalAuthor.handle == handle)).scalar_one_or_none()
        if author is None:
            summary = add_x_signal_author(session, handle)
            author = session.get(XSignalAuthor, summary.id)
        existing_ids = set(
            session.execute(
                select(XSignalPost.external_post_id).where(
                    XSignalPost.author_id == author.id,
                    XSignalPost.external_post_id.is_not(None),
                )
            ).scalars()
        )

    missing_ids = [tweet_id for tweet_id in tweet_ids if tweet_id not in existing_ids]
    print(f"status_ids={len(tweet_ids)} existing={len(existing_ids)} missing={len(missing_ids)}", flush=True)

    js_text = load_main_js()
    bearer, features, fields, query_id = extract_graphql_metadata(js_text)
    opener, headers = make_opener(bearer)

    fetched_posts: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for index, tweet_id in enumerate(missing_ids, start=1):
        if tweet_id in cached_posts:
            fetched_posts.append(cached_posts[tweet_id])
            continue
        try:
            data = fetch_tweet(
                opener,
                headers,
                query_id=query_id,
                features=features,
                fields=fields,
                tweet_id=tweet_id,
            )
        except urllib.error.HTTPError as error:
            if error.code in {401, 403, 429}:
                try:
                    opener, headers = make_opener(bearer)
                    data = fetch_tweet(
                        opener,
                        headers,
                        query_id=query_id,
                        features=features,
                        fields=fields,
                        tweet_id=tweet_id,
                    )
                except Exception as retry_error:  # noqa: BLE001
                    errors.append({"id": tweet_id, "error": repr(retry_error)})
                    continue
            else:
                errors.append({"id": tweet_id, "error": f"HTTP {error.code}"})
                continue
        except Exception as error:  # noqa: BLE001
            errors.append({"id": tweet_id, "error": repr(error)})
            continue

        tweet = unwrap_tweet(data.get("data", {}).get("tweetResult", {}).get("result"))
        if tweet is None:
            errors.append({"id": tweet_id, "error": "missing tweet"})
            continue

        legacy = tweet.get("legacy") or {}
        user = (tweet.get("core") or {}).get("user_results", {}).get("result", {})
        screen_name = (
            (user.get("core") or {}).get("screen_name")
            or (user.get("legacy") or {}).get("screen_name")
            or ""
        ).lower()
        if screen_name != handle:
            continue
        created_at = parsedate_to_datetime(legacy["created_at"]).astimezone(UTC)
        if not (cutoff <= created_at < until):
            continue
        content = html.unescape(best_tweet_text(tweet)).strip()
        if not content:
            continue
        fetched_posts.append(
            {
                "id": tweet_id,
                "posted_at": created_at.isoformat(),
                "content": content,
                "source_url": f"https://x.com/{handle}/status/{tweet_id}",
                "is_reply": bool(legacy.get("in_reply_to_status_id_str")),
                "in_reply_to_status_id": legacy.get("in_reply_to_status_id_str"),
                "quoted_status_id": legacy.get("quoted_status_id_str"),
            }
        )
        print(f"fetched {index}/{len(missing_ids)} kept={len(fetched_posts)} errors={len(errors)}", flush=True)
        time.sleep(args.sleep)

    merged_cache = cached_posts | {post["id"]: post for post in fetched_posts}
    cache_path.write_text(
        json.dumps(
            {
                "handle": handle,
                "captured_at": datetime.now(UTC).isoformat(),
                "posts": list(merged_cache.values()),
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    import_posts = [
        ImportedXPost(
            posted_at=datetime.fromisoformat(post["posted_at"]),
            content=post["content"],
            external_post_id=post["id"],
            source_url=post["source_url"],
            raw_payload={
                "capture_source": "posts_only_small_window_graphql",
                "is_reply": post.get("is_reply"),
                "in_reply_to_status_id": post.get("in_reply_to_status_id"),
                "quoted_status_id": post.get("quoted_status_id"),
            },
        )
        for post in fetched_posts
    ]
    with SessionLocal() as session:
        author = session.execute(select(XSignalAuthor).where(XSignalAuthor.handle == handle)).scalar_one()
        import_result = import_x_signal_posts(session, author.id, import_posts)
        analysis_result = None if args.skip_analysis else analyze_x_signal_author_posts(session, author.id)
        total_posts = session.execute(
            select(func.count(XSignalPost.id)).where(XSignalPost.author_id == author.id)
        ).scalar_one()
        total_mentions = session.execute(
            select(func.count(XSignalMention.id)).where(XSignalMention.author_id == author.id)
        ).scalar_one()
        oldest = session.execute(select(func.min(XSignalPost.posted_at)).where(XSignalPost.author_id == author.id)).scalar_one()
        newest = session.execute(select(func.max(XSignalPost.posted_at)).where(XSignalPost.author_id == author.id)).scalar_one()

    print(
        json.dumps(
            {
                "fetched_or_reused": len(fetched_posts),
                "errors": len(errors),
                "import_created": import_result.created_count,
                "import_updated": import_result.updated_count,
                "total_posts": int(total_posts),
                "analysis_mentions": analysis_result.mention_count if analysis_result else None,
                "total_mentions": int(total_mentions),
                "oldest": oldest.isoformat() if oldest else None,
                "newest": newest.isoformat() if newest else None,
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
