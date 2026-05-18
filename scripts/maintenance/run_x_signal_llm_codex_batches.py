from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any


CANONICAL_SYMBOLS = {
    "GOOGL": "GOOG",
    "ETORO": "ETOR",
}
SENTIMENT_MAP = {
    "看多": "bullish",
    "多": "bullish",
    "bull": "bullish",
    "positive": "bullish",
    "看涨": "bullish",
    "看好": "bullish",
    "bullish": "bullish",
    "看空": "bearish",
    "空": "bearish",
    "bear": "bearish",
    "negative": "bearish",
    "看跌": "bearish",
    "bearish": "bearish",
    "unknown": "无法判断",
    "unclear": "无法判断",
    "neutral": "无法判断",
    "不明": "无法判断",
    "无法确定": "无法判断",
    "无法判定": "无法判断",
    "无法判断": "无法判断",
}
VALID_SENTIMENTS = {"bullish", "bearish", "无法判断"}


def _extract_json_object(text: str) -> dict[str, Any]:
    stripped = text.strip()
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start < 0 or end < start:
            raise
        payload = json.loads(stripped[start : end + 1])
    if not isinstance(payload, dict):
        raise ValueError("Codex output must be a JSON object.")
    return payload


def _batch_post_ids(batch: dict[str, Any]) -> set[int]:
    return {int(post["post_id"]) for post in batch.get("posts", [])}


def _result_valid(path: Path, expected_ids: set[int], analysis_source: str) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if payload.get("analysis_source") != analysis_source:
        return False
    results = payload.get("results")
    if not isinstance(results, list):
        return False
    seen: set[int] = set()
    for row in results:
        if not isinstance(row, dict):
            return False
        try:
            post_id = int(row.get("post_id"))
        except (TypeError, ValueError):
            return False
        if post_id in seen:
            return False
        seen.add(post_id)
        if not isinstance(row.get("items", []), list):
            return False
    return seen == expected_ids


def _normalize_result(payload: dict[str, Any], batch: dict[str, Any]) -> dict[str, Any]:
    analysis_source = str(batch.get("analysis_source") or "").strip()
    expected_ids = _batch_post_ids(batch)
    results = payload.get("results")
    if not isinstance(results, list):
        raise ValueError("results must be a list.")

    normalized_results: list[dict[str, Any]] = []
    seen: set[int] = set()
    for row in results:
        if not isinstance(row, dict):
            continue
        post_id = int(row.get("post_id"))
        if post_id not in expected_ids or post_id in seen:
            continue
        seen.add(post_id)

        items = row.get("items") or []
        if not isinstance(items, list):
            items = []
        normalized_items: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or "").strip().upper().lstrip("$")
            symbol = CANONICAL_SYMBOLS.get(symbol, symbol)
            if not symbol:
                continue
            raw_sentiment = str(item.get("sentiment") or "无法判断").strip()
            sentiment = SENTIMENT_MAP.get(
                raw_sentiment.lower(),
                SENTIMENT_MAP.get(raw_sentiment, raw_sentiment),
            )
            if sentiment not in VALID_SENTIMENTS:
                sentiment = "无法判断"
            mention_kind = str(item.get("mention_kind") or "").strip().lower()
            is_sector_proxy = bool(item.get("is_sector_proxy")) or mention_kind in {
                "sector",
                "sector_proxy",
                "theme_proxy",
            }
            confidence = item.get("confidence")
            if not isinstance(confidence, (int, float)):
                confidence = None
            normalized_items.append(
                {
                    "symbol": symbol,
                    "mention_kind": "sector_proxy" if is_sector_proxy else "stock",
                    "sector_label": str(item.get("sector_label") or "").strip() or None,
                    "sentiment": sentiment,
                    "confidence": confidence,
                    "reason": str(item.get("reason") or "").strip() or None,
                    "is_sector_proxy": is_sector_proxy,
                    "proxy_reason": str(item.get("proxy_reason") or "").strip() or None,
                }
            )
        normalized_results.append({"post_id": post_id, "items": normalized_items})

    missing = expected_ids - seen
    if missing:
        raise ValueError(f"missing post ids: {sorted(missing)[:8]} total={len(missing)}")
    return {"analysis_source": analysis_source, "results": normalized_results}


def _build_prompt(batch: dict[str, Any]) -> str:
    return (
        "你是 stockAnalyse 的 X 跟投信号分析器。请只根据下面 batch JSON 中每条帖子的完整 content "
        "进行分析。返回且只返回一个合法 JSON 对象，不要 Markdown，不要解释，不要调用工具。\n\n"
        "任务规则：\n"
        "1. 不使用关键词模板判断方向，必须读全文语气。\n"
        "2. 方向只允许：bullish、bearish、无法判断。\n"
        "3. 只记录作者正在评价、买入、卖出、做空、推荐、警告或表达市场观点的直接可投资标的。\n"
        "4. 客户名单、供应商、同业参照、类比例子、benchmark、背景公司、指数成分，除非有独立投资观点，否则不要记录为股票项。\n"
        "5. 当帖子用同业 ticker 来说明目标标的，记录目标标的和明确板块代理，不要把同业 ticker 当作独立股票信号。\n"
        "6. 如果原文明确提到板块/主题，也记录该板块/主题的前两名龙头作为 sector_proxy。\n"
        "7. 如果全文没有个股但明确说了板块/主题，记录该板块/主题前两名龙头作为 sector_proxy。\n"
        "8. sector_proxy 必须 mention_kind=sector_proxy, is_sector_proxy=true, 填 sector_label 和 proxy_reason。\n"
        "9. 每个 post_id 都必须出现在 results 里，没有可记录股票/板块则 items=[]。\n"
        "10. 使用 tracker 的标准 ticker，例如 Google/Alphabet 用 GOOG，eToro 用 ETOR。\n"
        "11. reason 保持简短中文，不要复述全文。\n\n"
        '输出 JSON schema：{"analysis_source":"5.5ExtraHigh","results":[{"post_id":123,"items":[{"symbol":"NVDA",'
        '"mention_kind":"stock","sector_label":null,"sentiment":"bullish","confidence":0.8,'
        '"reason":"简短中文原因","is_sector_proxy":false,"proxy_reason":null}]}]}\n\n'
        "下面是 batch JSON：\n"
        + json.dumps(batch, ensure_ascii=False)
    )


def _run_codex(
    *,
    batch: dict[str, Any],
    output_tmp: Path,
    model: str,
    reasoning_effort: str,
    timeout: int,
    cwd: Path,
) -> dict[str, Any]:
    if output_tmp.exists():
        output_tmp.unlink()
    command = [
        "codex",
        "exec",
        "-m",
        model,
        "-c",
        f'model_reasoning_effort="{reasoning_effort}"',
        "--ephemeral",
        "--sandbox",
        "read-only",
        "-C",
        str(cwd),
        "--output-last-message",
        str(output_tmp),
        "-",
    ]
    completed = subprocess.run(
        command,
        input=_build_prompt(batch),
        text=True,
        capture_output=True,
        cwd=str(cwd),
        timeout=timeout,
    )
    if completed.returncode != 0:
        tail = (completed.stderr or completed.stdout or "")[-1200:]
        raise RuntimeError(f"codex exit={completed.returncode}: {tail}")
    return _extract_json_object(output_tmp.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run X signal LLM JSON batches through Codex CLI, one batch at a time."
    )
    parser.add_argument("--batch-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--model", default="gpt-5.5")
    parser.add_argument("--reasoning-effort", default="xhigh")
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--limit", type=int, help="Optional maximum number of pending batches to run.")
    args = parser.parse_args()

    cwd = Path.cwd()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    batch_files = sorted(args.batch_dir.glob("batch_*.json"))
    if args.limit is not None:
        batch_files = batch_files[: args.limit]

    pending: list[tuple[Path, Path, dict[str, Any], set[int]]] = []
    total_posts = 0
    for batch_file in batch_files:
        batch = json.loads(batch_file.read_text(encoding="utf-8"))
        analysis_source = str(batch.get("analysis_source") or "").strip()
        expected_ids = _batch_post_ids(batch)
        total_posts += len(expected_ids)
        result_file = args.output_dir / batch_file.name
        if _result_valid(result_file, expected_ids, analysis_source):
            continue
        pending.append((batch_file, result_file, batch, expected_ids))

    print(
        json.dumps(
            {
                "batch_dir": str(args.batch_dir),
                "output_dir": str(args.output_dir),
                "model": args.model,
                "reasoning_effort": args.reasoning_effort,
                "batch_count": len(batch_files),
                "total_posts": total_posts,
                "pending_batches": len(pending),
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )

    completed_batches = 0
    completed_posts = 0
    completed_items = 0
    started = time.time()
    for index, (batch_file, result_file, batch, expected_ids) in enumerate(pending, 1):
        label = batch_file.stem
        last_error: Exception | None = None
        for attempt in range(1, args.max_retries + 1):
            tmp = Path(tempfile.gettempdir()) / f"x_signal_{args.model}_{label}_attempt{attempt}.json"
            attempt_started = time.time()
            try:
                raw_result = _run_codex(
                    batch=batch,
                    output_tmp=tmp,
                    model=args.model,
                    reasoning_effort=args.reasoning_effort,
                    timeout=args.timeout,
                    cwd=cwd,
                )
                normalized = _normalize_result(raw_result, batch)
                result_file.write_text(
                    json.dumps(normalized, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8",
                )
                item_count = sum(len(row.get("items") or []) for row in normalized["results"])
                completed_batches += 1
                completed_posts += len(expected_ids)
                completed_items += item_count
                print(
                    json.dumps(
                        {
                            "status": "done",
                            "batch": label,
                            "index": index,
                            "pending": len(pending),
                            "posts": len(expected_ids),
                            "items": item_count,
                            "elapsed_seconds": round(time.time() - attempt_started, 1),
                            "completed_posts": completed_posts,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    flush=True,
                )
                break
            except Exception as exc:
                last_error = exc
                print(
                    json.dumps(
                        {
                            "status": "retry",
                            "batch": label,
                            "attempt": attempt,
                            "error": str(exc)[-1000:],
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    flush=True,
                )
        else:
            raise SystemExit(f"{label} failed after retries: {last_error}")

    print(
        json.dumps(
            {
                "status": "finished",
                "completed_batches": completed_batches,
                "completed_posts": completed_posts,
                "completed_items": completed_items,
                "elapsed_seconds": round(time.time() - started, 1),
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
