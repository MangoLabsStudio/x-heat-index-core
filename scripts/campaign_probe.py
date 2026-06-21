#!/usr/bin/env python3
"""Entity-first campaign discovery probe.

Pulls a watch handle timeline, extracts campaign-related observation nodes by
identity terms, and can write them into DATA_DIR/campaign_graphs/<id>/nodes.jsonl
for the dashboard's entity_graph campaign model.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HOST = "twitter241.p.rapidapi.com"
BASE = f"https://{HOST}"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def call_api(path: str, retries: int = 3) -> dict:
    key = os.environ.get("TWITTER241_RAPIDAPI_KEY", "").strip()
    if not key:
        raise RuntimeError("TWITTER241_RAPIDAPI_KEY is required")
    req = urllib.request.Request(
        BASE + path,
        headers={"x-rapidapi-key": key, "x-rapidapi-host": HOST},
    )
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            last_err = exc
            if attempt < retries - 1:
                time.sleep(2**attempt)
                continue
            raise
    raise RuntimeError(f"call_api failed: {last_err}")


def walk(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from walk(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from walk(item)


def first_string(obj: Any, keys: tuple[str, ...]) -> str:
    for row in walk(obj):
        for key in keys:
            value = row.get(key)
            if isinstance(value, str) and value:
                return value
            if isinstance(value, int):
                return str(value)
    return ""


def parse_tweet_node(node: dict) -> dict | None:
    if not isinstance(node, dict):
        return None
    if node.get("__typename") == "TweetWithVisibilityResults":
        node = node.get("tweet", {}) or {}
    if node.get("__typename") not in ("Tweet", None):
        return None
    legacy = node.get("legacy") or {}
    if not legacy:
        return None

    user_result = node.get("core", {}).get("user_results", {}).get("result", {}) or {}
    user_legacy = user_result.get("legacy") or {}
    user_core = user_result.get("core") or {}
    screen_name = user_core.get("screen_name") or user_legacy.get("screen_name") or ""
    tweet_id = str(node.get("rest_id") or legacy.get("id_str") or "")
    if not tweet_id:
        return None

    views_count = 0
    views = node.get("views") or {}
    if views.get("count"):
        try:
            views_count = int(views["count"])
        except (TypeError, ValueError):
            views_count = 0

    return {
        "node_id": tweet_id,
        "tweet_id": tweet_id,
        "type": "tweet",
        "author": screen_name.lower(),
        "author_followers": user_legacy.get("followers_count", 0),
        "text": legacy.get("full_text") or legacy.get("text") or "",
        "created_at": legacy.get("created_at", ""),
        "metrics": {
            "views": views_count,
            "likes": legacy.get("favorite_count", 0),
            "retweets": legacy.get("retweet_count", 0),
            "replies": legacy.get("reply_count", 0),
            "quotes": legacy.get("quote_count", 0),
            "bookmarks": legacy.get("bookmark_count", 0),
        },
        "relations": {
            "conversation_id": str(legacy.get("conversation_id_str", "") or ""),
            "in_reply_to_status_id": str(legacy.get("in_reply_to_status_id_str", "") or ""),
            "quoted_status_id": str(legacy.get("quoted_status_id_str", "") or ""),
        },
    }


def extract_tweets(payload: dict) -> list[dict]:
    seen: set[str] = set()
    tweets: list[dict] = []
    for row in walk(payload):
        result = row.get("tweet_results", {}).get("result") if isinstance(row.get("tweet_results"), dict) else None
        rec = parse_tweet_node(result or row)
        if not rec:
            continue
        tid = rec["tweet_id"]
        if tid in seen:
            continue
        seen.add(tid)
        tweets.append(rec)
    return tweets


def resolve_user_id(handle: str) -> tuple[str, dict]:
    payload = call_api(f"/user?username={urllib.parse.quote(handle.lstrip('@'))}")
    rest_id = first_string(payload, ("rest_id", "id_str", "user_id", "id"))
    if not rest_id:
        raise RuntimeError(f"could not resolve user id for @{handle}")
    return rest_id, payload


def fetch_user_tweets(user_id: str, count: int) -> dict:
    return call_api(f"/user-tweets?user={urllib.parse.quote(user_id)}&count={count}")


def score_affinity(tweet: dict, terms: list[str], handle: str) -> tuple[float, list[str]]:
    text = (tweet.get("text") or "").lower()
    reasons: list[str] = []
    score = 0.0

    exact_hits = []
    for term in terms:
        normalized = term.lower().strip()
        if normalized and normalized in text:
            exact_hits.append(term)
    if exact_hits:
        score += min(0.75, 0.35 + 0.15 * len(exact_hits))
        reasons.append("identity_term:" + ",".join(exact_hits[:4]))

    author = (tweet.get("author") or "").lower().lstrip("@")
    if author == handle.lower().lstrip("@"):
        score += 0.15
        reasons.append("watch_handle_author")

    url_like = any(marker in text for marker in ("https://", "http://", ".com", ".ai", ".xyz", "github.com"))
    if url_like and exact_hits:
        score += 0.10
        reasons.append("identity_with_url")

    metrics = tweet.get("metrics") or {}
    explicit_engagement = (
        int(metrics.get("likes") or 0)
        + int(metrics.get("retweets") or 0)
        + int(metrics.get("replies") or 0)
        + int(metrics.get("quotes") or 0)
    )
    if explicit_engagement >= 25 and exact_hits:
        score += 0.05
        reasons.append("engaged_identity_post")

    return round(min(score, 1.0), 3), reasons


def unique_strings(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in values:
        value = str(item).strip()
        key = value.lower()
        if value and key not in seen:
            seen.add(key)
            out.append(value)
    return out


def upsert_campaign_config(data_dir: Path, campaign_id: str, name: str, terms: list[str], handle: str) -> None:
    campaign_dir = data_dir / "campaign_graphs" / campaign_id
    campaign_dir.mkdir(parents=True, exist_ok=True)
    config_path = campaign_dir / "config.json"
    raw: dict[str, Any] = {}
    if config_path.exists():
        try:
            raw = json.loads(config_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            raw = {}

    identity = raw.get("identity") if isinstance(raw.get("identity"), dict) else {}
    identity["names"] = unique_strings([*(identity.get("names") or []), *terms])
    identity["watch_handles"] = unique_strings([*(identity.get("watch_handles") or []), handle.lstrip("@")])
    raw.update({
        "campaign_id": raw.get("campaign_id") or campaign_id,
        "name": raw.get("name") or name or campaign_id,
        "source_mode": "entity_graph",
        "identity": identity,
    })
    config_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe entity-first campaign candidate discovery.")
    parser.add_argument("--handle", required=True, help="Watch handle to pull as an entity source.")
    parser.add_argument("--term", action="append", default=[], help="Campaign identity term. Repeatable.")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--min-affinity", type=float, default=0.35)
    parser.add_argument("--jsonl", default="", help="Optional output JSONL path.")
    parser.add_argument("--campaign-id", default="", help="Write into DATA_DIR/campaign_graphs/<campaign-id>/nodes.jsonl.")
    parser.add_argument("--campaign-name", default="", help="Name used when creating campaign graph config.json.")
    parser.add_argument("--data-dir", default=os.environ.get("DATA_DIR", ""), help="DATA_DIR root for --campaign-id output.")
    parser.add_argument("--append", action="store_true", help="Append nodes instead of replacing the output JSONL.")
    args = parser.parse_args()

    if not args.term:
        raise SystemExit("--term is required at least once")

    handle = args.handle.lstrip("@")
    user_id, _ = resolve_user_id(handle)
    payload = fetch_user_tweets(user_id, args.limit)
    tweets = extract_tweets(payload)

    nodes = []
    for tweet in tweets:
        affinity, reasons = score_affinity(tweet, args.term, handle)
        node = {
            **tweet,
            "campaign_affinity": affinity,
            "affinity_reason": reasons,
            "source": "twitter241_user_tweets",
            "source_handle": handle,
            "fetched_at": now_iso(),
        }
        if affinity >= args.min_affinity:
            nodes.append(node)

    result = {
        "handle": handle,
        "user_id": user_id,
        "terms": args.term,
        "fetched_tweets": len(tweets),
        "candidate_nodes": len(nodes),
        "nodes": nodes,
    }

    jsonl_path = args.jsonl
    if not jsonl_path and args.campaign_id:
        if not args.data_dir:
            raise SystemExit("--data-dir or DATA_DIR is required with --campaign-id")
        data_dir = Path(args.data_dir)
        upsert_campaign_config(data_dir, args.campaign_id, args.campaign_name, args.term, handle)
        jsonl_path = str(data_dir / "campaign_graphs" / args.campaign_id / "nodes.jsonl")

    if jsonl_path:
        out = Path(jsonl_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if args.append else "w"
        with out.open(mode, encoding="utf-8") as fh:
            for node in nodes:
                fh.write(json.dumps(node, ensure_ascii=False) + "\n")
        result["jsonl"] = str(out)
        result["jsonl_mode"] = mode

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
