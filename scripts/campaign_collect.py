#!/usr/bin/env python3
"""Identity-first campaign collector.

This collector is the first pass at a real campaign-level discovery flow:

1. Discover candidates from identity search queries.
2. Supplement with official handle timelines/replies.
3. Expand matched nodes through direct replies/quotes.
4. Expand authors discovered from matched nodes.

Output stays compatible with the existing entity-graph dashboard:

DATA_DIR/campaign_graphs/<campaign_id>/
├── config.json
├── nodes.jsonl
└── collector_state.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from campaign_core.config import list_config_strings
from campaign_core.identity import is_article_url, normalize_handle, term_in_text, unique_strings
from campaign_core.io import atomic_write_json, atomic_write_jsonl, load_json_object, load_jsonl
from campaign_core.metrics import safe_float, safe_int, should_replace_observation
from campaign_core.paid import PAID_DELIVERABLE_SEED_SOURCE, load_paid_delivery_seeds, normalize_paid_source
from campaign_core.timeutils import parse_iso_utc, parse_twitter_created_at

socket.setdefaulttimeout(20)

HOST = "twitter241.p.rapidapi.com"
BASE = f"https://{HOST}"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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
        views_count = safe_int(views.get("count"))

    entities = legacy.get("entities") or {}
    expanded_urls = [
        u.get("expanded_url", "") for u in entities.get("urls", []) if isinstance(u, dict)
    ]
    mentioned_handles = [
        normalize_handle(m.get("screen_name", ""))
        for m in entities.get("user_mentions", [])
        if isinstance(m, dict)
    ]

    return {
        "node_id": tweet_id,
        "tweet_id": tweet_id,
        "type": "tweet",
        "author": normalize_handle(screen_name),
        "author_followers": safe_int(user_legacy.get("followers_count")),
        "author_created_at": user_legacy.get("created_at", ""),
        "text": legacy.get("full_text") or legacy.get("text") or "",
        "created_at": legacy.get("created_at", ""),
        "metrics": {
            "views": views_count,
            "likes": safe_int(legacy.get("favorite_count")),
            "retweets": safe_int(legacy.get("retweet_count")),
            "replies": safe_int(legacy.get("reply_count")),
            "quotes": safe_int(legacy.get("quote_count")),
            "bookmarks": safe_int(legacy.get("bookmark_count")),
        },
        "relations": {
            "conversation_id": str(legacy.get("conversation_id_str", "") or ""),
            "in_reply_to_status_id": str(legacy.get("in_reply_to_status_id_str", "") or ""),
            "quoted_status_id": str(legacy.get("quoted_status_id_str", "") or ""),
        },
        "urls": [u for u in expanded_urls if u],
        "mentions": [m for m in mentioned_handles if m],
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


def extract_bottom_cursor(payload: dict) -> str:
    for row in walk(payload):
        entry_id = str(row.get("entryId") or "")
        if "cursor-bottom" not in entry_id:
            continue
        content = row.get("content") if isinstance(row.get("content"), dict) else {}
        for candidate in (
            content.get("value"),
            (content.get("itemContent") or {}).get("value") if isinstance(content.get("itemContent"), dict) else "",
            row.get("value"),
        ):
            if isinstance(candidate, str) and candidate:
                return candidate
    return ""


def graph_node_attention(row: dict) -> float:
    metrics = row.get("metrics") or {}
    affinity = safe_float(row.get("campaign_affinity"), 1.0)
    return affinity * (
        safe_int(metrics.get("views")) * 0.10
        + safe_int(metrics.get("likes")) * 1.0
        + safe_int(metrics.get("replies")) * 2.7
        + safe_int(metrics.get("retweets")) * 2.0
        + safe_int(metrics.get("quotes")) * 2.0
        + safe_int(metrics.get("bookmarks")) * 1.5
    )


class Twitter241Client:
    def __init__(self) -> None:
        self.key_primary = os.environ.get("TWITTER241_RAPIDAPI_KEY", "").strip()
        self.key_fallback = os.environ.get("TWITTER241_RAPIDAPI_KEY_FALLBACK", "").strip()
        if not self.key_primary:
            raise RuntimeError("TWITTER241_RAPIDAPI_KEY is required")
        self._active_key = self.key_primary
        self._using_fallback = False
        self.search_path_kind = ""
        self.user_replies_kind = ""

    def _switch_to_fallback(self) -> bool:
        if self._using_fallback or not self.key_fallback:
            return False
        self._using_fallback = True
        self._active_key = self.key_fallback
        print(f"[{now_iso()}] [quota] switching to fallback key", flush=True)
        return True

    def call_api(self, path: str, retries: int = 3) -> dict:
        url = BASE + path
        last_err: Exception | None = None
        for attempt in range(retries):
            req = urllib.request.Request(
                url,
                headers={"x-rapidapi-key": self._active_key, "x-rapidapi-host": HOST},
            )
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                    if isinstance(data, dict) and data.get("message", "").lower().startswith("you have exceeded"):
                        if self._switch_to_fallback():
                            continue
                        raise RuntimeError(f"quota exhausted: {data.get('message')}")
                    return data
            except urllib.error.HTTPError as exc:
                last_err = exc
                if exc.code == 429:
                    if self._switch_to_fallback():
                        continue
                    time.sleep(2**attempt)
                    continue
                if exc.code in (502, 503, 504) and attempt < retries - 1:
                    time.sleep(2**attempt)
                    continue
                raise
            except (urllib.error.URLError, TimeoutError) as exc:
                last_err = exc
                if attempt < retries - 1:
                    time.sleep(2**attempt)
                    continue
                raise
        raise RuntimeError(f"call_api failed: {last_err}")

    def _try_candidates(self, candidates: list[tuple[str, str]]) -> tuple[dict, str]:
        errors: list[str] = []
        for kind, path in candidates:
            try:
                return self.call_api(path), kind
            except Exception as exc:  # network and endpoint probing are expected here
                errors.append(f"{kind}: {exc}")
                continue
        raise RuntimeError("; ".join(errors) if errors else "no candidate paths")

    def resolve_user_id(self, handle: str) -> tuple[str, dict]:
        payload = self.call_api(f"/user?username={urllib.parse.quote(handle.lstrip('@'))}")
        rest_id = first_string(payload, ("rest_id", "id_str", "user_id", "id"))
        if not rest_id:
            raise RuntimeError(f"could not resolve user id for @{handle}")
        return rest_id, payload

    def fetch_user_tweets(self, user_id: str, count: int, cursor: str = "") -> dict:
        path = f"/user-tweets?user={urllib.parse.quote(user_id)}&count={count}"
        if cursor:
            path += f"&cursor={urllib.parse.quote(cursor)}"
        return self.call_api(path)

    def fetch_tweet(self, tweet_id: str) -> dict:
        return self.call_api(f"/tweet?pid={urllib.parse.quote(tweet_id)}")

    def fetch_user_replies(self, user_id: str, count: int, cursor: str = "") -> dict:
        encoded_user = urllib.parse.quote(user_id)
        encoded_cursor = urllib.parse.quote(cursor) if cursor else ""
        candidates: list[tuple[str, str]] = []
        if self.user_replies_kind:
            path = f"{self.user_replies_kind}?user={encoded_user}&count={count}"
            if cursor:
                path += f"&cursor={encoded_cursor}"
            candidates.append(("cached", path))
        for base in ("/user-replies-v2", "/user-replies"):
            path = f"{base}?user={encoded_user}&count={count}"
            if cursor:
                path += f"&cursor={encoded_cursor}"
            candidates.append((base, path))
        payload, kind = self._try_candidates(candidates)
        if kind != "cached":
            self.user_replies_kind = kind
        return payload

    def search_tweets(self, query: str, search_type: str, count: int, cursor: str = "") -> dict:
        encoded_query = urllib.parse.quote(query)
        encoded_cursor = urllib.parse.quote(cursor) if cursor else ""
        candidates: list[tuple[str, str]] = []
        if self.search_path_kind:
            base, param = self.search_path_kind.split("|", 1)
            path = f"{base}?type={urllib.parse.quote(search_type)}&count={count}&{param}={encoded_query}"
            if cursor:
                path += f"&cursor={encoded_cursor}"
            candidates.append(("cached", path))
        for base in ("/search-v2", "/search"):
            for param in ("query", "search_query"):
                path = f"{base}?type={urllib.parse.quote(search_type)}&count={count}&{param}={encoded_query}"
                if cursor:
                    path += f"&cursor={encoded_cursor}"
                candidates.append((f"{base}:{param}", path))
        payload, kind = self._try_candidates(candidates)
        if kind != "cached":
            base, param = kind.split(":", 1)
            self.search_path_kind = f"{base}|{param}"
        return payload

    def fetch_comments(self, tweet_id: str, count: int, cursor: str = "") -> dict:
        path = f"/comments?pid={urllib.parse.quote(tweet_id)}&count={count}"
        if cursor:
            path += f"&cursor={urllib.parse.quote(cursor)}"
        return self.call_api(path)

    def fetch_quotes(self, tweet_id: str, count: int, cursor: str = "") -> dict:
        path = f"/quotes?pid={urllib.parse.quote(tweet_id)}&count={count}"
        if cursor:
            path += f"&cursor={urllib.parse.quote(cursor)}"
        return self.call_api(path)


@dataclass
class DiscoveryConfig:
    campaign_id: str
    name: str
    data_dir: Path
    terms: list[str]
    official_handles: list[str]
    watch_handles: list[str]
    min_affinity: float
    search_types: list[str]
    search_pages: int
    search_count: int
    timeline_pages: int
    timeline_count: int
    reply_pages: int
    quote_pages: int
    related_count: int
    author_expand_limit: int
    author_expand_pages: int
    author_expand_count: int
    search_language: str = ""
    max_runtime_seconds: int = 540
    campaign_start_at: str = ""  # ISO UTC, used to stop timeline pagination early
    paid_tweet_ids: tuple[str, ...] = ()


def merge_identity(raw: dict, terms: list[str], official_handles: list[str], watch_handles: list[str]) -> tuple[list[str], list[str], list[str]]:
    identity = raw.get("identity") if isinstance(raw.get("identity"), dict) else {}
    merged_terms = unique_strings([
        *list_config_strings(raw, "terms", "keywords", "identity_terms"),
        *list_config_strings(identity, "names", "aliases", "hashtags", "urls", "tickers"),
        *terms,
    ])
    merged_official = unique_strings([
        *[normalize_handle(h) for h in list_config_strings(raw, "official_handles")],
        *[normalize_handle(h) for h in list_config_strings(identity, "official_handles")],
        *[normalize_handle(h) for h in official_handles],
    ])
    merged_watch = unique_strings([
        *[normalize_handle(h) for h in list_config_strings(raw, "watch_handles", "kol_handles")],
        *[normalize_handle(h) for h in list_config_strings(identity, "watch_handles", "kol_handles")],
        *[normalize_handle(h) for h in watch_handles],
    ])
    merged_official = [h for h in merged_official if h]
    merged_watch = [h for h in merged_watch if h and h not in merged_official]
    return filter_brand_scoped_terms(merged_terms, merged_official + merged_watch), merged_official, merged_watch


def filter_brand_scoped_terms(terms: list[str], handles: list[str]) -> list[str]:
    anchors = {normalize_handle(handle) for handle in handles if normalize_handle(handle)}
    for term in terms:
        normalized = normalize_handle(term)
        if normalized and " " not in str(term).strip() and len(normalized) >= 4:
            anchors.add(normalized)
    if not anchors:
        return terms
    scoped = []
    for term in terms:
        text = str(term or "").strip()
        lowered = text.lower()
        normalized_text = normalize_handle(text)
        if text.startswith(("from:", "to:", "@", "#", "url:")):
            scoped.append(term)
            continue
        if any(anchor and (anchor in lowered or anchor in normalized_text) for anchor in anchors):
            scoped.append(term)
    return unique_strings(scoped)


def build_runtime_config(args: argparse.Namespace) -> DiscoveryConfig:
    raw_data_dir = str(args.data_dir or os.environ.get("DATA_DIR", "")).strip()
    if not raw_data_dir:
        raise SystemExit("--data-dir or DATA_DIR is required")
    data_dir = Path(raw_data_dir).expanduser()
    campaign_dir = data_dir / "campaign_graphs" / args.campaign_id
    config_path = campaign_dir / "config.json"
    raw = load_json_object(config_path, default={})
    terms, official_handles, watch_handles = merge_identity(raw, args.term, args.official_handle, args.watch_handle)
    paid_tweet_ids = tuple(sorted({
        str(seed.get("tid") or "").strip()
        for seed in load_paid_delivery_seeds(campaign_dir, raw)
        if str(seed.get("tid") or "").strip()
    }))
    if not terms and not official_handles and not watch_handles:
        raise SystemExit("identity terms or handles are required")
    name = str(args.campaign_name or raw.get("name") or args.campaign_id).strip() or args.campaign_id
    search_types = unique_strings(args.search_type or ["Latest", "Top"])
    return DiscoveryConfig(
        campaign_id=args.campaign_id,
        name=name,
        data_dir=data_dir,
        terms=terms,
        official_handles=official_handles,
        watch_handles=watch_handles,
        min_affinity=args.min_affinity,
        search_types=search_types,
        search_pages=args.search_pages,
        search_count=args.search_count,
        timeline_pages=args.timeline_pages,
        timeline_count=args.timeline_count,
        reply_pages=args.reply_pages,
        quote_pages=args.quote_pages,
        related_count=args.related_count,
        author_expand_limit=args.author_expand_limit,
        author_expand_pages=args.author_expand_pages,
        author_expand_count=args.author_expand_count,
        search_language=str(raw.get("search_language") or raw.get("language") or "").strip(),
        max_runtime_seconds=safe_int(
            raw.get("collector_max_runtime_seconds")
            or os.environ.get("XHI_COLLECTOR_MAX_RUNTIME_SECONDS")
            or 540,
        ),
        campaign_start_at=str(raw.get("campaign_start_at") or "").strip(),
        paid_tweet_ids=paid_tweet_ids,
    )


def fetch_fxtwitter(handle: str, tid: str) -> dict | None:
    url = f"https://api.fxtwitter.com/{handle}/status/{tid}"
    req = urllib.request.Request(url, headers={"User-Agent": "CampaignCollector/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


def fxtwitter_article_terms(fxt_data: dict, terms: list[str]) -> list[str]:
    tweet = fxt_data.get("tweet") or {}
    article = tweet.get("article") or {}
    blocks = (article.get("content") or {}).get("blocks") or []
    body_parts: list[str] = []
    for block in blocks:
        if isinstance(block, dict):
            text = block.get("text")
            if isinstance(text, str):
                body_parts.append(text)
    body = " ".join(body_parts).lower()
    title = (article.get("title") or "").lower()
    return [t for t in terms if t.lower() in title or t.lower() in body]


def encode_query_term(term: str) -> str:
    value = str(term or "").strip()
    if not value:
        return ""
    if value.startswith(("from:", "to:", "@", "#", "url:")):
        return value
    if " " in value:
        return f"\"{value}\""
    return value


def build_search_queries(cfg: DiscoveryConfig) -> list[str]:
    queries: list[str] = []
    for term in cfg.terms:
        encoded = encode_query_term(term)
        if encoded:
            queries.append(encoded)
    for handle in cfg.official_handles:
        queries.append(f"@{handle}")
    if cfg.terms:
        encoded_terms = []
        for term in cfg.terms[:6]:
            encoded = encode_query_term(term)
            if encoded:
                encoded_terms.append(encoded)
        if encoded_terms:
            queries.append(" OR ".join(encoded_terms))
    language = cfg.search_language.lower().replace("_", "-")
    if language and not language.startswith("lang:"):
        language = f"lang:{language}"
    if language:
        queries = [query if "lang:" in query.lower() else f"{query} {language}" for query in queries]
    return unique_strings(queries)


def tweet_matches_language(tweet: dict, cfg: DiscoveryConfig) -> bool:
    language = cfg.search_language.lower().replace("_", "-")
    if language in {"zh", "zh-cn", "zh-tw", "cn"}:
        return bool(re.search(r"[\u3400-\u9fff]", str(tweet.get("text") or "")))
    return True


def score_affinity(
    tweet: dict,
    *,
    cfg: DiscoveryConfig,
    matched_tweet_ids: set[str],
    matched_conversation_ids: set[str],
    known_authors: set[str],
    source_kind: str,
    parent_tweet_id: str = "",
    relation: str = "",
) -> tuple[float, list[str]]:
    text = (tweet.get("text") or "").lower()
    author = normalize_handle(tweet.get("author") or "")
    metrics = tweet.get("metrics") or {}
    relations = tweet.get("relations") or {}
    urls_list = [str(u).lower() for u in (tweet.get("urls") or [])]
    url_blob = " ".join(urls_list)
    mention_set = {normalize_handle(m) for m in (tweet.get("mentions") or [])}
    tweet_id = str(tweet.get("tweet_id") or tweet.get("node_id") or tweet.get("id") or "").strip()

    reasons: list[str] = []
    score = 0.0

    exact_hits = [term for term in cfg.terms if term_in_text(text, term)]
    if exact_hits:
        score += min(0.72, 0.42 + 0.12 * (len(exact_hits) - 1))
        reasons.append("identity_term:" + ",".join(exact_hits[:4]))

    url_term_hits = [term for term in cfg.terms if term and term.lower() in url_blob]
    if url_term_hits:
        score += min(0.32, 0.22 + 0.05 * (len(url_term_hits) - 1))
        reasons.append("identity_url:" + ",".join(url_term_hits[:4]))

    handle_hits = [handle for handle in cfg.official_handles if f"@{handle}" in text]
    if handle_hits:
        score += min(0.18, 0.1 + 0.04 * len(handle_hits))
        reasons.append("handle_mention:" + ",".join(handle_hits[:3]))

    entity_mention_hits = [h for h in cfg.official_handles if normalize_handle(h) in mention_set]
    if entity_mention_hits and not handle_hits:
        score += min(0.18, 0.10 + 0.04 * len(entity_mention_hits))
        reasons.append("entity_mention:" + ",".join(entity_mention_hits[:3]))

    if author in set(cfg.official_handles) | set(cfg.watch_handles):
        score += 0.18
        reasons.append("official_or_watch_author")

    if source_kind in {"official_tweets", "official_replies"} and (exact_hits or handle_hits or url_term_hits or entity_mention_hits):
        score += 0.08
        reasons.append("official_source_match")
    elif source_kind in {"watch_tweets", "watch_replies"} and (exact_hits or handle_hits or url_term_hits or entity_mention_hits):
        score += 0.04
        reasons.append("watch_source_match")

    # Exact paid deliverables are campaign seeds. A watch handle's unrelated
    # timeline is not; it still needs an identity/relation signal below.
    if tweet_id and tweet_id in set(cfg.paid_tweet_ids):
        score = max(score, 0.86)
        if PAID_DELIVERABLE_SEED_SOURCE not in reasons:
            reasons.append(PAID_DELIVERABLE_SEED_SOURCE)

    if relation == "quote" and parent_tweet_id and parent_tweet_id in matched_tweet_ids:
        score += 0.24
        reasons.append("quoted_matched_node")
    elif relation == "reply" and parent_tweet_id and parent_tweet_id in matched_tweet_ids:
        score += 0.18
        reasons.append("reply_to_matched_node")

    conversation_id = str(relations.get("conversation_id") or "")
    if conversation_id and conversation_id in matched_conversation_ids:
        score += 0.10
        reasons.append("matched_conversation")

    if author and author in known_authors:
        score += 0.06
        reasons.append("known_campaign_author")

    url_like = bool(urls_list) or any(
        marker in text for marker in ("https://", "http://", ".com", ".ai", ".xyz", "github.com")
    )
    if url_like and (exact_hits or url_term_hits):
        score += 0.08
        reasons.append("identity_with_url")

    explicit_engagement = (
        safe_int(metrics.get("likes"))
        + safe_int(metrics.get("retweets"))
        + safe_int(metrics.get("replies"))
        + safe_int(metrics.get("quotes"))
    )
    if explicit_engagement >= 25 and (exact_hits or relation):
        score += 0.05
        reasons.append("engaged_identity_post")
    if safe_int(metrics.get("views")) >= 5000 and (exact_hits or relation):
        score += 0.03
        reasons.append("high_view_match")

    return round(min(score, 1.0), 3), reasons


def acceptance_threshold(base: float, relation: str, source_kind: str) -> float:
    threshold = base
    if relation == "quote":
        threshold = min(threshold, 0.26)
    elif relation == "reply":
        threshold = min(threshold, 0.30)
    if source_kind == "official_replies":
        threshold = min(threshold, 0.32)
    return max(0.20, threshold)


class IdentityCollector:
    def __init__(self, cfg: DiscoveryConfig, client: Twitter241Client) -> None:
        self.cfg = cfg
        self.client = client
        self.run_id = os.environ.get("XHI_COLLECT_RUN_ID", "").strip() or str(uuid.uuid4())
        self.started_at = now_iso()
        self.started_monotonic = time.monotonic()
        self.deadline_monotonic = (
            self.started_monotonic + cfg.max_runtime_seconds
            if cfg.max_runtime_seconds > 0
            else None
        )
        self.campaign_dir = cfg.data_dir / "campaign_graphs" / cfg.campaign_id
        self.config_path = self.campaign_dir / "config.json"
        self.nodes_path = self.campaign_dir / "nodes.jsonl"
        self.state_path = self.campaign_dir / "collector_state.json"
        self.paid_audit_path = self.campaign_dir / "paid_graph_match_audit.json"

        self.existing_rows = load_jsonl(self.nodes_path)
        self.best_existing: dict[str, dict] = {}
        for row in self.existing_rows:
            tid = str(row.get("tweet_id") or row.get("node_id") or "").strip()
            if not tid:
                continue
            if not tweet_matches_language(row, cfg):
                continue
            existing_source = str(((row.get("source_meta") or {}).get("kind") if isinstance(row.get("source_meta"), dict) else "") or row.get("source") or "existing_snapshot")
            affinity, reasons = score_affinity(
                row,
                cfg=cfg,
                matched_tweet_ids=set(),
                matched_conversation_ids=set(),
                known_authors=set(),
                source_kind=existing_source,
            )
            if affinity < cfg.min_affinity:
                continue
            row = {
                **row,
                "campaign_affinity": affinity,
                "affinity_reason": reasons,
            }
            if should_replace_observation(self.best_existing.get(tid), row):
                self.best_existing[tid] = row

        self.best_run: dict[str, dict] = {}
        self.paid_root_errors: dict[str, str] = {}
        self.related_page_diagnostics: dict[str, dict[str, dict]] = {}
        self.matched_tweet_ids: set[str] = set()
        self.matched_conversation_ids: set[str] = set()
        self.known_campaign_authors: set[str] = set()
        self.user_id_cache: dict[str, str] = {}
        self.summary = {
            "search_queries": 0,
            "search_candidates": 0,
            "official_candidates": 0,
            "expanded_related_candidates": 0,
            "paid_root_fetch_attempts": 0,
            "paid_root_fetch_found": 0,
            "paid_reply_quote_roots": 0,
            "paid_graph_matched_count": 0,
            "paid_missing_count": 0,
            "paid_seeded_count": 0,
            "expanded_author_candidates": 0,
            "matched_nodes": 0,
            "written_rows": 0,
            "deadline_exceeded": False,
            "errors": [],
        }

        for row in self.best_existing.values():
            if safe_float(row.get("campaign_affinity")) >= cfg.min_affinity:
                self._register_match(row)

    def _matched_rows(self) -> list[dict]:
        merged = dict(self.best_existing)
        merged.update(self.best_run)
        rows = [
            row for row in merged.values()
            if safe_float(row.get("campaign_affinity")) >= self.cfg.min_affinity
        ]
        return rows

    def _all_rows_by_tweet_id(self) -> dict[str, dict]:
        merged = dict(self.best_existing)
        merged.update(self.best_run)
        return {str(row.get("tweet_id") or row.get("node_id") or ""): row for row in merged.values() if row.get("tweet_id") or row.get("node_id")}

    def _register_match(self, row: dict) -> None:
        tid = str(row.get("tweet_id") or row.get("node_id") or "").strip()
        if tid:
            self.matched_tweet_ids.add(tid)
        conversation_id = str((row.get("relations") or {}).get("conversation_id") or "")
        if conversation_id:
            self.matched_conversation_ids.add(conversation_id)
        author = normalize_handle(row.get("author") or "")
        if author:
            self.known_campaign_authors.add(author)

    def _deadline_exceeded(self, phase: str) -> bool:
        if self.deadline_monotonic is None or time.monotonic() <= self.deadline_monotonic:
            return False
        self.summary["deadline_exceeded"] = True
        message = f"deadline exceeded during {phase}"
        if message not in self.summary["errors"]:
            self.summary["errors"].append(message)
        return True

    def _maybe_add(self, tweet: dict, *, source_kind: str, source_query: str = "", source_handle: str = "", parent_tweet_id: str = "", relation: str = "") -> None:
        force_paid_related = bool(parent_tweet_id and parent_tweet_id in set(self.cfg.paid_tweet_ids) and relation in {"reply", "quote"})
        if source_kind != "paid_deliverable_seed" and not force_paid_related and not tweet_matches_language(tweet, self.cfg):
            return
        affinity, reasons = score_affinity(
            tweet,
            cfg=self.cfg,
            matched_tweet_ids=self.matched_tweet_ids,
            matched_conversation_ids=self.matched_conversation_ids,
            known_authors=self.known_campaign_authors,
            source_kind=source_kind,
            parent_tweet_id=parent_tweet_id,
            relation=relation,
        )
        threshold = acceptance_threshold(self.cfg.min_affinity, relation, source_kind)
        if force_paid_related:
            affinity = max(affinity, 0.62)
            if "paid_root_reply_quote_backfill" not in reasons:
                reasons.append("paid_root_reply_quote_backfill")
            threshold = min(threshold, 0.62)
        if affinity < threshold:
            return
        tid = str(tweet.get("tweet_id") or tweet.get("node_id") or "").strip()
        if not tid:
            return
        relation_type = relation or "root"
        is_paid_root = source_kind == "paid_deliverable_seed"
        is_paid_related = bool(parent_tweet_id and parent_tweet_id in set(self.cfg.paid_tweet_ids) and relation in {"reply", "quote"})
        row = {
            **tweet,
            "campaign_affinity": affinity,
            "affinity_reason": reasons,
            "collector_version": "identity_first_v2",
            "source": source_kind,
            "node_role": (
                "paid_root" if is_paid_root
                else f"paid_{relation}" if is_paid_related
                else relation or "organic_node"
            ),
            "origin_root_tweet_id": parent_tweet_id or (tid if is_paid_root else ""),
            "relation_type": relation_type,
            "metric_status": str(tweet.get("metric_status") or "observed"),
            "evidence_status": str(tweet.get("evidence_status") or "observed"),
            "source_meta": {
                "kind": source_kind,
                "query": source_query,
                "handle": normalize_handle(source_handle),
                "parent_tweet_id": parent_tweet_id,
                "relation": relation,
            },
            "source_handle": normalize_handle(source_handle),
            "fetched_at": now_iso(),
        }
        tid = row["tweet_id"]
        if should_replace_observation(self.best_run.get(tid) or self.best_existing.get(tid), row):
            self.best_run[tid] = row
            self._register_match(row)

    def _collect_paid_roots(self) -> None:
        if not self.cfg.paid_tweet_ids:
            return
        rows_by_id = self._all_rows_by_tweet_id()
        for tid in self.cfg.paid_tweet_ids:
            if self._deadline_exceeded("paid_root_fetch"):
                return
            if tid in rows_by_id:
                self._register_match(rows_by_id[tid])
                continue
            self.summary["paid_root_fetch_attempts"] += 1
            try:
                payload = self.client.fetch_tweet(tid)
            except Exception as exc:
                message = f"paid root fetch {tid}: {exc}"
                self.paid_root_errors[tid] = message
                self.summary["errors"].append(message)
                continue
            tweets = extract_tweets(payload)
            root = next((tweet for tweet in tweets if str(tweet.get("tweet_id") or "") == tid), tweets[0] if tweets else None)
            if not root:
                message = f"paid root fetch {tid}: no tweet node returned"
                self.paid_root_errors[tid] = message
                self.summary["errors"].append(message)
                continue
            self.summary["paid_root_fetch_found"] += 1
            self._maybe_add(root, source_kind="paid_deliverable_seed")
            rows_by_id = self._all_rows_by_tweet_id()

    def _resolve_user_id(self, handle: str) -> str:
        handle = normalize_handle(handle)
        cached = self.user_id_cache.get(handle)
        if cached:
            return cached
        user_id, _ = self.client.resolve_user_id(handle)
        self.user_id_cache[handle] = user_id
        return user_id

    def _collect_search(self) -> None:
        queries = build_search_queries(self.cfg)
        self.summary["search_queries"] = len(queries)
        for query in queries:
            if self._deadline_exceeded("search"):
                return
            for search_type in self.cfg.search_types:
                if self._deadline_exceeded("search"):
                    return
                cursor = ""
                for _page in range(self.cfg.search_pages):
                    if self._deadline_exceeded("search"):
                        return
                    try:
                        payload = self.client.search_tweets(query, search_type, self.cfg.search_count, cursor)
                    except Exception as exc:
                        self.summary["errors"].append(f"search[{search_type}] {query}: {exc}")
                        break
                    tweets = extract_tweets(payload)
                    self.summary["search_candidates"] += len(tweets)
                    for tweet in tweets:
                        self._maybe_add(tweet, source_kind="search", source_query=query)
                    next_cursor = extract_bottom_cursor(payload)
                    if not next_cursor or next_cursor == cursor:
                        break
                    cursor = next_cursor

    def _collect_official_handles(self) -> None:
        seed_handles = (
            [("official", handle) for handle in self.cfg.official_handles]
            + [("watch", handle) for handle in self.cfg.watch_handles]
        )
        campaign_start_dt = parse_iso_utc(self.cfg.campaign_start_at)
        # Hard cap on pagination depth to avoid runaway on brand-new accounts or API quirks.
        # With time-window stop, we normally exit much earlier than this.
        max_pages = max(self.cfg.timeline_pages, 10 if campaign_start_dt else self.cfg.timeline_pages)

        for handle_role, handle in seed_handles:
            if self._deadline_exceeded("official_handles"):
                return
            try:
                user_id = self._resolve_user_id(handle)
            except Exception as exc:
                self.summary["errors"].append(f"resolve @{handle}: {exc}")
                continue

            for fetch_kind, _page_limit in (
                (f"{handle_role}_tweets", self.cfg.timeline_pages),
                (f"{handle_role}_replies", self.cfg.timeline_pages),
            ):
                cursor = ""
                crossed_window = False
                for _page in range(max_pages):
                    if self._deadline_exceeded("official_handles"):
                        return
                    try:
                        if fetch_kind.endswith("_tweets"):
                            payload = self.client.fetch_user_tweets(user_id, self.cfg.timeline_count, cursor)
                        else:
                            payload = self.client.fetch_user_replies(user_id, self.cfg.timeline_count, cursor)
                    except Exception as exc:
                        self.summary["errors"].append(f"{fetch_kind} @{handle}: {exc}")
                        break
                    tweets = extract_tweets(payload)
                    self.summary["official_candidates"] += len(tweets)
                    # Time-window guard: once the oldest tweet on this page is before
                    # campaign_start, stop paging. Paid KOL tweets posted BEFORE the
                    # campaign aren't causally attributable anyway.
                    if campaign_start_dt and tweets:
                        oldest_ts = min(
                            (parse_twitter_created_at(t.get("created_at") or "") or datetime.max.replace(tzinfo=timezone.utc))
                            for t in tweets
                        )
                        if oldest_ts < campaign_start_dt:
                            crossed_window = True
                    for tweet in tweets:
                        self._maybe_add(tweet, source_kind=fetch_kind, source_handle=handle)
                    if crossed_window:
                        break
                    next_cursor = extract_bottom_cursor(payload)
                    if not next_cursor or next_cursor == cursor:
                        break
                    cursor = next_cursor

    def _expand_related(self) -> None:
        rows_by_id = self._all_rows_by_tweet_id()
        paid_rows = [rows_by_id[tid] for tid in self.cfg.paid_tweet_ids if tid in rows_by_id]
        ranked = sorted(
            self._matched_rows(),
            key=lambda row: (safe_float(row.get("campaign_affinity")), graph_node_attention(row)),
            reverse=True,
        )
        paid_ids = {str(row.get("tweet_id") or "") for row in paid_rows}
        rows_to_expand = [
            *paid_rows,
            *[row for row in ranked[: max(12, self.cfg.author_expand_limit * 2)] if str(row.get("tweet_id") or "") not in paid_ids],
        ]
        self.summary["paid_reply_quote_roots"] = len(paid_rows)
        for row in rows_to_expand:
            if self._deadline_exceeded("reply_quote_expand"):
                return
            tid = row["tweet_id"]
            for relation, relation_label, fetcher, page_limit in (
                ("reply", "replies", self.client.fetch_comments, self.cfg.reply_pages),
                ("quote", "quotes", self.client.fetch_quotes, self.cfg.quote_pages),
            ):
                cursor = ""
                page_count = 0
                stop_reason = "page_cap_reached" if page_limit > 0 else "disabled"
                repeated_cursor = False
                endpoint_error = ""
                rate_limit = False
                for _page in range(page_limit):
                    if self._deadline_exceeded("reply_quote_expand"):
                        stop_reason = "deadline_exceeded"
                        return
                    try:
                        payload = fetcher(tid, self.cfg.related_count, cursor)
                    except Exception as exc:
                        endpoint_error = str(exc)
                        rate_limit = "429" in endpoint_error or "rate" in endpoint_error.lower()
                        stop_reason = "rate_limit" if rate_limit else "endpoint_error"
                        self.summary["errors"].append(f"{relation} expand {tid}: {exc}")
                        break
                    page_count += 1
                    tweets = extract_tweets(payload)
                    self.summary["expanded_related_candidates"] += len(tweets)
                    for tweet in tweets:
                        relations = tweet.get("relations") or {}
                        if relation == "quote" and str(relations.get("quoted_status_id") or "") != tid:
                            continue
                        if relation == "reply":
                            in_reply_to = str(relations.get("in_reply_to_status_id") or "")
                            conversation_id = str(relations.get("conversation_id") or "")
                            quoted_status_id = str(relations.get("quoted_status_id") or "")
                            if not (in_reply_to == tid or (conversation_id == tid and not quoted_status_id and not in_reply_to)):
                                continue
                        self._maybe_add(tweet, source_kind=f"matched_{relation_label}", parent_tweet_id=tid, relation=relation)
                    next_cursor = extract_bottom_cursor(payload)
                    if not next_cursor:
                        stop_reason = "no_next_cursor"
                        break
                    if next_cursor == cursor:
                        repeated_cursor = True
                        stop_reason = "repeated_cursor"
                        break
                    cursor = next_cursor
                self.related_page_diagnostics.setdefault(tid, {})[relation] = {
                    "page_count": page_count,
                    "cursor_stop_reason": stop_reason,
                    "page_cap_reached": stop_reason == "page_cap_reached",
                    "repeated_cursor": repeated_cursor,
                    "rate_limit": rate_limit,
                    "endpoint_error": endpoint_error,
                }

    def _paid_graph_match_audit(self) -> list[dict]:
        rows_by_id = self._all_rows_by_tweet_id()
        audit_rows: list[dict] = []
        for tid in self.cfg.paid_tweet_ids:
            row = rows_by_id.get(tid)
            if not row:
                status = "fetch_failed" if tid in self.paid_root_errors else "fetch_failed"
                legacy_status = "missing"
                evidence_level = "missing"
                metrics = {}
                observed_at = ""
                source = ""
                metric_status_value = "fetch_failed"
                evidence_status_value = "failed"
            else:
                metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
                source = normalize_paid_source(row.get("source") or "")
                metric_status_value = str(row.get("metric_status") or "").strip().lower()
                evidence_status_value = str(row.get("evidence_status") or "").strip().lower()
                if metric_status_value in {"seed_metric", "pending_metric_fetch"} or evidence_status_value == "seeded":
                    status = "seeded_pending_metric_fetch"
                    legacy_status = "seeded"
                else:
                    status = "matched_observed"
                    legacy_status = "matched"
                evidence_level = "root_observed"
                observed_at = str(row.get("fetched_at") or row.get("observed_at") or "")
            children = [
                candidate
                for candidate in rows_by_id.values()
                if str(((candidate.get("source_meta") or {}).get("parent_tweet_id") if isinstance(candidate.get("source_meta"), dict) else "") or "") == tid
            ]
            reply_count = sum(1 for child in children if ((child.get("source_meta") or {}).get("relation") if isinstance(child.get("source_meta"), dict) else "") == "reply")
            quote_count = sum(1 for child in children if ((child.get("source_meta") or {}).get("relation") if isinstance(child.get("source_meta"), dict) else "") == "quote")
            audit_rows.append(
                {
                    "campaign_id": self.cfg.campaign_id,
                    "tweet_id": tid,
                    "graph_match_status": status,
                    "legacy_graph_match_status": legacy_status,
                    "evidence_level": evidence_level,
                    "source": source,
                    "metric_status": metric_status_value,
                    "evidence_status": evidence_status_value,
                    "diagnostic_reason": self.paid_root_errors.get(tid, ""),
                    "collected_at": self.started_at,
                    "observed_at": observed_at,
                    "source_snapshot_id": self.run_id,
                    "collection_diagnostics": self.related_page_diagnostics.get(tid, {}),
                    "reply_quote_coverage": {
                        "reply_nodes": reply_count,
                        "quote_nodes": quote_count,
                        "total_children": reply_count + quote_count,
                    },
                    "metrics": metrics,
                }
            )
        return audit_rows

    def _write_paid_graph_match_audit(self) -> None:
        audit_rows = self._paid_graph_match_audit()
        counts = {
            "matched_observed": 0,
            "seeded_pending_metric_fetch": 0,
            "fetch_failed": 0,
            "invalid_manifest_row": 0,
            "filtered_by_window": 0,
        }
        legacy_counts = {"matched": 0, "seeded": 0, "missing": 0}
        for row in audit_rows:
            status = str(row.get("graph_match_status") or "fetch_failed")
            counts[status] = counts.get(status, 0) + 1
            legacy_status = str(row.get("legacy_graph_match_status") or "missing")
            legacy_counts[legacy_status] = legacy_counts.get(legacy_status, 0) + 1
        self.summary["paid_graph_matched_count"] = counts.get("matched_observed", 0)
        self.summary["paid_seeded_count"] = counts.get("seeded_pending_metric_fetch", 0)
        self.summary["paid_missing_count"] = counts.get("fetch_failed", 0)
        atomic_write_json(
            self.paid_audit_path,
            {
                "campaign_id": self.cfg.campaign_id,
                "collector_version": "identity_first_v2",
                "run_id": self.run_id,
                "updated_at": now_iso(),
                "paid_deliverable_count": len(self.cfg.paid_tweet_ids),
                "status_counts": counts,
                "legacy_status_counts": legacy_counts,
                "paid_deliverables": audit_rows,
            },
        )

    def _expand_authors(self) -> None:
        if self.cfg.author_expand_limit <= 0:
            return
        author_scores: dict[str, float] = {}
        for row in self._matched_rows():
            author = normalize_handle(row.get("author") or "")
            if not author or author in set(self.cfg.official_handles) | set(self.cfg.watch_handles):
                continue
            author_scores[author] = max(
                author_scores.get(author, 0.0),
                safe_float(row.get("campaign_affinity")) * 1000.0 + graph_node_attention(row),
            )

        expanded = 0
        for author, _score in sorted(author_scores.items(), key=lambda item: item[1], reverse=True):
            if self._deadline_exceeded("author_expand"):
                return
            if expanded >= self.cfg.author_expand_limit:
                break
            try:
                user_id = self._resolve_user_id(author)
            except Exception as exc:
                self.summary["errors"].append(f"resolve expanded @{author}: {exc}")
                continue
            cursor = ""
            for _page in range(self.cfg.author_expand_pages):
                if self._deadline_exceeded("author_expand"):
                    return
                try:
                    payload = self.client.fetch_user_tweets(user_id, self.cfg.author_expand_count, cursor)
                except Exception as exc:
                    self.summary["errors"].append(f"expand author @{author}: {exc}")
                    break
                tweets = extract_tweets(payload)
                self.summary["expanded_author_candidates"] += len(tweets)
                for tweet in tweets:
                    self._maybe_add(tweet, source_kind="expanded_author_tweets", source_handle=author)
                next_cursor = extract_bottom_cursor(payload)
                if not next_cursor or next_cursor == cursor:
                    break
                cursor = next_cursor
            expanded += 1

    def _enrich_articles(self) -> None:
        watch_set = set(self.cfg.watch_handles)
        enriched = 0
        candidates_seen = 0
        fetch_misses = 0
        no_hit = 0
        parse_errors = 0
        # Merge best_existing + best_run, keeping the better observation per tid
        candidates: dict[str, dict] = {}
        for tid, row in self.best_existing.items():
            candidates[tid] = row
        for tid, row in self.best_run.items():
            if should_replace_observation(candidates.get(tid), row):
                candidates[tid] = row
        for tid, row in candidates.items():
            if self._deadline_exceeded("article_enrich"):
                return
            author = normalize_handle(row.get("author") or "")
            if author not in watch_set:
                continue
            reasons = row.get("affinity_reason") or []
            if any(r.startswith("identity_term:") or r.startswith("article_identity_term:") for r in reasons):
                continue
            urls = [str(u).lower() for u in (row.get("urls") or [])]
            if not any(is_article_url(u) for u in urls):
                continue
            candidates_seen += 1
            fxt = fetch_fxtwitter(author, tid)
            if not fxt:
                fetch_misses += 1
                continue
            try:
                hits = fxtwitter_article_terms(fxt, self.cfg.terms)
            except Exception as exc:
                self.summary["errors"].append(f"article parse @{author}/{tid}: {exc}")
                parse_errors += 1
                continue
            if hits:
                import copy
                enriched_row = copy.deepcopy(row)
                enriched_row["affinity_reason"] = enriched_row.get("affinity_reason", []) + [f"article_identity_term:{','.join(hits[:3])}"]
                enriched_row["campaign_affinity"] = max(enriched_row.get("campaign_affinity", 0), 0.9)
                enriched_row["article_brand_match"] = True
                self.best_run[tid] = enriched_row
                self._register_match(row)
                enriched += 1
            else:
                no_hit += 1
            time.sleep(0.3)
        self.summary["article_enriched"] = enriched
        self.summary["article_candidates"] = candidates_seen
        self.summary["article_fetch_misses"] = fetch_misses
        self.summary["article_no_hit"] = no_hit
        self.summary["article_parse_errors"] = parse_errors

    def _upsert_config(self) -> None:
        raw = load_json_object(self.config_path, default={})
        identity = raw.get("identity") if isinstance(raw.get("identity"), dict) else {}
        identity["names"] = unique_strings([*(identity.get("names") or []), *self.cfg.terms])
        identity["official_handles"] = unique_strings([*(identity.get("official_handles") or []), *self.cfg.official_handles])
        identity["watch_handles"] = unique_strings([*(identity.get("watch_handles") or []), *self.cfg.watch_handles])
        raw.update({
            "campaign_id": raw.get("campaign_id") or self.cfg.campaign_id,
            "name": raw.get("name") or self.cfg.name,
            "source_mode": "entity_graph",
            "identity": identity,
            "collector": {
                "version": "identity_first_v2",
                "strategy": [
                    "search_identity_stream",
                    "official_handle_activity",
                    "matched_reply_quote_expansion",
                    "matched_author_expansion",
                ],
                "min_affinity": self.cfg.min_affinity,
                "search_types": self.cfg.search_types,
                "last_run_at": now_iso(),
            },
        })
        atomic_write_json(self.config_path, raw)

    def _write_nodes(self) -> None:
        self.campaign_dir.mkdir(parents=True, exist_ok=True)
        rows_to_write = sorted(self._matched_rows(), key=lambda row: row.get("created_at", ""))
        atomic_write_jsonl(self.nodes_path, rows_to_write)
        self.summary["written_rows"] = len(rows_to_write)

    def _write_state(self, status: str = "succeeded", error: str = "") -> None:
        state = {
            "collector_version": "identity_first_v2",
            "run_id": self.run_id,
            "status": status,
            "campaign_id": self.cfg.campaign_id,
            "started_at": self.started_at,
            "updated_at": now_iso(),
            "duration_seconds": round(time.monotonic() - self.started_monotonic, 1),
            "terms": self.cfg.terms,
            "search_language": self.cfg.search_language,
            "paid_deliverable_count": len(self.cfg.paid_tweet_ids),
            "official_handles": self.cfg.official_handles,
            "watch_handles": self.cfg.watch_handles,
            "matched_nodes": len(self._matched_rows()),
            "matched_tweet_ids": sorted(self.matched_tweet_ids),
            "search_path_kind": self.client.search_path_kind,
            "user_replies_kind": self.client.user_replies_kind,
            "deadline_exceeded": bool(self.summary.get("deadline_exceeded")),
            "error": error,
            "summary": self.summary,
        }
        if status in {"succeeded", "failed", "timeout"}:
            state["completed_at"] = now_iso()
        atomic_write_json(self.state_path, state)

    def run(self) -> dict:
        t0 = time.monotonic()
        tag = f"[collector {self.cfg.campaign_id}]"
        print(f"{tag} start 4-step identity-first discovery"
              f"  watch={len(self.cfg.watch_handles)}  terms={len(self.cfg.terms)}"
              f"  min_affinity={self.cfg.min_affinity}", flush=True)
        self._write_state("running")

        try:
            t_step = time.monotonic()
            self._collect_search()
            print(f"{tag} step 1/4 search_identity:"
                  f"  queries={self.summary['search_queries']} candidates={self.summary['search_candidates']}"
                  f"  ({time.monotonic() - t_step:.1f}s)", flush=True)

            t_step = time.monotonic()
            self._collect_official_handles()
            print(f"{tag} step 2/4 watch+official timeline:"
                  f"  candidates={self.summary['official_candidates']}"
                  f"  ({time.monotonic() - t_step:.1f}s)", flush=True)

            t_step = time.monotonic()
            self._collect_paid_roots()
            print(f"{tag} step 2.5/4 paid root fetch:"
                  f"  paid={len(self.cfg.paid_tweet_ids)} attempts={self.summary['paid_root_fetch_attempts']}"
                  f"  found={self.summary['paid_root_fetch_found']}"
                  f"  ({time.monotonic() - t_step:.1f}s)", flush=True)

            t_step = time.monotonic()
            self._expand_related()
            print(f"{tag} step 3/4 reply/quote expand:"
                  f"  candidates={self.summary['expanded_related_candidates']}"
                  f"  ({time.monotonic() - t_step:.1f}s)", flush=True)

            t_step = time.monotonic()
            self._expand_authors()
            print(f"{tag} step 4/4 author expand:"
                  f"  candidates={self.summary['expanded_author_candidates']}"
                  f"  ({time.monotonic() - t_step:.1f}s)", flush=True)

            t_step = time.monotonic()
            self._enrich_articles()
            print(f"{tag} step 4.5 article enrich (FxTwitter):"
                  f"  enriched={self.summary.get('article_enriched', 0)}"
                  f"  ({time.monotonic() - t_step:.1f}s)", flush=True)

            self.summary["matched_nodes"] = len(self._matched_rows())
            self._upsert_config()
            self._write_nodes()
            self._write_paid_graph_match_audit()
            self._write_state("succeeded")
            print(f"{tag} done: matched={self.summary['matched_nodes']} written={self.summary['written_rows']}"
                  f"  errors={len(self.summary['errors'])}  total={time.monotonic() - t0:.1f}s", flush=True)
        except Exception as exc:
            self.summary["errors"].append(f"collector failed: {exc}")
            self.summary["matched_nodes"] = len(self._matched_rows())
            self._write_state("failed", str(exc))
            raise
        return {
            "campaign_id": self.cfg.campaign_id,
            "name": self.cfg.name,
            "run_id": self.run_id,
            "terms": self.cfg.terms,
            "official_handles": self.cfg.official_handles,
            "watch_handles": self.cfg.watch_handles,
            **self.summary,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Identity-first campaign collector for x-heat-index.")
    parser.add_argument("--campaign-id", required=True, help="Campaign ID under DATA_DIR/campaign_graphs/<campaign_id>/")
    parser.add_argument("--campaign-name", default="", help="Campaign name used when creating config.json.")
    parser.add_argument("--data-dir", default=os.environ.get("DATA_DIR", ""), help="DATA_DIR root.")
    parser.add_argument("--term", action="append", default=[], help="Campaign identity term. Repeatable.")
    parser.add_argument("--official-handle", action="append", default=[], help="Official/canonical handle. Repeatable.")
    parser.add_argument("--watch-handle", action="append", default=[], help="Extra watch/KOL handle. Repeatable.")
    parser.add_argument("--min-affinity", type=float, default=0.42)
    parser.add_argument("--search-type", action="append", default=[], help="Search type, e.g. Latest or Top.")
    parser.add_argument("--search-pages", type=int, default=2)
    parser.add_argument("--search-count", type=int, default=20)
    parser.add_argument("--timeline-pages", type=int, default=5)
    parser.add_argument("--timeline-count", type=int, default=40)
    parser.add_argument("--reply-pages", type=int, default=1)
    parser.add_argument("--quote-pages", type=int, default=1)
    parser.add_argument("--related-count", type=int, default=20)
    parser.add_argument("--author-expand-limit", type=int, default=8)
    parser.add_argument("--author-expand-pages", type=int, default=1)
    parser.add_argument("--author-expand-count", type=int, default=10)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    cfg = build_runtime_config(args)
    client = Twitter241Client()
    collector = IdentityCollector(cfg, client)
    result = collector.run()
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    exit_code = 1
    try:
        exit_code = main()
    except BrokenPipeError:
        exit_code = 0
    finally:
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except Exception:
            pass
    os._exit(exit_code)
