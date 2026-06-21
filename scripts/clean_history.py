#!/usr/bin/env python3
"""Conservatively clean historically polluted raw interaction files.

This keeps trustworthy root metrics history intact while trimming obviously
mis-attributed replies/quotes/cascade nodes from older JSONL snapshots.
After cleaning raw files, it rebuilds derived outputs from the cleaned data.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict, deque
from pathlib import Path

from backfill_history import (
    backup_file,
    event_dt,
    load_json,
    load_jsonl,
    normalize_handle,
    now_iso,
    process_tweet_dir,
    write_json,
    write_jsonl,
)

LEADING_MENTION_RE = re.compile(r"^@([A-Za-z0-9_]{1,15})\b")
HANDLE_RE = re.compile(r"@([A-Za-z0-9_]{1,15})\b")
TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9_+-]{2,}")

STOPWORDS = {
    "about",
    "after",
    "agent",
    "agentic",
    "agents",
    "along",
    "also",
    "another",
    "any",
    "apis",
    "around",
    "because",
    "before",
    "being",
    "better",
    "build",
    "built",
    "code",
    "command",
    "compatible",
    "crypto",
    "data",
    "environment",
    "every",
    "fluent",
    "from",
    "have",
    "here",
    "http",
    "https",
    "into",
    "just",
    "line",
    "more",
    "released",
    "replaces",
    "skill",
    "skills",
    "than",
    "that",
    "their",
    "them",
    "there",
    "these",
    "they",
    "this",
    "today",
    "try",
    "using",
    "with",
    "work",
    "your",
}

PHRASE_TERMS = (
    "claude code",
    "api credit",
    "api credits",
)


def unique_by_tweet_id(rows: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for row in rows:
        tid = str(row.get("tweet_id") or "")
        if not tid or tid in seen:
            continue
        seen.add(tid)
        out.append(row)
    return out


def first_leading_mention(text: str) -> str:
    raw = (text or "").lstrip()
    while raw and raw[0] in ".!,:;?()[]{}\"'":
        raw = raw[1:].lstrip()
    match = LEADING_MENTION_RE.match(raw)
    return normalize_handle(match.group(1) if match else "")


def all_mentions(text: str) -> set[str]:
    return {normalize_handle(handle) for handle in HANDLE_RE.findall(text or "") if handle}


def text_has_term(text: str, term: str) -> bool:
    low = (text or "").lower()
    if not term:
        return False
    if " " in term:
        return term in low
    return bool(re.search(rf"\b{re.escape(term)}\b", low))


def discover_root_row(tweet_dir: Path, tweet_id: str) -> dict:
    for name in ("metrics.jsonl", "cascade_nodes.jsonl", "replies.jsonl", "quotes.jsonl"):
        for row in load_jsonl(tweet_dir / name):
            if str(row.get("tweet_id") or "") == tweet_id:
                return row
    return {}


def extract_theme_terms(root_row: dict) -> list[str]:
    text = root_row.get("text", "") or ""
    terms: list[str] = []
    seen: set[str] = set()
    low = text.lower()

    def add_term(term: str) -> None:
        normalized = term.strip().lower()
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        terms.append(normalized)

    for phrase in PHRASE_TERMS:
        if phrase in low:
            add_term(phrase)

    for handle in HANDLE_RE.findall(text):
        add_term(normalize_handle(handle))

    words = TOKEN_RE.findall(text)
    for token in words:
        base = token.lower()
        if base in STOPWORDS or len(base) < 4:
            continue
        if base.startswith("http") or any(ch.isdigit() for ch in base):
            continue
        if any(ch.isupper() for ch in token) or base.endswith("ai"):
            add_term(base)

    for idx in range(len(words) - 1):
        left = words[idx]
        right = words[idx + 1]
        left_base = left.lower()
        right_base = right.lower()
        if len(left_base) < 4 or len(right_base) < 4:
            continue
        if left_base in STOPWORDS or right_base in STOPWORDS:
            continue
        if left_base.startswith("http") or right_base.startswith("http"):
            continue
        if any(ch.isdigit() for ch in left_base + right_base):
            continue
        if any(ch.isupper() for ch in left) or any(ch.isupper() for ch in right):
            add_term(f"{left_base} {right_base}")

    return terms[:12]


def direct_reply_matches(row: dict, root_id: str, root_author: str) -> bool:
    if str(row.get("in_reply_to_status_id") or "") == root_id:
        return True
    if str(row.get("conversation_id") or "") == root_id and not row.get("quoted_status_id"):
        return True
    return first_leading_mention(row.get("text", "")) == root_author


def direct_quote_matches(row: dict, root_id: str, root_author: str, theme_terms: list[str]) -> bool:
    if str(row.get("quoted_status_id") or "") == root_id:
        return True
    mentions = all_mentions(row.get("text", ""))
    if root_author in mentions:
        return True
    return any(text_has_term(row.get("text", ""), term) for term in theme_terms)


def reply_descendant_matches(row: dict, parent_author: str) -> bool:
    normalized_parent = normalize_handle(parent_author)
    if not normalized_parent:
        return False
    if first_leading_mention(row.get("text", "")) == normalized_parent:
        return True
    return normalize_handle(row.get("in_reply_to_username", "")) == normalized_parent


def quote_descendant_matches(row: dict, root_author: str, parent_author: str, theme_terms: list[str]) -> bool:
    if str(row.get("quoted_status_id") or ""):
        return True
    mentions = all_mentions(row.get("text", ""))
    normalized_parent = normalize_handle(parent_author)
    if root_author in mentions or (normalized_parent and normalized_parent in mentions):
        return True
    return any(text_has_term(row.get("text", ""), term) for term in theme_terms)


def build_cascade_candidates(cascade_nodes: list[dict], cascade_edges: list[dict], root_id: str) -> dict[str, list[tuple[str, str]]]:
    children_by_parent: dict[str, list[tuple[str, str]]] = defaultdict(list)
    seen_edges: set[tuple[str, str, str]] = set()

    def register_edge(parent_id: str, child_id: str, edge_type: str) -> None:
        edge_type = edge_type or "reply"
        if not parent_id or not child_id or parent_id == child_id or child_id == root_id:
            return
        key = (parent_id, child_id, edge_type)
        if key in seen_edges:
            return
        seen_edges.add(key)
        children_by_parent[parent_id].append((child_id, edge_type))

    for edge in cascade_edges:
        register_edge(str(edge.get("parent_id") or ""), str(edge.get("child_id") or ""), str(edge.get("edge_type") or "reply"))

    for row in cascade_nodes:
        register_edge(str(row.get("parent_id") or ""), str(row.get("tweet_id") or ""), str(row.get("edge_type") or "reply"))

    return children_by_parent


def clean_cascade(
    root_id: str,
    root_author: str,
    theme_terms: list[str],
    direct_rows: list[dict],
    cascade_nodes: list[dict],
    cascade_edges: list[dict],
) -> tuple[list[dict], list[dict], dict]:
    direct_by_id = {str(row.get("tweet_id") or ""): row for row in direct_rows if row.get("tweet_id")}
    node_by_id = {str(row.get("tweet_id") or ""): row for row in unique_by_tweet_id(cascade_nodes) if row.get("tweet_id")}
    children_by_parent = build_cascade_candidates(cascade_nodes, cascade_edges, root_id)

    frontier = deque(direct_by_id)
    seen_ids = set(direct_by_id)
    kept_nodes: list[dict] = []
    kept_edges: list[dict] = []
    edge_type_counter: Counter[str] = Counter()

    while frontier:
        parent_id = frontier.popleft()
        parent_row = direct_by_id.get(parent_id) or node_by_id.get(parent_id) or {}
        parent_author = parent_row.get("author_username", "")
        for child_id, edge_type in children_by_parent.get(parent_id, []):
            if child_id in seen_ids:
                continue
            child_row = node_by_id.get(child_id)
            if not child_row:
                continue
            if edge_type == "reply":
                keep = reply_descendant_matches(child_row, parent_author)
            else:
                keep = quote_descendant_matches(child_row, root_author, parent_author, theme_terms)
            if not keep:
                continue

            seen_ids.add(child_id)
            frontier.append(child_id)
            kept_row = dict(child_row)
            kept_row["parent_id"] = parent_id
            kept_row["parent_author"] = parent_author
            kept_row["depth"] = 2
            kept_row["edge_type"] = edge_type
            kept_nodes.append(kept_row)
            kept_edges.append(
                {
                    "parent_id": parent_id,
                    "child_id": child_id,
                    "edge_type": edge_type,
                    "discovered_at": child_row.get("fetched_at") or child_row.get("discovered_at") or now_iso(),
                }
            )
            edge_type_counter[edge_type] += 1

    kept_nodes.sort(key=lambda row: event_dt(row, "fetched_at", "discovered_at", "created_at"))
    kept_edges.sort(key=lambda row: event_dt(row, "discovered_at"))
    return kept_nodes, kept_edges, {"edge_types": dict(edge_type_counter)}


def clean_tweet_dir(tweet_dir: Path, backup: bool, dry_run: bool) -> dict:
    tweet_id = tweet_dir.name
    metrics_rows = load_jsonl(tweet_dir / "metrics.jsonl")
    if not metrics_rows:
        return {"tweet_id": tweet_id, "status": "skipped", "reason": "no metrics"}

    root_row = discover_root_row(tweet_dir, tweet_id)
    root_author = normalize_handle((metrics_rows[-1].get("author_username") or root_row.get("author_username") or ""))
    if not root_author:
        return {"tweet_id": tweet_id, "status": "skipped", "reason": "unknown root author"}

    theme_terms = extract_theme_terms(root_row)
    latest_reply_count = int(metrics_rows[-1].get("reply_count") or 0)
    latest_quote_count = int(metrics_rows[-1].get("quote_count") or 0)

    replies = unique_by_tweet_id(load_jsonl(tweet_dir / "replies.jsonl"))
    quotes = unique_by_tweet_id(load_jsonl(tweet_dir / "quotes.jsonl"))
    cascade_nodes = unique_by_tweet_id(load_jsonl(tweet_dir / "cascade_nodes.jsonl"))
    cascade_edges = load_jsonl(tweet_dir / "cascade_edges.jsonl")
    state = load_json(tweet_dir / "state.json")
    walker_state = load_json(tweet_dir / "walker_state.json")

    cleaned_replies = [row for row in replies if direct_reply_matches(row, tweet_id, root_author)]
    quote_is_trusted = len(quotes) <= max(latest_quote_count + 8, int(latest_quote_count * 1.35))
    cleaned_quotes = list(quotes if quote_is_trusted else [row for row in quotes if direct_quote_matches(row, tweet_id, root_author, theme_terms)])
    cleaned_direct_rows = cleaned_replies + cleaned_quotes

    cleaned_nodes, cleaned_edges, cascade_stats = clean_cascade(
        root_id=tweet_id,
        root_author=root_author,
        theme_terms=theme_terms,
        direct_rows=cleaned_direct_rows,
        cascade_nodes=cascade_nodes,
        cascade_edges=cascade_edges,
    )

    cleaned_reply_ids = [str(row.get("tweet_id")) for row in cleaned_replies if row.get("tweet_id")]
    cleaned_quote_ids = [str(row.get("tweet_id")) for row in cleaned_quotes if row.get("tweet_id")]
    cleaned_node_ids = [str(row.get("tweet_id")) for row in cleaned_nodes if row.get("tweet_id")]

    new_state = dict(state)
    new_state["seen_reply_ids"] = cleaned_reply_ids
    new_state["seen_quote_ids"] = cleaned_quote_ids

    new_walker_state = dict(walker_state)
    new_walker_state["walked_node_ids"] = sorted({*cleaned_reply_ids, *cleaned_quote_ids})
    new_walker_state["seen_sub_node_ids"] = cleaned_node_ids
    new_walker_state["started_at"] = new_walker_state.get("started_at") or state.get("started_at") or metrics_rows[0].get("ts") or now_iso()

    report = {
        "tweet_id": tweet_id,
        "cleaned_at": now_iso(),
        "root_author": root_author,
        "theme_terms": theme_terms,
        "latest_counts": {
            "reply_count": latest_reply_count,
            "quote_count": latest_quote_count,
        },
        "before": {
            "replies": len(replies),
            "quotes": len(quotes),
            "cascade_nodes": len(cascade_nodes),
            "cascade_edges": len(cascade_edges),
        },
        "after": {
            "replies": len(cleaned_replies),
            "quotes": len(cleaned_quotes),
            "cascade_nodes": len(cleaned_nodes),
            "cascade_edges": len(cleaned_edges),
        },
        "quote_cleaning_mode": "keep_all" if quote_is_trusted else "theme_filter",
        "cascade": cascade_stats,
    }

    if dry_run:
        report["status"] = "dry_run"
        return report

    backups: list[str] = []
    if backup:
        for path in (
            tweet_dir / "replies.jsonl",
            tweet_dir / "quotes.jsonl",
            tweet_dir / "cascade_nodes.jsonl",
            tweet_dir / "cascade_edges.jsonl",
            tweet_dir / "state.json",
            tweet_dir / "walker_state.json",
            tweet_dir / "cleanup_report.json",
        ):
            copy = backup_file(path)
            if copy:
                backups.append(str(copy))

    write_jsonl(tweet_dir / "replies.jsonl", cleaned_replies)
    write_jsonl(tweet_dir / "quotes.jsonl", cleaned_quotes)
    write_jsonl(tweet_dir / "cascade_nodes.jsonl", cleaned_nodes)
    write_jsonl(tweet_dir / "cascade_edges.jsonl", cleaned_edges)
    write_json(tweet_dir / "state.json", new_state)
    write_json(tweet_dir / "walker_state.json", new_walker_state)
    write_json(tweet_dir / "cleanup_report.json", report)

    rebuilt = process_tweet_dir(tweet_dir, backup=backup)
    rebuilt["cleanup_report"] = report
    rebuilt["raw_backups"] = backups
    return rebuilt


def tweet_dirs_for_args(data_dir: Path, tweet_ids: list[str] | None) -> list[Path]:
    if tweet_ids:
        return [data_dir / tid for tid in tweet_ids if (data_dir / tid).is_dir()]
    return sorted(path for path in data_dir.iterdir() if path.is_dir())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="/opt/tweet-tracker/data", help="Tweet tracker data root")
    parser.add_argument("--tweet-id", action="append", help="Specific tweet ID to clean; repeatable")
    parser.add_argument("--no-backup", action="store_true", help="Rewrite files without creating .bak copies")
    parser.add_argument("--dry-run", action="store_true", help="Analyze cleanup effect without writing files")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"ERROR: data dir not found: {data_dir}")
        return 1

    targets = tweet_dirs_for_args(data_dir, args.tweet_id)
    if not targets:
        print("ERROR: no tweet directories matched")
        return 1

    results = []
    for tweet_dir in targets:
        result = clean_tweet_dir(tweet_dir, backup=not args.no_backup, dry_run=args.dry_run)
        results.append(result)
        if result.get("status") == "ok":
            report = result.get("cleanup_report", {})
            print(
                f"[ok] {result['tweet_id']}: replies {report['before']['replies']}→{report['after']['replies']} "
                f"quotes {report['before']['quotes']}→{report['after']['quotes']} "
                f"cascade {report['before']['cascade_nodes']}→{report['after']['cascade_nodes']}"
            )
        elif result.get("status") == "dry_run":
            print(
                f"[dry-run] {result['tweet_id']}: replies {result['before']['replies']}→{result['after']['replies']} "
                f"quotes {result['before']['quotes']}→{result['after']['quotes']} "
                f"cascade {result['before']['cascade_nodes']}→{result['after']['cascade_nodes']}"
            )
        else:
            print(f"[skip] {result['tweet_id']}: {result.get('reason', 'unknown')}")

    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
