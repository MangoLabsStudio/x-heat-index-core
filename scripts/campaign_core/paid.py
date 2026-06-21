"""Paid deliverable manifest parsing for tracker dispatch and pool scoring."""

from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path
from typing import Any

from .identity import normalize_handle


DELIVERY_CONFIG_KEYS = ("paid_deliverables", "paid_tweets", "paid_tweet_ids", "delivery_tweets")
TWEET_ID_RE = re.compile(r"(?<!\d)(\d{12,25})(?!\d)")
TIME_KEYS = ("created_at", "submitted_at", "posted_at", "expected_at")
URL_KEYS = ("url", "tweet_url", "tweetUrl", "x_url", "status_url")
AUTHOR_KEYS = ("handle", "author", "username", "screen_name", "kol_handle")
PAID_DELIVERABLE_SEED_SOURCE = "paid_deliverable_seed"
PAID_DELIVERABLE_TRACKER_SOURCE = "paid_deliverable_tracker"
PAID_DELIVERABLE_SOURCE_ALIASES = {
    PAID_DELIVERABLE_SEED_SOURCE,
    PAID_DELIVERABLE_TRACKER_SOURCE,
    "paid_delivery_seed",
    "paid_delivery_tracker",
}
PAID_DELIVERABLE_SIGNAL_ALIASES = {
    PAID_DELIVERABLE_SEED_SOURCE,
    "paid_delivery_seed",
}


def normalize_paid_source(value: Any) -> str:
    source = str(value or "").strip()
    if source == "paid_delivery_seed":
        return PAID_DELIVERABLE_SEED_SOURCE
    if source == "paid_delivery_tracker":
        return PAID_DELIVERABLE_TRACKER_SOURCE
    return source


def is_paid_source(value: Any) -> bool:
    return normalize_paid_source(value) in {PAID_DELIVERABLE_SEED_SOURCE, PAID_DELIVERABLE_TRACKER_SOURCE}


def has_paid_deliverable_signal(reasons: Any) -> bool:
    if not isinstance(reasons, list):
        reasons = [reasons]
    return any(str(reason).startswith(tuple(PAID_DELIVERABLE_SIGNAL_ALIASES)) for reason in reasons)


def extract_tweet_id(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    status_match = re.search(r"/(?:status|statuses)/(\d{12,25})", raw)
    if status_match:
        return status_match.group(1)
    if re.fullmatch(r"\d{12,25}", raw):
        return raw
    any_match = TWEET_ID_RE.search(raw)
    return any_match.group(1) if any_match else ""


def manifest_entries(value: Any) -> list[Any]:
    if not value:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        for key in ("deliverables", "tweets", "tweet_ids", "paid_deliverables"):
            nested = value.get(key)
            if isinstance(nested, list):
                return nested
        return [value]
    return [value]


def _campaign_dir(campaign_id: str, base_dir: Path | str | None) -> Path | None:
    if base_dir is None:
        return None
    root = Path(base_dir)
    if root.name == str(campaign_id):
        return root
    if root.name == "campaign_graphs":
        return root / str(campaign_id)
    return root / "campaign_graphs" / str(campaign_id)


def _entry_value(entry: Any, *keys: str) -> Any:
    if not isinstance(entry, dict):
        return ""
    for key in keys:
        value = entry.get(key)
        if value not in (None, ""):
            return value
    return ""


def normalize_paid_deliverable(row: Any, *, source: str = "unknown") -> dict[str, Any]:
    """Normalize a paid deliverable row while preserving invalid-row diagnostics."""
    if isinstance(row, dict):
        raw_tid = _entry_value(row, "tweet_id", "tweetId", "tid", "id", *URL_KEYS)
        url = str(_entry_value(row, *URL_KEYS) or "")
        author = normalize_handle(_entry_value(row, *AUTHOR_KEYS))
        delivered_at = str(_entry_value(row, *TIME_KEYS) or "")
        label = str(_entry_value(row, "label", "name", "title") or "")
        metrics = row.get("participant_metrics")
        if not isinstance(metrics, dict):
            metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
        raw = row
    else:
        raw_tid = row
        url = str(row or "")
        author = ""
        delivered_at = ""
        label = ""
        metrics = {}
        raw = {"value": row}

    tweet_id = extract_tweet_id(raw_tid)
    if not tweet_id and url:
        tweet_id = extract_tweet_id(url)

    reasons: list[str] = []
    if not tweet_id:
        reasons.append("missing_tweet_id")
    if not delivered_at:
        reasons.append("missing_time")
    if not author:
        reasons.append("missing_author")

    return {
        "tweet_id": tweet_id,
        "tid": tweet_id,
        "author": author,
        "normalized_author": author,
        "url": url,
        "tweet_url": url,
        "label": label,
        "expected_at": delivered_at,
        "delivered_at": delivered_at,
        "metrics": metrics,
        "participant_metrics": metrics,
        "source": source,
        "sources": [source],
        "merged_sources": [source],
        "valid": not reasons,
        "diagnostic_reason": reasons[0] if reasons else "included",
        "diagnostic_reasons": reasons,
        "raw": raw,
    }


def _dedupe_key(item: dict[str, Any]) -> tuple[str, str]:
    if item.get("tweet_id"):
        return ("tweet_id", str(item["tweet_id"]))
    url = str(item.get("url") or item.get("tweet_url") or "").strip().lower()
    author = str(item.get("normalized_author") or "").strip().lower()
    return ("url_author", f"{url}|{author}")


def _merge_paid_deliverable(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for key, value in incoming.items():
        if key in {"sources", "merged_sources", "diagnostic_reasons"}:
            continue
        if merged.get(key) in (None, "", [], {}) and value not in (None, "", [], {}):
            merged[key] = value
    sources = [*existing.get("merged_sources", existing.get("sources", [])), *incoming.get("merged_sources", incoming.get("sources", []))]
    merged["sources"] = list(dict.fromkeys(str(source) for source in sources if source))
    merged["merged_sources"] = merged["sources"]
    reasons: list[str] = []
    if not merged.get("tweet_id"):
        reasons.append("missing_tweet_id")
    if not (merged.get("expected_at") or merged.get("delivered_at")):
        reasons.append("missing_time")
    if not merged.get("normalized_author"):
        reasons.append("missing_author")
    merged["diagnostic_reasons"] = reasons
    merged["valid"] = not merged["diagnostic_reasons"]
    merged["diagnostic_reason"] = merged["diagnostic_reasons"][0] if merged["diagnostic_reasons"] else "included"
    return merged


def load_paid_deliverables(
    campaign_id: str,
    cfg: dict[str, Any],
    base_dir: Path | str | None = None,
    *,
    include_external: bool = True,
) -> list[dict[str, Any]]:
    entries: list[tuple[Any, str]] = []
    blocks = [(cfg, "config")]
    identity = cfg.get("identity") if isinstance(cfg.get("identity"), dict) else {}
    if identity:
        blocks.append((identity, "identity"))
    for block, source in blocks:
        for key in DELIVERY_CONFIG_KEYS:
            entries.extend((entry, source) for entry in manifest_entries(block.get(key)))

    cdir = _campaign_dir(campaign_id, base_dir)
    if include_external and cdir is not None:
        json_path = cdir / "paid_deliverables.json"
        if json_path.exists():
            try:
                entries.extend((entry, "external_json") for entry in manifest_entries(json.loads(json_path.read_text(encoding="utf-8"))))
            except Exception as exc:
                print(f"WARN: could not read {json_path}: {exc}", file=sys.stderr)

        csv_path = cdir / "paid_deliverables.csv"
        if csv_path.exists():
            try:
                with csv_path.open(newline="", encoding="utf-8") as fh:
                    entries.extend((dict(row), "csv") for row in csv.DictReader(fh))
            except Exception as exc:
                print(f"WARN: could not read {csv_path}: {exc}", file=sys.stderr)

    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for entry, source in entries:
        item = normalize_paid_deliverable(entry, source=source)
        key = _dedupe_key(item)
        if not key[1]:
            key = ("row", str(len(deduped)))
        if key in deduped:
            deduped[key] = _merge_paid_deliverable(deduped[key], item)
        else:
            deduped[key] = item
    return list(deduped.values())


def diagnose_paid_deliverable_inclusion(row: Any, pool: dict[str, Any] | None = None, nodes: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    item = row if isinstance(row, dict) and "diagnostic_reason" in row else normalize_paid_deliverable(row)
    reasons = list(item.get("diagnostic_reasons") or [])
    node = None
    if nodes is not None and item.get("tweet_id"):
        node = next((candidate for candidate in nodes if str(candidate.get("tweet_id") or "") == item["tweet_id"]), None)
        if node is None:
            reasons.append("pending_metric_fetch")
    if pool and node is not None:
        scope = pool.get("scope") if isinstance(pool.get("scope"), dict) else {}
        handles = {normalize_handle(handle) for handle in scope.get("handles", [])}
        tweet_ids = {str(tweet_id).strip() for tweet_id in scope.get("tweet_ids", [])}
        sources = {str(source).strip() for source in scope.get("sources", [])}
        author = normalize_handle(node.get("author") or node.get("author_username") or item.get("normalized_author"))
        if handles and author not in handles:
            reasons.append("filtered_by_scope")
        if tweet_ids and str(node.get("tweet_id") or "") not in tweet_ids:
            reasons.append("filtered_by_scope")
        if sources and str(node.get("source") or "") not in sources:
            reasons.append("filtered_by_scope")
    reasons = list(dict.fromkeys(reasons))
    status = "included" if not reasons else reasons[0]
    return {
        "tweet_id": item.get("tweet_id") or "",
        "normalized_author": item.get("normalized_author") or "",
        "status": status,
        "reasons": reasons,
        "merged_sources": item.get("merged_sources") or item.get("sources") or [],
    }


def coerce_paid_seed(entry: Any) -> dict[str, str] | None:
    normalized = normalize_paid_deliverable(entry)
    tid = str(normalized.get("tweet_id") or "")
    if not tid:
        return None
    return {
        "tid": tid,
        "author": str(normalized.get("normalized_author") or ""),
        "url": str(normalized.get("url") or ""),
        "label": str(normalized.get("label") or ""),
        "expected_at": str(normalized.get("expected_at") or ""),
    }


def load_paid_delivery_seeds(cdir: Path, cfg: dict[str, Any]) -> list[dict[str, str]]:
    campaign_id = str(cfg.get("campaign_id") or cdir.name)
    seeds: dict[str, dict[str, str]] = {}
    for item in load_paid_deliverables(campaign_id, cfg, cdir):
        seed = coerce_paid_seed(item)
        if seed:
            seeds[seed["tid"]] = seed
    return list(seeds.values())
