"""Metric extraction and observation deduplication helpers."""

from __future__ import annotations

from datetime import datetime
from typing import Any


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def metrics_view_count(row: dict[str, Any]) -> int:
    return safe_int((row.get("metrics") or {}).get("views"))


def metrics_engagement_count(row: dict[str, Any]) -> int:
    metrics = row.get("metrics") or {}
    return (
        safe_int(metrics.get("likes"))
        + safe_int(metrics.get("retweets"))
        + safe_int(metrics.get("replies"))
        + safe_int(metrics.get("quotes"))
        + safe_int(metrics.get("bookmarks"))
    )


def metric_status(row: dict[str, Any]) -> str:
    return str(row.get("metric_status") or "").strip().lower()


def evidence_status(row: dict[str, Any]) -> str:
    return str(row.get("evidence_status") or "").strip().lower()


def evidence_rank(row: dict[str, Any]) -> int:
    """Rank records by evidence reliability before comparing metric values."""
    status = metric_status(row)
    evidence = evidence_status(row)
    source = str(row.get("source") or "").strip().lower()
    if evidence == "failed" or status == "fetch_failed":
        return 0
    if status == "pending_metric_fetch":
        return 1
    if status == "seed_metric" or "seed" in source or evidence == "seeded":
        return 2
    if "tracker" in source:
        return 4
    if status == "observed" or evidence == "observed":
        return 5
    if metrics_view_count(row) > 0 or metrics_engagement_count(row) > 0:
        return 4
    return 3


def metric_completeness(row: dict[str, Any]) -> int:
    metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
    return sum(
        1
        for key in ("views", "likes", "retweets", "replies", "quotes", "bookmarks")
        if metrics.get(key) not in (None, "")
    )


def fetched_at_key(row: dict[str, Any]) -> datetime:
    raw = str(row.get("fetched_at") or row.get("observed_at") or "").strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return datetime.min


def should_replace_observation(existing: dict[str, Any] | None, candidate: dict[str, Any]) -> bool:
    """Prefer reliable evidence, then freshness/completeness, then richer metrics."""
    if not existing:
        return True

    cand_rank = evidence_rank(candidate)
    cur_rank = evidence_rank(existing)
    if cand_rank != cur_rank:
        return cand_rank > cur_rank

    cand_fetched = fetched_at_key(candidate)
    cur_fetched = fetched_at_key(existing)
    if cand_fetched != cur_fetched:
        return cand_fetched > cur_fetched

    cand_complete = metric_completeness(candidate)
    cur_complete = metric_completeness(existing)
    if cand_complete != cur_complete:
        return cand_complete > cur_complete

    cand_views = metrics_view_count(candidate)
    cur_views = metrics_view_count(existing)
    if cand_views != cur_views:
        return cand_views > cur_views

    cand_aff = safe_float(candidate.get("campaign_affinity"))
    cur_aff = safe_float(existing.get("campaign_affinity"))
    if cand_aff != cur_aff:
        return cand_aff > cur_aff

    cand_eng = metrics_engagement_count(candidate)
    cur_eng = metrics_engagement_count(existing)
    if cand_eng != cur_eng:
        return cand_eng > cur_eng

    cand_reason_count = len(candidate.get("affinity_reason") or [])
    cur_reason_count = len(existing.get("affinity_reason") or [])
    if cand_reason_count != cur_reason_count:
        return cand_reason_count > cur_reason_count
    return False
