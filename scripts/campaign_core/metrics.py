"""Metric extraction and observation deduplication helpers."""

from __future__ import annotations

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


def should_replace_observation(existing: dict[str, Any] | None, candidate: dict[str, Any]) -> bool:
    """Prefer fresher metrics, then richer campaign attribution metadata."""
    if not existing:
        return True

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

