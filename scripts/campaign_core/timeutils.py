"""Timestamp parsing helpers shared by campaign attribution scripts."""

from __future__ import annotations

from datetime import datetime, timezone


def parse_twitter_created_at(value: object) -> datetime | None:
    """Parse Twitter created_at format and return a UTC-aware datetime."""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%a %b %d %H:%M:%S %z %Y").astimezone(timezone.utc)
    except ValueError:
        return parse_iso_utc(text)


def parse_iso_utc(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_iso_utc_required(value: object, label: str = "timestamp") -> datetime:
    dt = parse_iso_utc(value)
    if not dt:
        raise ValueError(f"{label} must be an ISO UTC timestamp")
    return dt


def hour_bucket(dt: datetime) -> str:
    utc = dt.astimezone(timezone.utc)
    return utc.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:00:00Z")
