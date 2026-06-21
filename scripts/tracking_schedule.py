"""Shared tracking schedule helpers for tweet-level collectors."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping


DEFAULT_TRACKER_SCHEDULE = "1h:300s:5,6h:900s:5,24h:1800s:3,72h:3600s:2"
DEFAULT_WALKER_SCHEDULE = "6h:900s,24h:1800s,72h:3600s"
DEFAULT_TRACKING_RETENTION = "72h"

_DURATION_RE = re.compile(r"^\s*(\d+)\s*([smhd]?)\s*$", re.IGNORECASE)
_DURATION_MULTIPLIERS = {
    "": 1,
    "s": 1,
    "m": 60,
    "h": 60 * 60,
    "d": 24 * 60 * 60,
}


@dataclass(frozen=True)
class SchedulePhase:
    """One cumulative tracking phase."""

    until_seconds: int
    interval_seconds: int
    max_pages: int | None = None

    @property
    def label(self) -> str:
        parts = [
            f"until={format_duration(self.until_seconds)}",
            f"interval={format_duration(self.interval_seconds)}",
        ]
        if self.max_pages is not None:
            parts.append(f"max_pages={self.max_pages}")
        return " ".join(parts)


@dataclass(frozen=True)
class TrackingPolicy:
    """Cumulative schedule plus an optional hard retention window."""

    name: str
    phases: tuple[SchedulePhase, ...]
    stop_after_seconds: int | None

    def phase_for_age(self, age_seconds: float) -> SchedulePhase | None:
        if self.stop_after_seconds is not None and age_seconds >= self.stop_after_seconds:
            return None
        for phase in self.phases:
            if age_seconds < phase.until_seconds:
                return phase
        return self.phases[-1] if self.phases and self.stop_after_seconds is None else None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def age_seconds(started_at: str | None, now: datetime | None = None) -> float:
    started = parse_iso_datetime(started_at)
    if started is None:
        return 0.0
    current = now or now_utc()
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return max(0.0, (current.astimezone(timezone.utc) - started).total_seconds())


def parse_duration(value: str | int | None, *, default: int | None = None) -> int:
    if value is None or value == "":
        if default is None:
            raise ValueError("missing duration")
        return default
    if isinstance(value, int):
        return value
    match = _DURATION_RE.match(str(value))
    if not match:
        raise ValueError(f"invalid duration: {value!r}")
    amount = int(match.group(1))
    suffix = match.group(2).lower()
    return amount * _DURATION_MULTIPLIERS[suffix]


def format_duration(seconds: int | float | None) -> str:
    if seconds is None:
        return "forever"
    seconds = int(seconds)
    units = (("d", 86400), ("h", 3600), ("m", 60))
    for suffix, size in units:
        if seconds >= size and seconds % size == 0:
            return f"{seconds // size}{suffix}"
    return f"{seconds}s"


def parse_schedule(spec: str, *, default_max_pages: int | None = None) -> tuple[SchedulePhase, ...]:
    phases: list[SchedulePhase] = []
    for raw_part in spec.split(","):
        part = raw_part.strip()
        if not part:
            continue
        fields = [field.strip() for field in part.split(":")]
        if len(fields) not in (2, 3):
            raise ValueError(f"invalid schedule phase: {part!r}")
        until_seconds = parse_duration(fields[0])
        interval_seconds = parse_duration(fields[1])
        max_pages = int(fields[2]) if len(fields) == 3 and fields[2] else default_max_pages
        if until_seconds <= 0 or interval_seconds <= 0:
            raise ValueError(f"schedule values must be positive: {part!r}")
        if max_pages is not None and max_pages <= 0:
            raise ValueError(f"max_pages must be positive: {part!r}")
        if phases and until_seconds <= phases[-1].until_seconds:
            raise ValueError("schedule phases must be cumulative and increasing")
        phases.append(SchedulePhase(until_seconds, interval_seconds, max_pages))
    if not phases:
        raise ValueError("schedule must contain at least one phase")
    return tuple(phases)


def load_tracker_policy(env: Mapping[str, str] | None = None) -> TrackingPolicy:
    env = env or os.environ
    default_max_pages = int(env.get("MAX_PAGES_PER_CYCLE", "5"))
    retention_value = env.get("TRACKING_RETENTION")
    retention = parse_duration(retention_value or DEFAULT_TRACKING_RETENTION)

    if "TRACKER_SCHEDULE" in env:
        phases = parse_schedule(env["TRACKER_SCHEDULE"], default_max_pages=default_max_pages)
        if retention_value is None:
            retention = phases[-1].until_seconds
    elif "SNAPSHOT_INTERVAL_SEC" in env:
        interval = parse_duration(env["SNAPSHOT_INTERVAL_SEC"])
        phases = (SchedulePhase(retention, interval, default_max_pages),)
    else:
        phases = parse_schedule(DEFAULT_TRACKER_SCHEDULE, default_max_pages=default_max_pages)
        retention = phases[-1].until_seconds

    return TrackingPolicy("tracker", phases, retention)


def load_walker_policy(env: Mapping[str, str] | None = None) -> TrackingPolicy:
    env = env or os.environ
    retention_value = env.get("TRACKING_RETENTION")
    retention = parse_duration(retention_value or DEFAULT_TRACKING_RETENTION)

    if "WALKER_SCHEDULE" in env:
        phases = parse_schedule(env["WALKER_SCHEDULE"])
        if retention_value is None:
            retention = phases[-1].until_seconds
    elif "WALKER_INTERVAL_SEC" in env:
        interval = parse_duration(env["WALKER_INTERVAL_SEC"])
        phases = (SchedulePhase(retention, interval),)
    else:
        phases = parse_schedule(DEFAULT_WALKER_SCHEDULE)
        retention = phases[-1].until_seconds

    return TrackingPolicy("walker", phases, retention)
