"""Campaign config normalization and validation."""

from __future__ import annotations

from typing import Any, Iterable

from .identity import normalize_handle, unique_strings
from .timeutils import parse_iso_utc


def list_config_strings(raw: dict[str, Any], *keys: str) -> list[str]:
    values: list[str] = []
    for key in keys:
        item = raw.get(key)
        if isinstance(item, str):
            values.append(item)
        elif isinstance(item, list):
            values.extend(str(v) for v in item)
    return unique_strings(values)


def normalized_handles(values: Iterable[Any]) -> list[str]:
    return [h for h in unique_strings(normalize_handle(v) for v in values) if h]


def campaign_identity(config: dict[str, Any]) -> dict[str, Any]:
    return config.get("identity") if isinstance(config.get("identity"), dict) else {}


def campaign_terms(config: dict[str, Any]) -> list[str]:
    identity = campaign_identity(config)
    return unique_strings([
        *list_config_strings(config, "terms", "keywords", "identity_terms"),
        *list_config_strings(identity, "names", "aliases", "hashtags", "urls", "tickers"),
    ])


def campaign_watch_handles(config: dict[str, Any]) -> list[str]:
    identity = campaign_identity(config)
    return normalized_handles([
        *list_config_strings(config, "watch_handles", "kol_handles"),
        *list_config_strings(identity, "watch_handles", "kol_handles"),
    ])


def campaign_official_handles(config: dict[str, Any]) -> list[str]:
    identity = campaign_identity(config)
    return normalized_handles([
        *list_config_strings(config, "official_handles"),
        *list_config_strings(identity, "official_handles"),
    ])


def validate_campaign_config(config: dict[str, Any], campaign_id: str = "") -> list[str]:
    errors: list[str] = []

    if campaign_id and config.get("campaign_id") != campaign_id:
        errors.append(f"campaign_id mismatch: config={config.get('campaign_id')!r} arg={campaign_id}")
    if not config.get("campaign_id"):
        errors.append("campaign_id is required")

    identity = campaign_identity(config)
    if not identity:
        errors.append("missing 'identity' block")

    terms = campaign_terms(config)
    official_handles = campaign_official_handles(config)
    watch_handles = campaign_watch_handles(config)
    if not terms and not official_handles and not watch_handles:
        errors.append("identity terms or handles are required")

    start_raw = str(config.get("campaign_start_at") or "")
    start = parse_iso_utc(start_raw)
    if not start:
        errors.append("campaign_start_at is required (ISO UTC, e.g. 2026-04-21T10:00:00Z)")

    end_raw = str(config.get("campaign_end_at") or "")
    end = parse_iso_utc(end_raw) if end_raw else None
    if end_raw and not end:
        errors.append("campaign_end_at must be ISO UTC when present")
    if start and end and end < start:
        errors.append("campaign_end_at must be after campaign_start_at")

    overlap = set(watch_handles) & set(official_handles)
    if overlap:
        errors.append(f"handles cannot be both watch and official: {', '.join(sorted(overlap))}")

    return errors
