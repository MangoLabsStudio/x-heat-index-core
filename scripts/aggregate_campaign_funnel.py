#!/usr/bin/env python3
"""Aggregate raw referral/pixel funnel events for a campaign.

Input can be CSV, JSON, or JSONL. The script accepts three common shapes:

1. Event rows: one row per event with event_type/event_name.
2. Timeline rows: one row per user/referral with click_ts/register_ts/etc.
3. Aggregate rows: one row per KOL/KOC with clicks/registrations/etc.

Output matches scripts/validate_campaign_funnel.py.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


METRICS = ("clicks", "registrations", "activations", "paid_conversions")
REVENUE_METRICS = ("revenue_usd",)

KEY_FIELDS = (
    "handle",
    "twitter_handle",
    "x_handle",
    "kol_handle",
    "koc_handle",
    "name",
    "display_name",
    "participant",
    "referral_code",
    "ref_code",
    "code",
    "utm_content",
    "utm_campaign",
)

MAPPING_LOOKUP_FIELDS = (
    "referral_code",
    "ref_code",
    "code",
    "utm_content",
    "utm_campaign",
    "handle",
    "twitter_handle",
    "x_handle",
    "kol_handle",
    "koc_handle",
    "name",
)

EVENT_TYPE_FIELDS = ("event_type", "event", "event_name", "type", "activity", "conversion_type")

EVENT_ALIASES = {
    "clicks": {
        "click",
        "clicked",
        "link_click",
        "referral_click",
        "page_view",
        "landing_page_view",
        "visit",
    },
    "registrations": {
        "register",
        "registered",
        "registration",
        "signup",
        "sign_up",
        "user_signup",
        "new_user",
    },
    "activations": {
        "activate",
        "activated",
        "activation",
        "first_use",
        "used",
        "product_used",
        "workspace_created",
        "agent_created",
    },
    "paid_conversions": {
        "paid",
        "payment",
        "purchase",
        "purchased",
        "subscribe",
        "subscribed",
        "subscription",
        "paid_conversion",
    },
}

COUNT_FIELDS = {
    "clicks": ("clicks", "click", "click_count", "total_clicks"),
    "registrations": ("registrations", "registration", "registers", "reg", "regs", "signups", "signup_count"),
    "activations": ("activations", "activation", "activated", "active_users", "used", "usage_count"),
    "paid_conversions": ("paid_conversions", "paid", "payments", "purchases", "payment_count"),
    "revenue_usd": ("revenue_usd", "revenue", "amount_usd", "payment_usd", "paid_amount"),
}

TIMESTAMP_FIELDS = {
    "clicks": ("click_ts", "click_at", "clicked_at", "first_click_at", "visit_ts", "visited_at"),
    "registrations": ("register_ts", "registered_at", "registration_ts", "signup_ts", "signed_up_at"),
    "activations": ("activation_ts", "activated_at", "first_use_at", "used_at", "agent_created_at"),
    "paid_conversions": ("paid_ts", "payment_ts", "paid_at", "purchase_ts", "purchased_at", "subscribed_at"),
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_field(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name or "").strip().lower()).strip("_")


def normalize_value(value: Any) -> str:
    return str(value or "").strip()


def normalize_key_value(value: Any) -> str:
    return normalize_value(value).lower().lstrip("@")


def normalize_event_value(value: Any) -> str:
    return normalize_field(str(value or ""))


def parse_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    text = text.lstrip("$")
    multiplier = 1.0
    if text[-1:].lower() == "k":
        multiplier = 1000.0
        text = text[:-1]
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def is_truthy(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text not in ("", "0", "false", "no", "none", "null", "nan")


def normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {normalize_field(key): value for key, value in row.items()}


def load_json_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        for key in ("events", "rows", "data"):
            if isinstance(data.get(key), list):
                rows = data[key]
                break
        else:
            sections = data.get("sections")
            if isinstance(sections, dict):
                collected: list[dict[str, Any]] = []
                for section in sections.values():
                    if isinstance(section, dict) and isinstance(section.get("rows"), list):
                        collected.extend(section["rows"])
                rows = collected
            else:
                rows = [data]
    else:
        raise ValueError(f"{path} must contain a JSON object or array")
    return [normalize_row(row) for row in rows if isinstance(row, dict)]


def load_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if isinstance(obj, dict):
                rows.append(normalize_row(obj))
    return rows


def load_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8-sig") as fh:
        return [normalize_row(row) for row in csv.DictReader(fh)]


def load_rows(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return load_csv_rows(path)
    if suffix in (".jsonl", ".ndjson"):
        return load_jsonl_rows(path)
    if suffix == ".json":
        return load_json_rows(path)
    raise ValueError(f"Unsupported input format: {path}")


def load_mapping(path: Path | None) -> dict[str, dict[str, Any]]:
    if not path:
        return {}
    keyed_rows: list[dict[str, Any]] = []
    if path.suffix.lower() == ".json":
        with path.open(encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict) and not any(key in data for key in ("events", "rows", "data", "sections")):
            for key, value in data.items():
                if isinstance(value, dict):
                    keyed_rows.append({"code": key, **normalize_row(value)})
    rows = keyed_rows or load_rows(path)
    mapping: dict[str, dict[str, Any]] = {}
    for row in rows:
        identity = {
            "handle": normalize_key_value(row.get("handle") or row.get("twitter_handle") or row.get("x_handle")),
            "name": normalize_value(row.get("name") or row.get("display_name") or row.get("participant")),
            "referral_code": normalize_value(row.get("referral_code") or row.get("ref_code") or row.get("code")),
        }
        identity = {key: value for key, value in identity.items() if value}
        if not identity:
            continue
        for field in MAPPING_LOOKUP_FIELDS:
            value = normalize_key_value(row.get(field))
            if value:
                mapping[value] = identity
    return mapping


def resolve_identity(row: dict[str, Any], mapping: dict[str, dict[str, Any]]) -> tuple[str, dict[str, Any]]:
    for field in MAPPING_LOOKUP_FIELDS:
        value = normalize_key_value(row.get(field))
        if value and value in mapping:
            identity = dict(mapping[value])
            if row.get("referral_code") and not identity.get("referral_code"):
                identity["referral_code"] = normalize_value(row.get("referral_code"))
            key = normalize_key_value(identity.get("handle") or identity.get("name") or identity.get("referral_code"))
            return key, identity

    for field in KEY_FIELDS:
        value = normalize_value(row.get(field))
        if not value:
            continue
        if "handle" in field:
            value = value.lstrip("@")
        identity = {field: value}
        if "handle" in field:
            identity = {"handle": normalize_key_value(value)}
        elif "ref" in field or field in ("code", "utm_content", "utm_campaign"):
            identity = {"referral_code": value}
        else:
            identity = {"name": value}
        return normalize_key_value(value), identity
    return "", {}


def row_mode(row: dict[str, Any], forced_mode: str) -> str:
    if forced_mode != "auto":
        return forced_mode
    if any(any(field in row for field in COUNT_FIELDS[metric]) for metric in METRICS):
        return "aggregate"
    if any(field in row for field in EVENT_TYPE_FIELDS):
        return "event"
    return "timeline"


def metric_from_event(row: dict[str, Any]) -> str:
    for field in EVENT_TYPE_FIELDS:
        event_type = normalize_event_value(row.get(field))
        if not event_type:
            continue
        for metric, aliases in EVENT_ALIASES.items():
            if event_type in aliases:
                return metric
    return ""


def counts_from_row(row: dict[str, Any], mode: str) -> dict[str, float]:
    counts = {metric: 0.0 for metric in METRICS + REVENUE_METRICS}
    if mode == "aggregate":
        for metric, fields in COUNT_FIELDS.items():
            for field in fields:
                value = parse_number(row.get(field))
                if value is not None:
                    counts[metric] += value
                    break
        return counts

    if mode == "event":
        metric = metric_from_event(row)
        if metric:
            counts[metric] = 1.0
        for field in COUNT_FIELDS["revenue_usd"]:
            value = parse_number(row.get(field))
            if value is not None:
                counts["revenue_usd"] += value
                break
        return counts

    for metric, fields in TIMESTAMP_FIELDS.items():
        if any(is_truthy(row.get(field)) for field in fields):
            counts[metric] = 1.0
    for field in COUNT_FIELDS["revenue_usd"]:
        value = parse_number(row.get(field))
        if value is not None:
            counts["revenue_usd"] += value
            break
    return counts


def aggregate_rows(
    rows: list[dict[str, Any]],
    *,
    mapping: dict[str, dict[str, Any]],
    mode: str,
    dedupe_by: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    stats = {"input_rows": len(rows), "matched_rows": 0, "unmatched_rows": 0, "deduped_events": 0}
    by_key: dict[str, dict[str, Any]] = {}
    seen: set[tuple[str, str, str]] = set()

    for row in rows:
        key, identity = resolve_identity(row, mapping)
        if not key:
            stats["unmatched_rows"] += 1
            continue
        stats["matched_rows"] += 1
        target = by_key.setdefault(key, {**identity, **{metric: 0.0 for metric in METRICS + REVENUE_METRICS}})
        target.update({field: value for field, value in identity.items() if value})

        counts = counts_from_row(row, row_mode(row, mode))
        for metric, count in counts.items():
            if not count:
                continue
            if dedupe_by and metric in METRICS:
                dedupe_value = normalize_key_value(row.get(dedupe_by))
                if dedupe_value:
                    dedupe_key = (key, metric, dedupe_value)
                    if dedupe_key in seen:
                        stats["deduped_events"] += 1
                        continue
                    seen.add(dedupe_key)
            target[metric] += count

    rows_out = []
    for row in by_key.values():
        clean = dict(row)
        for metric in METRICS:
            value = clean.get(metric, 0)
            clean[metric] = int(value) if float(value).is_integer() else value
        if not clean.get("revenue_usd"):
            clean.pop("revenue_usd", None)
        rows_out.append(clean)

    rows_out.sort(key=lambda row: (
        -float(row.get("registrations", 0) or 0),
        -float(row.get("clicks", 0) or 0),
        str(row.get("handle") or row.get("name") or row.get("referral_code") or ""),
    ))
    return rows_out, stats


def build_output(args: argparse.Namespace, rows: list[dict[str, Any]], stats: dict[str, Any]) -> dict[str, Any]:
    totals: dict[str, Any] = {}
    for metric in METRICS:
        totals[metric] = sum(int(row.get(metric, 0) or 0) for row in rows)
    revenue = sum(float(row.get("revenue_usd", 0) or 0) for row in rows)
    if revenue:
        totals["revenue_usd"] = round(revenue, 2)

    section = {
        "description": args.description or "",
        "totals": totals,
        "rows": rows,
        "stats": stats,
    }
    return {
        "version": 1,
        "campaign_id": args.campaign_id,
        "generated_at": now_iso(),
        "source_files": [str(path) for path in args.input],
        "sections": {args.section: section},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, action="append", required=True,
                        help="CSV, JSON, or JSONL raw referral/pixel export. Repeatable.")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mapping", type=Path, default=None,
                        help="Optional CSV/JSON/JSONL identity map for referral_code -> handle/name.")
    parser.add_argument("--campaign-id", default="")
    parser.add_argument("--section", default="kol_direct")
    parser.add_argument("--description", default="")
    parser.add_argument("--mode", choices=("auto", "event", "timeline", "aggregate"), default="auto")
    parser.add_argument("--dedupe-by", default="",
                        help="Optional field used to count at most once per metric per identity, e.g. user_id.")
    args = parser.parse_args()

    mapping = load_mapping(args.mapping)
    rows: list[dict[str, Any]] = []
    for path in args.input:
        rows.extend(load_rows(path))

    dedupe_by = normalize_field(args.dedupe_by)
    rows_out, stats = aggregate_rows(rows, mapping=mapping, mode=args.mode, dedupe_by=dedupe_by)
    output = build_output(args, rows_out, stats)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as fh:
        json.dump(output, fh, ensure_ascii=False, indent=2)
        fh.write("\n")

    section = output["sections"][args.section]
    print(f"[funnel] Read {stats['input_rows']} rows from {len(args.input)} file(s)", file=sys.stderr)
    print(f"[funnel] Matched rows: {stats['matched_rows']}  unmatched: {stats['unmatched_rows']}", file=sys.stderr)
    if stats["deduped_events"]:
        print(f"[funnel] Deduped events: {stats['deduped_events']}", file=sys.stderr)
    print(f"[funnel] Totals: {section['totals']}", file=sys.stderr)
    print(f"[funnel] Wrote {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
