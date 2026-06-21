#!/usr/bin/env python3
"""Validate campaign funnel aggregates against a customer benchmark.

The benchmark is a validation oracle, not a production data source. Use it to
check that raw referral/pixel imports aggregate back to the customer-reported
numbers for clicks, registrations, activations, and paid conversions.
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from pathlib import Path
from typing import Any

from campaign_core.io import load_json_object


DEFAULT_METRICS = ("clicks", "registrations", "activations", "paid_conversions")
DEFAULT_KEYS = ("handle", "name", "display_name", "participant", "referral_code")


def parse_number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    multiplier = 1.0
    if text[-1:].lower() == "k":
        multiplier = 1000.0
        text = text[:-1]
    text = text.strip().lstrip("$")
    if text.endswith("%"):
        text = text[:-1]
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def load_actual(path: Path) -> dict[str, Any]:
    if path.suffix.lower() == ".csv":
        with path.open(newline="", encoding="utf-8-sig") as fh:
            rows = list(csv.DictReader(fh))
        return {"rows": rows}
    return load_json_object(path)


def get_section(data: dict[str, Any], section: str) -> dict[str, Any]:
    sections = data.get("sections")
    if isinstance(sections, dict) and section in sections:
        obj = sections[section]
    else:
        obj = data
    if not isinstance(obj, dict):
        raise ValueError(f"section {section!r} must be an object")
    obj.setdefault("rows", [])
    obj.setdefault("totals", {})
    return obj


def row_key(row: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return str(value).strip().lower().lstrip("@")
    return ""


def index_rows(rows: list[dict[str, Any]], keys: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = row_key(row, keys)
        if key:
            indexed[key] = row
    return indexed


def close_enough(expected: float, actual: float, abs_tol: float, rel_tol: float) -> bool:
    return math.isclose(actual, expected, abs_tol=abs_tol, rel_tol=rel_tol)


def compare_value(label: str, expected: Any, actual: Any, abs_tol: float, rel_tol: float) -> tuple[bool, str]:
    exp = parse_number(expected)
    act = parse_number(actual)
    if exp is None:
        return True, f"[skip] {label}: benchmark value is non-numeric"
    if act is None:
        return False, f"[missing] {label}: expected {expected}, actual missing/non-numeric"
    ok = close_enough(exp, act, abs_tol, rel_tol)
    status = "ok" if ok else "mismatch"
    return ok, f"[{status}] {label}: expected {exp:g}, actual {act:g}, delta {act - exp:g}"


def compare_sections(
    benchmark: dict[str, Any],
    actual: dict[str, Any],
    *,
    metrics: tuple[str, ...],
    keys: tuple[str, ...],
    abs_tol: float,
    rel_tol: float,
    allow_missing: bool,
) -> int:
    failures = 0

    bench_totals = benchmark.get("totals") or {}
    actual_totals = actual.get("totals") or {}
    actual_rows = actual.get("rows") or []
    actual_index = index_rows(actual_rows, keys)

    computed_totals: dict[str, float] = {}
    for metric in metrics:
        total = 0.0
        seen = False
        for row in actual_rows:
            if not isinstance(row, dict):
                continue
            value = parse_number(row.get(metric))
            if value is None:
                continue
            total += value
            seen = True
        if seen:
            computed_totals[metric] = total

    print("Totals")
    for metric in metrics:
        if metric not in bench_totals:
            continue
        actual_value = actual_totals.get(metric, computed_totals.get(metric))
        ok, line = compare_value(f"totals.{metric}", bench_totals.get(metric), actual_value, abs_tol, rel_tol)
        print(line)
        if not ok:
            failures += 1

    print("\nRows")
    for bench_row in benchmark.get("rows") or []:
        if not isinstance(bench_row, dict):
            continue
        key = row_key(bench_row, keys)
        if not key:
            print(f"[skip] row without key: {bench_row}")
            continue
        actual_row = actual_index.get(key)
        if actual_row is None:
            line = f"[missing] row.{key}: no actual row"
            print(line)
            if not allow_missing:
                failures += 1
            continue
        for metric in metrics:
            if metric not in bench_row:
                continue
            ok, line = compare_value(f"row.{key}.{metric}", bench_row.get(metric), actual_row.get(metric), abs_tol, rel_tol)
            print(line)
            if not ok:
                failures += 1

    return failures


def internal_check(section: dict[str, Any], metrics: tuple[str, ...]) -> int:
    failures = 0
    totals = section.get("totals") or {}
    rows = section.get("rows") or []
    print("Benchmark Internal Check")
    for metric in metrics:
        if metric not in totals:
            continue
        row_sum = sum(parse_number(row.get(metric)) or 0.0 for row in rows if isinstance(row, dict))
        ok, line = compare_value(f"row_sum.{metric}", totals.get(metric), row_sum, 0.0, 0.0)
        print(line)
        if not ok:
            failures += 1
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark", type=Path, required=True)
    parser.add_argument("--actual", type=Path, default=None,
                        help="Aggregated raw referral/pixel output as JSON or CSV. If omitted, only benchmark internals are checked.")
    parser.add_argument("--section", default="kol_direct")
    parser.add_argument("--metrics", default=",".join(DEFAULT_METRICS),
                        help="Comma-separated metrics to compare.")
    parser.add_argument("--keys", default=",".join(DEFAULT_KEYS),
                        help="Comma-separated row key fields, in priority order.")
    parser.add_argument("--abs-tol", type=float, default=0.0)
    parser.add_argument("--rel-tol", type=float, default=0.0)
    parser.add_argument("--allow-missing", action="store_true")
    args = parser.parse_args()

    metrics = tuple(item.strip() for item in args.metrics.split(",") if item.strip())
    keys = tuple(item.strip() for item in args.keys.split(",") if item.strip())
    benchmark = get_section(load_json_object(args.benchmark), args.section)

    if args.actual is None:
        failures = internal_check(benchmark, metrics)
    else:
        actual = get_section(load_actual(args.actual), args.section)
        failures = compare_sections(
            benchmark,
            actual,
            metrics=metrics,
            keys=keys,
            abs_tol=args.abs_tol,
            rel_tol=args.rel_tol,
            allow_missing=args.allow_missing,
        )

    if failures:
        print(f"\nFAIL: {failures} validation issue(s)", file=sys.stderr)
        return 1
    print("\nPASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
