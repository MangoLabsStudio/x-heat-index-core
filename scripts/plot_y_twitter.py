#!/usr/bin/env python3
"""
ASCII plot of a campaign's Y_twitter(t) time series.

Usage:
  python3 plot_y_twitter.py <campaign_id>               # reads Y_twitter.jsonl
  python3 plot_y_twitter.py <campaign_id> --baseline    # show pre-campaign baseline instead
  python3 plot_y_twitter.py --y-path /path/to/Y.jsonl   # any jsonl
  python3 plot_y_twitter.py <campaign_id> --range 24h   # only last 24h
  python3 plot_y_twitter.py <campaign_id> --format daily|hourly|both

Output: two ASCII charts (daily total bars + hourly log-scale sparklines per day) +
summary stats (mean/median/p75/p90/p95/max).

Stdlib only. Prints to stdout — pipe-friendly.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import statistics
import subprocess
import sys
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from pathlib import Path


LEGEND_HOURLY = (
    "  0=.    1-9=▁    10-99=▂    100-999=▃    1K-9.9K=▄"
    "    10K-99K=▅    100K-999K=▆    1M+=▇"
)


def scale_char(v: float) -> str:
    if v <= 0: return "."
    if v < 10: return "▁"
    if v < 100: return "▂"
    if v < 1_000: return "▃"
    if v < 10_000: return "▄"
    if v < 100_000: return "▅"
    if v < 1_000_000: return "▆"
    return "▇"


def resolve_y_path(args) -> Path:
    """Figure out which Y_twitter.jsonl to read."""
    if args.y_path:
        return Path(args.y_path)
    if not args.campaign_id:
        sys.exit("ERROR: provide <campaign_id> or --y-path")
    base = Path(args.data_dir) / "campaign_graphs" / args.campaign_id
    if args.baseline:
        # Generate on-the-fly from nodes.jsonl with until=campaign_start_at
        nodes = base / "nodes.jsonl"
        cfg = base / "config.json"
        if not nodes.exists():
            sys.exit(f"ERROR: {nodes} does not exist")
        if not cfg.exists():
            sys.exit(f"ERROR: {cfg} does not exist (needed for campaign_start_at)")
        with cfg.open() as f:
            c = json.load(f)
        start_at = c.get("campaign_start_at")
        if not start_at:
            sys.exit("ERROR: config.json missing campaign_start_at")
        # Spawn aggregator in subprocess for --baseline mode
        import subprocess, tempfile
        tmp = Path(tempfile.mkstemp(suffix="_baseline.jsonl")[1])
        agg = Path(__file__).parent / "aggregate_hourly_attention.py"
        subprocess.run([
            "python3", str(agg),
            "--nodes-path", str(nodes),
            "--output", str(tmp),
            "--since", "2020-01-01T00:00:00Z",
            "--until", start_at,
            "--quiet",
        ], check=True)
        return tmp
    return base / "Y_twitter.jsonl"


def load_rows(path: Path) -> list[dict]:
    if not path.exists():
        sys.exit(f"ERROR: {path} does not exist")
    rows = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def filter_range(rows: list[dict], range_spec: str) -> list[dict]:
    if range_spec == "all" or not rows:
        return rows
    last = datetime.fromisoformat(rows[-1]["hour_utc"].replace("Z", "+00:00"))
    if range_spec.endswith("h"):
        delta = timedelta(hours=int(range_spec[:-1]))
    elif range_spec.endswith("d"):
        delta = timedelta(days=int(range_spec[:-1]))
    else:
        sys.exit(f"ERROR: invalid --range {range_spec!r} (use e.g. 24h / 7d / all)")
    cutoff = last - delta
    return [r for r in rows
            if datetime.fromisoformat(r["hour_utc"].replace("Z", "+00:00")) >= cutoff]


def densify_hourly(rows: list[dict]) -> list[tuple[str, float]]:
    """Fill zero-hour gaps so chart is continuous."""
    if not rows:
        return []
    start = datetime.fromisoformat(rows[0]["hour_utc"].replace("Z", "+00:00"))
    end = datetime.fromisoformat(rows[-1]["hour_utc"].replace("Z", "+00:00"))
    m = {r["hour_utc"]: r["attention_mass"] for r in rows}
    out = []
    cur = start
    while cur <= end:
        key = cur.strftime("%Y-%m-%dT%H:00:00Z")
        out.append((key, m.get(key, 0.0)))
        cur += timedelta(hours=1)
    return out


def print_hourly_chart(hours: list[tuple[str, float]]) -> None:
    print("HOURLY (log10 scale, 1 char = 1 hour UTC)")
    print("Legend:" + LEGEND_HOURLY)
    print()

    # Chunk by day
    by_day: OrderedDict[str, list[tuple[str, float]]] = OrderedDict()
    for h, v in hours:
        d = h[:10]
        by_day.setdefault(d, []).append((h, v))

    for day, lst in by_day.items():
        bar = "".join(scale_char(v) for _, v in lst)
        daily = sum(v for _, v in lst)
        nz = sum(1 for _, v in lst if v > 0)
        print(f"  {day}  [{bar:<24}]  nz={nz:>2}  total={daily:>11,.0f}")


def print_daily_chart(hours: list[tuple[str, float]]) -> None:
    print("DAILY TOTAL (log10 scale bar)")
    by_day: OrderedDict[str, float] = OrderedDict()
    for h, v in hours:
        d = h[:10]
        by_day[d] = by_day.get(d, 0.0) + v
    if not by_day:
        return
    max_log = max((math.log10(v + 1) for v in by_day.values()), default=1)
    for day, total in by_day.items():
        if total <= 0:
            bar = ""
        else:
            width = int(math.log10(total + 1) / max_log * 50)
            bar = "█" * max(1, width)
        print(f"  {day}  {total:>11,.0f}  {bar}")


def print_summary(rows: list[dict]) -> None:
    if not rows:
        print("(empty dataset)")
        return
    masses = [r["attention_mass"] for r in rows if r["attention_mass"] > 0]
    if not masses:
        print("(all zero hours)")
        return
    print("SUMMARY (non-zero hours only)")
    print(f"  buckets (nonzero): {len(masses)}")
    print(f"  total mass:     {sum(masses):>12,.0f}")
    print(f"  mean:           {statistics.mean(masses):>12,.0f}")
    print(f"  median (p50):   {statistics.median(masses):>12,.0f}")
    if len(masses) >= 20:
        qs = statistics.quantiles(masses, n=20)
        print(f"  p75:            {qs[14]:>12,.0f}")
        print(f"  p90:            {qs[17]:>12,.0f}")
        print(f"  p95:            {qs[18]:>12,.0f}")
    print(f"  max:            {max(masses):>12,.0f}")


def human_age(ts: datetime | None) -> str:
    """Render a datetime as 'N sec/min/hour ago' relative to now (UTC)."""
    if not ts:
        return "—"
    now = datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    delta = now - ts
    secs = int(delta.total_seconds())
    if secs < 60: return f"{secs}s ago"
    if secs < 3600: return f"{secs // 60}m ago"
    if secs < 86400: return f"{secs // 3600}h {(secs % 3600) // 60}m ago"
    return f"{secs // 86400}d ago"


def collector_running(campaign_id: str) -> tuple[bool, str]:
    """Check if a campaign_collect.py process is running for this campaign.
    Returns (is_running, info_string). info contains PID + elapsed time."""
    if not campaign_id:
        return False, ""
    try:
        # Match python3 running campaign_collect.py (NOT bash wait-loops that also
        # reference the script name in their command line)
        result = subprocess.run(
            ["pgrep", "-af", f"python.*campaign_collect\\.py.*{re.escape(campaign_id)}"],
            capture_output=True, text=True, timeout=5,
        )
        lines = [ln for ln in result.stdout.strip().splitlines() if "python" in ln.lower()]
        if result.returncode != 0 or not lines:
            return False, ""
        first = lines[0]
        pid = first.split()[0]
        # etime via /proc
        try:
            etimes = subprocess.run(
                ["ps", "-o", "etime=", "-p", pid],
                capture_output=True, text=True, timeout=3,
            )
            elapsed = etimes.stdout.strip()
        except Exception:
            elapsed = "?"
        return True, f"PID {pid}, elapsed {elapsed}"
    except Exception:
        return False, ""


def print_health(args, y_path: Path) -> None:
    """Show freshness + collector status above the chart."""
    cid = args.campaign_id
    data_dir = Path(args.data_dir)

    # Collector running?
    running, info = collector_running(cid) if cid else (False, "")

    # Y file mtime
    y_mtime = None
    if y_path.exists():
        y_mtime = datetime.fromtimestamp(y_path.stat().st_mtime, tz=timezone.utc)

    # nodes.jsonl size + mtime
    nodes_info = None
    if cid:
        nodes_path = data_dir / "campaign_graphs" / cid / "nodes.jsonl"
        if nodes_path.exists():
            # Count lines without loading (fast)
            try:
                with nodes_path.open("rb") as fh:
                    count = sum(1 for _ in fh)
            except Exception:
                count = "?"
            n_mtime = datetime.fromtimestamp(nodes_path.stat().st_mtime, tz=timezone.utc)
            nodes_info = (count, n_mtime)

    # Last collector completion from state
    collector_last = None
    if cid:
        state_path = data_dir / "campaign_graphs" / cid / "collector_state.json"
        if state_path.exists():
            try:
                with state_path.open() as fh:
                    s = json.load(fh)
                ca = s.get("completed_at")
                if ca:
                    collector_last = datetime.fromisoformat(ca.replace("Z", "+00:00"))
            except Exception:
                pass

    # Next timer ETA (best-effort, may fail if no sudo)
    next_run = None
    if cid:
        try:
            out = subprocess.run(
                ["systemctl", "list-timers", f"campaign-collect@{cid}.timer", "--no-pager"],
                capture_output=True, text=True, timeout=3,
            ).stdout
            # First timer row: e.g., "Tue 2026-04-21 20:06:03 CST ..."
            for line in out.splitlines():
                m = re.search(r"(\w{3}\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+\w+)", line)
                if m and "timers" not in line.lower():
                    next_run = m.group(1)
                    break
        except Exception:
            pass

    print(f"   collector:          {'⏳ RUNNING (' + info + ')' if running else '○ idle'}")
    if collector_last:
        print(f"   last complete run:  {collector_last.isoformat()}  ({human_age(collector_last)})")
    if nodes_info:
        print(f"   nodes.jsonl:        {nodes_info[0]} rows, mtime {human_age(nodes_info[1])}")
    if y_mtime:
        print(f"   Y_twitter.jsonl:    mtime {human_age(y_mtime)}")
    if next_run:
        print(f"   next auto collect:  {next_run}")
    if running:
        print(f"   ⚠️  data below may be stale — collector is writing new nodes.jsonl NOW.")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("campaign_id", nargs="?", help="Campaign ID (reads campaign_graphs/<id>/Y_twitter.jsonl)")
    p.add_argument("--data-dir", default="/opt/tweet-tracker/data",
                   help="DATA_DIR root (default: /opt/tweet-tracker/data)")
    p.add_argument("--y-path", default=None, help="Direct path to Y_*.jsonl file (overrides campaign_id)")
    p.add_argument("--baseline", action="store_true",
                   help="Show pre-campaign baseline (up to campaign_start_at) instead of active Y")
    p.add_argument("--range", default="all", help="Time range: all | 24h | 7d | 14d")
    p.add_argument("--format", default="both", choices=["daily", "hourly", "both"],
                   help="Chart type (default: both)")
    p.add_argument("--no-health", action="store_true",
                   help="Skip health header (collector status / freshness)")
    args = p.parse_args()

    y_path = resolve_y_path(args)
    rows = load_rows(y_path)
    rows = filter_range(rows, args.range)

    label = "BASELINE (pre-campaign)" if args.baseline else "Y_TWITTER"
    print(f"== {label}: {args.campaign_id or args.y_path} ==")
    print(f"   source: {y_path}")
    print(f"   range:  {args.range}")
    if rows:
        print(f"   span:   {rows[0]['hour_utc']} → {rows[-1]['hour_utc']}   ({len(rows)} nonzero hours)")
    if not args.no_health and not args.baseline:
        print_health(args, y_path)
    print()

    if rows:
        hours = densify_hourly(rows)
        if args.format in ("hourly", "both"):
            print_hourly_chart(hours)
            print()
        if args.format in ("daily", "both"):
            print_daily_chart(hours)
            print()
    print_summary(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
