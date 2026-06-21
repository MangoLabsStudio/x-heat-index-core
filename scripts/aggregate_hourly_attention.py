#!/usr/bin/env python3
"""
Aggregate hourly attention from campaign nodes.jsonl → Y_twitter(t) time series.

Scope (per 决策 #22 in attribution-architecture.md §11.1):
  Only nodes causally linked to paid KOL (source tag ∈ CAUSAL_SOURCES).
  Excludes official handle tweets and independent identity-search matches —
  these are not ripples caused by paid KOL activity.

Formula (v0.5, per 决策 #23):
  Y_twitter(t) = Σ views(node) × affinity(node)
  where t is hourly bucket (UTC) by node.created_at.

Output: campaign_graphs/<id>/Y_twitter.jsonl
  One line per hour bucket with populated data:
    {
      "hour_utc":       "YYYY-MM-DDTHH:00:00Z",
      "attention_mass": float,    # Σ views × affinity
      "node_count":     int,
      "views_sum":      int,
      "avg_affinity":   float,    # mean affinity within hour
      "by_source":      {source_tag: count, ...}
    }

Stdlib only (Python 3.12+), consistent with x-heat-index repo convention.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from campaign_core.identity import has_identity_signal, node_conversation_id
from campaign_core.metrics import metrics_view_count, safe_float, should_replace_observation
from campaign_core.paid import PAID_DELIVERABLE_SEED_SOURCE, PAID_DELIVERABLE_TRACKER_SOURCE, normalize_paid_source
from campaign_core.timeutils import hour_bucket, parse_iso_utc, parse_twitter_created_at


# 决策 #22: source tag 白名单 —— 只纳入因果链可追溯到 paid KOL 的 node
CAUSAL_SOURCES = frozenset({
    "watch_tweets",              # paid KOL 自己发的
    "watch_replies",             # paid KOL 的回复
    "matched_replies",           # 其他用户 reply 到 paid KOL 推文
    "matched_quotes",            # 其他用户 quote 到 paid KOL 推文
    "expanded_author_tweets",    # Step 4 卷进来新作者的 identity 相关后续推文
    "search",                    # 搜索发现的有机节点（需 signal filter）
    PAID_DELIVERABLE_SEED_SOURCE,
    PAID_DELIVERABLE_TRACKER_SOURCE,
})

# 需 identity signal 或 campaign 对话链才纳入的 source：
#   watch_tweets/watch_replies — watch handle 时间线仍需身份信号或已命中对话链
#   search — 搜索发现的有机节点，只纳入含 identity 信号的
SIGNAL_REQUIRED_SOURCES = frozenset({"watch_tweets", "watch_replies", "search"})


def aggregate(nodes_path: Path, since: datetime | None = None, until: datetime | None = None) -> tuple[list[dict], dict]:
    """Read nodes.jsonl, dedup by tweet_id (keep best observation), filter
    by CAUSAL_SOURCES + optional time window, bucket by hour.
    Returns (rows, stats)."""

    # Dedup: nodes.jsonl is append-only; same tweet_id may appear multiple times
    # with updated metrics or richer affinity metadata. Keep the best observation.
    best: dict[str, dict] = {}
    total_lines = 0
    parse_errors = 0

    with nodes_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            total_lines += 1
            try:
                node = json.loads(line)
            except json.JSONDecodeError:
                parse_errors += 1
                continue

            tid = str(node.get("tweet_id") or node.get("node_id") or "").strip()
            if not tid:
                continue

            if should_replace_observation(best.get(tid), node):
                best[tid] = node

    # Aggregate
    bucket_stats: dict[str, dict] = defaultdict(lambda: {
        "attention_mass": 0.0,
        "node_count": 0,
        "views_sum": 0,
        "affinity_sum": 0.0,
        "by_source": defaultdict(int),
    })

    # Pass 1: find conversations that contain at least one signaled node
    signaled_conversations: set[str] = set()
    for node in best.values():
        if has_identity_signal(node):
            conv = node_conversation_id(node)
            if conv:
                signaled_conversations.add(conv)

    # Pass 2: aggregate with signal filter for watch sources
    filtered_out: dict[str, int] = defaultdict(int)
    watch_noise_filtered = 0
    no_created_at = 0
    outside_window = 0

    for node in best.values():
        source = normalize_paid_source(node.get("source", "") or "empty")
        if source not in CAUSAL_SOURCES:
            filtered_out[source] += 1
            continue

        # For signal-required sources, require identity signal OR membership in a signaled conversation
        if source in SIGNAL_REQUIRED_SOURCES:
            has_signal = has_identity_signal(node)
            conv = node_conversation_id(node)
            in_signaled_conv = conv in signaled_conversations
            if not has_signal and not in_signaled_conv:
                watch_noise_filtered += 1
                continue

        dt = parse_twitter_created_at(node.get("created_at", ""))
        if not dt:
            no_created_at += 1
            continue

        # Time window filter (campaign start/end)
        if since and dt < since:
            outside_window += 1
            continue
        if until and dt > until:
            outside_window += 1
            continue

        hour = hour_bucket(dt)
        views = metrics_view_count(node)
        affinity = safe_float(node.get("campaign_affinity"))

        s = bucket_stats[hour]
        s["attention_mass"] += views * affinity
        s["node_count"] += 1
        s["views_sum"] += views
        s["affinity_sum"] += affinity
        metric_status = str(node.get("metric_status") or "").strip().lower()
        evidence_bucket = "paid_seed_estimate" if source in {PAID_DELIVERABLE_SEED_SOURCE, PAID_DELIVERABLE_TRACKER_SOURCE} and metric_status in {"seed_metric", "pending_metric_fetch"} else source
        s["by_source"][evidence_bucket] += 1

    rows = []
    for hour in sorted(bucket_stats.keys()):
        s = bucket_stats[hour]
        rows.append({
            "hour_utc": hour,
            "attention_mass": round(s["attention_mass"], 2),
            "node_count": s["node_count"],
            "views_sum": s["views_sum"],
            "avg_affinity": round(s["affinity_sum"] / s["node_count"], 3) if s["node_count"] else 0.0,
            "by_source": dict(s["by_source"]),
        })

    stats = {
        "total_lines_read": total_lines,
        "parse_errors": parse_errors,
        "unique_tweets": len(best),
        "included_nodes": sum(s["node_count"] for s in bucket_stats.values()),
        "filtered_out_by_source": dict(filtered_out),
        "watch_noise_filtered": watch_noise_filtered,
        "signaled_conversations": len(signaled_conversations),
        "no_created_at": no_created_at,
        "outside_window": outside_window,
        "window_since": since.isoformat() if since else None,
        "window_until": until.isoformat() if until else None,
        "hour_buckets": len(rows),
        "hour_range": (rows[0]["hour_utc"], rows[-1]["hour_utc"]) if rows else None,
        "total_attention_mass": round(sum(r["attention_mass"] for r in rows), 2),
    }
    return rows, stats


def resolve_window_from_config(nodes_path: Path) -> tuple[datetime | None, datetime | None]:
    """If nodes_path's sibling config.json has campaign_start_at / campaign_end_at,
    parse them as ISO UTC. Used when --since / --until not passed on CLI."""
    config_path = nodes_path.parent / "config.json"
    if not config_path.exists():
        return None, None
    try:
        with config_path.open("r", encoding="utf-8") as fh:
            cfg = json.load(fh)
    except Exception:
        return None, None
    return parse_iso_utc(cfg.get("campaign_start_at") or ""), parse_iso_utc(cfg.get("campaign_end_at") or "")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--nodes-path", type=Path, required=True,
                   help="Path to campaign nodes.jsonl")
    p.add_argument("--output", type=Path, default=None,
                   help="Output Y_twitter.jsonl path (default: sibling of nodes.jsonl)")
    p.add_argument("--since", type=str, default=None,
                   help="ISO UTC timestamp (e.g. 2026-04-21T10:00:00Z). Only nodes with created_at >= since are included. "
                        "If not passed, reads config.json 'campaign_start_at'.")
    p.add_argument("--until", type=str, default=None,
                   help="ISO UTC timestamp. Only nodes with created_at <= until are included. "
                        "If not passed, reads config.json 'campaign_end_at'.")
    p.add_argument("--quiet", action="store_true", help="Suppress stats to stderr")
    args = p.parse_args()

    if not args.nodes_path.exists():
        print(f"ERROR: {args.nodes_path} does not exist", file=sys.stderr)
        return 1

    output_path = args.output or args.nodes_path.parent / "Y_twitter.jsonl"

    # Resolve time window: CLI args > config.json > no filter
    config_since, config_until = resolve_window_from_config(args.nodes_path)
    since = parse_iso_utc(args.since) if args.since else config_since
    until = parse_iso_utc(args.until) if args.until else config_until

    if args.since and not since:
        print(f"ERROR: invalid --since value {args.since!r}", file=sys.stderr)
        return 1
    if args.until and not until:
        print(f"ERROR: invalid --until value {args.until!r}", file=sys.stderr)
        return 1

    rows, stats = aggregate(args.nodes_path, since=since, until=until)

    with output_path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    if not args.quiet:
        print(f"[aggregate] Read {stats['total_lines_read']} lines from {args.nodes_path}", file=sys.stderr)
        print(f"  Unique tweets (after dedup): {stats['unique_tweets']}", file=sys.stderr)
        if stats["window_since"] or stats["window_until"]:
            print(f"  Time window: {stats['window_since'] or '—'} → {stats['window_until'] or '—'}", file=sys.stderr)
            print(f"  Outside window (excluded):  {stats['outside_window']}", file=sys.stderr)
        print(f"  Included (causal scope + window): {stats['included_nodes']}", file=sys.stderr)
        if stats["filtered_out_by_source"]:
            print(f"  Filtered out by non-causal source:", file=sys.stderr)
            for src, cnt in sorted(stats["filtered_out_by_source"].items(), key=lambda x: -x[1]):
                print(f"    {src}: {cnt}", file=sys.stderr)
        if stats.get("watch_noise_filtered"):
            print(f"  Watch noise filtered (no signal, not in signaled conv): {stats['watch_noise_filtered']}", file=sys.stderr)
            print(f"  Signaled conversations: {stats['signaled_conversations']}", file=sys.stderr)
        if stats["parse_errors"]:
            print(f"  JSON parse errors: {stats['parse_errors']}", file=sys.stderr)
        if stats["no_created_at"]:
            print(f"  Skipped (no/invalid created_at): {stats['no_created_at']}", file=sys.stderr)
        print(f"  Hour buckets: {stats['hour_buckets']} ({stats['hour_range'][0] if stats['hour_range'] else '—'} → {stats['hour_range'][1] if stats['hour_range'] else '—'})", file=sys.stderr)
        print(f"  Total attention_mass: {stats['total_attention_mass']:,.0f}", file=sys.stderr)
        print(f"[aggregate] Wrote {output_path}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
