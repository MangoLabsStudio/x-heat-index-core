#!/usr/bin/env python3
"""
Auto-start tweet-tracker@<tid> + cascade-walker@<tid> for paid campaign
deliverables discovered by campaign_collect.

Usage:
  sudo python3 tweet_tracker_dispatcher.py --campaign-id <cid> [--data-dir /opt/...]

Logic:
  1. Read nodes.jsonl, filter to paid watch handles only.
  2. Keep campaign-window root/quote nodes with identity signal OR signaled conversation.
  3. Track paid deliverables even at 0 views; do not use a views gate by default.
  4. Skip if tid already in tracked_tweets.json
  5. For each new tid: systemctl start tweet-tracker@<tid>.service
     (Phase 2 cascade-walker is auto-chained — walker's unit has After=tracker)
  6. Record in tracked_tweets.json with paid_delivery=true and tracking_reason.

Reply/thread-part deliverables should be supplied in paid_deliverables.csv/json
or config.paid_deliverables. The default auto-discovery path intentionally avoids
starting an independent tracker for every KOL reply inside a branded conversation.

Scheduling: hook to systemd timer 10min. Idempotent — rerun safe.

Stdlib only.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from campaign_core.identity import (
    has_identity_signal,
    is_reply_node,
    is_retweet_node,
    node_conversation_id,
    normalize_handle,
)
from campaign_core.io import atomic_write_json, load_json_object
from campaign_core.metrics import metrics_view_count
from campaign_core.paid import load_paid_delivery_seeds
from campaign_core.timeutils import parse_iso_utc, parse_twitter_created_at


TRACK_SOURCES = frozenset({
    "watch_tweets",
    "watch_replies",
})

DEFAULT_MIN_VIEWS = 0            # paid deliverables must be tracked from the start
DEFAULT_MAX_TRACK_AGE_DAYS = 30  # catch delayed dispatcher runs without tracking stale history forever


def systemctl(args: list[str]) -> tuple[int, str]:
    """Run systemctl, return (returncode, stdout+stderr)."""
    try:
        r = subprocess.run(["systemctl"] + args, capture_output=True, text=True, timeout=15)
        return r.returncode, (r.stdout + r.stderr).strip()
    except Exception as e:
        return -1, str(e)


def is_active(unit: str) -> bool:
    rc, _ = systemctl(["is-active", "--quiet", unit])
    return rc == 0


def tracker_unit(tid: str) -> str:
    return f"tweet-tracker@{tid}.service"


def walker_unit(tid: str) -> str:
    return f"cascade-walker@{tid}.service"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--campaign-id", required=True)
    p.add_argument("--data-dir", default="/opt/tweet-tracker/data")
    p.add_argument("--min-views", type=int, default=DEFAULT_MIN_VIEWS,
                   help=f"Only start tracker if current views >= this (default {DEFAULT_MIN_VIEWS})")
    p.add_argument("--max-track-age-days", type=int, default=DEFAULT_MAX_TRACK_AGE_DAYS,
                   help=f"Only start trackers for tweets newer than this many days (default {DEFAULT_MAX_TRACK_AGE_DAYS}; 0 disables age filter)")
    p.add_argument("--ignore-tracked", action="store_true",
                   help="Evaluate candidates even if they are already present in tracked_tweets.json. Useful for audits/dry-runs.")
    p.add_argument("--include-watch-replies", action="store_true",
                   help="Also auto-track watch_replies/reply nodes. Default false; use paid_deliverables.csv/json for contracted reply deliverables.")
    p.add_argument("--prune-stale-paid", action="store_true",
                   help="Remove old paid_delivery registry entries that no longer match the current dispatcher rules.")
    p.add_argument("--stop-stale", action="store_true",
                   help="With --prune-stale-paid, stop tweet-tracker/cascade-walker units for stale paid_delivery entries.")
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would start, don't actually systemctl start")
    args = p.parse_args()
    if args.stop_stale and not args.prune_stale_paid:
        p.error("--stop-stale requires --prune-stale-paid")

    cdir = Path(args.data_dir) / "campaign_graphs" / args.campaign_id
    nodes_path = cdir / "nodes.jsonl"
    config_path = cdir / "config.json"
    tracked_path = cdir / "tracked_tweets.json"

    if not nodes_path.exists():
        print(f"ERROR: {nodes_path} does not exist", file=sys.stderr)
        return 1
    if not config_path.exists():
        print(f"ERROR: {config_path} does not exist (need campaign_start_at)", file=sys.stderr)
        return 1

    cfg = load_json_object(config_path)
    campaign_start = parse_iso_utc(str(cfg.get("campaign_start_at") or ""))
    if not campaign_start:
        print("ERROR: config.json missing campaign_start_at", file=sys.stderr)
        return 1
    campaign_end = parse_iso_utc(str(cfg.get("campaign_end_at") or ""))  # optional
    identity = cfg.get("identity") if isinstance(cfg.get("identity"), dict) else {}
    watch_handles = {
        normalize_handle(handle)
        for handle in (identity.get("watch_handles") or identity.get("kol_handles") or [])
        if normalize_handle(handle)
    }
    if not watch_handles:
        print("No identity.watch_handles configured; skip paid tweet tracker dispatch for brand-only campaign.")
        return 0

    # Load tracked registry
    tracked = {}
    if tracked_path.exists():
        try:
            with tracked_path.open() as fh:
                tracked = json.load(fh)
        except Exception:
            tracked = {}

    now = datetime.now(timezone.utc)
    cutoff_old = now - timedelta(days=args.max_track_age_days) if args.max_track_age_days > 0 else None

    # Pass 1: collect signaled conversations from all best observations.
    signaled_conversations: set[str] = set()
    raw_nodes: list[dict] = []
    with nodes_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                n = json.loads(line)
            except json.JSONDecodeError:
                continue
            raw_nodes.append(n)
            if has_identity_signal(n):
                conv = node_conversation_id(n)
                if conv:
                    signaled_conversations.add(conv)

    # Pass 2: scan nodes.jsonl, dedup paid deliverables by tid.
    best: dict[str, dict] = {}
    skipped_noise = 0
    skipped_non_watch = 0
    skipped_age = 0
    skipped_reply = 0
    skipped_retweet = 0
    for n in raw_nodes:
        src = n.get("source", "")
        if src not in TRACK_SOURCES:
            continue
        author = normalize_handle(n.get("author") or "")
        if author not in watch_handles:
            skipped_non_watch += 1
            continue
        tid = n.get("tweet_id") or n.get("node_id") or ""
        if not tid:
            continue
        dt = parse_twitter_created_at(str(n.get("created_at") or ""))
        if not dt:
            continue
        if dt < campaign_start:
            continue
        if campaign_end and dt > campaign_end:
            continue
        if cutoff_old and dt < cutoff_old:
            skipped_age += 1
            continue
        if not args.include_watch_replies and (src == "watch_replies" or is_reply_node(n)):
            skipped_reply += 1
            continue
        if is_retweet_node(n):
            skipped_retweet += 1
            continue
        direct_signal = has_identity_signal(n)
        conv = node_conversation_id(n)
        in_signaled_conv = bool(conv and conv in signaled_conversations)
        if not direct_signal and not in_signaled_conv:
            skipped_noise += 1
            continue
        views = metrics_view_count(n)
        prev = best.get(tid)
        if prev is None or views > prev["views"]:
            best[tid] = {
                "tid": tid,
                "author": author,
                "source": src,
                "views": views,
                "created_at": n.get("created_at", ""),
                "text": (n.get("text") or "")[:80],
                "affinity": n.get("campaign_affinity", 0.0),
                "paid_delivery": True,
                "tracking_reason": "identity_signal" if direct_signal else "signaled_conversation",
                "affinity_reason": n.get("affinity_reason") or [],
            }

    explicit_seeds = load_paid_delivery_seeds(cdir, cfg)
    for seed in explicit_seeds:
        tid = seed["tid"]
        existing = best.get(tid)
        if existing:
            existing["paid_delivery_seed"] = True
            existing["tracking_reason"] = "explicit_paid_seed"
            if seed.get("author") and not existing.get("author"):
                existing["author"] = seed["author"]
            if seed.get("url"):
                existing["url"] = seed["url"]
            continue
        best[tid] = {
            "tid": tid,
            "author": seed.get("author") or "",
            "source": "paid_deliverable_manifest",
            "views": 0,
            "created_at": seed.get("expected_at") or "",
            "text": seed.get("label") or seed.get("url") or "",
            "url": seed.get("url") or "",
            "affinity": 1.0,
            "paid_delivery": True,
            "paid_delivery_seed": True,
            "tracking_reason": "explicit_paid_seed",
            "affinity_reason": ["paid_delivery_seed"],
        }

    # Apply min_views gate
    candidates = [b for b in best.values() if b["views"] >= args.min_views or b.get("paid_delivery_seed")]
    candidate_ids = {str(c["tid"]) for c in candidates}
    stale_paid = {
        str(tid): value
        for tid, value in tracked.items()
        if isinstance(value, dict)
        and value.get("paid_delivery") is True
        and str(tid) not in candidate_ids
    }
    start_candidates = []
    inactive_tracked = 0
    for c in candidates:
        tid = str(c["tid"])
        if args.ignore_tracked or tid not in tracked:
            c["_start_reason"] = "new"
            start_candidates.append(c)
            continue
        tracker_active = is_active(tracker_unit(tid))
        walker_active = is_active(walker_unit(tid))
        if not tracker_active or not walker_active:
            inactive_tracked += 1
            c["_start_reason"] = "restart_inactive"
            c["_tracker_active"] = tracker_active
            c["_walker_active"] = walker_active
            start_candidates.append(c)
    refresh_candidates = [
        c for c in candidates
        if str(c["tid"]) in tracked and isinstance(tracked.get(str(c["tid"])), dict)
    ]
    below = len(best) - len(candidates)

    print(f"[dispatcher {args.campaign_id}] eligible candidates: {len(candidates)} "
          f"(below min_views={args.min_views}: {below})  "
          f"to_start={len(start_candidates)}  "
          f"inactive_tracked={inactive_tracked}  "
          f"to_refresh={len(refresh_candidates)}  "
          f"already tracked: {len(tracked)}  "
          f"stale_paid={len(stale_paid)}  "
          f"explicit_seeds={len(explicit_seeds)}  "
          f"skipped_noise={skipped_noise} skipped_non_watch={skipped_non_watch} "
          f"skipped_reply={skipped_reply} skipped_retweet={skipped_retweet} skipped_age={skipped_age}", flush=True)

    if args.dry_run and refresh_candidates:
        print(f"  --dry-run, would refresh tracked paid_delivery metadata: {len(refresh_candidates)}", flush=True)
    elif refresh_candidates:
        for c in refresh_candidates:
            tid = str(c["tid"])
            previous = tracked.get(tid) if isinstance(tracked.get(tid), dict) else {}
            preserved = {
                key: previous[key]
                for key in ("started_at", "tracker_unit", "walker_unit", "note")
                if key in previous
            }
            clean_candidate = {k: v for k, v in c.items() if not k.startswith("_")}
            tracked[tid] = {**previous, **clean_candidate, **preserved}

    if args.prune_stale_paid:
        if args.dry_run:
            print(f"  --dry-run, would prune stale paid_delivery entries: {len(stale_paid)}", flush=True)
        else:
            if tracked_path.exists() and stale_paid:
                stamp = now.strftime("%Y%m%dT%H%M%SZ")
                backup_path = tracked_path.with_name(f"{tracked_path.name}.bak_prune_{stamp}")
                backup_path.write_text(json.dumps(tracked, ensure_ascii=False, indent=2), encoding="utf-8")
                print(f"  backed up registry: {backup_path}", flush=True)
            stopped_units = 0
            if args.stop_stale:
                for tid in sorted(stale_paid):
                    for unit in (f"tweet-tracker@{tid}.service", f"cascade-walker@{tid}.service"):
                        rc, out = systemctl(["stop", unit])
                        if rc == 0:
                            stopped_units += 1
                        elif out:
                            print(f"  WARN stop {unit}: {out}", flush=True)
            for tid in stale_paid:
                tracked.pop(tid, None)
            if stale_paid:
                atomic_write_json(tracked_path, tracked)
            print(f"  pruned stale paid_delivery entries={len(stale_paid)} stopped_units={stopped_units}", flush=True)

    if args.dry_run:
        print("  --dry-run, not starting:", flush=True)
        show_candidates = candidates if args.ignore_tracked else start_candidates
        for c in show_candidates:
            print(f"    would-start tid={c['tid']} @{c['author']} views={c['views']} aff={c['affinity']:.2f} reason={c.get('_start_reason', c['tracking_reason'])} | {c['text']}", flush=True)
        return 0

    # Start trackers
    started = 0
    for c in start_candidates:
        tid = str(c["tid"])
        tracker = tracker_unit(tid)
        walker = walker_unit(tid)
        tracker_active = bool(c.get("_tracker_active")) if "_tracker_active" in c else is_active(tracker)
        walker_active = bool(c.get("_walker_active")) if "_walker_active" in c else is_active(walker)

        # Ensure data dir exists
        tdir = Path(args.data_dir) / tid
        tdir.mkdir(parents=True, exist_ok=True)

        if not tracker_active:
            rc, out = systemctl(["start", tracker])
            if rc != 0:
                print(f"  ✗ start {tracker} failed: {out}", flush=True)
                continue
        if not walker_active:
            rc2, out2 = systemctl(["start", walker])
            if rc2 != 0:
                print(f"  ⚠ tracker active but walker failed for {tid}: {out2}", flush=True)
            else:
                print(f"  ✓ active tid={tid} @{c['author']} views={c['views']} | {c['text']}", flush=True)
        else:
            print(f"  ✓ tracker active tid={tid} @{c['author']} views={c['views']} | {c['text']}", flush=True)

        tracked[tid] = {
            **{k: v for k, v in c.items() if not k.startswith("_")},
            "started_at": now.isoformat(),
            "tracker_unit": tracker,
            "walker_unit": walker,
        }
        started += 1

    atomic_write_json(tracked_path, tracked)
    print(f"[dispatcher {args.campaign_id}] done. started={started}, total tracked={len(tracked)}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
