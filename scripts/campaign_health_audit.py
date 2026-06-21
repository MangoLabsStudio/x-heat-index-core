#!/usr/bin/env python3
"""Audit campaign graph health after collection.

This is a generic guardrail for two failure modes:

1. Misses: expected watch handles have no/low campaign-signaled observations, or
   article URLs were not enriched.
2. Noise: watch/search observations without identity signal are entering the
   raw node stream.

It does not prove X/Twitter API completeness. It makes the collector's known
blind spots visible before a report is written.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from campaign_core.collection_state import load_collection_events, load_collection_jobs
from campaign_core.identity import has_identity_signal, is_article_url, normalize_handle, node_conversation_id
from campaign_core.io import load_json_object
from campaign_core.metrics import metrics_view_count, safe_float, should_replace_observation
from campaign_core.timeutils import parse_iso_utc, parse_twitter_created_at


SIGNAL_REQUIRED_SOURCES = frozenset({"watch_tweets", "watch_replies", "search"})


def load_nodes(path: Path) -> tuple[list[dict], dict[str, dict], int]:
    rows: list[dict] = []
    best: dict[str, dict] = {}
    parse_errors = 0
    if not path.exists():
        return rows, best, 0
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                parse_errors += 1
                continue
            if not isinstance(row, dict):
                continue
            rows.append(row)
            tid = str(row.get("tweet_id") or row.get("node_id") or "").strip()
            if tid and should_replace_observation(best.get(tid), row):
                best[tid] = row
    return rows, best, parse_errors


def article_url(row: dict) -> bool:
    return any(is_article_url(url) for url in (row.get("urls") or []))


def in_window(row: dict, since: datetime | None, until: datetime | None) -> bool:
    dt = parse_twitter_created_at(str(row.get("created_at") or ""))
    if not dt:
        return False
    if since and dt < since:
        return False
    if until and dt > until:
        return False
    return True


def write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def paid_graph_audit(config_path: Path, nodes_path: Path) -> dict[str, Any]:
    audit_path = config_path.parent / "paid_graph_match_audit.json"
    if not audit_path.exists():
        return {"status": "missing_audit", "path": str(audit_path), "status_counts": {}}
    audit = load_json_object(audit_path)
    counts = audit.get("status_counts") if isinstance(audit.get("status_counts"), dict) else {}
    legacy_counts = audit.get("legacy_status_counts") if isinstance(audit.get("legacy_status_counts"), dict) else {}
    audit_mtime = audit_path.stat().st_mtime_ns
    input_mtime = 0
    for path in (
        config_path,
        nodes_path,
        config_path.parent / "paid_deliverables.json",
        config_path.parent / "paid_deliverables.csv",
    ):
        try:
            input_mtime = max(input_mtime, path.stat().st_mtime_ns)
        except OSError:
            continue
    stale = input_mtime > audit_mtime
    missing = int(counts.get("fetch_failed") or counts.get("missing") or legacy_counts.get("missing") or 0)
    seeded = int(counts.get("seeded_pending_metric_fetch") or counts.get("seeded") or legacy_counts.get("seeded") or 0)
    matched = int(counts.get("matched_observed") or counts.get("matched") or legacy_counts.get("matched") or 0)
    paid_rows = audit.get("paid_deliverables") if isinstance(audit.get("paid_deliverables"), list) else []
    failed_tweet_ids = [
        str(row.get("tweet_id") or "")
        for row in paid_rows
        if str(row.get("graph_match_status") or "") in {"fetch_failed", "missing"}
    ]
    seeded_tweet_ids = [
        str(row.get("tweet_id") or "")
        for row in paid_rows
        if str(row.get("graph_match_status") or "") in {"seeded_pending_metric_fetch", "seeded"}
    ]
    if stale:
        status = "stale"
    elif missing:
        status = "not_ready"
    elif seeded:
        status = "estimate_only"
    else:
        status = "ready" if audit.get("paid_deliverable_count") else "no_paid_deliverables"
    return {
        "status": status,
        "status_counts": counts,
        "legacy_status_counts": legacy_counts,
        "paid_deliverable_count": audit.get("paid_deliverable_count", 0),
        "matched_roots": matched,
        "missing_roots": missing,
        "seed_only_roots": seeded,
        "failed_tweet_ids": [tid for tid in failed_tweet_ids if tid],
        "seeded_tweet_ids": [tid for tid in seeded_tweet_ids if tid],
        "updated_at": audit.get("updated_at") or audit.get("generated_at"),
        "source_snapshot_id": audit.get("run_id") or audit.get("source_snapshot_id"),
        "audit_stale": stale,
    }


def collection_diagnostics(config_path: Path, paid_audit: dict[str, Any]) -> dict[str, Any]:
    campaign_dir = config_path.parent
    jobs = load_collection_jobs(campaign_dir)
    events = load_collection_events(campaign_dir)
    paid_jobs = [job for job in jobs if str(job.get("source") or "") == "paid_manifest"]
    paid_observed = [
        job for job in paid_jobs
        if str(job.get("status") or "") == "observed"
        or str(job.get("root_fetch_status") or "") in {"observed", "existing_observed"}
    ]
    reply_attempted = [
        job for job in paid_jobs
        if str(job.get("reply_collection_status") or "") not in {"", "disabled"}
    ]
    reply_completed = [
        job for job in reply_attempted
        if str(job.get("reply_collection_status") or "") in {"no_next_cursor", "page_cap_reached", "repeated_cursor"}
    ]
    quote_attempted = [
        job for job in paid_jobs
        if str(job.get("quote_collection_status") or "") not in {"", "disabled"}
    ]
    quote_completed = [
        job for job in quote_attempted
        if str(job.get("quote_collection_status") or "") in {"no_next_cursor", "page_cap_reached", "repeated_cursor"}
    ]
    tracker_events = [event for event in events if str(event.get("event") or "").startswith("tracker_")]
    cascade_events = [event for event in events if str(event.get("event") or "").startswith("cascade_")]
    tracker_roots = {str(event.get("tweet_id") or "") for event in tracker_events if event.get("tweet_id")}
    cascade_roots = {str(event.get("tweet_id") or event.get("parent_tweet_id") or "") for event in cascade_events if event.get("tweet_id") or event.get("parent_tweet_id")}
    failed_events = [
        event for event in events
        if str(event.get("event") or "").endswith("_failed")
        or str(event.get("status") or "") in {"fetch_failed", "endpoint_failed", "rate_limit"}
    ]
    endpoint_failures = [
        {
            "tweet_id": event.get("tweet_id") or event.get("parent_tweet_id") or "",
            "endpoint": event.get("endpoint") or event.get("relation") or event.get("event") or "",
            "event": event.get("event") or "",
            "status": event.get("status") or "",
            "error": event.get("error") or "",
            "ts": event.get("ts") or "",
        }
        for event in failed_events
    ]
    paid_total = int(paid_audit.get("paid_deliverable_count") or len(paid_jobs) or 0)
    paid_observed_count = int(paid_audit.get("matched_roots") or len(paid_observed) or 0)
    tracked_paid = len(tracker_roots & {str(job.get("tweet_id") or "") for job in paid_observed})
    cascade_completed = len(cascade_roots & {str(job.get("tweet_id") or "") for job in paid_observed})
    return {
        "job_count": len(jobs),
        "event_count": len(events),
        "paid_job_count": len(paid_jobs),
        "paid_terminal_job_count": len([job for job in paid_jobs if str(job.get("status") or "") in {"observed", "seeded_pending_metric_fetch", "fetch_failed", "invalid"}]),
        "paid_root_coverage": round(paid_observed_count / paid_total, 3) if paid_total else 1.0,
        "reply_page_coverage": round(len(reply_completed) / len(reply_attempted), 3) if reply_attempted else None,
        "quote_page_coverage": round(len(quote_completed) / len(quote_attempted), 3) if quote_attempted else None,
        "tracker_coverage": round(tracked_paid / len(paid_observed), 3) if paid_observed else None,
        "cascade_coverage": round(cascade_completed / tracked_paid, 3) if tracked_paid else None,
        "endpoint_failures": endpoint_failures[:50],
    }


def audit(config_path: Path, nodes_path: Path) -> dict[str, Any]:
    config = load_json_object(config_path)
    identity = config.get("identity") if isinstance(config.get("identity"), dict) else {}
    watch_handles = [normalize_handle(h) for h in identity.get("watch_handles") or [] if normalize_handle(h)]
    official_handles = [normalize_handle(h) for h in identity.get("official_handles") or [] if normalize_handle(h)]
    since = parse_iso_utc(str(config.get("campaign_start_at") or ""))
    until = parse_iso_utc(str(config.get("campaign_end_at") or ""))

    all_rows, best, parse_errors = load_nodes(nodes_path)
    best_rows = list(best.values())
    window_rows = [row for row in best_rows if in_window(row, since, until)]

    signaled_conversations: set[str] = set()
    for row in best_rows:
        if has_identity_signal(row):
            conv = node_conversation_id(row)
            if conv:
                signaled_conversations.add(conv)

    source_counts = Counter(str(row.get("source") or "empty") for row in best_rows)
    affinity_buckets = Counter()
    for row in best_rows:
        affinity = safe_float(row.get("campaign_affinity"))
        if affinity >= 0.9:
            affinity_buckets["0.90-1.00"] += 1
        elif affinity >= 0.7:
            affinity_buckets["0.70-0.89"] += 1
        elif affinity >= 0.42:
            affinity_buckets["0.42-0.69"] += 1
        elif affinity > 0:
            affinity_buckets["0.01-0.41"] += 1
        else:
            affinity_buckets["0"] += 1

    per_watch: dict[str, dict[str, Any]] = {}
    for handle in watch_handles:
        rows = [row for row in window_rows if normalize_handle(row.get("author") or "") == handle]
        signaled = [row for row in rows if has_identity_signal(row)]
        articles = [row for row in rows if article_url(row)]
        enriched_articles = [
            row for row in articles
            if any(str(reason).startswith("article_identity_term") for reason in (row.get("affinity_reason") or []))
        ]
        per_watch[handle] = {
            "nodes": len(rows),
            "signaled_nodes": len(signaled),
            "article_url_nodes": len(articles),
            "article_enriched_nodes": len(enriched_articles),
            "views": sum(metrics_view_count(row) for row in rows),
            "signaled_views": sum(metrics_view_count(row) for row in signaled),
            "risk": "ok",
        }
        if not signaled:
            per_watch[handle]["risk"] = "missing_signal"
            if articles:
                per_watch[handle]["risk"] = "missing_signal_with_article_urls"

    noise_candidates = []
    for row in best_rows:
        source = str(row.get("source") or "empty")
        if source not in SIGNAL_REQUIRED_SOURCES:
            continue
        conv = node_conversation_id(row)
        if has_identity_signal(row) or conv in signaled_conversations:
            continue
        noise_candidates.append(row)

    high_view_noise = sorted(noise_candidates, key=metrics_view_count, reverse=True)[:20]
    article_enrichment_gaps = sorted(
        [
            row for row in best_rows
            if article_url(row)
            and not any(str(reason).startswith("article_identity_term") for reason in (row.get("affinity_reason") or []))
        ],
        key=metrics_view_count,
        reverse=True,
    )[:20]

    paid_audit = paid_graph_audit(config_path, nodes_path)
    collection_completeness = collection_diagnostics(config_path, paid_audit)
    risks: list[str] = []
    missing_handles = [
        handle for handle, row in per_watch.items()
        if row["risk"] in {"missing_signal", "missing_signal_with_article_urls"}
    ]
    article_gap_handles = [
        handle for handle, row in per_watch.items()
        if row["risk"] == "missing_signal_with_article_urls"
    ]
    if missing_handles:
        risks.append(f"watch handles with no signaled nodes: {', '.join(missing_handles)}")
    if article_gap_handles:
        risks.append(f"watch handles with no signal but article URLs to review: {', '.join(article_gap_handles)}")
    if noise_candidates:
        risks.append(f"{len(noise_candidates)} raw watch/search nodes lack identity signal and signaled conversation membership; these should be filtered out of Y_twitter")
    if parse_errors:
        risks.append(f"{parse_errors} JSON parse errors in nodes.jsonl")
    if paid_audit["status"] == "missing_audit":
        risks.append("paid graph match audit missing")
    elif paid_audit["status"] == "stale":
        risks.append("paid graph match audit is stale")
    elif paid_audit["status"] in {"not_ready", "estimate_only"}:
        risks.append(f"paid graph readiness is {paid_audit['status']}: {paid_audit.get('status_counts', {})}")
        if paid_audit.get("failed_tweet_ids"):
            risks.append(f"paid root fetch failed: {', '.join(paid_audit['failed_tweet_ids'][:20])}")
    for failure in collection_completeness.get("endpoint_failures") or []:
        tweet_id = failure.get("tweet_id") or "unknown"
        endpoint = failure.get("endpoint") or failure.get("event") or "unknown"
        risks.append(f"collection endpoint failed: tweet={tweet_id} endpoint={endpoint}")

    return {
        "campaign_id": config.get("campaign_id"),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "config": {
            "campaign_start_at": config.get("campaign_start_at"),
            "campaign_end_at": config.get("campaign_end_at"),
            "watch_handles": len(watch_handles),
            "official_handles": len(official_handles),
        },
        "summary": {
            "raw_rows": len(all_rows),
            "unique_tweets": len(best_rows),
            "window_nodes": len(window_rows),
            "parse_errors": parse_errors,
            "source_counts": dict(source_counts),
            "affinity_buckets": dict(affinity_buckets),
            "signaled_conversations": len(signaled_conversations),
            "noise_candidates": len(noise_candidates),
            "article_enrichment_gaps": len(article_enrichment_gaps),
        },
        "paid_graph_readiness": paid_audit,
        "collection_completeness": collection_completeness,
        "per_watch_handle": per_watch,
        "top_noise_candidates": [
            {
                "tweet_id": row.get("tweet_id") or row.get("node_id"),
                "author": row.get("author"),
                "source": row.get("source"),
                "views": metrics_view_count(row),
                "affinity": safe_float(row.get("campaign_affinity")),
                "reasons": row.get("affinity_reason") or [],
                "text": str(row.get("text") or "")[:180],
            }
            for row in high_view_noise
        ],
        "top_article_enrichment_gaps": [
            {
                "tweet_id": row.get("tweet_id") or row.get("node_id"),
                "author": row.get("author"),
                "source": row.get("source"),
                "views": metrics_view_count(row),
                "affinity": safe_float(row.get("campaign_affinity")),
                "reasons": row.get("affinity_reason") or [],
                "urls": row.get("urls") or [],
            }
            for row in article_enrichment_gaps
        ],
        "risks": risks,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--nodes-path", type=Path, default=None)
    parser.add_argument("--campaign-id", default="")
    parser.add_argument("--data-dir", type=Path, default=Path("/opt/tweet-tracker/data"))
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--fail-on-risk", action="store_true")
    args = parser.parse_args()

    if args.config:
        config_path = args.config
    elif args.campaign_id:
        config_path = args.data_dir / "campaign_graphs" / args.campaign_id / "config.json"
    else:
        print("ERROR: pass --config or --campaign-id", file=sys.stderr)
        return 2

    nodes_path = args.nodes_path or config_path.parent / "nodes.jsonl"
    report = audit(config_path, nodes_path)

    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        write_text_atomic(args.output, text + "\n")
        print(f"[health] Wrote {args.output}", file=sys.stderr)
    else:
        print(text)

    if report["risks"]:
        print("[health] Risks:", file=sys.stderr)
        for risk in report["risks"]:
            print(f"  - {risk}", file=sys.stderr)
        if args.fail_on_risk:
            return 1
    else:
        print("[health] No obvious collector health risks", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
