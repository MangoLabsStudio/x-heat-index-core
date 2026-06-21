"""Durable collection job/event helpers for campaign graph pipelines."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .io import atomic_write_json, load_jsonl


TERMINAL_JOB_STATUSES = {
    "observed",
    "seeded_pending_metric_fetch",
    "fetch_failed",
    "invalid",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def collection_paths(campaign_dir: Path) -> dict[str, Path]:
    return {
        "jobs": campaign_dir / "collection_jobs.jsonl",
        "events": campaign_dir / "collection_events.jsonl",
        "state": campaign_dir / "collection_state.json",
    }


def collection_job(
    *,
    campaign_id: str,
    tweet_id: str,
    role: str = "paid_root",
    source: str = "paid_manifest",
    status: str = "pending",
    updated_at: str | None = None,
) -> dict[str, Any]:
    return {
        "job_id": f"{campaign_id}:{role}:{tweet_id}",
        "campaign_id": campaign_id,
        "tweet_id": str(tweet_id),
        "role": role,
        "source": source,
        "status": status,
        "root_fetch_status": "",
        "reply_collection_status": "",
        "quote_collection_status": "",
        "tracker_status": "",
        "cascade_status": "",
        "updated_at": updated_at or now_iso(),
    }


def append_collection_event(campaign_dir: Path, event: dict[str, Any]) -> None:
    paths = collection_paths(campaign_dir)
    paths["events"].parent.mkdir(parents=True, exist_ok=True)
    record = {"ts": now_iso(), **event}
    with paths["events"].open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def write_collection_jobs(campaign_dir: Path, jobs: list[dict[str, Any]]) -> None:
    paths = collection_paths(campaign_dir)
    paths["jobs"].parent.mkdir(parents=True, exist_ok=True)
    tmp = paths["jobs"].with_suffix(paths["jobs"].suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        for job in jobs:
            fh.write(json.dumps(job, ensure_ascii=False) + "\n")
    tmp.replace(paths["jobs"])


def load_collection_events(campaign_dir: Path) -> list[dict[str, Any]]:
    return load_jsonl(collection_paths(campaign_dir)["events"])


def load_collection_jobs(campaign_dir: Path) -> list[dict[str, Any]]:
    return load_jsonl(collection_paths(campaign_dir)["jobs"])


def collection_state_summary(jobs: list[dict[str, Any]], events: list[dict[str, Any]]) -> dict[str, Any]:
    paid_jobs = [job for job in jobs if str(job.get("source") or "") == "paid_manifest"]
    terminal = [job for job in paid_jobs if str(job.get("status") or "") in TERMINAL_JOB_STATUSES]
    failed_events = [
        event for event in events
        if str(event.get("status") or "").endswith("failed") or str(event.get("event") or "").endswith("_failed")
    ]
    endpoint_failures = [
        {
            "tweet_id": event.get("tweet_id") or event.get("parent_tweet_id") or "",
            "endpoint": event.get("endpoint") or event.get("relation") or event.get("event") or "",
            "event": event.get("event") or "",
            "error": event.get("error") or "",
            "ts": event.get("ts") or "",
        }
        for event in failed_events
    ]
    return {
        "paid_job_count": len(paid_jobs),
        "paid_terminal_job_count": len(terminal),
        "paid_job_terminal_coverage": round(len(terminal) / len(paid_jobs), 3) if paid_jobs else 1.0,
        "event_count": len(events),
        "failed_event_count": len(failed_events),
        "endpoint_failures": endpoint_failures[:50],
    }


def write_collection_state(
    campaign_dir: Path,
    *,
    campaign_id: str,
    run_id: str = "",
    jobs: list[dict[str, Any]] | None = None,
    status: str = "updated",
) -> dict[str, Any]:
    jobs = jobs if jobs is not None else load_collection_jobs(campaign_dir)
    events = load_collection_events(campaign_dir)
    state = {
        "campaign_id": campaign_id,
        "run_id": run_id,
        "status": status,
        "updated_at": now_iso(),
        "summary": collection_state_summary(jobs, events),
    }
    atomic_write_json(collection_paths(campaign_dir)["state"], state)
    return state
