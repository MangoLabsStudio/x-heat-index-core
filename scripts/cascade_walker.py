#!/usr/bin/env python3
"""Cascade Walker — Phase 2 for tweet-tracker.

Reads Phase 1's known replies + quotes, then for each one fetches its
1-hop sub-engagement via Twitter241 /comments + /quotes endpoints.
Builds cascade tree and computes cascade metrics + layered reach.

API: Twitter241 (RapidAPI)

Required env:
  TWITTER241_RAPIDAPI_KEY    primary
  TWEET_ID                   root tweet ID

Optional env:
  TWITTER241_RAPIDAPI_KEY_FALLBACK   optional
  DATA_DIR                   default /opt/tweet-tracker/data
  WALKER_SCHEDULE            cumulative phases: until:interval,...
                             default 6h:900s,24h:1800s,72h:3600s
  TRACKING_RETENTION         default 72h
  WALKER_INTERVAL_SEC        legacy fixed interval override
"""

import json
import math
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

from tracking_schedule import age_seconds, format_duration, load_walker_policy

socket.setdefaulttimeout(20)

# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────
KEY_PRIMARY = os.environ["TWITTER241_RAPIDAPI_KEY"]
KEY_FALLBACK = os.environ.get("TWITTER241_RAPIDAPI_KEY_FALLBACK", "")
HOST = "twitter241.p.rapidapi.com"
TWEET_ID = os.environ["TWEET_ID"]
DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/tweet-tracker/data"))
TRACKING_POLICY = load_walker_policy(os.environ)
WALKER_ONESHOT = os.environ.get("WALKER_ONESHOT", "0").strip().lower() in {"1", "true", "yes"}
try:
    WALKER_ONESHOT_INITIAL_WAIT_SECONDS = max(0.0, float(os.environ.get("WALKER_ONESHOT_INITIAL_WAIT_SECONDS", "10")))
except ValueError:
    WALKER_ONESHOT_INITIAL_WAIT_SECONDS = 10.0

TWEET_DIR = DATA_DIR / TWEET_ID
REPLIES_FILE = TWEET_DIR / "replies.jsonl"
QUOTES_FILE = TWEET_DIR / "quotes.jsonl"
ROOT_METRICS_FILE = TWEET_DIR / "metrics.jsonl"
CASCADE_NODES_FILE = TWEET_DIR / "cascade_nodes.jsonl"
CASCADE_EDGES_FILE = TWEET_DIR / "cascade_edges.jsonl"
CASCADE_METRICS_FILE = TWEET_DIR / "cascade_metrics.jsonl"
WALKER_STATE_FILE = TWEET_DIR / "walker_state.json"
TRACKER_STATE_FILE = TWEET_DIR / "state.json"

_active_key = KEY_PRIMARY
_using_fallback = False


# ──────────────────────────────────────────────────────────────
# Twitter241 HTTP client
# ──────────────────────────────────────────────────────────────
def _switch_to_fallback() -> bool:
    global _active_key, _using_fallback
    if _using_fallback or not KEY_FALLBACK:
        return False
    _using_fallback = True
    _active_key = KEY_FALLBACK
    print(f"[{now_iso()}] [QUOTA] switching to fallback key", flush=True)
    return True


def call_api(path: str, retries: int = 3) -> dict:
    url = f"https://{HOST}{path}"
    last_err = None
    for attempt in range(retries):
        headers = {"x-rapidapi-key": _active_key, "x-rapidapi-host": HOST}
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
                if isinstance(data, dict) and data.get("message", "").lower().startswith("you have exceeded"):
                    if _switch_to_fallback():
                        continue
                    raise RuntimeError(f"quota exhausted: {data.get('message')}")
                return data
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code == 429:
                if _switch_to_fallback():
                    continue
                time.sleep(2 ** attempt)
                continue
            if e.code in (502, 503, 504) and attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
    raise RuntimeError(f"call_api failed: {last_err}")


# ──────────────────────────────────────────────────────────────
# Parsers
# ──────────────────────────────────────────────────────────────
def parse_tweet_node(node: dict) -> dict | None:
    if not node or node.get("__typename") not in ("Tweet", None):
        return None
    legacy = node.get("legacy") or {}
    if not legacy:
        return None
    user_result = node.get("core", {}).get("user_results", {}).get("result", {}) or {}
    user_legacy = user_result.get("legacy") or {}
    user_core = user_result.get("core") or {}
    screen_name = user_core.get("screen_name") or user_legacy.get("screen_name") or ""
    views_count = 0
    views = node.get("views") or {}
    if views.get("count"):
        try:
            views_count = int(views["count"])
        except (TypeError, ValueError):
            pass
    return {
        "tweet_id": str(node.get("rest_id") or legacy.get("id_str", "")),
        "author_username": screen_name.lower(),
        "author_followers": user_legacy.get("followers_count", 0),
        "author_created_at": user_legacy.get("created_at", ""),
        "conversation_id": str(legacy.get("conversation_id_str", "")),
        "in_reply_to_status_id": str(legacy.get("in_reply_to_status_id_str", "") or ""),
        "quoted_status_id": str(legacy.get("quoted_status_id_str", "") or ""),
        "text": legacy.get("full_text", ""),
        "created_at": legacy.get("created_at", ""),
        "view_count": views_count,
        "favorite_count": legacy.get("favorite_count", 0),
        "retweet_count": legacy.get("retweet_count", 0),
        "reply_count": legacy.get("reply_count", 0),
        "quote_count": legacy.get("quote_count", 0),
    }


def record_matches_relation(rec: dict, target_tid: str, relation: str) -> bool:
    if not rec or not target_tid:
        return False
    if relation == "quote":
        return rec.get("quoted_status_id") == target_tid
    if relation == "reply":
        return rec.get("in_reply_to_status_id") == target_tid
    return False


def _parse_item_content(ic: dict, root_tid: str, relation: str) -> dict | None:
    result = ic.get("tweet_results", {}).get("result", {}) or {}
    if result.get("__typename") == "TweetWithVisibilityResults":
        result = result.get("tweet", {}) or {}
    rec = parse_tweet_node(result)
    if rec and rec.get("tweet_id") and rec["tweet_id"] != root_tid and record_matches_relation(rec, root_tid, relation):
        return rec
    return None


def extract_tweets_from_instructions(instructions: list, root_tid: str, relation: str) -> list[dict]:
    tweets = []
    for inst in instructions or []:
        for entry in inst.get("entries", []):
            eid = entry.get("entryId", "")
            content = entry.get("content", {}) or {}
            if "cursor" in eid:
                continue
            if eid.startswith("tweet-"):
                rec = _parse_item_content(content.get("itemContent", {}), root_tid, relation)
                if rec:
                    tweets.append(rec)
            elif eid.startswith("conversationthread-"):
                for item in content.get("items", []):
                    item_inner = item.get("item", {}) or {}
                    rec = _parse_item_content(item_inner.get("itemContent", {}), root_tid, relation)
                    if rec:
                        tweets.append(rec)
    return tweets


def fetch_sub_replies(parent_tid: str) -> list[dict]:
    try:
        data = call_api(f"/comments?pid={parent_tid}&count=20")
    except Exception as e:
        print(f"[{now_iso()}] WARN: sub_replies({parent_tid}): {e}", flush=True)
        return []
    inst = data.get("result", {}).get("instructions", [])
    return extract_tweets_from_instructions(inst, parent_tid, "reply")


def fetch_sub_quotes(parent_tid: str) -> list[dict]:
    try:
        data = call_api(f"/quotes?pid={parent_tid}&count=20")
    except Exception as e:
        print(f"[{now_iso()}] WARN: sub_quotes({parent_tid}): {e}", flush=True)
        return []
    inst = data.get("result", {}).get("timeline", {}).get("instructions", [])
    return extract_tweets_from_instructions(inst, parent_tid, "quote")


# ──────────────────────────────────────────────────────────────
# State helpers
# ──────────────────────────────────────────────────────────────
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def normalize_handle(handle: str) -> str:
    return (handle or "").strip().lower().lstrip("@")


def compress_followers(followers: int) -> int:
    followers = max(0, followers)
    if followers <= 0:
        return 0
    return min(followers, int(math.sqrt(followers) * 120))


def compute_overlap_factor(author_contribs: dict[str, int]) -> float:
    if not author_contribs:
        return 0.0
    total = sum(author_contribs.values())
    if total <= 0:
        return 0.0
    author_count = len(author_contribs)
    shares = [value / total for value in author_contribs.values() if value > 0]
    hhi = sum(share * share for share in shares)
    baseline_hhi = 1.0 / author_count
    breadth_discount = min(0.7, 0.08 * math.log1p(max(0, author_count - 1)))
    concentration_discount = min(0.25, max(0.0, hhi - baseline_hhi) * 1.5)
    return max(0.15, min(1.0, 1.0 - breadth_discount - concentration_discount))

def load_state() -> dict:
    if WALKER_STATE_FILE.exists():
        return json.loads(WALKER_STATE_FILE.read_text())
    return {"walked_node_ids": [], "seen_sub_node_ids": [], "cycle_count": 0, "started_at": now_iso()}

def load_tracking_started_at(state: dict) -> str:
    if TRACKER_STATE_FILE.exists():
        try:
            root_state = json.loads(TRACKER_STATE_FILE.read_text())
            if root_state.get("started_at"):
                return str(root_state["started_at"])
        except Exception:
            pass
    return str(state.get("started_at") or now_iso())

def save_state(state: dict) -> None:
    tmp = WALKER_STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(WALKER_STATE_FILE)

def append_jsonl(path: Path, record: dict) -> None:
    with path.open("a") as f:
        f.write(json.dumps(record) + "\n")

def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return out


# ──────────────────────────────────────────────────────────────
# Cascade metrics
# ──────────────────────────────────────────────────────────────
def compute_wiener_index(nodes_by_parent: dict, root_id: str) -> float:
    adj = defaultdict(set)
    all_node_ids = {root_id}
    for parent_id, children in nodes_by_parent.items():
        all_node_ids.add(parent_id)
        for c in children:
            cid = c["tweet_id"]
            all_node_ids.add(cid)
            adj[parent_id].add(cid)
            adj[cid].add(parent_id)
    n = len(all_node_ids)
    if n < 2:
        return 0.0
    total_dist = 0
    pair_count = 0
    nodes_list = list(all_node_ids)
    for i, src in enumerate(nodes_list):
        dists = {src: 0}
        q = deque([src])
        while q:
            cur = q.popleft()
            for nb in adj.get(cur, ()):
                if nb not in dists:
                    dists[nb] = dists[cur] + 1
                    q.append(nb)
        for j in range(i + 1, len(nodes_list)):
            d = dists.get(nodes_list[j])
            if d is not None:
                total_dist += d
                pair_count += 1
    return total_dist / pair_count if pair_count else 0.0


def compute_cascade_metrics(
    root_id: str,
    direct_replies: list[dict],
    direct_quotes: list[dict],
    sub_nodes_by_parent: dict,
    root_author_followers: int = 0,
) -> dict:
    nodes_by_parent = defaultdict(list)
    for r in direct_replies:
        nodes_by_parent[root_id].append(r)
    for q in direct_quotes:
        nodes_by_parent[root_id].append(q)
    for parent_id, sub_nodes in sub_nodes_by_parent.items():
        nodes_by_parent[parent_id].extend(sub_nodes)

    layer_0 = 1
    layer_1 = len(direct_replies) + len(direct_quotes)
    layer_2 = sum(len(s) for s in sub_nodes_by_parent.values())
    cascade_size = layer_0 + layer_1 + layer_2
    breadth = [layer_0, layer_1, layer_2]
    max_depth = 2 if layer_2 else (1 if layer_1 else 0)
    wiener = compute_wiener_index(nodes_by_parent, root_id)

    # ── Engager Count (discussion participants, deduplicated) ──
    engagers = set()
    for r in direct_replies:
        h = r.get("author_username", "")
        if h: engagers.add(h)
    for q in direct_quotes:
        h = q.get("author_username", "")
        if h: engagers.add(h)
    for subs in sub_nodes_by_parent.values():
        for s in subs:
            h = s.get("author_username", "")
            if h: engagers.add(h)

    # Distribution potential: a conservative proxy for how much further the
    # conversation could spread. This is not actual people reached.
    potential_detail = {"l1_quote": 0, "l1_reply": 0, "l2_quote": 0, "l2_reply": 0}
    author_best: dict[str, dict] = {}

    def register_author(handle: str, followers: int, detail_key: str, weight: float) -> None:
        normalized = normalize_handle(handle)
        if not normalized or followers <= 0:
            return
        contribution = int(compress_followers(followers) * weight)
        if contribution <= 0:
            return
        prev = author_best.get(normalized)
        if prev is None:
            author_best[normalized] = {"contribution": contribution, "detail_key": detail_key}
            potential_detail[detail_key] += contribution
            return
        if contribution > prev["contribution"]:
            potential_detail[prev["detail_key"]] -= prev["contribution"]
            potential_detail[detail_key] += contribution
            author_best[normalized] = {"contribution": contribution, "detail_key": detail_key}

    for q in direct_quotes:
        register_author(q.get("author_username", ""), safe_int(q.get("author_followers", 0)), "l1_quote", 1.0)

    for r in direct_replies:
        register_author(r.get("author_username", ""), safe_int(r.get("author_followers", 0)), "l1_reply", 0.12)

    for subs in sub_nodes_by_parent.values():
        for s in subs:
            edge_type = s.get("edge_type", "reply")
            detail_key = "l2_quote" if edge_type == "quote" else "l2_reply"
            weight = 0.35 if edge_type == "quote" else 0.03
            register_author(s.get("author_username", ""), safe_int(s.get("author_followers", 0)), detail_key, weight)

    potential_gross = sum(potential_detail.values())
    overlap_factor = compute_overlap_factor({handle: row["contribution"] for handle, row in author_best.items()})
    potential_score = int(potential_gross * overlap_factor)

    return {
        "cascade_size": cascade_size,
        "cascade_max_depth": max_depth,
        "cascade_breadth_per_layer": breadth,
        "structural_virality_wiener": round(wiener, 3),
        "unique_engager_count": len(engagers),
        "distribution_potential_gross": potential_gross,
        "distribution_potential_score": potential_score,
        "distribution_potential_overlap_discount": round(overlap_factor, 2),
        "distribution_potential_detail": potential_detail,
        "distribution_potential_accounts": len(author_best),
        # Legacy aliases kept so older frontend/API consumers do not break.
        "reach_gross": potential_gross,
        "reach_adjusted": potential_score,
        "reach_overlap_discount": round(overlap_factor, 2),
        "reach_detail": potential_detail,
        "reach_observed_accounts": len(author_best),
        "reach_followers_sum": potential_score,
    }


# ──────────────────────────────────────────────────────────────
# Main cycle
# ──────────────────────────────────────────────────────────────
def cycle(state: dict, phase) -> None:
    state["cycle_count"] += 1
    state["last_schedule_phase"] = phase.label
    state["last_schedule_interval_sec"] = phase.interval_seconds
    ts = now_iso()
    print(f"[{ts}] cascade walker cycle #{state['cycle_count']}", flush=True)

    direct_replies = load_jsonl(REPLIES_FILE)
    direct_quotes = load_jsonl(QUOTES_FILE)

    walked = set(state["walked_node_ids"])
    seen_sub = set(state["seen_sub_node_ids"])

    new_to_walk = []
    for rec in direct_replies + direct_quotes:
        tid = rec.get("tweet_id")
        if tid and tid not in walked:
            new_to_walk.append(rec)

    print(f"  Phase 1: {len(direct_replies)} replies + {len(direct_quotes)} quotes", flush=True)
    print(f"  new to expand: {len(new_to_walk)}", flush=True)

    new_sub_nodes_count = 0
    for idx, parent_rec in enumerate(new_to_walk):
        parent_tid = parent_rec["tweet_id"]
        parent_handle = parent_rec.get("author_username", "")

        sub_replies = fetch_sub_replies(parent_tid)
        for sn in sub_replies:
            tid = sn.get("tweet_id")
            if not tid or tid in seen_sub:
                continue
            seen_sub.add(tid)
            append_jsonl(CASCADE_NODES_FILE, {**sn, "parent_id": parent_tid, "parent_author": parent_handle, "depth": 2, "edge_type": "reply", "fetched_at": ts})
            append_jsonl(CASCADE_EDGES_FILE, {"parent_id": parent_tid, "child_id": tid, "edge_type": "reply", "discovered_at": ts})
            new_sub_nodes_count += 1

        sub_quotes = fetch_sub_quotes(parent_tid)
        for sn in sub_quotes:
            tid = sn.get("tweet_id")
            if not tid or tid in seen_sub:
                continue
            seen_sub.add(tid)
            append_jsonl(CASCADE_NODES_FILE, {**sn, "parent_id": parent_tid, "parent_author": parent_handle, "depth": 2, "edge_type": "quote", "fetched_at": ts})
            append_jsonl(CASCADE_EDGES_FILE, {"parent_id": parent_tid, "child_id": tid, "edge_type": "quote", "discovered_at": ts})
            new_sub_nodes_count += 1

        walked.add(parent_tid)
        if (idx + 1) % 10 == 0:
            state["walked_node_ids"] = list(walked)
            state["seen_sub_node_ids"] = list(seen_sub)
            save_state(state)
            print(f"  [{idx + 1}/{len(new_to_walk)}] checkpoint", flush=True)
        time.sleep(0.5)

    state["walked_node_ids"] = list(walked)
    state["seen_sub_node_ids"] = list(seen_sub)

    all_sub_nodes = load_jsonl(CASCADE_NODES_FILE)
    sub_by_parent = defaultdict(list)
    for sn in all_sub_nodes:
        sub_by_parent[sn["parent_id"]].append(sn)

    root_metrics = load_jsonl(ROOT_METRICS_FILE)
    root_author_followers = int(root_metrics[-1].get("author_followers", 0)) if root_metrics else 0

    metrics = compute_cascade_metrics(
        root_id=TWEET_ID,
        direct_replies=direct_replies,
        direct_quotes=direct_quotes,
        sub_nodes_by_parent=sub_by_parent,
        root_author_followers=root_author_followers,
    )
    metrics["ts"] = ts
    metrics["cycle"] = state["cycle_count"]
    metrics["new_sub_nodes_this_cycle"] = new_sub_nodes_count
    metrics["walked_nodes_this_cycle"] = len(new_to_walk)
    metrics["schedule_phase"] = phase.label
    metrics["schedule_interval_sec"] = phase.interval_seconds
    append_jsonl(CASCADE_METRICS_FILE, metrics)
    save_state(state)

    print(
        f"  + {new_sub_nodes_count} sub-nodes | size={metrics['cascade_size']} "
        f"depth={metrics['cascade_max_depth']} wiener={metrics['structural_virality_wiener']:.2f} "
        f"engagers={metrics['unique_engager_count']} "
        f"reach_adj={metrics['reach_adjusted']:,} (gross={metrics['reach_gross']:,} x{metrics['reach_overlap_discount']})",
        flush=True,
    )


def main():
    print("=== Cascade Walker started ===", flush=True)
    print(f"  TWEET_ID:  {TWEET_ID}", flush=True)
    print(f"  DATA_DIR:  {TWEET_DIR}", flush=True)
    print(f"  POLICY:    {TRACKING_POLICY.name}", flush=True)
    for phase in TRACKING_POLICY.phases:
        print(f"    - {phase.label}", flush=True)
    print(f"  RETENTION: {format_duration(TRACKING_POLICY.stop_after_seconds)}", flush=True)
    print(f"  ONESHOT:   {WALKER_ONESHOT}", flush=True)
    print(f"  API:       Twitter241", flush=True)

    if not TWEET_DIR.exists():
        print(f"ERROR: Phase 1 data dir does not exist: {TWEET_DIR}", flush=True)
        return

    state = load_state()
    if not state.get("started_at"):
        state["started_at"] = now_iso()
    if state["cycle_count"] == 0:
        initial_wait = WALKER_ONESHOT_INITIAL_WAIT_SECONDS if WALKER_ONESHOT else 90.0
        print(f"  Waiting {initial_wait:.0f}s for Phase 1 to collect initial data...", flush=True)
        time.sleep(initial_wait)
    while True:
        elapsed = age_seconds(load_tracking_started_at(state))
        phase = TRACKING_POLICY.phase_for_age(elapsed)
        if phase is None:
            state["stopped_at"] = now_iso()
            state["stop_reason"] = f"{TRACKING_POLICY.name} retention reached"
            save_state(state)
            print(
                f"[{state['stopped_at']}] tracking window ended at age={format_duration(elapsed)}; stopping",
                flush=True,
            )
            return

        try:
            cycle(state, phase)
        except Exception as e:
            print(f"[{now_iso()}] ERROR cycle: {e}", flush=True)
            import traceback
            traceback.print_exc()
        if WALKER_ONESHOT:
            state["stopped_at"] = now_iso()
            state["stop_reason"] = "oneshot collection completed"
            save_state(state)
            print(f"[{state['stopped_at']}] oneshot collection completed; stopping", flush=True)
            return
        time.sleep(phase.interval_seconds)


if __name__ == "__main__":
    main()
