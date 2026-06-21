#!/usr/bin/env python3
"""Rebuild historical derived and cascade metrics from raw JSONL files.

Reads existing snapshot files under a tweet data directory and rewrites:
  - derived.jsonl
  - cascade_metrics.jsonl

Old files are backed up by default with a timestamp suffix.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

W_VIEWS = 0.01
W_LIKES = 1.0
W_RTS = 2.0
W_REPLIES = 3.0
W_QUOTES = 5.0
W_QUOTES_WITH_COMMENTARY = 7.0
W_REPLY_CATALYST = 4.5
W_BOOKMARKS = 2.0
VELOCITY_EMA_ALPHA = 0.35

QUALITY_TIERS = [
    (100_000, 3.0),
    (10_000, 1.5),
    (1_000, 1.0),
    (0, 0.8),
]
QUALITY_NEW_ACCOUNT_DAYS = 30
QUALITY_NEW_ACCOUNT_MULTIPLIER = 0.3

TEMPORAL_DECAY = [
    (1, 1.0),
    (6, 0.85),
    (24, 0.5),
    (72, 0.2),
    (None, 0.05),
]

BONUS_QUOTE_AND_REPLY = 2.0
BONUS_CATALYST_QUOTE = 3.0
BONUS_BURST_QUOTES = 5.0
COMMENTARY_MIN_CHARS = 50


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


def parse_twitter_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%a %b %d %H:%M:%S %z %Y")
    except (TypeError, ValueError):
        return None


def parse_iso_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def parse_any_datetime(value: str) -> datetime | None:
    return parse_iso_datetime(value) or parse_twitter_datetime(value)


def event_dt(rec: dict, *keys: str) -> datetime:
    for key in keys:
        dt = parse_any_datetime(rec.get(key, ""))
        if dt:
            return dt
    return datetime.max.replace(tzinfo=timezone.utc)


def interaction_timestamp(rec: dict) -> str:
    return rec.get("created_at") or rec.get("fetched_at") or ""


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out: list[dict] = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}


def write_json(path: Path, payload: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2))
    tmp.replace(path)


def write_jsonl(path: Path, rows: list[dict]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    tmp.replace(path)


def backup_file(path: Path) -> Path | None:
    if not path.exists():
        return None
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = path.with_name(f"{path.name}.bak.{stamp}")
    backup.write_bytes(path.read_bytes())
    return backup


def quality_multiplier(followers: int, account_created_at: str = "") -> float:
    q = 1.0
    for threshold, mult in QUALITY_TIERS:
        if followers >= threshold:
            q = mult
            break
    created = parse_twitter_datetime(account_created_at)
    if created and (datetime.now(timezone.utc) - created).days < QUALITY_NEW_ACCOUNT_DAYS:
        q *= QUALITY_NEW_ACCOUNT_MULTIPLIER
    if followers > 0:
        follower_signal = max(0.8, min(1.6, math.log10(followers + 10) / 3.0 + 0.3))
        q *= follower_signal
    return q


def temporal_decay(post_created_at: str, action_time: str) -> float:
    t_post = parse_any_datetime(post_created_at)
    t_action = parse_any_datetime(action_time)
    if not t_post or not t_action:
        return 1.0
    delta_hours = max(0, (t_action - t_post).total_seconds() / 3600)
    for max_hours, mult in TEMPORAL_DECAY:
        if max_hours is None or delta_hours <= max_hours:
            return mult
    return 0.05


def reply_base_weight(text: str) -> float:
    return W_REPLY_CATALYST if len((text or "").strip()) >= COMMENTARY_MIN_CHARS else W_REPLIES


def quote_base_weight(text: str) -> float:
    return W_QUOTES_WITH_COMMENTARY if len((text or "").strip()) >= COMMENTARY_MIN_CHARS else W_QUOTES


def coverage_ratio(observed_count: int, total_count: int) -> float:
    total = max(0, safe_int(total_count))
    if total == 0:
        return 1.0
    return min(1.0, observed_count / total)


def estimate_missing_score(total_count: int, observed_count: int, observed_score: float, default_unit_score: float) -> float:
    missing = max(0, safe_int(total_count) - observed_count)
    if missing == 0:
        return 0.0
    if observed_count <= 0:
        return missing * default_unit_score
    sample_confidence = min(0.85, observed_count / 20.0)
    observed_unit_score = observed_score / observed_count
    blended_unit_score = (sample_confidence * observed_unit_score) + ((1.0 - sample_confidence) * default_unit_score)
    return missing * blended_unit_score


def compute_interaction_bonus(replies: list[dict], quotes: list[dict]) -> float:
    bonus = 0.0
    reply_authors = {normalize_handle(r.get("author_username", "")) for r in replies if r.get("author_username")}
    quote_authors = {normalize_handle(q.get("author_username", "")) for q in quotes if q.get("author_username")}
    bonus += len(reply_authors & quote_authors) * BONUS_QUOTE_AND_REPLY

    catalyst_quotes = sum(1 for q in quotes if quote_base_weight(q.get("text", "")) > W_QUOTES)
    if catalyst_quotes:
        bonus += catalyst_quotes * BONUS_CATALYST_QUOTE

    quote_times: list[datetime] = []
    for q in quotes:
        qt = parse_twitter_datetime(q.get("created_at", ""))
        if qt:
            quote_times.append(qt)
    quote_times.sort()
    for i in range(len(quote_times) - 2):
        if (quote_times[i + 2] - quote_times[i]).total_seconds() <= 600:
            bonus += BONUS_BURST_QUOTES
            break
    return bonus


def compute_heat(metrics: dict, replies: list[dict], quotes: list[dict], post_created_at: str) -> dict:
    views_score = W_VIEWS * safe_int(metrics.get("view_count", 0))
    likes_score = W_LIKES * safe_int(metrics.get("favorite_count", 0))
    rts_score = W_RTS * safe_int(metrics.get("retweet_count", 0))
    bookmarks_score = W_BOOKMARKS * safe_int(metrics.get("bookmark_count", 0))

    replies_score = 0.0
    for rec in replies:
        q = quality_multiplier(safe_int(rec.get("author_followers", 0)), rec.get("author_created_at", ""))
        d = temporal_decay(post_created_at, interaction_timestamp(rec))
        replies_score += reply_base_weight(rec.get("text", "")) * q * d
    replies_score += estimate_missing_score(metrics.get("reply_count", 0), len(replies), replies_score, W_REPLIES)

    quotes_score = 0.0
    for rec in quotes:
        q = quality_multiplier(safe_int(rec.get("author_followers", 0)), rec.get("author_created_at", ""))
        d = temporal_decay(post_created_at, interaction_timestamp(rec))
        quotes_score += quote_base_weight(rec.get("text", "")) * q * d
    quotes_score += estimate_missing_score(metrics.get("quote_count", 0), len(quotes), quotes_score, W_QUOTES)

    bonus = compute_interaction_bonus(replies, quotes)
    heat_raw = views_score + likes_score + rts_score + bookmarks_score + replies_score + quotes_score + bonus
    return {
        "heat_raw": heat_raw,
        "components": {
            "views": round(views_score, 2),
            "likes": round(likes_score, 2),
            "rts": round(rts_score, 2),
            "replies": round(replies_score, 2),
            "quotes": round(quotes_score, 2),
            "bookmarks": round(bookmarks_score, 2),
            "bonus": round(bonus, 2),
        },
        "coverage": {
            "replies": round(coverage_ratio(len(replies), metrics.get("reply_count", 0)), 4),
            "quotes": round(coverage_ratio(len(quotes), metrics.get("quote_count", 0)), 4),
        },
    }


def compute_engagement_rate(metrics: dict) -> float:
    views = safe_int(metrics.get("view_count", 0))
    if views <= 0:
        return 0.0
    return (
        safe_int(metrics.get("favorite_count", 0))
        + safe_int(metrics.get("retweet_count", 0))
        + safe_int(metrics.get("reply_count", 0))
        + safe_int(metrics.get("quote_count", 0))
    ) / views


def compute_weighted_engagement_rate(metrics: dict) -> float:
    views = safe_int(metrics.get("view_count", 0))
    if views <= 0:
        return 0.0
    weighted = (
        W_LIKES * safe_int(metrics.get("favorite_count", 0))
        + W_RTS * safe_int(metrics.get("retweet_count", 0))
        + W_REPLIES * safe_int(metrics.get("reply_count", 0))
        + W_QUOTES * safe_int(metrics.get("quote_count", 0))
        + W_BOOKMARKS * safe_int(metrics.get("bookmark_count", 0))
    )
    return weighted / views


def compute_growth_metrics(metrics: dict, last_metrics: dict | None, ts: str, last_ts: str | None) -> dict:
    growth = {
        "elapsed_minutes": 0.0,
        "view_delta": 0,
        "favorite_delta": 0,
        "retweet_delta": 0,
        "reply_delta": 0,
        "quote_delta": 0,
        "bookmark_delta": 0,
        "view_velocity_per_min": 0.0,
        "engagement_velocity_per_min": 0.0,
    }
    if not last_metrics or not last_ts:
        return growth

    last_ts_dt = parse_iso_datetime(last_ts)
    cur_ts_dt = parse_iso_datetime(ts)
    if not last_ts_dt or not cur_ts_dt:
        return growth

    elapsed_min = max(0.0, (cur_ts_dt - last_ts_dt).total_seconds() / 60.0)
    growth["elapsed_minutes"] = round(elapsed_min, 3)

    for field in ("view_count", "favorite_count", "retweet_count", "reply_count", "quote_count", "bookmark_count"):
        delta_key = field.replace("_count", "_delta")
        growth[delta_key] = safe_int(metrics.get(field, 0)) - safe_int(last_metrics.get(field, 0))

    if elapsed_min > 0:
        growth["view_velocity_per_min"] = growth["view_delta"] / elapsed_min
        engagement_delta = (
            growth["favorite_delta"]
            + growth["retweet_delta"]
            + growth["reply_delta"]
            + growth["quote_delta"]
            + growth["bookmark_delta"]
        )
        growth["engagement_velocity_per_min"] = engagement_delta / elapsed_min
    return growth


def smooth_velocity(current_velocity: float, previous_velocity_ema: float | None) -> float:
    if previous_velocity_ema is None:
        return current_velocity
    return (VELOCITY_EMA_ALPHA * current_velocity) + ((1.0 - VELOCITY_EMA_ALPHA) * previous_velocity_ema)


def classify_stage(heat_velocity: float, heat_velocity_ema: float, growth: dict) -> str:
    view_velocity = growth.get("view_velocity_per_min", 0.0)
    elapsed = growth.get("elapsed_minutes", 0.0)
    conversation_velocity = 0.0
    if elapsed > 0:
        conversation_velocity = (growth.get("reply_delta", 0) + growth.get("quote_delta", 0)) / elapsed
    signal = heat_velocity_ema

    if signal < 0.5 and view_velocity < 3 and conversation_velocity < 0.2:
        return "dead"
    if signal < 1.5 and conversation_velocity < 0.5:
        return "decay"
    if heat_velocity < signal * 0.7 and signal < 10:
        return "saturation"
    if heat_velocity > signal * 1.15 and signal < 10:
        return "discovery"
    return "amplification"


def render_dashboard(tweet_id: str, metrics: dict, derived: dict, state: dict, new_replies: int, new_quotes: int) -> str:
    lines = [
        "═" * 60,
        f" XHI Tweet Tracker — {tweet_id}",
        f" Updated: {derived['ts']}",
        " API: Twitter241",
        "═" * 60,
        "",
        f" Author:        @{metrics.get('author_username', '?')} ({safe_int(metrics.get('author_followers', 0)):,} followers)",
        f" Cycle:         #{state.get('cycle_count', 0)}",
        f" Tracker age:   {state.get('started_at', '—')} → now",
        "",
        "─" * 60,
        " Raw counts",
        "─" * 60,
        f"  views:     {safe_int(metrics.get('view_count', 0)):>12,}",
        f"  likes:     {safe_int(metrics.get('favorite_count', 0)):>12,}",
        f"  retweets:  {safe_int(metrics.get('retweet_count', 0)):>12,}",
        f"  replies:   {safe_int(metrics.get('reply_count', 0)):>12,}  (+{new_replies} new)",
        f"  quotes:    {safe_int(metrics.get('quote_count', 0)):>12,}  (+{new_quotes} new)",
        f"  bookmarks: {safe_int(metrics.get('bookmark_count', 0)):>12,}",
        "",
        "─" * 60,
        " Derived (XHI v2)",
        "─" * 60,
        f"  Heat Score:        {derived['heat_score']:>12,.0f}",
        f"  Heat Delta:        {derived['heat_delta']:>12,.1f}",
        f"  Heat Velocity:     {derived['heat_velocity_per_min']:>12,.1f}  per minute",
        f"  Engagement Rate:   {derived['engagement_rate']:>12,.2%}",
        f"  Weighted E.R.:     {derived['weighted_engagement_rate']:>12,.2%}",
        f"  Velocity EMA:      {derived['heat_velocity_ema_per_min']:>12,.1f}  per minute",
        f"  Stage:             {derived['stage']:>12s}",
        f"  Coverage:          replies {derived['observed_reply_coverage']:.0%} / quotes {derived['observed_quote_coverage']:.0%}",
    ]
    lines += ["", "  Score breakdown:"]
    for key in ("views", "likes", "rts", "replies", "quotes", "bookmarks", "bonus"):
        lines.append(f"    {key:8s} {derived['heat_components'].get(key, 0):>10,.1f}")
    lines += ["", "═" * 60]
    return "\n".join(lines) + "\n"


def compute_wiener_index(nodes_by_parent: dict, root_id: str) -> float:
    adj = defaultdict(set)
    all_node_ids = {root_id}
    for parent_id, children in nodes_by_parent.items():
        all_node_ids.add(parent_id)
        for child in children:
            cid = child["tweet_id"]
            all_node_ids.add(cid)
            adj[parent_id].add(cid)
            adj[cid].add(parent_id)
    if len(all_node_ids) < 2:
        return 0.0

    total_dist = 0
    pair_count = 0
    nodes_list = list(all_node_ids)
    for idx, src in enumerate(nodes_list):
        dists = {src: 0}
        queue = deque([src])
        while queue:
            cur = queue.popleft()
            for nb in adj.get(cur, ()):
                if nb not in dists:
                    dists[nb] = dists[cur] + 1
                    queue.append(nb)
        for j in range(idx + 1, len(nodes_list)):
            dist = dists.get(nodes_list[j])
            if dist is not None:
                total_dist += dist
                pair_count += 1
    return total_dist / pair_count if pair_count else 0.0


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


def compute_cascade_metrics(root_id: str, direct_replies: list[dict], direct_quotes: list[dict], sub_nodes_by_parent: dict, root_author_followers: int = 0) -> dict:
    nodes_by_parent = defaultdict(list)
    for row in direct_replies:
        nodes_by_parent[root_id].append(row)
    for row in direct_quotes:
        nodes_by_parent[root_id].append(row)
    for parent_id, children in sub_nodes_by_parent.items():
        nodes_by_parent[parent_id].extend(children)

    layer_0 = 1
    layer_1 = len(direct_replies) + len(direct_quotes)
    layer_2 = sum(len(children) for children in sub_nodes_by_parent.values())
    wiener = compute_wiener_index(nodes_by_parent, root_id)

    engagers = set()
    for row in direct_replies + direct_quotes:
        handle = row.get("author_username", "")
        if handle:
            engagers.add(handle)
    for children in sub_nodes_by_parent.values():
        for row in children:
            handle = row.get("author_username", "")
            if handle:
                engagers.add(handle)

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

    for row in direct_quotes:
        register_author(row.get("author_username", ""), safe_int(row.get("author_followers", 0)), "l1_quote", 1.0)
    for row in direct_replies:
        register_author(row.get("author_username", ""), safe_int(row.get("author_followers", 0)), "l1_reply", 0.12)
    for children in sub_nodes_by_parent.values():
        for row in children:
            edge_type = row.get("edge_type", "reply")
            detail_key = "l2_quote" if edge_type == "quote" else "l2_reply"
            weight = 0.35 if edge_type == "quote" else 0.03
            register_author(row.get("author_username", ""), safe_int(row.get("author_followers", 0)), detail_key, weight)

    potential_gross = sum(potential_detail.values())
    overlap_factor = compute_overlap_factor({k: v["contribution"] for k, v in author_best.items()})
    potential_score = int(potential_gross * overlap_factor)

    return {
        "cascade_size": layer_0 + layer_1 + layer_2,
        "cascade_max_depth": 2 if layer_2 else (1 if layer_1 else 0),
        "cascade_breadth_per_layer": [layer_0, layer_1, layer_2],
        "structural_virality_wiener": round(wiener, 3),
        "unique_engager_count": len(engagers),
        "distribution_potential_gross": potential_gross,
        "distribution_potential_score": potential_score,
        "distribution_potential_overlap_discount": round(overlap_factor, 2),
        "distribution_potential_detail": potential_detail,
        "distribution_potential_accounts": len(author_best),
        "reach_gross": potential_gross,
        "reach_adjusted": potential_score,
        "reach_overlap_discount": round(overlap_factor, 2),
        "reach_detail": potential_detail,
        "reach_observed_accounts": len(author_best),
        "reach_followers_sum": potential_score,
    }


def rebuild_derived(tweet_dir: Path, state: dict, config: dict) -> tuple[list[dict], dict]:
    metrics_rows = load_jsonl(tweet_dir / "metrics.jsonl")
    replies = sorted(load_jsonl(tweet_dir / "replies.jsonl"), key=lambda row: event_dt(row, "fetched_at", "created_at"))
    quotes = sorted(load_jsonl(tweet_dir / "quotes.jsonl"), key=lambda row: event_dt(row, "fetched_at", "created_at"))

    reply_idx = 0
    quote_idx = 0
    cumulative_replies: list[dict] = []
    cumulative_quotes: list[dict] = []
    seen_reply_ids: set[str] = set()
    seen_quote_ids: set[str] = set()

    derived_rows: list[dict] = []
    prev_metrics: dict | None = None
    prev_heat = 0.0
    prev_ts: str | None = None
    prev_velocity_ema: float | None = None

    for metrics in metrics_rows:
        ts = metrics.get("ts")
        if not ts:
            continue
        cur_dt = parse_iso_datetime(ts)
        if not cur_dt:
            continue

        new_replies = 0
        new_quotes = 0

        while reply_idx < len(replies) and event_dt(replies[reply_idx], "fetched_at", "created_at") <= cur_dt:
            rec = replies[reply_idx]
            reply_idx += 1
            tid = str(rec.get("tweet_id") or "")
            if tid and tid not in seen_reply_ids:
                seen_reply_ids.add(tid)
                cumulative_replies.append(rec)
                new_replies += 1

        while quote_idx < len(quotes) and event_dt(quotes[quote_idx], "fetched_at", "created_at") <= cur_dt:
            rec = quotes[quote_idx]
            quote_idx += 1
            tid = str(rec.get("tweet_id") or "")
            if tid and tid not in seen_quote_ids:
                seen_quote_ids.add(tid)
                cumulative_quotes.append(rec)
                new_quotes += 1

        post_created_at = metrics.get("created_at", "")
        heat_result = compute_heat(metrics, cumulative_replies, cumulative_quotes, post_created_at)
        heat = heat_result["heat_raw"]
        heat_delta = 0.0
        velocity = 0.0
        if prev_ts is not None:
            last_ts_dt = parse_iso_datetime(prev_ts)
            if last_ts_dt:
                elapsed_min = (cur_dt - last_ts_dt).total_seconds() / 60.0
                heat_delta = heat - prev_heat
                if elapsed_min > 0:
                    velocity = heat_delta / elapsed_min

        growth = compute_growth_metrics(metrics, prev_metrics, ts, prev_ts)
        velocity_ema = smooth_velocity(velocity, prev_velocity_ema)
        stage = classify_stage(velocity, velocity_ema, growth)

        derived = {
            "ts": ts,
            "heat_score": heat,
            "heat_delta": round(heat_delta, 2),
            "heat_velocity_per_min": round(velocity, 4),
            "heat_velocity_ema_per_min": round(velocity_ema, 4),
            "engagement_rate": compute_engagement_rate(metrics),
            "weighted_engagement_rate": compute_weighted_engagement_rate(metrics),
            "heat_components": heat_result["components"],
            "observed_reply_coverage": heat_result["coverage"]["replies"],
            "observed_quote_coverage": heat_result["coverage"]["quotes"],
            "scoring_version": "xhi-v3",
            "stage": stage,
            "view_count": safe_int(metrics.get("view_count", 0)),
            "favorite_count": safe_int(metrics.get("favorite_count", 0)),
            "retweet_count": safe_int(metrics.get("retweet_count", 0)),
            "reply_count": safe_int(metrics.get("reply_count", 0)),
            "quote_count": safe_int(metrics.get("quote_count", 0)),
            "bookmark_count": safe_int(metrics.get("bookmark_count", 0)),
            "new_replies_this_cycle": new_replies,
            "new_quotes_this_cycle": new_quotes,
            "cumulative_replies_seen": len(seen_reply_ids),
            "cumulative_quotes_seen": len(seen_quote_ids),
            **growth,
        }
        derived_rows.append(derived)
        prev_metrics = metrics
        prev_heat = heat
        prev_ts = ts
        prev_velocity_ema = velocity_ema

    state_out = dict(state)
    if derived_rows and metrics_rows:
        state_out["last_heat"] = derived_rows[-1]["heat_score"]
        state_out["last_ts"] = derived_rows[-1]["ts"]
        state_out["last_metrics"] = metrics_rows[-1]
        state_out["last_heat_velocity_ema"] = derived_rows[-1]["heat_velocity_ema_per_min"]
        state_out["started_at"] = state_out.get("started_at") or config.get("tracker_started_at") or metrics_rows[0].get("ts") or now_iso()
        state_out["cycle_count"] = len(metrics_rows)
    return derived_rows, state_out


def rebuild_cascade(tweet_id: str, tweet_dir: Path) -> list[dict]:
    old_rows = load_jsonl(tweet_dir / "cascade_metrics.jsonl")
    if not old_rows:
        return []

    metrics_rows = load_jsonl(tweet_dir / "metrics.jsonl")
    replies = sorted(load_jsonl(tweet_dir / "replies.jsonl"), key=lambda row: event_dt(row, "fetched_at", "created_at"))
    quotes = sorted(load_jsonl(tweet_dir / "quotes.jsonl"), key=lambda row: event_dt(row, "fetched_at", "created_at"))
    sub_nodes = sorted(load_jsonl(tweet_dir / "cascade_nodes.jsonl"), key=lambda row: event_dt(row, "fetched_at", "discovered_at", "created_at"))

    reply_idx = 0
    quote_idx = 0
    sub_idx = 0
    direct_replies: list[dict] = []
    direct_quotes: list[dict] = []
    sub_by_parent: dict[str, list[dict]] = defaultdict(list)
    metric_idx = 0
    last_metric_row: dict | None = None
    prev_ts_dt: datetime | None = None
    rebuilt: list[dict] = []

    for idx, old_row in enumerate(old_rows, start=1):
        ts = old_row.get("ts")
        cur_dt = parse_iso_datetime(ts or "")
        if not ts or not cur_dt:
            continue

        while reply_idx < len(replies) and event_dt(replies[reply_idx], "fetched_at", "created_at") <= cur_dt:
            direct_replies.append(replies[reply_idx])
            reply_idx += 1
        while quote_idx < len(quotes) and event_dt(quotes[quote_idx], "fetched_at", "created_at") <= cur_dt:
            direct_quotes.append(quotes[quote_idx])
            quote_idx += 1

        new_sub_nodes = 0
        while sub_idx < len(sub_nodes) and event_dt(sub_nodes[sub_idx], "fetched_at", "discovered_at", "created_at") <= cur_dt:
            row = sub_nodes[sub_idx]
            sub_idx += 1
            parent_id = row.get("parent_id")
            if parent_id:
                sub_by_parent[parent_id].append(row)
            if prev_ts_dt is None or event_dt(row, "fetched_at", "discovered_at", "created_at") > prev_ts_dt:
                new_sub_nodes += 1

        while metric_idx < len(metrics_rows):
            row_dt = parse_iso_datetime(metrics_rows[metric_idx].get("ts", ""))
            if not row_dt or row_dt > cur_dt:
                break
            last_metric_row = metrics_rows[metric_idx]
            metric_idx += 1
        root_author_followers = safe_int((last_metric_row or {}).get("author_followers", 0))

        cascade_metrics = compute_cascade_metrics(
            root_id=tweet_id,
            direct_replies=direct_replies,
            direct_quotes=direct_quotes,
            sub_nodes_by_parent=sub_by_parent,
            root_author_followers=root_author_followers,
        )
        cascade_metrics["ts"] = ts
        cascade_metrics["cycle"] = old_row.get("cycle", idx)
        cascade_metrics["new_sub_nodes_this_cycle"] = new_sub_nodes
        cascade_metrics["walked_nodes_this_cycle"] = old_row.get("walked_nodes_this_cycle", 0)
        rebuilt.append(cascade_metrics)
        prev_ts_dt = cur_dt

    return rebuilt


def tweet_dirs_for_args(data_dir: Path, tweet_ids: list[str] | None) -> list[Path]:
    if tweet_ids:
        return [data_dir / tid for tid in tweet_ids if (data_dir / tid).is_dir()]
    return sorted([path for path in data_dir.iterdir() if path.is_dir()])


def process_tweet_dir(tweet_dir: Path, backup: bool) -> dict:
    tweet_id = tweet_dir.name
    state_path = tweet_dir / "state.json"
    derived_path = tweet_dir / "derived.jsonl"
    cascade_path = tweet_dir / "cascade_metrics.jsonl"
    dashboard_path = tweet_dir / "dashboard.txt"
    metrics_path = tweet_dir / "metrics.jsonl"

    state = load_json(state_path)
    config = load_json(tweet_dir / "config.json")
    derived_rows, new_state = rebuild_derived(tweet_dir, state, config)
    if not derived_rows:
        return {"tweet_id": tweet_id, "status": "skipped", "reason": "no metrics"}

    cascade_rows = rebuild_cascade(tweet_id, tweet_dir)

    backups: list[str] = []
    if backup:
        for path in (derived_path, cascade_path):
            copy = backup_file(path)
            if copy:
                backups.append(str(copy))

    write_jsonl(derived_path, derived_rows)
    if cascade_rows:
        write_jsonl(cascade_path, cascade_rows)
    write_json(state_path, new_state)

    latest_metrics = load_jsonl(metrics_path)[-1]
    latest_derived = derived_rows[-1]
    dashboard_state = {
        "started_at": new_state.get("started_at", now_iso()),
        "cycle_count": len(load_jsonl(metrics_path)),
    }
    dashboard_path.write_text(
        render_dashboard(
            tweet_id=tweet_id,
            metrics=latest_metrics,
            derived=latest_derived,
            state=dashboard_state,
            new_replies=latest_derived.get("new_replies_this_cycle", 0),
            new_quotes=latest_derived.get("new_quotes_this_cycle", 0),
        )
    )

    return {
        "tweet_id": tweet_id,
        "status": "ok",
        "derived_rows": len(derived_rows),
        "cascade_rows": len(cascade_rows),
        "backups": backups,
        "latest_stage": latest_derived.get("stage"),
        "latest_heat": round(latest_derived.get("heat_score", 0), 2),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="/opt/tweet-tracker/data", help="Tweet tracker data root")
    parser.add_argument("--tweet-id", action="append", help="Specific tweet ID to rebuild; repeatable")
    parser.add_argument("--no-backup", action="store_true", help="Rewrite files without creating .bak copies")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"ERROR: data dir not found: {data_dir}")
        return 1

    targets = tweet_dirs_for_args(data_dir, args.tweet_id)
    if not targets:
        print("ERROR: no tweet directories matched")
        return 1

    results = []
    for tweet_dir in targets:
        result = process_tweet_dir(tweet_dir, backup=not args.no_backup)
        results.append(result)
        if result["status"] == "ok":
            print(
                f"[ok] {result['tweet_id']}: derived={result['derived_rows']} "
                f"cascade={result['cascade_rows']} stage={result['latest_stage']} "
                f"heat={result['latest_heat']}"
            )
        else:
            print(f"[skip] {result['tweet_id']}: {result.get('reason', 'unknown')}")

    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
