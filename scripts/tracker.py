#!/usr/bin/env python3
"""Tweet Tracker — high-frequency snapshot of one tweet's engagement.

Phase 1 only (Phase 2 cascade walker is a separate script).

API: Twitter241 (RapidAPI) — supports cursor pagination for replies/quotes.

Required env:
  TWITTER241_RAPIDAPI_KEY    Twitter241 API key
  TWEET_ID                   target tweet pid

Optional env:
  TWITTER241_RAPIDAPI_KEY_FALLBACK   optional fallback key
  DATA_DIR                   default /opt/tweet-tracker/data
  TRACKER_SCHEDULE           cumulative phases: until:interval:max_pages,...
                             default 1h:300s:5,6h:900s:5,24h:1800s:3,72h:3600s:2
  TRACKING_RETENTION         default 72h
  SNAPSHOT_INTERVAL_SEC      legacy fixed interval override
  MAX_PAGES_PER_CYCLE        legacy/default pagination cap
"""

import json
import math
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from campaign_core.collection_state import append_collection_event
from tracking_schedule import age_seconds, format_duration, load_tracker_policy

# Hard socket timeout
socket.setdefaulttimeout(20)

# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────
KEY_PRIMARY = os.environ["TWITTER241_RAPIDAPI_KEY"]
KEY_FALLBACK = os.environ.get("TWITTER241_RAPIDAPI_KEY_FALLBACK", "")
HOST = "twitter241.p.rapidapi.com"
TWEET_ID = os.environ["TWEET_ID"]

_active_key = KEY_PRIMARY
_using_fallback = False
DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/tweet-tracker/data"))
CAMPAIGN_ID = os.environ.get("CAMPAIGN_ID", "").strip()
XHI_CAMPAIGN_DIR = os.environ.get("XHI_CAMPAIGN_DIR", "").strip()
TRACKING_POLICY = load_tracker_policy(os.environ)
TRACKER_ONESHOT = os.environ.get("TRACKER_ONESHOT", "0").strip().lower() in {"1", "true", "yes"}

TWEET_DIR = DATA_DIR / TWEET_ID
TWEET_DIR.mkdir(parents=True, exist_ok=True)

METRICS_FILE = TWEET_DIR / "metrics.jsonl"
REPLIES_FILE = TWEET_DIR / "replies.jsonl"
QUOTES_FILE = TWEET_DIR / "quotes.jsonl"
DERIVED_FILE = TWEET_DIR / "derived.jsonl"
ERRORS_FILE = TWEET_DIR / "tracker_errors.jsonl"
STATE_FILE = TWEET_DIR / "state.json"
CONFIG_FILE = TWEET_DIR / "config.json"
DASHBOARD_FILE = TWEET_DIR / "dashboard.txt"


def campaign_dir() -> Path | None:
    if XHI_CAMPAIGN_DIR:
        return Path(XHI_CAMPAIGN_DIR)
    if CAMPAIGN_ID:
        return DATA_DIR / "campaign_graphs" / CAMPAIGN_ID
    return None


def emit_collection_event(event: str, **fields) -> None:
    cdir = campaign_dir()
    if cdir is None:
        return
    append_collection_event(
        cdir,
        {
            "event": event,
            "campaign_id": CAMPAIGN_ID or cdir.name,
            "tweet_id": TWEET_ID,
            **fields,
        },
    )

# ──────────────────────────────────────────────────────────────
# XHI™ Signal Weight Framework v2
# ──────────────────────────────────────────────────────────────
W_VIEWS = 0.01
W_LIKES = 1.0
W_RTS = 2.0
W_REPLIES = 3.0
W_QUOTES = 5.0
W_QUOTES_WITH_COMMENTARY = 7.0
W_REPLY_CATALYST = 4.5
W_BOOKMARKS = 2.0

QUALITY_TIERS = [
    (100_000, 3.0),
    (10_000,  1.5),
    (1_000,   1.0),
    (0,       0.8),
]
QUALITY_NEW_ACCOUNT_DAYS = 30
QUALITY_NEW_ACCOUNT_MULTIPLIER = 0.3

TEMPORAL_DECAY = [
    (1,   1.0),
    (6,   0.85),
    (24,  0.5),
    (72,  0.2),
    (None, 0.05),
]

BONUS_QUOTE_AND_REPLY = 2.0
BONUS_CATALYST_QUOTE = 3.0
BONUS_BURST_QUOTES = 5.0
BONUS_RT_TO_QUOTE_UPGRADE = 4.0
COMMENTARY_MIN_CHARS = 50
VELOCITY_EMA_ALPHA = 0.35


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
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
        except Exception as e:
            last_err = e
            raise
    raise RuntimeError(f"call_api failed: {last_err}")


# ──────────────────────────────────────────────────────────────
# Parsers (Twitter241 nested format)
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
        "bookmark_count": legacy.get("bookmark_count", 0),
    }


def record_matches_relation(rec: dict, target_tid: str, relation: str) -> bool:
    if not rec or not target_tid:
        return False
    if relation == "quote":
        return rec.get("quoted_status_id") == target_tid
    if relation == "reply":
        in_reply_to = rec.get("in_reply_to_status_id")
        conversation_id = rec.get("conversation_id")
        quoted_status_id = rec.get("quoted_status_id")
        return in_reply_to == target_tid or (
            conversation_id == target_tid and not quoted_status_id and not in_reply_to
        )
    return False


def _parse_item_content(ic: dict, root_tid: str, relation: str) -> dict | None:
    result = ic.get("tweet_results", {}).get("result", ) or {}
    if result.get("__typename") == "TweetWithVisibilityResults":
        result = result.get("tweet", {}) or {}
    rec = parse_tweet_node(result)
    if rec and rec.get("tweet_id") and rec["tweet_id"] != root_tid and record_matches_relation(rec, root_tid, relation):
        return rec
    return None


def extract_tweets_from_instructions(instructions: list, root_tid: str, relation: str) -> tuple[list[dict], str | None]:
    tweets = []
    next_cursor = None
    for inst in instructions or []:
        for entry in inst.get("entries", []):
            eid = entry.get("entryId", "")
            content = entry.get("content", {}) or {}
            if "cursor-bottom" in eid:
                next_cursor = content.get("value") or content.get("itemContent", {}).get("value")
                continue
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
    return tweets, next_cursor


# ──────────────────────────────────────────────────────────────
# Fetch functions (Twitter241 with pagination)
# ──────────────────────────────────────────────────────────────
def fetch_root_metrics() -> dict | None:
    data = call_api(f"/tweet?pid={TWEET_ID}")
    inst = (data.get("data", {})
            .get("threaded_conversation_with_injections_v2", {})
            .get("instructions", []))
    for i in inst:
        for entry in i.get("entries", []):
            if entry.get("entryId") == f"tweet-{TWEET_ID}":
                content = entry.get("content", {}) or {}
                result = (content.get("itemContent", {})
                          .get("tweet_results", {}).get("result", {}))
                if result.get("__typename") == "TweetWithVisibilityResults":
                    result = result.get("tweet", {})
                return parse_tweet_node(result)
    return None


def fetch_replies_page(cursor: str = "") -> tuple[list[dict], str | None]:
    path = f"/comments?pid={TWEET_ID}&count=20"
    if cursor:
        path += f"&cursor={urllib.parse.quote(cursor)}"
    data = call_api(path)
    inst = data.get("result", {}).get("instructions", [])
    return extract_tweets_from_instructions(inst, TWEET_ID, "reply")


def fetch_quotes_page(cursor: str = "") -> tuple[list[dict], str | None]:
    path = f"/quotes?pid={TWEET_ID}&count=20"
    if cursor:
        path += f"&cursor={urllib.parse.quote(cursor)}"
    data = call_api(path)
    inst = data.get("result", {}).get("timeline", {}).get("instructions", [])
    return extract_tweets_from_instructions(inst, TWEET_ID, "quote")


# ──────────────────────────────────────────────────────────────
# State & Helpers
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


def interaction_timestamp(rec: dict) -> str:
    return rec.get("created_at") or rec.get("fetched_at") or ""


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
        current = safe_int(metrics.get(field, 0))
        previous = safe_int(last_metrics.get(field, 0))
        growth[delta_key] = current - previous

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
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {
        "started_at": now_iso(),
        "last_metrics": None, "last_heat": 0.0, "last_ts": None,
        "last_heat_velocity_ema": None,
        "replies_cursor": "", "quotes_cursor": "",
        "seen_reply_ids": [], "seen_quote_ids": [],
        "cycle_count": 0,
    }


def save_state(state: dict) -> None:
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(STATE_FILE)


def default_config() -> dict:
    return {
        "tweet_id": TWEET_ID, "tracker_started_at": now_iso(),
        "promotion_started_at": None, "promotion_kol_handles": [],
        "promotion_channels": [], "promotion_budget_usd": None, "notes": "",
    }


def load_config() -> dict:
    if CONFIG_FILE.exists():
        try:
            raw = CONFIG_FILE.read_text().strip()
            if raw:
                cfg = json.loads(raw)
                if isinstance(cfg, dict):
                    return cfg
            print(f"[{now_iso()}] WARN invalid/empty config.json for {TWEET_ID}; rewriting default", flush=True)
        except Exception as exc:
            print(f"[{now_iso()}] WARN could not read config.json for {TWEET_ID}: {exc}; rewriting default", flush=True)
    cfg = default_config()
    CONFIG_FILE.write_text(json.dumps(cfg, indent=2))
    return cfg


def save_config(cfg: dict) -> None:
    CONFIG_FILE.write_text(json.dumps(cfg, indent=2))


def is_promoted(handle: str, cfg: dict) -> bool:
    if not handle:
        return False
    normalized = normalize_handle(handle)
    return normalized in [normalize_handle(h) for h in cfg.get("promotion_kol_handles", [])]


# ──────────────────────────────────────────────────────────────
# XHI v2 Scoring
# ──────────────────────────────────────────────────────────────
def quality_multiplier(followers: int, account_created_at: str = "") -> float:
    q = 1.0
    for threshold, mult in QUALITY_TIERS:
        if followers >= threshold:
            q = mult
            break
    if account_created_at:
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
    cleaned = (text or "").strip()
    return W_REPLY_CATALYST if len(cleaned) >= COMMENTARY_MIN_CHARS else W_REPLIES


def quote_base_weight(text: str) -> float:
    cleaned = (text or "").strip()
    return W_QUOTES_WITH_COMMENTARY if len(cleaned) >= COMMENTARY_MIN_CHARS else W_QUOTES


def compute_interaction_bonus(replies: list, quotes: list) -> float:
    bonus = 0.0
    reply_authors = {normalize_handle(r.get("author_username", "")) for r in replies if r.get("author_username")}
    quote_authors = {normalize_handle(q.get("author_username", "")) for q in quotes if q.get("author_username")}
    bonus += len(reply_authors & quote_authors) * BONUS_QUOTE_AND_REPLY

    catalyst_quotes = sum(1 for q in quotes if quote_base_weight(q.get("text", "")) > W_QUOTES)
    if catalyst_quotes:
        bonus += catalyst_quotes * BONUS_CATALYST_QUOTE

    quote_times = []
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


def compute_heat_v2(metrics: dict, replies: list, quotes: list, post_created_at: str = "") -> dict:
    views_score = W_VIEWS * metrics["view_count"]
    likes_score = W_LIKES * metrics["favorite_count"]
    rts_score = W_RTS * metrics["retweet_count"]
    bookmarks_score = W_BOOKMARKS * metrics["bookmark_count"]

    replies_score = 0.0
    for r in replies:
        q = quality_multiplier(safe_int(r.get("author_followers", 0)), r.get("author_created_at", ""))
        d = temporal_decay(post_created_at, interaction_timestamp(r))
        replies_score += reply_base_weight(r.get("text", "")) * q * d
    replies_score += estimate_missing_score(metrics["reply_count"], len(replies), replies_score, W_REPLIES)

    quotes_score = 0.0
    for q in quotes:
        w = quote_base_weight(q.get("text", ""))
        qm = quality_multiplier(safe_int(q.get("author_followers", 0)), q.get("author_created_at", ""))
        d = temporal_decay(post_created_at, interaction_timestamp(q))
        quotes_score += w * qm * d
    quotes_score += estimate_missing_score(metrics["quote_count"], len(quotes), quotes_score, W_QUOTES)

    bonus = compute_interaction_bonus(replies, quotes)
    heat_raw = views_score + likes_score + rts_score + bookmarks_score + replies_score + quotes_score + bonus
    reply_coverage = coverage_ratio(len(replies), metrics["reply_count"])
    quote_coverage = coverage_ratio(len(quotes), metrics["quote_count"])

    return {
        "heat_raw": heat_raw,
        "components": {
            "views": round(views_score, 2), "likes": round(likes_score, 2),
            "rts": round(rts_score, 2), "replies": round(replies_score, 2),
            "quotes": round(quotes_score, 2), "bookmarks": round(bookmarks_score, 2),
            "bonus": round(bonus, 2),
        },
        "coverage": {"replies": round(reply_coverage, 4), "quotes": round(quote_coverage, 4)},
    }


def compute_engagement_rate(m: dict) -> float:
    if m["view_count"] == 0:
        return 0.0
    return (m["favorite_count"] + m["retweet_count"] + m["reply_count"] + m["quote_count"]) / m["view_count"]


def compute_weighted_engagement_rate(m: dict) -> float:
    if m["view_count"] == 0:
        return 0.0
    weighted = (
        W_LIKES * m["favorite_count"]
        + W_RTS * m["retweet_count"]
        + W_REPLIES * m["reply_count"]
        + W_QUOTES * m["quote_count"]
        + W_BOOKMARKS * m["bookmark_count"]
    )
    return weighted / m["view_count"]


# ──────────────────────────────────────────────────────────────
# Dashboard
# ──────────────────────────────────────────────────────────────
def render_dashboard(metrics, derived, state, cfg, new_replies, new_quotes):
    lines = [
        "═" * 60,
        f" XHI Tweet Tracker — {TWEET_ID}",
        f" Updated: {derived['ts']}",
        f" API: Twitter241",
        "═" * 60, "",
        f" Author:        @{metrics.get('author_username', '?')} ({metrics.get('author_followers', 0):,} followers)",
        f" Cycle:         #{state['cycle_count']}",
        f" Tracker age:   {state['started_at']} → now",
        f" Schedule:      {state.get('last_schedule_phase', 'n/a')}",
        "",
        "─" * 60, " Raw counts", "─" * 60,
        f"  views:     {metrics['view_count']:>12,}",
        f"  likes:     {metrics['favorite_count']:>12,}",
        f"  retweets:  {metrics['retweet_count']:>12,}",
        f"  replies:   {metrics['reply_count']:>12,}  (+{new_replies} new)",
        f"  quotes:    {metrics['quote_count']:>12,}  (+{new_quotes} new)",
        f"  bookmarks: {metrics['bookmark_count']:>12,}", "",
        "─" * 60, " Derived (XHI v2)", "─" * 60,
        f"  Heat Score:        {derived['heat_score']:>12,.0f}",
        f"  Heat Delta:        {derived['heat_delta']:>12,.1f}",
        f"  Heat Velocity:     {derived['heat_velocity_per_min']:>12,.1f}  per minute",
        f"  Engagement Rate:   {derived['engagement_rate']:>12,.2%}",
        f"  Weighted E.R.:     {derived['weighted_engagement_rate']:>12,.2%}",
        f"  Velocity EMA:      {derived['heat_velocity_ema_per_min']:>12,.1f}  per minute",
        f"  Stage:             {derived['stage']:>12s}",
        f"  Coverage:          replies {derived['observed_reply_coverage']:.0%} / quotes {derived['observed_quote_coverage']:.0%}",
    ]
    components = derived.get("heat_components", {})
    if components:
        lines += ["", "  Score breakdown:"]
        for k in ("views", "likes", "rts", "replies", "quotes", "bookmarks", "bonus"):
            lines.append(f"    {k:8s} {components.get(k, 0):>10,.1f}")
    lines += ["", "═" * 60]
    return "\n".join(lines) + "\n"


# ──────────────────────────────────────────────────────────────
# Main loop
# ──────────────────────────────────────────────────────────────
def cycle(state, cfg, phase):
    ts = now_iso()
    state["cycle_count"] += 1
    state["last_schedule_phase"] = phase.label
    state["last_schedule_interval_sec"] = phase.interval_seconds
    state["last_max_pages_per_endpoint"] = phase.max_pages
    max_pages = phase.max_pages or 1

    try:
        metrics = fetch_root_metrics()
    except Exception as exc:
        append_jsonl(ERRORS_FILE, {
            "ts": ts,
            "tweet_id": TWEET_ID,
            "stage": "root_metrics",
            "status": "root_metric_fetch_error",
            "error_type": exc.__class__.__name__,
            "error": str(exc),
        })
        emit_collection_event(
            "tracker_root_fetch_failed",
            endpoint="tweet",
            status="endpoint_failed",
            error_type=exc.__class__.__name__,
            error=str(exc),
        )
        print(f"[{ts}] WARN: root metrics failed for {TWEET_ID}: {exc.__class__.__name__}: {exc}", flush=True)
        return
    if not metrics:
        append_jsonl(ERRORS_FILE, {
            "ts": ts,
            "tweet_id": TWEET_ID,
            "stage": "root_metrics",
            "status": "root_metric_unavailable",
            "error": "tweet payload did not contain the requested root tweet",
        })
        emit_collection_event(
            "tracker_root_fetch_failed",
            endpoint="tweet",
            status="fetch_failed",
            error="tweet payload did not contain the requested root tweet",
        )
        print(f"[{ts}] WARN: root metrics unavailable for {TWEET_ID}; skipping", flush=True)
        return
    metrics["ts"] = ts
    append_jsonl(METRICS_FILE, metrics)

    # Fetch replies (paginated)
    seen_replies = set(state["seen_reply_ids"])
    new_replies = 0
    cursor = state.get("replies_cursor", "") or ""
    for page in range(max_pages):
        try:
            tweets, next_cursor = fetch_replies_page(cursor)
        except Exception as e:
            emit_collection_event(
                "tracker_reply_page_failed",
                endpoint="comments",
                relation="reply",
                status="endpoint_failed",
                page=page + 1,
                error=str(e),
            )
            print(f"[{ts}] WARN: replies page={page}: {e}", flush=True)
            break
        if not tweets:
            break
        any_new = False
        for t in tweets:
            tid = t["tweet_id"]
            if tid and tid not in seen_replies:
                seen_replies.add(tid)
                t["fetched_at"] = ts
                t["is_promoted_kol"] = is_promoted(t.get("author_username", ""), cfg)
                append_jsonl(REPLIES_FILE, t)
                new_replies += 1
                any_new = True
        if not any_new or not next_cursor:
            break
        cursor = next_cursor
    state["replies_cursor"] = cursor
    state["seen_reply_ids"] = list(seen_replies)

    # Fetch quotes (paginated)
    seen_quotes = set(state["seen_quote_ids"])
    new_quotes = 0
    cursor = state.get("quotes_cursor", "") or ""
    for page in range(max_pages):
        try:
            tweets, next_cursor = fetch_quotes_page(cursor)
        except Exception as e:
            emit_collection_event(
                "tracker_quote_page_failed",
                endpoint="quotes",
                relation="quote",
                status="endpoint_failed",
                page=page + 1,
                error=str(e),
            )
            print(f"[{ts}] WARN: quotes page={page}: {e}", flush=True)
            break
        if not tweets:
            break
        any_new = False
        for t in tweets:
            tid = t["tweet_id"]
            if tid and tid not in seen_quotes:
                seen_quotes.add(tid)
                t["fetched_at"] = ts
                t["is_promoted_kol"] = is_promoted(t.get("author_username", ""), cfg)
                append_jsonl(QUOTES_FILE, t)
                new_quotes += 1
                any_new = True
        if not any_new or not next_cursor:
            break
        cursor = next_cursor
    state["quotes_cursor"] = cursor
    state["seen_quote_ids"] = list(seen_quotes)

    # XHI v2 scoring
    all_replies = load_jsonl(REPLIES_FILE)
    all_quotes = load_jsonl(QUOTES_FILE)
    post_created_at = metrics.get("created_at", "")

    heat_result = compute_heat_v2(metrics, all_replies, all_quotes, post_created_at)
    heat = heat_result["heat_raw"]
    heat_delta = 0.0
    velocity = 0.0
    if state.get("last_ts") is not None:
        last_ts_dt = parse_iso_datetime(state["last_ts"])
        cur_ts_dt = parse_iso_datetime(ts)
        elapsed_min = 0.0
        if last_ts_dt and cur_ts_dt:
            elapsed_min = (cur_ts_dt - last_ts_dt).total_seconds() / 60.0
        heat_delta = heat - state["last_heat"]
        if elapsed_min > 0:
            velocity = heat_delta / elapsed_min

    growth = compute_growth_metrics(metrics, state.get("last_metrics"), ts, state.get("last_ts"))
    velocity_ema = smooth_velocity(velocity, state.get("last_heat_velocity_ema"))
    stage = classify_stage(velocity, velocity_ema, growth)

    derived = {
        "ts": ts, "heat_score": heat, "heat_delta": round(heat_delta, 2),
        "heat_velocity_per_min": round(velocity, 4),
        "heat_velocity_ema_per_min": round(velocity_ema, 4),
        "engagement_rate": compute_engagement_rate(metrics),
        "weighted_engagement_rate": compute_weighted_engagement_rate(metrics),
        "heat_components": heat_result["components"],
        "observed_reply_coverage": heat_result["coverage"]["replies"],
        "observed_quote_coverage": heat_result["coverage"]["quotes"],
        "scoring_version": "xhi-v3",
        "stage": stage,
        "view_count": metrics["view_count"],
        "favorite_count": metrics["favorite_count"],
        "retweet_count": metrics["retweet_count"],
        "reply_count": metrics["reply_count"],
        "quote_count": metrics["quote_count"],
        "bookmark_count": metrics["bookmark_count"],
        "new_replies_this_cycle": new_replies,
        "new_quotes_this_cycle": new_quotes,
        "cumulative_replies_seen": len(seen_replies),
        "cumulative_quotes_seen": len(seen_quotes),
        "schedule_phase": phase.label,
        "schedule_interval_sec": phase.interval_seconds,
        "max_pages_per_endpoint": max_pages,
        "tracking_age_hours": round(age_seconds(state.get("started_at"), parse_iso_datetime(ts)) / 3600.0, 4),
        **growth,
    }
    append_jsonl(DERIVED_FILE, derived)

    state["last_heat"] = heat
    state["last_ts"] = ts
    state["last_metrics"] = metrics
    state["last_heat_velocity_ema"] = velocity_ema
    save_state(state)

    DASHBOARD_FILE.write_text(render_dashboard(metrics, derived, state, cfg, new_replies, new_quotes))

    print(
        f"[{ts}] cycle #{state['cycle_count']}: heat={heat:.0f} delta={heat_delta:.0f} "
        f"velocity={velocity:.1f}/min views={metrics['view_count']:,} "
        f"likes={metrics['favorite_count']:,} RTs={metrics['retweet_count']:,} "
        f"new_replies={new_replies} new_quotes={new_quotes} "
        f"next={format_duration(phase.interval_seconds)} pages={max_pages}",
        flush=True,
    )


def main():
    print("=== XHI Tweet Tracker started ===", flush=True)
    print(f"  TWEET_ID:  {TWEET_ID}", flush=True)
    print(f"  DATA_DIR:  {TWEET_DIR}", flush=True)
    print(f"  POLICY:    {TRACKING_POLICY.name}", flush=True)
    for phase in TRACKING_POLICY.phases:
        print(f"    - {phase.label}", flush=True)
    print(f"  RETENTION: {format_duration(TRACKING_POLICY.stop_after_seconds)}", flush=True)
    print(f"  ONESHOT:   {TRACKER_ONESHOT}", flush=True)
    print(f"  API:       Twitter241", flush=True)

    state = load_state()
    if not state.get("started_at"):
        state["started_at"] = now_iso()
    cfg = load_config()
    save_config(cfg)

    while True:
        elapsed = age_seconds(state.get("started_at"))
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
            cycle(state, cfg, phase)
            cfg = load_config()
        except Exception as e:
            print(f"[{now_iso()}] ERROR cycle: {e}", flush=True)
            import traceback
            traceback.print_exc()
        if TRACKER_ONESHOT:
            state["stopped_at"] = now_iso()
            state["stop_reason"] = "oneshot collection completed"
            save_state(state)
            print(f"[{state['stopped_at']}] oneshot collection completed; stopping", flush=True)
            return
        time.sleep(phase.interval_seconds)


if __name__ == "__main__":
    main()
