#!/usr/bin/env python3
"""x-heat-index frontend — single-file HTTP server + dashboard.

Serves:
  GET /                        — dashboard HTML
  GET /api/tweets              — list all tracked tweets (for tweet selector)
  GET /api/data/<tweet_id>     — derived + cascade_metrics time series + raw metrics

Stdlib only. Chart.js and vanilla JS on the client side. Reads JSONL
files on each request (no caching — data is small and freshness matters).

Required env:
  DATA_DIR        default /opt/tweet-tracker/data
  FRONTEND_PORT   default 3301
  FRONTEND_BIND   default 127.0.0.1 (use 0.0.0.0 ONLY behind a reverse proxy)
"""

import json
import mimetypes
import os
import subprocess
import sys
import threading
import time
from math import ceil, log1p, sqrt
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse

from campaign_core.identity import has_identity_signal
from campaign_core.paid import PAID_DELIVERABLE_SEED_SOURCE, extract_tweet_id as extract_paid_tweet_id
from campaign_core.paid import has_paid_deliverable_signal, load_paid_deliverables

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/tweet-tracker/data"))
PORT = int(os.environ.get("PORT", os.environ.get("FRONTEND_PORT", "3301")))
BIND = os.environ.get("FRONTEND_BIND", "0.0.0.0")
TRACK_ADMIN_TOKEN = os.environ.get("TRACK_ADMIN_TOKEN", "").strip()
VENDOR_DIR = Path(__file__).resolve().parents[1] / "frontend" / "vendor"
INCLUDE_ALL_TRACKED = os.environ.get("XHI_INCLUDE_ALL_TRACKED", "1").strip().lower() not in {"0", "false", "no"}
ALLOW_UNAUTH_TRACK = os.environ.get("XHI_ALLOW_UNAUTH_TRACK", "0").strip().lower() in {"1", "true", "yes"}
REQUIRE_READ_TOKEN = os.environ.get("XHI_REQUIRE_READ_TOKEN", "0").strip().lower() in {"1", "true", "yes"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default

# Track running tracker/walker processes per tweet_id
_running_trackers: dict[str, subprocess.Popen] = {}
_running_walkers: dict[str, subprocess.Popen] = {}
_lock = threading.Lock()
_jsonl_cache: dict[Path, tuple[int, int, list]] = {}
_jsonl_latest_cache: dict[Path, tuple[int, int, dict]] = {}
_jsonl_cache_lock = threading.Lock()
_campaign_summary_cache: dict[str, tuple[float, dict]] = {}
_campaign_summary_cache_lock = threading.Lock()
CAMPAIGN_SUMMARY_CACHE_TTL_SECONDS = max(0, _env_int("XHI_CAMPAIGN_SUMMARY_CACHE_TTL_SECONDS", 30))
_campaign_configs_cache: tuple[float, tuple, list[dict]] | None = None
_campaign_configs_cache_lock = threading.Lock()
CAMPAIGN_CONFIGS_CACHE_TTL_SECONDS = max(0, _env_int("XHI_CAMPAIGN_CONFIGS_CACHE_TTL_SECONDS", 30))
_campaign_graph_nodes_cache: dict[tuple, list[dict]] = {}
_campaign_graph_nodes_cache_lock = threading.Lock()
CAMPAIGN_GRAPH_NODES_CACHE_MAX_ITEMS = max(1, _env_int("XHI_CAMPAIGN_GRAPH_NODES_CACHE_MAX_ITEMS", 64))
CAMPAIGN_GRAPH_NODES_SNAPSHOT_VERSION = 6
LEGACY_PAID_TIMELINE_BYPASS_REASON = "paid_kol_timeline_bypass"
LEGACY_TIMELINE_BYPASS_SOURCES = {"watch_tweets", "watch_replies", "expanded_author_tweets", "search"}


# ──────────────────────────────────────────────────────────────
# HTML (inline, zero-build, single-file deployment)
# ──────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>x-heat-index · 传播实时监控</title>
<link rel="preload" href="vendor/chart.umd.min.js" as="script">
<link rel="preload" href="vendor/chartjs-adapter-date-fns.bundle.min.js" as="script">
<style>
  :root {
    --bg: #0b1016;
    --surface: #121923;
    --surface-hover: #18212c;
    --border: rgba(159, 178, 201, 0.16);
    --border-strong: rgba(183, 202, 226, 0.26);
    --text: #c9d4e3;
    --text-bright: #eef5ff;
    --muted: #90a0b6;
    --muted-dim: #607086;
    --accent: #ff8e72;
    --green: #63d58c;
    --yellow: #f1c464;
    --red: #ff6f6f;
    --blue: #7bb8ff;
    --purple: #b99bff;
    --grid: rgba(151, 173, 199, 0.12);
    --grid-strong: rgba(151, 173, 199, 0.2);
    --panel-shadow: 0 22px 60px rgba(0, 0, 0, 0.28);
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    padding: 0;
    font-family: -apple-system, BlinkMacSystemFont, "PingFang SC", "Microsoft YaHei", Segoe UI, sans-serif;
    background:
      radial-gradient(circle at top left, rgba(123,184,255,0.12), transparent 28%),
      radial-gradient(circle at top right, rgba(255,142,114,0.12), transparent 24%),
      linear-gradient(180deg, #0a0f15 0%, var(--bg) 18%, #0a1016 100%);
    color: var(--text);
    font-size: 14px;
    line-height: 1.6;
  }
  header {
    border-bottom: 1px solid var(--border);
    padding: 16px 32px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: rgba(11, 16, 22, 0.72);
    backdrop-filter: blur(18px);
    flex-wrap: wrap;
    gap: 12px;
    position: sticky;
    top: 0;
    z-index: 20;
  }
  header h1 {
    margin: 0;
    font-size: 18px;
    font-weight: 600;
    letter-spacing: -0.01em;
  }
  header h1 .accent { color: var(--accent); }
  header h1 .sub { color: var(--muted); font-weight: 400; font-size: 13px; margin-left: 10px; }
  header .meta {
    color: var(--muted);
    font-size: 11px;
    font-family: "SF Mono", Menlo, Consolas, monospace;
  }
  #tweet-selector {
    background: var(--surface);
    border: 1px solid var(--border);
    color: var(--text);
    padding: 8px 12px;
    border-radius: 10px;
    font-family: "SF Mono", Menlo, monospace;
    font-size: 12px;
    min-width: 320px;
    cursor: pointer;
  }
  #view-selector, #campaign-selector {
    background: var(--surface);
    border: 1px solid var(--border);
    color: var(--text);
    padding: 8px 12px;
    border-radius: 10px;
    font-family: "SF Mono", Menlo, monospace;
    font-size: 12px;
    cursor: pointer;
  }
  #campaign-selector { min-width: 260px; }
  main {
    padding: 24px 32px 48px;
    max-width: 1400px;
    margin: 0 auto;
  }

  /* ── Hero status card ── */
  .hero {
    background:
      linear-gradient(135deg, rgba(255,255,255,0.02), transparent 32%),
      linear-gradient(180deg, rgba(17, 23, 32, 0.92), rgba(17, 23, 32, 0.98));
    border: 1px solid var(--border);
    border-left-width: 4px;
    border-radius: 18px;
    padding: 24px 28px;
    margin-bottom: 24px;
    box-shadow: var(--panel-shadow);
  }
  .hero.stage-amplification { border-left-color: var(--green); background: linear-gradient(90deg, rgba(63,185,80,0.08), var(--surface) 40%); }
  .hero.stage-discovery { border-left-color: var(--blue); background: linear-gradient(90deg, rgba(88,166,255,0.08), var(--surface) 40%); }
  .hero.stage-saturation { border-left-color: var(--yellow); background: linear-gradient(90deg, rgba(210,153,34,0.08), var(--surface) 40%); }
  .hero.stage-decay { border-left-color: var(--red); background: linear-gradient(90deg, rgba(248,81,73,0.08), var(--surface) 40%); }
  .hero.stage-dead { border-left-color: var(--muted-dim); background: linear-gradient(90deg, rgba(139,148,158,0.06), var(--surface) 40%); }
  .hero.stage-unknown { border-left-color: var(--muted-dim); }

  .hero-headline {
    font-size: 24px;
    font-weight: 600;
    color: var(--text-bright);
    margin: 0 0 8px 0;
    letter-spacing: -0.01em;
  }
  .hero-emoji { font-size: 28px; margin-right: 8px; vertical-align: middle; }
  .hero-narrative {
    font-size: 14px;
    color: var(--text);
    margin: 12px 0 0 0;
    max-width: 780px;
  }
  .hero-narrative p { margin: 6px 0; }
  /* ── KPI row ── */
  .section-label {
    color: var(--muted);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin: 32px 0 12px;
    font-weight: 600;
  }
  .kpi-row {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
    gap: 12px;
  }
  .kpi {
    background:
      linear-gradient(180deg, rgba(255,255,255,0.02), transparent 42%),
      var(--surface);
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: 16px 20px;
    position: relative;
    cursor: help;
    transition: background 0.12s, border-color 0.12s, transform 0.12s;
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.03);
  }
  .kpi:hover {
    background: var(--surface-hover);
    border-color: var(--border-strong);
    transform: translateY(-1px);
  }
  .kpi .label {
    color: var(--muted);
    font-size: 12px;
    margin-bottom: 6px;
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .kpi .label .q {
    display: inline-block;
    width: 14px;
    height: 14px;
    border: 1px solid var(--muted-dim);
    border-radius: 50%;
    text-align: center;
    line-height: 12px;
    font-size: 10px;
    color: var(--muted-dim);
  }
  .kpi .value {
    font-size: 26px;
    font-weight: 600;
    font-feature-settings: "tnum";
    letter-spacing: -0.02em;
    color: var(--text-bright);
  }
  .kpi .value .unit { font-size: 13px; color: var(--muted); margin-left: 4px; font-weight: 400; }
  .kpi .value .arrow { font-size: 16px; margin-left: 6px; }
  .kpi .arrow.up { color: var(--green); }
  .kpi .arrow.down { color: var(--red); }
  .kpi .arrow.flat { color: var(--muted); }
  .kpi .subvalue {
    color: var(--muted);
    font-size: 11px;
    margin-top: 4px;
  }
  .kpi .tooltip {
    position: absolute;
    top: 100%;
    left: 0;
    right: 0;
    z-index: 10;
    background: #0d1117;
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 10px 14px;
    font-size: 12px;
    color: var(--text);
    box-shadow: 0 6px 20px rgba(0,0,0,0.4);
    opacity: 0;
    transform: translateY(-4px);
    pointer-events: none;
    transition: opacity 0.12s, transform 0.12s;
    margin-top: 6px;
  }
  .kpi:hover .tooltip { opacity: 1; transform: translateY(0); }

  /* ── Charts ── */
  .charts {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 18px;
  }
  .chart-card {
    background:
      radial-gradient(circle at top right, rgba(123,184,255,0.08), transparent 34%),
      linear-gradient(180deg, rgba(255,255,255,0.02), transparent 28%),
      var(--surface);
    border: 1px solid var(--border);
    border-radius: 20px;
    padding: 20px 22px 18px;
    box-shadow: var(--panel-shadow);
    overflow: hidden;
  }
  .chart-card.full { grid-column: 1 / -1; }
  .chart-title {
    font-size: 18px;
    font-weight: 650;
    margin-bottom: 6px;
    color: var(--text-bright);
    letter-spacing: -0.02em;
  }
  .chart-desc {
    font-size: 12px;
    color: var(--muted);
    margin-bottom: 16px;
    line-height: 1.6;
    max-width: 72ch;
  }
  .chart-wrap {
    position: relative;
    height: 310px;
    padding-top: 4px;
  }
  .chart-wrap.tall { height: 380px; }
  .chart-wrap.medium { height: 300px; }

  .empty {
    color: var(--muted);
    text-align: center;
    padding: 60px 0;
    font-style: italic;
  }
  .contribution-list {
    display: grid;
    gap: 10px;
    margin-top: 14px;
  }
  .contribution-row {
    display: grid;
    grid-template-columns: minmax(160px, 1.2fr) 2fr auto;
    align-items: center;
    gap: 14px;
    padding: 12px 0;
    border-bottom: 1px solid rgba(159,178,201,0.1);
  }
  .contribution-row:last-child { border-bottom: 0; }
  .contribution-label {
    font-family: "SF Mono", Menlo, monospace;
    color: var(--text-bright);
    font-size: 12px;
  }
  .contribution-sub {
    color: var(--muted);
    font-size: 11px;
    margin-top: 2px;
  }
  .contribution-bar {
    height: 8px;
    border-radius: 99px;
    background: rgba(123,184,255,0.12);
    overflow: hidden;
  }
  .contribution-fill {
    height: 100%;
    border-radius: inherit;
    background: linear-gradient(90deg, var(--accent), var(--blue));
  }
  .contribution-value {
    color: var(--text);
    font-family: "SF Mono", Menlo, monospace;
    font-size: 12px;
    min-width: 76px;
    text-align: right;
  }
  footer {
    padding: 16px 32px;
    color: var(--muted-dim);
    font-size: 11px;
    text-align: center;
    border-top: 1px solid var(--border);
    margin-top: 32px;
    font-family: "SF Mono", monospace;
  }
  @media (max-width: 900px) {
    .charts { grid-template-columns: 1fr; }
    .chart-card.full { grid-column: 1; }
    header { padding: 12px 16px; }
    main { padding: 16px; }
    .hero { padding: 18px 20px; }
    .hero-headline { font-size: 20px; }
  }
</style>
</head>
<body>
  <header>
    <div>
      <h1 id="app-title">x-heat-<span class="accent">index</span><span class="sub" id="app-subtitle">推文传播实时监控</span></h1>
    </div>
    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
      <input id="tweet-url-input" type="text" placeholder="粘贴推文链接开始追踪…" style="
        background:var(--surface);border:1px solid var(--border);color:var(--text);
        padding:8px 12px;border-radius:6px;font-family:'SF Mono',Menlo,monospace;
        font-size:12px;min-width:320px;outline:none;
      ">
      <button id="track-btn" onclick="startTracking()" style="
        background:var(--accent);color:#fff;border:none;padding:8px 16px;
        border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;
        white-space:nowrap;
      ">开始追踪</button>
      <select id="view-selector">
        <option value="tweet">单条推文</option>
        <option value="campaign">Campaign</option>
      </select>
      <select id="tweet-selector"></select>
      <select id="campaign-selector" style="display:none"></select>
    </div>
    <div class="meta" id="updated-at">—</div>
  </header>

  <main>
    <div id="hero" class="hero stage-unknown">
      <div class="hero-headline"><span class="hero-emoji">⏳</span><span id="hero-title">加载中…</span></div>
      <div class="hero-narrative" id="hero-narrative"></div>
    </div>

    <div class="section-label">关键指标</div>
    <div class="kpi-row" id="kpi-row"></div>

    <div class="section-label">传播曲线</div>
    <div class="charts">
      <div class="chart-card full">
        <div class="chart-title" id="chart-title-heat">热度脉冲 & 传播速度</div>
        <div class="chart-desc" id="chart-desc-heat">橙色看单个采样周期的新增热度，蓝线看平滑后的传播速度。峰值和衰减会比原来更容易读。</div>
        <div class="chart-wrap tall"><canvas id="chart-heat"></canvas></div>
      </div>
      <div class="chart-card">
        <div class="chart-title" id="chart-title-counts">原始互动数据</div>
        <div class="chart-desc" id="chart-desc-counts">曝光单独走右轴，其余互动走左轴。累计型指标改成阶梯线，避免几条线糊成一团。</div>
        <div class="chart-wrap medium"><canvas id="chart-counts"></canvas></div>
      </div>
      <div class="chart-card">
        <div class="chart-title" id="chart-title-cascade">参与账户 & 扩散深度</div>
        <div class="chart-desc" id="chart-desc-cascade">橙线看有多少独立账户真正进场，蓝线看讨论是不是开始形成分叉，而不只是单层广播。</div>
        <div class="chart-wrap medium"><canvas id="chart-cascade"></canvas></div>
      </div>
      <div class="chart-card full">
        <div class="chart-title" id="chart-title-reach">扩散势能 & 讨论规模</div>
        <div class="chart-desc" id="chart-desc-reach">绿色是清洗后的扩散势能分数，金线是讨论节点总数。前者看潜在放大能力，后者看实际网络体量。</div>
        <div class="chart-wrap medium"><canvas id="chart-reach"></canvas></div>
      </div>
    </div>
    <div class="chart-card full" id="campaign-details" style="display:none;margin-top:18px;">
      <div class="chart-title">传播簇归因</div>
      <div class="chart-desc">按观测节点所属的传播簇做归因，先看哪类传播在贡献热度，再回看具体节点。</div>
      <div class="contribution-list" id="campaign-contributions"></div>
    </div>
  </main>

  <footer>
    每 30 秒自动刷新 · <span id="footer-info">—</span>
</footer>

<script src="vendor/chart.umd.min.js"></script>
<script src="vendor/chartjs-adapter-date-fns.bundle.min.js"></script>
<!-- XHI_RUNTIME_CONFIG -->
<script>
const $ = (id) => document.getElementById(id);
const fmt = (n) => new Intl.NumberFormat("zh-CN").format(Math.round(n));
const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (ch) => ({
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;",
}[ch]));
const AUTO_BASE = (() => {
  const parts = window.location.pathname.split("/").filter(Boolean);
  if (!parts.length || parts[0].includes(".")) return "";
  return `/${parts[0]}`;
})();
const API_BASE = String(window.__XHI_API_BASE__ || AUTO_BASE).replace(/\/$/, "");
const TRACK_ENABLED = !["0", "false", "no"].includes(String(window.__XHI_TRACK_ENABLED__ ?? "1").toLowerCase());
const ALLOWED_VIEWS = (() => {
  const raw = window.__XHI_ALLOWED_VIEWS__;
  const fallback = ["tweet", "campaign"];
  if (Array.isArray(raw)) {
    const values = raw.map(v => String(v || "").trim()).filter(Boolean);
    return values.length ? values : fallback;
  }
  if (typeof raw === "string") {
    const values = raw.split(",").map(v => v.trim()).filter(Boolean);
    return values.length ? values : fallback;
  }
  return fallback;
})();
const DEFAULT_VIEW = ALLOWED_VIEWS.includes(String(window.__XHI_DEFAULT_VIEW__ || "").trim())
  ? String(window.__XHI_DEFAULT_VIEW__).trim()
  : (ALLOWED_VIEWS[0] || "tweet");
const APP_SUBTITLE = String(window.__XHI_APP_SUBTITLE__ || "推文传播实时监控");
const APP_TITLE = String(window.__XHI_APP_TITLE__ || "x-heat-index");
const TRACK_TOKEN_KEY = "xhi-track-token";
const URL_PARAMS = new URLSearchParams(window.location.search);
const INITIAL_CAMPAIGN_ID = URL_PARAMS.get("campaignId") || URL_PARAMS.get("campaign") || "";
const fmtCompact = (n) => {
  if (n == null) return "—";
  if (Math.abs(n) < 1000) return String(Math.round(n));
  if (Math.abs(n) < 1e4) return (n / 1000).toFixed(1) + "K";
  if (Math.abs(n) < 1e8) return (n / 1e4).toFixed(1) + "万";
  return (n / 1e8).toFixed(2) + "亿";
};
const V2_WEIGHTS = { view: 0.01, like: 1.0, rt: 2.0, reply: 3.0, quote: 5.0, bookmark: 2.0 };

const apiUrl = (path) => `${API_BASE}${path}`;
const supportsView = (view) => ALLOWED_VIEWS.includes(view);

function getStoredTrackToken() {
  if (window.__XHI_TRACK_TOKEN__) return String(window.__XHI_TRACK_TOKEN__).trim();
  try {
    return (localStorage.getItem(TRACK_TOKEN_KEY) || "").trim();
  } catch {
    return "";
  }
}

function persistTrackToken(token) {
  if (!token) return;
  try {
    localStorage.setItem(TRACK_TOKEN_KEY, token);
  } catch {}
}

async function postJsonWithOptionalToken(path, payload) {
  let token = getStoredTrackToken();
  const makeRequest = async (trackToken) => {
    const headers = { "Content-Type": "application/json" };
    if (trackToken) headers.Authorization = `Bearer ${trackToken}`;
    return fetch(apiUrl(path), {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });
  };

  let resp = await makeRequest(token);
  if (resp.status !== 401) return resp;

  const prompted = window.prompt("需要管理 token 才能开始追踪。输入后只保存在当前浏览器。", "");
  if (!prompted || !prompted.trim()) return resp;
  token = prompted.trim();
  persistTrackToken(token);
  return makeRequest(token);
}

async function fetchJsonWithOptionalToken(path) {
  let token = getStoredTrackToken();
  const makeRequest = async (trackToken) => {
    const headers = {};
    if (trackToken) headers.Authorization = `Bearer ${trackToken}`;
    return fetch(apiUrl(path), { headers });
  };

  let resp = await makeRequest(token);
  if (resp.status === 401) {
    const prompted = window.prompt("需要管理 token 才能读取监控数据。输入后只保存在当前浏览器。", "");
    if (!prompted || !prompted.trim()) return { error: "unauthorized" };
    token = prompted.trim();
    persistTrackToken(token);
    resp = await makeRequest(token);
  }
  if (!resp.ok) return { error: `http_${resp.status}` };
  return resp.json();
}

function recalcHeatBase(d) {
  return (d.view_count || 0) * V2_WEIGHTS.view
       + (d.favorite_count || 0) * V2_WEIGHTS.like
       + (d.retweet_count || 0) * V2_WEIGHTS.rt
       + (d.reply_count || 0) * V2_WEIGHTS.reply
       + (d.quote_count || 0) * V2_WEIGHTS.quote
       + (d.bookmark_count || 0) * V2_WEIGHTS.bookmark;
}

function effectiveVelocity(d) {
  if (!d) return 0;
  return d.heat_velocity_ema_per_min ?? d.heat_velocity_per_min ?? 0;
}

const STAGE_META = {
  discovery: { label: "早期发现", emoji: "🌱" },
  amplification: { label: "爆发传播", emoji: "🔥" },
  saturation: { label: "进入平台期", emoji: "📈" },
  decay: { label: "长尾衰退", emoji: "📉" },
  dead: { label: "传播停止", emoji: "💤" },
  unknown: { label: "数据不足", emoji: "⏳" },
};

// ── Stage classification (prefer backend stage, then add rebound context) ──
function classifyStage(derived) {
  if (!derived || derived.length < 2) {
    return {
      stage: "unknown",
      label: STAGE_META.unknown.label,
      emoji: STAGE_META.unknown.emoji,
      ageHours: 0,
      rebound: false,
      velocityAvg: 0,
      velocitySlope: 0,
      backendStage: null,
    };
  }

  const latest = derived[derived.length - 1] || {};
  const recent = derived.slice(-6);
  const vs = recent.map(d => effectiveVelocity(d));
  const vAvg = vs.reduce((a, b) => a + b, 0) / vs.length;
  const ageHours = Math.max(0, (new Date(latest.ts) - new Date(derived[0].ts)) / 3600000);

  const n = vs.length;
  const xMean = (n - 1) / 2;
  const yMean = vAvg;
  let num = 0, den = 0;
  for (let i = 0; i < n; i++) {
    num += (i - xMean) * (vs[i] - yMean);
    den += (i - xMean) ** 2;
  }
  const slope = den > 0 ? num / den : 0;

  let baseStage = STAGE_META[latest.stage] ? latest.stage : null;
  if (!baseStage) {
    if (vAvg < 0.5) baseStage = "dead";
    else if (vAvg < 1.5 && slope < 0) baseStage = "decay";
    else if (slope < -1 && vAvg < 10) baseStage = "saturation";
    else if (vAvg > 50) baseStage = "amplification";
    else if (slope > 0 && vAvg < 10) baseStage = ageHours >= 24 ? "decay" : "discovery";
    else if (slope < 0 && vAvg > 3) baseStage = "saturation";
    else baseStage = "amplification";
  }

  const rebound = ageHours >= 24 && vAvg >= 0.5 && slope > 0.05;
  const meta = STAGE_META[baseStage] || STAGE_META.unknown;

  return {
    stage: baseStage,
    label: rebound && ["decay", "saturation", "dead"].includes(baseStage) ? "长尾回流" : meta.label,
    emoji: rebound && ["decay", "saturation", "dead"].includes(baseStage) ? "🔄" : meta.emoji,
    ageHours,
    rebound,
    velocityAvg: vAvg,
    velocitySlope: slope,
    backendStage: latest.stage || null,
  };
}

function stageEmoji(stage) {
  return {
    discovery: "🌱",
    amplification: "🔥",
    saturation: "📈",
    decay: "📉",
    dead: "💤",
    unknown: "⏳",
  }[stage] || "⏳";
}

// ── Narrative generator (plain-language description) ──
function generateNarrative(derived, cascade, cfg, stage) {
  if (!derived || derived.length < 2) {
    return {
      title: "正在收集数据",
      narrative: "<p>刚启动，还没有足够的 cycle 可以做趋势判断。大约 3 个 cycle（15 分钟）后会开始显示状态。</p>",
    };
  }

  const latest = derived[derived.length - 1];
  const latestCascade = cascade[cascade.length - 1] || {};
  const firstCascade = cascade[0] || {};
  const prev = derived[Math.max(0, derived.length - 7)]; // ~30min ago

  const views = latest.view_count || 0;
  const likes = latest.favorite_count || 0;
  const rts = latest.retweet_count || 0;
  const replies = latest.reply_count || 0;
  const quotes = latest.quote_count || 0;
  const velocity = effectiveVelocity(latest);
  const heat = latest.heat_score || 0;

  const dViews = views - (prev.view_count || 0);
  const dLikes = likes - (prev.favorite_count || 0);
  const dRts = rts - (prev.retweet_count || 0);

  const cascadeSize = latestCascade.cascade_size || 0;
  const wiener = latestCascade.structural_virality_wiener || 0;
  const engagers = latestCascade.unique_engager_count || 0;
  const potential = latestCascade.distribution_potential_score ?? latestCascade.reach_followers_sum ?? 0;

  // Title
  const title = stage.label;

  // Narrative paragraphs
  let narr = [];

  // 1. 当前传播状态
  if (stage.rebound) {
    narr.push("<p>这条推已经过了首轮传播高点，当前更像<strong>长尾回流</strong>：旧帖最近又有一波补量，不是刚进入冷启动阶段。</p>");
  } else if (stage.stage === "amplification") {
    narr.push(`<p>这条推<strong>正在活跃传播</strong>。每分钟在累积 <strong>${velocity.toFixed(1)}</strong> 的综合热度。</p>`);
  } else if (stage.stage === "discovery") {
    narr.push(`<p>这条推处于<strong>早期发现阶段</strong>，传播速度在上升但还没到峰值。</p>`);
  } else if (stage.stage === "saturation") {
    narr.push(`<p>这条推已经<strong>进入传播平台期</strong>——核心曝光池基本覆盖，曝光还在涨但增速在放缓。</p>`);
  } else if (stage.stage === "decay") {
    narr.push(`<p>这条推<strong>进入长尾衰退</strong>，每分钟新增互动很少。</p>`);
  } else if (stage.stage === "dead") {
    narr.push(`<p>传播基本停止了，最近几个 cycle 几乎没有新互动。</p>`);
  }

  // 2. 量级说明（最近 30 分钟的变化）
  if (stage.rebound) {
    narr.push(`<p>过去 30 分钟主要是<strong>补曝光回流</strong>：新增 <strong>${fmt(dViews)}</strong> 次曝光，<strong>${dLikes}</strong> 个赞，<strong>${dRts}</strong> 次转发。这更像二次分发/旧帖回看，不是首轮爆发。</p>`);
  } else if (dViews > 100 || dLikes > 5 || dRts > 2) {
    narr.push(`<p>过去 30 分钟新增：<strong>${fmt(dViews)}</strong> 次曝光，<strong>${dLikes}</strong> 个赞，<strong>${dRts}</strong> 次转发。</p>`);
  }

  // 3. 扩散结构
  if (cascadeSize > 0) {
    let shape;
    if (wiener < 1.3) shape = "<strong>浅层广播</strong>（大家都在直接回复作者，很少有人互相讨论）";
    else if (wiener < 2.5) shape = "<strong>开始有分叉</strong>（有人在回复别人的回复，形成对话）";
    else if (wiener < 4.0) shape = "<strong>树状扩散</strong>（真正的多层讨论在发生，传播结构有深度）";
    else shape = "<strong>深度病毒传播</strong>（极深的讨论嵌套，少见）";

    narr.push(`<p>已经有 <strong>${engagers}</strong> 个独立账户参与，总参与节点 <strong>${cascadeSize}</strong>（含回复的回复）。传播结构是${shape}。</p>`);
  }

  // 4. 扩散势能
  if (potential > 0) {
    const potentialAdj = latestCascade.distribution_potential_score ?? latestCascade.reach_adjusted ?? potential;
    const discount = latestCascade.distribution_potential_overlap_discount ?? latestCascade.reach_overlap_discount ?? '—';
    narr.push(`<p>当前扩散势能分数约 <strong>${fmtCompact(potentialAdj)}</strong>。这个值表示讨论网络的潜在分发能力，不是实际人数；实际曝光仍以 views 为准。</p>`);
  }

  return {
    title,
    narrative: narr.join(""),
  };
}

// ── Chart setup ──
Chart.defaults.color = "#c9d4e3";
Chart.defaults.borderColor = "rgba(151,173,199,0.12)";
Chart.defaults.font.family = "-apple-system, BlinkMacSystemFont, PingFang SC, Segoe UI, sans-serif";
Chart.defaults.font.size = 11;
Chart.defaults.plugins.legend.labels.usePointStyle = true;
Chart.defaults.plugins.legend.labels.pointStyle = "line";
Chart.defaults.plugins.legend.labels.color = "#d8e3f2";
Chart.defaults.elements.line.borderWidth = 2.25;
Chart.defaults.elements.line.tension = 0.32;
Chart.defaults.elements.line.cubicInterpolationMode = "monotone";
Chart.defaults.elements.point.radius = 0;
Chart.defaults.elements.point.hoverRadius = 4;
Chart.defaults.elements.point.hitRadius = 14;

// Date adapter loaded from local vendor bundle

let charts = {};
function fmtSigned(n, digits = 1) {
  if (n == null) return "—";
  if (Math.abs(n) >= 1000) return fmtCompact(n);
  if (Math.abs(n) >= 100) return Math.round(n).toString();
  return Number(n).toFixed(digits).replace(/\.0$/, "");
}

function inferTimeUnit(points) {
  if (!points || points.length < 2) return "hour";
  const start = new Date(points[0].x).getTime();
  const end = new Date(points[points.length - 1].x).getTime();
  const spanHours = (end - start) / 3600000;
  if (spanHours > 24 * 5) return "day";
  return "hour";
}

function downsamplePoints(points, maxPoints = 180, strategy = "last") {
  if (!points || points.length <= maxPoints) return points || [];
  const bucketSize = Math.ceil(points.length / maxPoints);
  const out = [];
  for (let i = 0; i < points.length; i += bucketSize) {
    const bucket = points.slice(i, i + bucketSize).filter(p => Number.isFinite(p?.y));
    if (!bucket.length) continue;
    if (strategy === "extrema") {
      let min = bucket[0];
      let max = bucket[0];
      for (const point of bucket) {
        if (point.y < min.y) min = point;
        if (point.y > max.y) max = point;
      }
      const chosen = [];
      const pushUnique = (point) => {
        if (!chosen.some(p => p.x === point.x && p.y === point.y)) chosen.push(point);
      };
      pushUnique(bucket[0]);
      pushUnique(min);
      pushUnique(max);
      pushUnique(bucket[bucket.length - 1]);
      chosen.sort((a, b) => new Date(a.x) - new Date(b.x));
      out.push(...chosen);
    } else {
      out.push(bucket[bucket.length - 1]);
    }
  }
  return out;
}

function styleDataset(dataset) {
  return {
    spanGaps: true,
    borderCapStyle: "round",
    borderJoinStyle: "round",
    pointRadius: dataset.pointRadius ?? 0,
    pointHoverRadius: dataset.pointHoverRadius ?? 4,
    pointHitRadius: dataset.pointHitRadius ?? 14,
    fill: dataset.fill ?? false,
    ...dataset,
  };
}

function makeLineChart(canvasId, datasets, opts = {}) {
  const ctx = $(canvasId).getContext("2d");
  if (charts[canvasId]) charts[canvasId].destroy();
  const styled = datasets.map(styleDataset);
  const allPoints = styled.flatMap(d => d.data || []);
  const timeUnit = opts.timeUnit || inferTimeUnit(allPoints);
  charts[canvasId] = new Chart(ctx, {
    type: "line",
    data: { datasets: styled },
    options: {
      parsing: false,
      normalized: true,
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: "index", intersect: false },
      layout: { padding: { top: 6, right: 6, bottom: 2, left: 2 } },
      scales: {
        x: {
          type: "time",
          time: { unit: timeUnit },
          grid: { color: "rgba(151,173,199,0.08)", tickLength: 0 },
          ticks: {
            color: "#8fa1b8",
            autoSkip: true,
            maxRotation: 0,
            minRotation: 0,
            maxTicksLimit: opts.xMaxTicks || 9,
          },
          border: { color: "rgba(151,173,199,0.16)" },
        },
        y: {
          beginAtZero: opts.yBeginAtZero ?? false,
          grid: { color: "rgba(151,173,199,0.08)" },
          ticks: {
            color: "#8fa1b8",
            maxTicksLimit: 6,
            callback: opts.yTick || ((v) => fmtCompact(v)),
          },
          border: { color: "rgba(151,173,199,0.16)" },
        },
        ...(opts.y1 ? { y1: {
          position: "right",
          beginAtZero: opts.y1BeginAtZero ?? false,
          grid: { drawOnChartArea: false },
          ticks: {
            color: "#8fa1b8",
            maxTicksLimit: 6,
            callback: opts.y1Tick || ((v) => fmtCompact(v)),
          },
          border: { color: "rgba(151,173,199,0.16)" },
        } } : {}),
      },
      plugins: {
        legend: {
          position: "top",
          align: "start",
          labels: { boxWidth: 18, boxHeight: 5, padding: 16, font: { size: 12, weight: 600 } },
        },
        tooltip: {
          backgroundColor: "rgba(10,15,21,0.96)",
          borderColor: "rgba(159,178,201,0.18)",
          borderWidth: 1,
          padding: 12,
          displayColors: true,
          callbacks: {
            label: (ctx) => {
              const formatter = ctx.dataset.valueFormatter;
              const formatted = formatter ? formatter(ctx.parsed.y, ctx) : fmtCompact(ctx.parsed.y);
              return `${ctx.dataset.label}: ${formatted}`;
            },
          },
        },
        decimation: {
          enabled: Boolean(opts.decimate),
          algorithm: "lttb",
          samples: opts.decimateSamples || 120,
        },
      },
    },
  });
}

// ── Data loading ──
async function loadTweets() { return fetchJsonWithOptionalToken("/api/tweets"); }
async function loadData(tid) { return fetchJsonWithOptionalToken(`/api/data/${tid}?compact=1`); }
async function loadCampaigns() { return fetchJsonWithOptionalToken("/api/campaigns"); }
async function loadCampaignData(cid) { return fetchJsonWithOptionalToken(`/api/campaigns/${encodeURIComponent(cid)}?compact=1`); }

// ── Arrow computation ──
function arrow(current, previous, threshold = 0.02) {
  if (previous == null || previous === 0) return { sym: "", cls: "" };
  const delta = (current - previous) / Math.abs(previous);
  if (delta > threshold) return { sym: "↑", cls: "up" };
  if (delta < -threshold) return { sym: "↓", cls: "down" };
  return { sym: "→", cls: "flat" };
}

function kpi(label, value, unit, arrow_, subvalue, tooltip) {
  return `
    <div class="kpi">
      <div class="label">${label} <span class="q">?</span></div>
      <div class="value">${value}${unit ? `<span class="unit">${unit}</span>` : ""}${arrow_ ? `<span class="arrow ${arrow_.cls}">${arrow_.sym}</span>` : ""}</div>
      ${subvalue ? `<div class="subvalue">${subvalue}</div>` : ""}
      <div class="tooltip">${tooltip}</div>
    </div>`;
}

// ── Render ──
async function renderTweetList() {
  const tweets = await loadTweets();
  const sel = $("tweet-selector");
  sel.innerHTML = "";
  if (!Array.isArray(tweets)) {
    const opt = document.createElement("option");
    opt.textContent = tweets?.error ? `推文加载失败：${tweets.error}` : "推文加载失败";
    sel.appendChild(opt);
    return null;
  }
  if (!tweets.length) {
    const opt = document.createElement("option");
    opt.textContent = "(尚无追踪的推文)";
    sel.appendChild(opt);
    return null;
  }
  for (const t of tweets) {
    const opt = document.createElement("option");
    opt.value = t.tweet_id;
    const author = t.author ? `@${t.author}` : "(unknown)";
    opt.textContent = `${t.tweet_id.slice(-8)} · ${author} · ${fmt(t.latest_views)} 次曝光 · 第 ${t.cycles} 次采样`;
    sel.appendChild(opt);
  }
  return tweets[0].tweet_id;
}

async function renderCampaignList() {
  const campaigns = await loadCampaigns();
  const sel = $("campaign-selector");
  sel.innerHTML = "";
  if (!Array.isArray(campaigns)) {
    const opt = document.createElement("option");
    opt.textContent = campaigns?.error ? `Campaign 加载失败：${campaigns.error}` : "Campaign 加载失败";
    sel.appendChild(opt);
    return null;
  }
  if (!campaigns.length) {
    const opt = document.createElement("option");
    opt.textContent = "(尚无 campaign)";
    sel.appendChild(opt);
    return null;
  }
  for (const c of campaigns) {
    const opt = document.createElement("option");
    opt.value = c.campaign_id;
    const isGraph = c.model === "entity_graph_v1" || c.source_mode === "entity_graph";
    const count = c.observed_node_count || c.tracked_tweet_count || c.tweet_count || 0;
    const viewsPart = c.total_views ? ` · ${fmt(c.total_views)} 次曝光` : "";
    const healthPart = c.health_risk_count ? ` · ${c.health_risk_count} 个健康提示` : "";
    opt.textContent = `${c.name} · ${fmt(count)} ${isGraph ? "节点" : "条"}${viewsPart}${healthPart}`;
    sel.appendChild(opt);
  }
  const selected = campaigns.find(c => c.campaign_id === INITIAL_CAMPAIGN_ID)?.campaign_id || campaigns[0].campaign_id;
  sel.value = selected;
  return selected;
}

function setChartCopy(mode) {
  const copy = mode === "campaign" ? {
    heat: ["Campaign 热度脉冲 & 观测动能", "橙色是观测节点在每个时间桶里的异常热度，蓝线是平滑后的 campaign 动能。曲线反映的是整个观测流的起伏，不是单条内容走势。"],
    counts: ["观测互动数据", "曝光单独走右轴，其余互动走左轴。这里统计已发现的实体相关传播节点，适合看历史传播重心。"],
    cascade: ["新增节点 & 参与账户", "橙线看每个时间桶新增了多少相关传播节点，蓝线看去重后的参与账户规模。"],
    reach: ["扩散势能 & 集中度风险", "绿色看观测网络的潜在分发能力，金线看单个节点/传播簇占比风险。风险越高，说明热度来源更集中。"],
  } : {
    heat: ["热度脉冲 & 传播速度", "橙色看单个采样周期的新增热度，蓝线看平滑后的传播速度。峰值和衰减会比原来更容易读。"],
    counts: ["原始互动数据", "曝光单独走右轴，其余互动走左轴。累计型指标改成阶梯线，避免几条线糊成一团。"],
    cascade: ["参与账户 & 扩散深度", "橙线看有多少独立账户真正进场，蓝线看讨论是不是开始形成分叉，而不只是单层广播。"],
    reach: ["扩散势能 & 讨论规模", "绿色是清洗后的扩散势能分数，金线是讨论节点总数。前者看潜在放大能力，后者看实际网络体量。"],
  };
  $("chart-title-heat").textContent = copy.heat[0];
  $("chart-desc-heat").textContent = copy.heat[1];
  $("chart-title-counts").textContent = copy.counts[0];
  $("chart-desc-counts").textContent = copy.counts[1];
  $("chart-title-cascade").textContent = copy.cascade[0];
  $("chart-desc-cascade").textContent = copy.cascade[1];
  $("chart-title-reach").textContent = copy.reach[0];
  $("chart-desc-reach").textContent = copy.reach[1];
}

async function renderDashboard(tid) {
  if (!tid) return;
  setChartCopy("tweet");
  $("campaign-details").style.display = "none";
  const data = await loadData(tid);
  const rawDerived = data.derived || [];
  const cascade = data.cascade || [];
  const cfg = data.config || {};
  const meta = data.meta || {};

  const derived = [];
  for (const entry of rawDerived) {
    const prev = derived.length ? derived[derived.length - 1] : null;
    const hasBackendScoring = Boolean(entry.scoring_version || entry.heat_components);
    const heat = hasBackendScoring ? (entry.heat_score ?? recalcHeatBase(entry)) : recalcHeatBase(entry);
    let heatDelta = entry.heat_delta;
    let velocity = entry.heat_velocity_per_min;

    if (heatDelta == null) {
      heatDelta = prev ? heat - prev.heat_score : 0;
    }
    if (velocity == null) {
      velocity = 0;
      if (prev && prev.ts && entry.ts) {
        const elapsed = (new Date(entry.ts) - new Date(prev.ts)) / 60000;
        if (elapsed > 0) velocity = heatDelta / elapsed;
      }
    }

    derived.push({
      ...entry,
      heat_score: heat,
      heat_delta: heatDelta,
      heat_velocity_per_min: velocity,
      heat_velocity_display_per_min: entry.heat_velocity_ema_per_min ?? entry.heat_velocity_per_min ?? velocity ?? 0,
    });
  }

  if (!derived.length) {
    $("kpi-row").innerHTML = '<div class="empty">暂无数据，等待第一次采样…</div>';
    return;
  }

  const latest = derived[derived.length - 1];
  const prev = derived[Math.max(0, derived.length - 7)]; // ~30 min ago
  const latestCascade = cascade[cascade.length - 1] || {};
  const prevCascade = cascade[Math.max(0, cascade.length - 2)] || {};
  const stage = classifyStage(derived);
  const narrative = generateNarrative(derived, cascade, cfg, stage);

  // ── Hero ──
  const hero = $("hero");
  hero.className = `hero stage-${stage.stage}`;
  $("hero-title").innerHTML = narrative.title;
  document.querySelector(".hero-emoji").textContent = stage.emoji || stageEmoji(stage.stage);
  $("hero-narrative").innerHTML = narrative.narrative;

  // ── KPI row ──
  const viewsArrow = arrow(latest.view_count, prev.view_count);
  const heatArrow = arrow(latest.heat_score, prev.heat_score);
  const latestPotential = latestCascade.distribution_potential_score ?? latestCascade.reach_adjusted ?? latestCascade.reach_followers_sum;
  const prevPotential = prevCascade.distribution_potential_score ?? prevCascade.reach_adjusted ?? prevCascade.reach_followers_sum;
  const reachArrow = arrow(latestPotential, prevPotential);
  const engagerArrow = arrow(latestCascade.unique_engager_count, prevCascade.unique_engager_count);
  const coverageSummary = latest.observed_reply_coverage != null || latest.observed_quote_coverage != null
    ? `reply抓取 ${(100 * (latest.observed_reply_coverage || 0)).toFixed(0)}% · quote抓取 ${(100 * (latest.observed_quote_coverage || 0)).toFixed(0)}%`
    : "XHI v2 多层信号评分";

  $("kpi-row").innerHTML = [
    kpi(
      "总曝光",
      fmt(latest.view_count || 0),
      "次", viewsArrow,
      "被展示在别人时间线上的次数",
      "每次推文出现在某个用户的时间线上都算 1 次曝光。不代表用户真的读了，只代表推文进入了他的 feed。"
    ),
    kpi(
      "综合热度",
      fmt(latest.heat_score || 0),
      "", heatArrow,
      coverageSummary,
      "XHI v2/v3 多层评分体系：Layer 1 基础权重（Quote 5.0 > Reply 3.0 > RT 2.0 > Like 1.0 > View 0.01），Layer 2 互动者影响力加权，Layer 3 时间衰减，Layer 4 信号组合加成。新版优先使用后端真实互动时间和覆盖率建模。"
    ),
    kpi(
      "当前传播速度",
      (latest.heat_velocity_display_per_min || 0).toFixed(1),
      "/分", null,
      velocityTierLabel(latest.heat_velocity_display_per_min),
      "每分钟热度的增量。优先显示后端平滑后的速度，避免单个 cycle 抖动把趋势看歪。0 = 不再传播，10+ = 活跃传播，50+ = 爆发。"
    ),
    kpi(
      "参与讨论人数",
      fmt(latestCascade.unique_engager_count || 0),
      "人", engagerArrow,
      "独立账户，含回复和引用者",
      "所有回复、引用原推文及其下层讨论的独立 X 账户数（去重）。包括「回复的回复」，所以大于原推 reply_count。"
    ),
    kpi(
      "扩散深度",
      (latestCascade.structural_virality_wiener || 0).toFixed(1),
      "", null,
      wienerLabelZh(latestCascade.structural_virality_wiener),
      "学术上叫 Wiener index（Goel et al. 2016），通俗理解：越接近 1 = 所有人都在「直接回原帖」（浅层广播）；越高 = 「有人在回别人的回复」（真·多层讨论）。2-4 之间算健康的树状扩散。"
    ),
    kpi(
      "扩散势能",
      fmtCompact(latestPotential || 0),
      "分", reachArrow,
      `毛分数 ${fmtCompact(latestCascade.distribution_potential_gross ?? latestCascade.reach_gross ?? 0)} × 重叠折扣 ${latestCascade.distribution_potential_overlap_discount ?? latestCascade.reach_overlap_discount ?? '—'}`,
      "这是潜在分发能力分数，不是人数估算。算法会压缩大号 follower 影响，只看 quote/reply 网络带来的二次传播能力，并对作者重叠做折扣。实际曝光看左上角的总曝光。"
    ),
  ].join("");

  $("updated-at").textContent = `最近更新：${latest.ts.replace("T", " ").replace(/\+.*$/, "")}`;
  const derivedLabel = meta.compact && meta.derived_points_raw > meta.derived_points
    ? `${meta.derived_points_raw} 次采样（展示 ${meta.derived_points} 点）`
    : `${derived.length} 次采样`;
  const cascadeLabel = meta.compact && meta.cascade_points_raw > meta.cascade_points
    ? `${meta.cascade_points_raw} 次扩散分析（展示 ${meta.cascade_points} 点）`
    : `${cascade.length} 次扩散分析`;
  $("footer-info").textContent = `${derivedLabel} · ${cascadeLabel} · 启动于 ${(cfg.tracker_started_at || "—").replace("T", " ").replace(/\+.*$/, "")}`;

  // ── Chart data prep ──
  const derivedPoints = (fn) => derived.map(d => ({ x: new Date(d.ts).getTime(), y: fn(d) }));
  const cascadePoints = (fn) => cascade.map(d => ({ x: new Date(d.ts).getTime(), y: fn(d) }));
  const sampledDerived = (fn, maxPoints = 160, strategy = "last") => downsamplePoints(derivedPoints(fn), maxPoints, strategy);
  const sampledCascade = (fn, maxPoints = 120, strategy = "last") => downsamplePoints(cascadePoints(fn), maxPoints, strategy);

  makeLineChart("chart-heat", [
    {
      label: "热度脉冲",
      data: sampledDerived(d => d.heat_delta || 0, 84, "extrema"),
      borderColor: "rgba(255,142,114,0.82)",
      backgroundColor: "rgba(255,142,114,0.12)",
      fill: true,
      borderWidth: 2.6,
      yAxisID: "y",
      valueFormatter: (v) => fmtSigned(v, 1),
    },
    {
      label: "传播速度",
      data: sampledDerived(d => d.heat_velocity_display_per_min || 0, 96, "extrema"),
      borderColor: "#7bb8ff",
      backgroundColor: "rgba(123,184,255,0.12)",
      borderDash: [8, 6],
      borderWidth: 2.2,
      yAxisID: "y1",
      valueFormatter: (v) => `${fmtSigned(v, 1)} /分`,
    },
  ], {
    y1: true,
    xMaxTicks: 10,
    yTick: (v) => fmtSigned(v, 0),
    y1Tick: (v) => fmtSigned(v, 1),
    decimate: true,
    decimateSamples: 120,
  });

  makeLineChart("chart-counts", [
    {
      label: "曝光数",
      data: sampledDerived(d => d.view_count, 140, "last"),
      borderColor: "#a5b0bc",
      backgroundColor: "rgba(165,176,188,0.08)",
      borderWidth: 2.4,
      fill: true,
      yAxisID: "y1",
      stepped: true,
      valueFormatter: (v) => fmtCompact(v),
    },
    {
      label: "点赞",
      data: sampledDerived(d => d.favorite_count, 140, "last"),
      borderColor: "#63d58c",
      borderWidth: 2.2,
      yAxisID: "y",
      stepped: true,
      valueFormatter: (v) => fmt(v),
    },
    {
      label: "转发",
      data: sampledDerived(d => d.retweet_count, 140, "last"),
      borderColor: "#7bb8ff",
      borderWidth: 2.2,
      yAxisID: "y",
      stepped: true,
      valueFormatter: (v) => fmt(v),
    },
    {
      label: "回复",
      data: sampledDerived(d => d.reply_count, 140, "last"),
      borderColor: "#f1c464",
      borderWidth: 2.2,
      yAxisID: "y",
      stepped: true,
      valueFormatter: (v) => fmt(v),
    },
    {
      label: "引用",
      data: sampledDerived(d => d.quote_count, 140, "last"),
      borderColor: "#ff6f6f",
      borderWidth: 2.2,
      yAxisID: "y",
      stepped: true,
      valueFormatter: (v) => fmt(v),
    },
  ], {
    y1: true,
    yBeginAtZero: true,
    y1BeginAtZero: true,
    xMaxTicks: 8,
    yTick: (v) => fmt(v),
    y1Tick: (v) => fmtCompact(v),
    decimate: true,
    decimateSamples: 120,
  });

  makeLineChart("chart-cascade", [
    {
      label: "参与独立账户",
      data: sampledCascade(d => d.unique_engager_count, 120, "last"),
      borderColor: "#ff8e72",
      backgroundColor: "rgba(255,142,114,0.12)",
      fill: true,
      stepped: true,
      yAxisID: "y",
      valueFormatter: (v) => `${fmt(v)} 人`,
    },
    {
      label: "扩散深度指数",
      data: sampledCascade(d => d.structural_virality_wiener, 120, "last"),
      borderColor: "#7bb8ff",
      borderDash: [8, 6],
      borderWidth: 2.2,
      yAxisID: "y1",
      valueFormatter: (v) => fmtSigned(v, 2),
    },
  ], {
    y1: true,
    yBeginAtZero: true,
    xMaxTicks: 8,
    yTick: (v) => fmt(v),
    y1Tick: (v) => fmtSigned(v, 1),
  });

  makeLineChart("chart-reach", [
    {
      label: "扩散势能分数",
      data: sampledCascade(d => d.distribution_potential_score ?? d.reach_adjusted ?? d.reach_followers_sum, 120, "last"),
      borderColor: "#63d58c",
      backgroundColor: "rgba(99,213,140,0.12)",
      fill: true,
      borderWidth: 2.5,
      yAxisID: "y",
      valueFormatter: (v) => `${fmtCompact(v)} 分`,
    },
    {
      label: "讨论节点总数",
      data: sampledCascade(d => d.cascade_size, 120, "last"),
      borderColor: "#f1c464",
      borderDash: [8, 6],
      borderWidth: 2.1,
      stepped: true,
      yAxisID: "y1",
      valueFormatter: (v) => fmt(v),
    },
  ], {
    y1: true,
    yBeginAtZero: true,
    y1BeginAtZero: true,
    xMaxTicks: 10,
    yTick: (v) => fmtCompact(v),
    y1Tick: (v) => fmt(v),
  });
}

function campaignStageLabel(stage) {
  return {
    launch: "启动期",
    discovery: "开始起量",
    amplification: "整体放大",
    saturation: "进入平台期",
    decay: "长尾衰退",
    dead: "基本停止",
  }[stage] || "状态未知";
}

function campaignStageEmoji(stage) {
  return {
    launch: "🚦",
    discovery: "🌱",
    amplification: "🔥",
    saturation: "📈",
    decay: "📉",
    dead: "💤",
  }[stage] || "⏳";
}

function concentrationLabel(value) {
  if (value == null) return "—";
  if (value >= 0.75) return "高度集中";
  if (value >= 0.5) return "偏集中";
  if (value >= 0.3) return "相对均衡";
  return "分布健康";
}

function renderCampaignContributions(contributions, model) {
  const list = $("campaign-contributions");
  if (!contributions || !contributions.length) {
    list.innerHTML = '<div class="empty">暂无贡献数据</div>';
    return;
  }
  const isGraph = model === "entity_graph_v1";
  list.innerHTML = contributions.slice(0, 10).map((item) => {
    const share = Math.max(0, Math.min(1, item.heat_share || 0));
    let label = "";
    let sub = "";
    let value = `${(share * 100).toFixed(1)}%`;
    if (isGraph) {
      label = item.label || item.cluster_id || "未分组传播簇";
      const topNodes = (item.top_nodes || []).slice(0, 3).map((node) => {
        const author = node.author ? `@${node.author}` : (node.tweet_id || "").slice(-8);
        return `${author} ${fmtCompact(node.views || 0)}曝光`;
      }).join(" / ");
      sub = `${fmt(item.node_count || 0)} 节点 · ${fmt(item.view_count || 0)} 曝光 · ${fmt(item.engagement_count || 0)} 互动${topNodes ? ` · Top: ${topNodes}` : ""}`;
      value = `${value}<div class="contribution-sub">heat ${fmtCompact(item.attributed_heat || 0)}</div>`;
    } else {
      const tid = String(item.tweet_id || "");
      label = item.author ? `@${item.author}` : tid.slice(-8);
      sub = `${tid.slice(-8)} · ${fmt(item.view_count || 0)} 曝光 · ${fmt(item.engagement_count || 0)} 互动`;
    }
    return `
      <div class="contribution-row">
        <div>
          <div class="contribution-label">${esc(label)}</div>
          <div class="contribution-sub">${esc(sub)}</div>
        </div>
        <div class="contribution-bar"><div class="contribution-fill" style="width:${(share * 100).toFixed(1)}%"></div></div>
        <div class="contribution-value">${value}</div>
      </div>`;
  }).join("");
}

async function renderCampaignDashboard(cid) {
  if (!cid) return;
  setChartCopy("campaign");
  $("campaign-details").style.display = "block";

  const data = await loadCampaignData(cid);
  if (data.error) {
    $("kpi-row").innerHTML = `<div class="empty">Campaign 加载失败：${data.error}</div>`;
    return;
  }
  const summary = data.summary || {};
  const series = data.series || [];
  const contributions = data.contributions || [];
  const meta = data.meta || {};
  const model = summary.model || meta.model || "";
  const isGraph = model === "entity_graph_v1";
  const health = summary.health || {};
  const healthRiskCount = Number(health.risk_count || summary.health_risk_count || 0);

  if (!series.length) {
    $("kpi-row").innerHTML = `<div class="empty">这个 campaign 暂无可聚合的${isGraph ? "观测节点" : "推文数据"}。</div>`;
    return;
  }

  const latest = series[series.length - 1];
  const prev = series[Math.max(0, series.length - 5)];
  const stage = summary.stage || "unknown";
  const hero = $("hero");
  hero.className = `hero stage-${stage === "amplification" ? "amplification" : stage === "dead" ? "dead" : stage === "decay" ? "decay" : stage === "saturation" ? "saturation" : "discovery"}`;
  $("hero-title").innerHTML = `${campaignStageLabel(stage)} · ${esc(summary.name || cid)}`;
  document.querySelector(".hero-emoji").textContent = campaignStageEmoji(stage);
  if (isGraph) {
    const identity = [
      ...(summary.identity_terms || []).slice(0, 3),
      ...(summary.watch_handles || []).slice(0, 2).map(h => `@${h}`),
    ].join(" / ");
    $("hero-narrative").innerHTML = [
      `<p>这个 campaign 当前已识别出 <strong>${fmt(summary.observed_node_count || summary.tracked_tweet_count || 0)}</strong> 个相关传播节点，累计 <strong>${fmt(summary.total_views || 0)}</strong> 次曝光，<strong>${fmt(summary.total_engagements || 0)}</strong> 次显性互动。</p>`,
      `<p>当前观测动能为 <strong>${(summary.campaign_momentum_per_min || 0).toFixed(2)}</strong> heat/分，最近时间桶新增 <strong>${fmt(summary.active_nodes || summary.active_tweets || 0)}</strong> 个节点。${identity ? `识别锚点：${esc(identity)}。` : ""}</p>`,
      `<p>集中度风险为 <strong>${((summary.concentration_risk || 0) * 100).toFixed(1)}%</strong>（${concentrationLabel(summary.concentration_risk)}）。它衡量当前观测流是否被少数传播簇主导，不代表去重触达人数。</p>`,
      health.generated_at ? `<p>托管健康审计：<strong>${healthRiskCount ? `${healthRiskCount} 个风险` : "OK"}</strong>${health.noise_candidates ? `，已过滤噪音候选 ${fmt(health.noise_candidates)} 条` : ""}。</p>` : "",
    ].join("");
  } else {
    $("hero-narrative").innerHTML = [
      `<p>这个 campaign 当前包含 <strong>${summary.tracked_tweet_count || 0}</strong> 个有数据的内容节点，累计 <strong>${fmt(summary.total_views || 0)}</strong> 次曝光，<strong>${fmt(summary.total_engagements || 0)}</strong> 次显性互动。</p>`,
      `<p>当前 campaign 动能为 <strong>${(summary.campaign_momentum_per_min || 0).toFixed(2)}</strong> heat/分，最近时间桶内仍有 <strong>${summary.active_tweets || 0}</strong> 个节点在增长。</p>`,
      `<p>集中度风险为 <strong>${((summary.concentration_risk || 0) * 100).toFixed(1)}%</strong>（${concentrationLabel(summary.concentration_risk)}）。它衡量当前热度是否集中在少数内容节点上。</p>`,
    ].join("");
  }

  const viewsArrow = arrow(latest.total_views, prev.total_views);
  const heatArrow = arrow(latest.campaign_heat_score, prev.campaign_heat_score);
  const potentialArrow = arrow(latest.distribution_potential_score, prev.distribution_potential_score);
  const detailsTitle = document.querySelector("#campaign-details .chart-title");
  const detailsDesc = document.querySelector("#campaign-details .chart-desc");
  if (detailsTitle && detailsDesc) {
    detailsTitle.textContent = isGraph ? "传播簇归因" : "内容节点贡献占比";
    detailsDesc.textContent = isGraph
      ? "按观测节点所属的传播簇做归因，先看哪类传播在贡献热度，再回看具体节点。"
      : "按当前热度贡献拆分，先看哪类内容节点在主导整个 campaign。";
  }

  $("kpi-row").innerHTML = [
    kpi(
      isGraph ? "观测曝光" : "Campaign 曝光",
      fmt(summary.total_views || 0),
      "次", viewsArrow,
      isGraph ? `${fmt(summary.observed_node_count || summary.tracked_tweet_count || 0)} 个相关传播节点` : `${summary.tracked_tweet_count || 0}/${summary.tweet_count || 0} 个内容节点有数据`,
      isGraph ? "已发现相关传播节点的曝光累计值。它不是去重人数，实际曝光仍以平台 views 为准。" : "Campaign 内所有内容节点当前曝光数的累计值。不同节点受众可能重叠，所以它不是去重人数。"
    ),
    kpi(
      isGraph ? "Entity Heat" : "Campaign Heat",
      fmt(summary.campaign_heat_score || 0),
      "", heatArrow,
      isGraph ? `观测 raw ${fmt(summary.campaign_heat_raw || 0)} · 模型 ${model}` : `原始 heat ${fmt(summary.campaign_heat_raw || 0)} · 已做集中度折扣`,
      isGraph ? "实体级热度：先从身份词和关注账号发现相关传播节点，再按时间桶计算相对基线的异常增量，最后做集中度折扣。" : "Campaign Heat：对内容节点簇的时间桶热度做聚合后，再进行集中度折扣。"
    ),
    kpi(
      "当前动能",
      (summary.campaign_momentum_per_min || 0).toFixed(2),
      "/分", null,
      `${meta.bucket_minutes || 15} 分钟时间桶`,
      isGraph ? "最近时间桶里的实体级热度增量速度。它回答的是这个 campaign 观测流现在是否仍在起量。" : "Campaign 在最近时间桶内的 heat 增量速度。它回答的是这组推文现在整体还热不热。"
    ),
    kpi(
      isGraph ? "新增节点" : "活跃节点",
      fmt(summary.active_nodes || summary.active_tweets || 0),
      "个", null,
      "最近时间桶",
      isGraph ? "最近一个时间桶内新进入观测流的相关传播节点数。这个数比累计节点更能反映当前是否还有新讨论。" : "最近一个时间桶内有新增曝光或互动的内容节点数。这个数比总节点数更能反映 campaign 当前是否还在动。"
    ),
    kpi(
      "参与账户",
      fmt(summary.unique_engagers || summary.unique_engagers_approx || 0),
      "人", null,
      isGraph ? "按观测节点作者去重" : (summary.unique_engagers ? "跨节点去重" : "按节点近似相加"),
      isGraph ? "当前版本用观测节点作者近似参与账户，后续可接 search/mention 流进一步补全。" : "Campaign 内回复、引用和二级讨论里的账户数。能去重时按 handle 去重，否则展示近似值。"
    ),
    kpi(
      "集中度风险",
      `${((summary.concentration_risk || 0) * 100).toFixed(1)}`,
      "%", null,
      concentrationLabel(summary.concentration_risk),
      isGraph ? "最高贡献节点或传播簇占观测热度的比例。越高说明更像单点驱动，越低说明讨论来源更分散。" : "最高贡献内容节点占总 heat 的比例。越高说明越像单点爆发，越低说明多节点共同贡献。"
    ),
    ...(isGraph ? [kpi(
      "采集健康",
      healthRiskCount ? String(healthRiskCount) : "OK",
      healthRiskCount ? "项" : "", null,
      health.generated_at ? `审计 ${String(health.generated_at).replace("T", " ").replace(/\+.*$/, "")}` : "等待 health audit",
      health.risks && health.risks.length ? esc(health.risks.join("；")) : "托管 health audit 会检查 paid handle 漏抓、Article enrichment 缺口和 watch/search 噪音。"
    )] : []),
  ].join("");

  $("updated-at").textContent = `最近更新：${(summary.latest_ts || "—").replace("T", " ").replace(/\+.*$/, "")}`;
  $("footer-info").textContent = `${summary.name || cid} · ${series.length} 个 campaign 时间桶 · ${fmt(summary.observed_node_count || summary.tracked_tweet_count || 0)} ${isGraph ? "个观测节点" : "个内容节点"}`;
  renderCampaignContributions(contributions, model);

  const points = (fn) => series.map(d => ({ x: new Date(d.ts).getTime(), y: fn(d) }));
  const sampled = (fn, maxPoints = 160, strategy = "last") => downsamplePoints(points(fn), maxPoints, strategy);
  const concentrationSeries = points(d => ((d.concentration_risk ?? summary.concentration_risk ?? 0) * 100));

  makeLineChart("chart-heat", [
    {
      label: isGraph ? "实体热度脉冲" : "Campaign heat 脉冲",
      data: sampled(d => d.campaign_heat_delta || 0, 120, "extrema"),
      borderColor: "rgba(255,142,114,0.82)",
      backgroundColor: "rgba(255,142,114,0.12)",
      fill: true,
      yAxisID: "y",
      valueFormatter: (v) => fmtSigned(v, 1),
    },
    {
      label: isGraph ? "观测动能" : "Campaign 动能",
      data: sampled(d => d.campaign_heat_velocity_per_min || 0, 120, "extrema"),
      borderColor: "#7bb8ff",
      borderDash: [8, 6],
      yAxisID: "y1",
      valueFormatter: (v) => `${fmtSigned(v, 2)} /分`,
    },
  ], {
    y1: true,
    xMaxTicks: 10,
    yTick: (v) => fmtSigned(v, 0),
    y1Tick: (v) => fmtSigned(v, 2),
  });

  makeLineChart("chart-counts", [
    {
      label: "总曝光",
      data: sampled(d => d.total_views || 0, 140, "last"),
      borderColor: "#a5b0bc",
      backgroundColor: "rgba(165,176,188,0.08)",
      fill: true,
      stepped: true,
      yAxisID: "y1",
      valueFormatter: (v) => fmtCompact(v),
    },
    {
      label: "点赞",
      data: sampled(d => d.total_likes || 0, 140, "last"),
      borderColor: "#63d58c",
      stepped: true,
      yAxisID: "y",
      valueFormatter: (v) => fmt(v),
    },
    {
      label: "转发",
      data: sampled(d => d.total_retweets || 0, 140, "last"),
      borderColor: "#7bb8ff",
      stepped: true,
      yAxisID: "y",
      valueFormatter: (v) => fmt(v),
    },
    {
      label: "回复",
      data: sampled(d => d.total_replies || 0, 140, "last"),
      borderColor: "#f1c464",
      stepped: true,
      yAxisID: "y",
      valueFormatter: (v) => fmt(v),
    },
    {
      label: "引用",
      data: sampled(d => d.total_quotes || 0, 140, "last"),
      borderColor: "#ff6f6f",
      stepped: true,
      yAxisID: "y",
      valueFormatter: (v) => fmt(v),
    },
  ], {
    y1: true,
    yBeginAtZero: true,
    y1BeginAtZero: true,
    xMaxTicks: 8,
    yTick: (v) => fmt(v),
    y1Tick: (v) => fmtCompact(v),
  });

  makeLineChart("chart-cascade", [
    {
      label: isGraph ? "新增观测节点" : "活跃节点",
      data: sampled(d => d.active_nodes || d.active_tweets || 0, 120, "last"),
      borderColor: "#ff8e72",
      backgroundColor: "rgba(255,142,114,0.12)",
      fill: true,
      stepped: true,
      yAxisID: "y",
      valueFormatter: (v) => `${fmt(v)} ${isGraph ? "个" : "条"}`,
    },
    {
      label: isGraph ? "参与账户" : "参与账户近似",
      data: sampled(d => d.unique_engagers_approx || 0, 120, "last"),
      borderColor: "#7bb8ff",
      borderDash: [8, 6],
      yAxisID: "y1",
      valueFormatter: (v) => `${fmt(v)} 人`,
    },
  ], {
    y1: true,
    yBeginAtZero: true,
    y1BeginAtZero: true,
    xMaxTicks: 8,
    yTick: (v) => fmt(v),
    y1Tick: (v) => fmt(v),
  });

  makeLineChart("chart-reach", [
    {
      label: "扩散势能",
      data: sampled(d => d.distribution_potential_score || 0, 120, "last"),
      borderColor: "#63d58c",
      backgroundColor: "rgba(99,213,140,0.12)",
      fill: true,
      yAxisID: "y",
      valueFormatter: (v) => `${fmtCompact(v)} 分`,
    },
    {
      label: "集中度风险",
      data: downsamplePoints(concentrationSeries, 120, "last"),
      borderColor: "#f1c464",
      borderDash: [8, 6],
      yAxisID: "y1",
      valueFormatter: (v) => `${fmtSigned(v, 1)}%`,
    },
  ], {
    y1: true,
    yBeginAtZero: true,
    y1BeginAtZero: true,
    xMaxTicks: 10,
    yTick: (v) => fmtCompact(v),
    y1Tick: (v) => `${fmtSigned(v, 0)}%`,
  });
}

function velocityTierLabel(v) {
  if (v == null) return "—";
  if (v > 50) return "🔥 爆发中";
  if (v > 10) return "⚡ 活跃放大";
  if (v > 1) return "🎯 稳定传播";
  if (v > 0.1) return "📉 减速";
  return "💤 基本停止";
}

function wienerLabelZh(w) {
  if (w == null || w === 0) return "—";
  if (w < 1.3) return "浅层广播";
  if (w < 2.5) return "开始分叉";
  if (w < 4.0) return "树状扩散";
  return "深度传播";
}

// ── Track new tweet ──
async function startTracking() {
  if (!TRACK_ENABLED) {
    alert("当前部署为只读模式，已禁用远程启动追踪。");
    return;
  }
  const input = $("tweet-url-input");
  const btn = $("track-btn");
  const raw = input.value.trim();
  if (!raw) return;

  // Extract tweet ID from URL or raw ID
  let tid = raw;
  const m = raw.match(/status\/(\d+)/);
  if (m) tid = m[1];
  if (!/^\d+$/.test(tid)) {
    alert("无法识别推文 ID，请粘贴完整推文链接或纯数字 ID");
    return;
  }

  btn.disabled = true;
  btn.textContent = "启动中…";
  try {
    const resp = await postJsonWithOptionalToken("/api/track", { tweet_id: tid });
    const data = await resp.json();
    if (data.error) {
      alert("启动失败: " + data.error);
    } else {
      input.value = "";
      // Refresh tweet list and switch to new tweet
      currentTid = await renderTweetList();
      // Select the new one
      const sel = $("tweet-selector");
      for (const opt of sel.options) {
        if (opt.value === tid) { sel.value = tid; break; }
      }
      currentTid = tid;
      $("view-selector").value = "tweet";
      syncModeControls();
      renderCurrent();
    }
  } catch (e) {
    alert("请求失败: " + e.message);
  } finally {
    btn.disabled = false;
    btn.textContent = "开始追踪";
  }
}

// Allow Enter key in input
$("tweet-url-input").addEventListener("keydown", (e) => {
  if (e.key === "Enter") startTracking();
});

// ── Main ──
let currentTid = null;
let currentCampaignId = null;
let currentMode = DEFAULT_VIEW;

function applyViewConfig() {
  document.title = `${APP_TITLE} · ${APP_SUBTITLE}`;
  const subtitle = $("app-subtitle");
  if (subtitle) subtitle.textContent = APP_SUBTITLE;
  if (DEFAULT_VIEW === "campaign" && supportsView("campaign")) {
    setChartCopy("campaign");
  }

  const viewSel = $("view-selector");
  for (const opt of Array.from(viewSel.options)) {
    opt.hidden = !supportsView(opt.value);
    opt.disabled = !supportsView(opt.value);
  }
  if (!supportsView(viewSel.value)) viewSel.value = DEFAULT_VIEW;
  viewSel.style.display = ALLOWED_VIEWS.length > 1 ? "" : "none";

  if (!supportsView("tweet")) {
    $("tweet-url-input").style.display = "none";
    $("track-btn").style.display = "none";
    $("tweet-selector").style.display = "none";
  }
  if (!supportsView("campaign")) {
    $("campaign-selector").style.display = "none";
  }
}

function syncModeControls() {
  if (!supportsView($("view-selector").value)) {
    $("view-selector").value = DEFAULT_VIEW;
  }
  currentMode = $("view-selector").value;
  const tweetVisible = currentMode === "tweet" && supportsView("tweet");
  const campaignVisible = currentMode === "campaign" && supportsView("campaign");
  $("tweet-selector").style.display = tweetVisible ? "" : "none";
  $("campaign-selector").style.display = campaignVisible ? "" : "none";
  $("tweet-url-input").style.display = (tweetVisible && TRACK_ENABLED) ? "" : "none";
  $("track-btn").style.display = (tweetVisible && TRACK_ENABLED) ? "" : "none";
}

function renderCurrent() {
  syncModeControls();
  if (currentMode === "campaign") {
    if (currentCampaignId) renderCampaignDashboard(currentCampaignId);
  } else if (currentTid) {
    renderDashboard(currentTid);
  }
}

async function init() {
  applyViewConfig();
  if (!TRACK_ENABLED || !supportsView("tweet")) {
    $("tweet-url-input").style.display = "none";
    $("track-btn").style.display = "none";
  }
  currentTid = supportsView("tweet") ? await renderTweetList() : null;
  currentCampaignId = supportsView("campaign") ? await renderCampaignList() : null;
  $("view-selector").value = DEFAULT_VIEW;
  if (INITIAL_CAMPAIGN_ID && supportsView("campaign") && currentCampaignId) {
    $("view-selector").value = "campaign";
  }
  syncModeControls();
  if (!supportsView("tweet") && currentCampaignId) {
    $("view-selector").value = "campaign";
    syncModeControls();
  } else if (!supportsView("campaign") && currentTid) {
    $("view-selector").value = "tweet";
    syncModeControls();
  } else if (!currentTid && currentCampaignId) {
    $("view-selector").value = "campaign";
    syncModeControls();
  }
  $("view-selector").addEventListener("change", () => {
    renderCurrent();
  });
  $("tweet-selector").addEventListener("change", (e) => {
    currentTid = e.target.value;
    renderCurrent();
  });
  $("campaign-selector").addEventListener("change", (e) => {
    currentCampaignId = e.target.value;
    renderCurrent();
  });
  renderCurrent();
  setInterval(() => { renderCurrent(); }, 30000);
}
init();
</script>
</body>
</html>
"""


def render_html() -> str:
    allowed = [v.strip() for v in os.environ.get("XHI_ALLOWED_VIEWS", "tweet,campaign").split(",") if v.strip()]
    if not allowed:
        allowed = ["tweet", "campaign"]
    config = {
        "__XHI_API_BASE__": os.environ.get("XHI_API_BASE", "").rstrip("/"),
        "__XHI_TRACK_ENABLED__": os.environ.get("XHI_TRACK_ENABLED", "1").strip().lower() not in {"0", "false", "no"},
        "__XHI_ALLOWED_VIEWS__": allowed,
        "__XHI_DEFAULT_VIEW__": os.environ.get("XHI_DEFAULT_VIEW", allowed[0]),
        "__XHI_APP_TITLE__": os.environ.get("XHI_APP_TITLE", "x-heat-index"),
        "__XHI_APP_SUBTITLE__": os.environ.get("XHI_APP_SUBTITLE", "推文传播实时监控"),
    }
    script = "<script>\n" + "\n".join(
        f"window.{key} = {json.dumps(value, ensure_ascii=False)};"
        for key, value in config.items()
    ) + "\n</script>"
    html = HTML.replace("<!-- XHI_RUNTIME_CONFIG -->", script)
    if os.environ.get("XHI_USE_CDN_ASSETS", "").strip().lower() in {"1", "true", "yes"}:
        html = html.replace("vendor/chart.umd.min.js", "https://cdn.jsdelivr.net/npm/chart.js@4.4.9/dist/chart.umd.min.js")
        html = html.replace(
            "vendor/chartjs-adapter-date-fns.bundle.min.js",
            "https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js",
        )
    return html


# ──────────────────────────────────────────────────────────────
# Tracker / Walker process management
# ──────────────────────────────────────────────────────────────
def _find_script(name: str) -> str:
    """Find a sibling script (tracker.py / cascade_walker.py)."""
    here = Path(__file__).resolve().parent
    path = here / name
    if path.exists():
        return str(path)
    raise FileNotFoundError(f"Cannot find {name} in {here}")


def start_tracker(tweet_id: str, env_overrides: dict[str, str] | None = None) -> dict:
    """Start tracker + walker for a tweet_id if not already running."""
    with _lock:
        # Check if already running
        if tweet_id in _running_trackers:
            proc = _running_trackers[tweet_id]
            if proc.poll() is None:  # still alive
                return {"status": "already_running", "tweet_id": tweet_id}

        # Ensure data dir exists
        tweet_dir = DATA_DIR / tweet_id
        tweet_dir.mkdir(parents=True, exist_ok=True)

        env = {**os.environ, "TWEET_ID": tweet_id, "DATA_DIR": str(DATA_DIR)}
        if env_overrides:
            env.update({str(key): str(value) for key, value in env_overrides.items()})

        # Start tracker
        tracker_script = _find_script("tracker.py")
        tracker_proc = subprocess.Popen(
            [sys.executable, tracker_script],
            env=env,
        )
        _running_trackers[tweet_id] = tracker_proc

        # Start walker (with slight delay handled by walker itself)
        walker_script = _find_script("cascade_walker.py")
        walker_proc = subprocess.Popen(
            [sys.executable, walker_script],
            env=env,
        )
        _running_walkers[tweet_id] = walker_proc

        print(f"[frontend] Started tracker (pid={tracker_proc.pid}) + walker (pid={walker_proc.pid}) for {tweet_id}", flush=True)
        return {"status": "started", "tweet_id": tweet_id}


def list_running() -> list:
    """List currently tracked tweet_ids with process status."""
    with _lock:
        out = []
        tweet_ids = sorted(set(_running_trackers) | set(_running_walkers))
        for tid in tweet_ids:
            tracker_proc = _running_trackers.get(tid)
            walker_proc = _running_walkers.get(tid)
            tracker_alive = bool(tracker_proc and tracker_proc.poll() is None)
            walker_alive = bool(walker_proc and walker_proc.poll() is None)
            out.append({"tweet_id": tid, "tracker_alive": tracker_alive, "walker_alive": walker_alive})
        return out


# ──────────────────────────────────────────────────────────────
# Data loading (stdlib, per-request)
# ──────────────────────────────────────────────────────────────
def load_jsonl(path: Path) -> list:
    if not path.exists():
        return []
    stat = path.stat()
    with _jsonl_cache_lock:
        cached = _jsonl_cache.get(path)
        if cached and cached[0] == stat.st_mtime_ns and cached[1] == stat.st_size:
            return cached[2]

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

    with _jsonl_cache_lock:
        _jsonl_cache[path] = (stat.st_mtime_ns, stat.st_size, out)
    return out


def load_jsonl_latest(path: Path) -> dict:
    if not path.exists():
        return {}
    stat = path.stat()
    with _jsonl_cache_lock:
        cached = _jsonl_latest_cache.get(path)
        if cached and cached[0] == stat.st_mtime_ns and cached[1] == stat.st_size:
            return cached[2]

    latest: dict = {}
    try:
        read_size = min(stat.st_size, 256 * 1024)
        with path.open("rb") as fh:
            fh.seek(max(0, stat.st_size - read_size))
            raw = fh.read(read_size)
        if stat.st_size > read_size:
            raw = raw.split(b"\n", 1)[-1]
        for line in reversed(raw.splitlines()):
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(parsed, dict):
                latest = parsed
                break
    except OSError:
        latest = {}

    if not latest:
        rows = load_jsonl(path)
        latest = rows[-1] if rows and isinstance(rows[-1], dict) else {}

    with _jsonl_cache_lock:
        _jsonl_latest_cache[path] = (stat.st_mtime_ns, stat.st_size, latest)
    return latest


def load_json_object(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return obj if isinstance(obj, dict) else {}


def load_campaign_health(campaign_id: str) -> dict:
    raw = load_json_object(graph_campaign_dir(campaign_id) / "health_audit.json")
    if not raw:
        return {}
    summary = raw.get("summary") if isinstance(raw.get("summary"), dict) else {}
    risks = raw.get("risks") if isinstance(raw.get("risks"), list) else []
    return {
        "generated_at": raw.get("generated_at") or "",
        "risk_count": len(risks),
        "risks": [str(r) for r in risks[:5]],
        "noise_candidates": safe_int(summary.get("noise_candidates")),
        "article_enrichment_gaps": safe_int(summary.get("article_enrichment_gaps")),
        "signaled_conversations": safe_int(summary.get("signaled_conversations")),
        "raw_rows": safe_int(summary.get("raw_rows")),
        "unique_tweets": safe_int(summary.get("unique_tweets")),
    }


def compact_series(rows: list, keep_recent: int, target_points: int) -> list:
    """Keep recent history at full resolution and sample the older tail."""
    if not rows or len(rows) <= target_points:
        return rows

    keep_recent = max(1, keep_recent)
    recent = rows[-keep_recent:]
    older = rows[:-keep_recent]
    if not older:
        return rows

    first = older[:1]
    body = older[1:]
    budget = max(0, target_points - len(recent) - len(first))
    if budget <= 0:
        return first + recent
    if len(body) <= budget:
        return first + body + recent

    bucket_size = ceil(len(body) / budget)
    sampled = []
    for idx in range(0, len(body), bucket_size):
        sampled.append(body[min(idx + bucket_size - 1, len(body) - 1)])
    if len(sampled) > budget:
        sampled = sampled[:budget]
    return first + sampled + recent


def list_tweets() -> list:
    tweets = []
    if not DATA_DIR.exists():
        return tweets
    for tid_dir in sorted(DATA_DIR.iterdir()):
        if not tid_dir.is_dir():
            continue
        if not tid_dir.name.isdigit():
            continue
        metrics = load_jsonl(tid_dir / "metrics.jsonl")
        latest = metrics[-1] if metrics else {}
        tweets.append({
            "tweet_id": tid_dir.name,
            "author": latest.get("author_username"),
            "latest_views": latest.get("view_count", 0),
            "cycles": len(metrics),
        })
    return tweets


def load_tweet_data(
    tid: str,
    *,
    compact: bool = False,
    derived_recent: int = 96,
    derived_target: int = 288,
    cascade_recent: int = 48,
    cascade_target: int = 120,
) -> dict:
    d = DATA_DIR / tid
    if not d.exists() or not d.is_dir():
        return {"error": "not found"}
    derived = load_jsonl(d / "derived.jsonl")
    cascade = load_jsonl(d / "cascade_metrics.jsonl")
    errors = load_jsonl(d / "tracker_errors.jsonl")
    derived_raw_count = len(derived)
    cascade_raw_count = len(cascade)
    if compact:
        derived = compact_series(derived, keep_recent=derived_recent, target_points=derived_target)
        cascade = compact_series(cascade, keep_recent=cascade_recent, target_points=cascade_target)
    config_file = d / "config.json"
    config = {}
    if config_file.exists():
        try:
            config = json.loads(config_file.read_text())
        except json.JSONDecodeError:
            pass
    return {
        "derived": derived,
        "cascade": cascade,
        "errors": errors[-20:],
        "config": config,
        "meta": {
            "compact": compact,
            "derived_points": len(derived),
            "derived_points_raw": derived_raw_count,
            "cascade_points": len(cascade),
            "cascade_points_raw": cascade_raw_count,
            "error_points": len(errors),
        },
    }


def numeric_tweet_ids() -> list[str]:
    if not DATA_DIR.exists():
        return []
    return sorted(child.name for child in DATA_DIR.iterdir() if child.is_dir() and child.name.isdigit())


def safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    raw = str(value)
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        pass
    try:
        return datetime.strptime(raw, "%a %b %d %H:%M:%S %z %Y")
    except ValueError:
        return None


def iso_from_timestamp(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds")


def bucket_timestamp(value: str, bucket_minutes: int) -> int | None:
    dt = parse_datetime(value)
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    seconds = int(dt.timestamp())
    bucket = max(60, bucket_minutes * 60)
    return seconds - (seconds % bucket)


def normalize_handle(handle: str) -> str:
    return (handle or "").strip().lower().lstrip("@")


def campaign_config_path(campaign_id: str) -> Path:
    return DATA_DIR / "campaigns" / f"{campaign_id}.json"


def graph_campaign_dir(campaign_id: str) -> Path:
    return DATA_DIR / "campaign_graphs" / campaign_id


def list_config_strings(raw: dict, *keys: str) -> list[str]:
    values = []
    for key in keys:
        item = raw.get(key)
        if isinstance(item, str):
            values.append(item)
        elif isinstance(item, list):
            values.extend(item)
    out = []
    for item in values:
        value = str(item).strip()
        if value and value not in out:
            out.append(value)
    return out


def campaign_identity_terms(raw: dict) -> list[str]:
    identity = raw.get("identity") if isinstance(raw.get("identity"), dict) else {}
    terms = []
    terms.extend(list_config_strings(raw, "terms", "keywords", "identity_terms"))
    terms.extend(list_config_strings(identity, "names", "aliases", "hashtags", "urls", "tickers"))
    return list(dict.fromkeys(terms))


def campaign_watch_handles(raw: dict) -> list[str]:
    identity = raw.get("identity") if isinstance(raw.get("identity"), dict) else {}
    handles = []
    handles.extend(list_config_strings(raw, "watch_handles", "official_handles", "kol_handles"))
    handles.extend(list_config_strings(identity, "official_handles", "watch_handles", "kol_handles"))
    return list(dict.fromkeys(normalize_handle(h) for h in handles if normalize_handle(h)))


def normalize_campaign_config(raw: dict, fallback_id: str) -> dict:
    campaign_id = str(raw.get("campaign_id") or raw.get("id") or fallback_id).strip()
    tweet_ids = []
    for item in raw.get("tweet_ids") or raw.get("root_tweets") or raw.get("tweets") or []:
        tid = str(item).strip()
        if tid.isdigit() and tid not in tweet_ids:
            tweet_ids.append(tid)
    identity_terms = campaign_identity_terms(raw)
    watch_handles = campaign_watch_handles(raw)
    source_mode = str(raw.get("source_mode") or raw.get("mode") or "").strip()
    if not source_mode:
        source_mode = "entity_graph" if identity_terms or watch_handles else "legacy_tweets"
    return {
        "campaign_id": campaign_id,
        "name": str(raw.get("name") or campaign_id),
        "tweet_ids": tweet_ids,
        "identity_terms": identity_terms,
        "watch_handles": watch_handles,
        "graph_nodes_paths": list_config_strings(raw, "graph_nodes_paths", "nodes_paths"),
        "dispatch_slots": raw.get("dispatch_slots") if isinstance(raw.get("dispatch_slots"), list) else [],
        "paid_deliverables": raw.get("paid_deliverables") if isinstance(raw.get("paid_deliverables"), list) else [],
        "source_mode": source_mode,
        "started_at": raw.get("started_at") or "",
        "ended_at": raw.get("ended_at") or "",
        "description": raw.get("description") or "",
    }


def load_campaign_config_file(path: Path, fallback_id: str) -> dict | None:
    try:
        raw = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    cfg = normalize_campaign_config(raw, fallback_id)
    if cfg["campaign_id"] and (cfg["tweet_ids"] or cfg["source_mode"] == "entity_graph"):
        return cfg
    return None


def campaign_configs_cache_state() -> tuple:
    state = []
    for path in (DATA_DIR / "campaigns", DATA_DIR / "campaign_graphs", DATA_DIR):
        try:
            stat = path.stat()
            state.append((str(path), stat.st_mtime_ns, stat.st_size))
        except OSError:
            state.append((str(path), 0, 0))
    return tuple(state)


def load_campaign_configs_uncached() -> list[dict]:
    configs = []
    campaigns_dir = DATA_DIR / "campaigns"
    if campaigns_dir.exists():
        for path in sorted(campaigns_dir.glob("*.json")):
            try:
                raw = json.loads(path.read_text())
            except (OSError, json.JSONDecodeError):
                continue
            cfg = normalize_campaign_config(raw, path.stem)
            if cfg["campaign_id"] and (cfg["tweet_ids"] or cfg["source_mode"] == "entity_graph"):
                configs.append(cfg)

    graph_root = DATA_DIR / "campaign_graphs"
    known_ids = {cfg["campaign_id"] for cfg in configs}
    if graph_root.exists():
        for path in sorted(graph_root.iterdir()):
            if not path.is_dir() or path.name in known_ids:
                continue
            nodes_file = path / "nodes.jsonl"
            cfg_file = path / "config.json"
            if not nodes_file.exists() and not cfg_file.exists():
                continue
            raw = {}
            if cfg_file.exists():
                try:
                    raw = json.loads(cfg_file.read_text())
                except (OSError, json.JSONDecodeError):
                    raw = {}
            cfg = normalize_campaign_config(raw, path.name)
            if not nodes_file.exists() and not cfg.get("graph_nodes_paths"):
                continue
            cfg["source_mode"] = "entity_graph"
            configs.append(cfg)

    all_tweets = numeric_tweet_ids()
    if INCLUDE_ALL_TRACKED and all_tweets:
        configs.insert(0, {
            "campaign_id": "all_tracked",
            "name": "All tracked tweets",
            "tweet_ids": all_tweets,
            "identity_terms": [],
            "watch_handles": [],
            "graph_nodes_paths": [],
            "source_mode": "legacy_tweets",
            "started_at": "",
            "ended_at": "",
            "description": "Auto-generated campaign containing every tracked tweet directory.",
        })
    return configs


def load_campaign_configs() -> list[dict]:
    global _campaign_configs_cache
    state = campaign_configs_cache_state()
    now = time.time()
    if CAMPAIGN_CONFIGS_CACHE_TTL_SECONDS:
        with _campaign_configs_cache_lock:
            cached = _campaign_configs_cache
            if cached is not None:
                cached_at, cached_state, configs = cached
                if cached_state == state and now - cached_at <= CAMPAIGN_CONFIGS_CACHE_TTL_SECONDS:
                    return configs
    configs = load_campaign_configs_uncached()
    if CAMPAIGN_CONFIGS_CACHE_TTL_SECONDS:
        with _campaign_configs_cache_lock:
            _campaign_configs_cache = (time.time(), state, configs)
    return configs


def get_campaign_config(campaign_id: str) -> dict | None:
    campaign_id = str(campaign_id or "").strip()
    if not campaign_id:
        return None
    direct = load_campaign_config_file(campaign_config_path(campaign_id), campaign_id)
    if direct:
        return direct
    graph_dir = graph_campaign_dir(campaign_id)
    nodes_file = graph_dir / "nodes.jsonl"
    graph_cfg = load_campaign_config_file(graph_dir / "config.json", campaign_id)
    if graph_cfg and (nodes_file.exists() or graph_cfg.get("graph_nodes_paths")):
        graph_cfg["source_mode"] = "entity_graph"
        return graph_cfg
    if campaign_id == "all_tracked" and INCLUDE_ALL_TRACKED:
        all_tweets = numeric_tweet_ids()
        if all_tweets:
            return {
                "campaign_id": "all_tracked",
                "name": "All tracked tweets",
                "tweet_ids": all_tweets,
                "identity_terms": [],
                "watch_handles": [],
                "graph_nodes_paths": [],
                "source_mode": "legacy_tweets",
                "started_at": "",
                "ended_at": "",
                "description": "Auto-generated campaign containing every tracked tweet directory.",
            }
    for cfg in load_campaign_configs():
        if cfg["campaign_id"] == campaign_id:
            return cfg
    return None


def campaign_graph_paths(cfg: dict) -> list[Path]:
    cid = cfg["campaign_id"]
    paths = [
        graph_campaign_dir(cid) / "nodes.jsonl",
        DATA_DIR / "campaigns" / cid / "nodes.jsonl",
    ]
    for item in cfg.get("graph_nodes_paths") or cfg.get("nodes_paths") or []:
        path = Path(str(item))
        if not path.is_absolute():
            path = DATA_DIR / path
        paths.append(path)
    return paths


def has_campaign_graph_nodes(cfg: dict) -> bool:
    return any(path.exists() and path.is_file() and path.stat().st_size > 0 for path in campaign_graph_paths(cfg))


def campaign_graph_paths_state(paths: list[Path]) -> tuple:
    path_state = []
    for path in paths:
        try:
            stat = path.stat()
            path_state.append((str(path), stat.st_mtime_ns, stat.st_size))
        except OSError:
            path_state.append((str(path), 0, 0))
    return tuple(path_state)


def campaign_graph_nodes_cache_key(cfg: dict, paths: list[Path], *, ignore_window: bool) -> tuple:
    return (
        cfg.get("campaign_id"),
        bool(ignore_window),
        str(cfg.get("started_at") or ""),
        str(cfg.get("ended_at") or ""),
        campaign_graph_paths_state(paths),
    )


def campaign_graph_nodes_snapshot_path(cfg: dict, *, ignore_window: bool) -> Path:
    suffix = "all" if ignore_window else "window"
    return graph_campaign_dir(str(cfg.get("campaign_id") or "")) / f"nodes.normalized.{suffix}.json"


def load_campaign_nodes_snapshot(cfg: dict, paths: list[Path], *, ignore_window: bool) -> list[dict] | None:
    snapshot_path = campaign_graph_nodes_snapshot_path(cfg, ignore_window=ignore_window)
    try:
        raw = json.loads(snapshot_path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    expected_state = [list(item) for item in campaign_graph_paths_state(paths)]
    if raw.get("version") != CAMPAIGN_GRAPH_NODES_SNAPSHOT_VERSION:
        return None
    if raw.get("campaign_id") != cfg.get("campaign_id"):
        return None
    if bool(raw.get("ignore_window")) != bool(ignore_window):
        return None
    if raw.get("started_at") != str(cfg.get("started_at") or ""):
        return None
    if raw.get("ended_at") != str(cfg.get("ended_at") or ""):
        return None
    if raw.get("path_state") != expected_state:
        return None
    nodes = raw.get("nodes")
    return nodes if isinstance(nodes, list) else None


def write_campaign_nodes_snapshot(cfg: dict, paths: list[Path], nodes: list[dict], *, ignore_window: bool) -> None:
    snapshot_path = campaign_graph_nodes_snapshot_path(cfg, ignore_window=ignore_window)
    try:
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = snapshot_path.with_suffix(snapshot_path.suffix + ".tmp")
        payload = {
            "version": CAMPAIGN_GRAPH_NODES_SNAPSHOT_VERSION,
            "campaign_id": cfg.get("campaign_id"),
            "ignore_window": bool(ignore_window),
            "started_at": str(cfg.get("started_at") or ""),
            "ended_at": str(cfg.get("ended_at") or ""),
            "path_state": [list(item) for item in campaign_graph_paths_state(paths)],
            "node_count": len(nodes),
            "nodes": nodes,
        }
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        tmp_path.replace(snapshot_path)
    except OSError:
        pass


def paid_deliverable_tracker_paths(cfg: dict) -> list[Path]:
    cid = str(cfg.get("campaign_id") or "")
    paths: list[Path] = [
        graph_campaign_dir(cid) / "config.json",
        graph_campaign_dir(cid) / "paid_deliverables.json",
        graph_campaign_dir(cid) / "paid_deliverables.csv",
        DATA_DIR / "campaigns" / f"{cid}.json",
    ]
    for item in load_paid_deliverables(cid, cfg, DATA_DIR):
        tid = str(item.get("tweet_id") or "")
        if not tid:
            continue
        tdir = DATA_DIR / tid
        paths.extend([tdir / "metrics.jsonl", tdir / "derived.jsonl", tdir / "tracker_errors.jsonl"])
    return paths


def iter_jsonl_objects(path: Path):
    try:
        with path.open() as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict):
                    yield row
    except OSError:
        return


def metric_from_node(row: dict, *keys: str) -> int:
    metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
    for key in keys:
        if key in metrics:
            return safe_int(metrics.get(key))
        if key in row:
            return safe_int(row.get(key))
    return 0


def normalize_graph_node(row: dict) -> dict | None:
    if not isinstance(row, dict):
        return None
    tweet_id = str(row.get("tweet_id") or row.get("node_id") or "").strip()
    if not tweet_id:
        return None
    created_at = row.get("created_at") or row.get("ts") or row.get("fetched_at") or ""
    dt = parse_datetime(str(created_at))
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    author = normalize_handle(row.get("author") or row.get("author_username") or "")
    affinity = max(0.0, min(1.0, safe_float(row.get("campaign_affinity"), 1.0)))
    metrics = {
        "views": metric_from_node(row, "views", "view_count"),
        "likes": metric_from_node(row, "likes", "favorite_count", "like_count"),
        "retweets": metric_from_node(row, "retweets", "retweet_count"),
        "replies": metric_from_node(row, "replies", "reply_count"),
        "quotes": metric_from_node(row, "quotes", "quote_count"),
        "bookmarks": metric_from_node(row, "bookmarks", "bookmark_count"),
    }
    return {
        "tweet_id": tweet_id,
        "node_id": str(row.get("node_id") or tweet_id),
        "type": row.get("type") or "tweet",
        "author": author,
        "author_followers": safe_int(row.get("author_followers") or row.get("followers_count")),
        "text": row.get("text") or "",
        "created_at": dt.isoformat(timespec="seconds"),
        "metrics": metrics,
        "campaign_affinity": affinity,
        "affinity_reason": row.get("affinity_reason") or [],
        "source": row.get("source") or "",
        "source_handle": normalize_handle(row.get("source_handle") or ""),
    }


def graph_node_has_identity_signal(node: dict) -> bool:
    if has_identity_signal(node):
        return True
    reasons = node.get("affinity_reason") or []
    return has_paid_deliverable_signal(reasons)


def should_keep_campaign_graph_node(node: dict) -> bool:
    reasons = {str(reason) for reason in (node.get("affinity_reason") or [])}
    if LEGACY_PAID_TIMELINE_BYPASS_REASON not in reasons:
        return True
    source = str(node.get("source") or "")
    if source and source not in LEGACY_TIMELINE_BYPASS_SOURCES:
        return True
    return graph_node_has_identity_signal(node)


def graph_node_attention(node: dict) -> float:
    metrics = node.get("metrics") or {}
    affinity = safe_float(node.get("campaign_affinity"), 1.0)
    return affinity * (
        safe_int(metrics.get("views")) * 0.10
        + safe_int(metrics.get("likes")) * 1.0
        + safe_int(metrics.get("replies")) * 2.7
        + safe_int(metrics.get("retweets")) * 2.0
        + safe_int(metrics.get("quotes")) * 2.0
        + safe_int(metrics.get("bookmarks")) * 1.5
    )


def first_metric_value(*values) -> int:
    for value in values:
        try:
            if value is None or value == "":
                continue
            return int(float(value))
        except (TypeError, ValueError):
            continue
    return 0


def paid_deliverable_seed_metrics(item: dict) -> dict:
    if not isinstance(item, dict):
        return {}
    raw = item.get("participant_metrics")
    if not isinstance(raw, dict):
        raw = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
    return {
        "views": first_metric_value(raw.get("views"), raw.get("view_count")),
        "likes": first_metric_value(raw.get("likes"), raw.get("favorite_count"), raw.get("like_count")),
        "retweets": first_metric_value(raw.get("retweets"), raw.get("retweet_count")),
        "replies": first_metric_value(raw.get("replies"), raw.get("reply_count")),
        "quotes": first_metric_value(raw.get("quotes"), raw.get("quote_count")),
        "bookmarks": first_metric_value(raw.get("bookmarks"), raw.get("bookmark_count")),
    }


def paid_deliverable_tracker_node(item: dict, cfg: dict) -> dict | None:
    tid = paid_deliverable_tweet_id(item)
    if not tid:
        return None
    tdir = DATA_DIR / tid
    metric_latest = load_jsonl_latest(tdir / "metrics.jsonl")
    derived_latest = load_jsonl_latest(tdir / "derived.jsonl")
    error_latest = load_jsonl_latest(tdir / "tracker_errors.jsonl")
    seed_metrics = paid_deliverable_seed_metrics(item)
    error_status = str(error_latest.get("status") or error_latest.get("error_type") or "").strip().lower()
    terminal_error_statuses = {"root_metric_unavailable", "root_unavailable", "tweet_unavailable", "tweet_not_found", "deleted", "private"}

    created_at = (
        metric_latest.get("created_at")
        or item.get("submitted_at")
        or item.get("posted_at")
        or item.get("expected_at")
        or item.get("delivered_at")
        or derived_latest.get("ts")
        or ""
    )
    dt = parse_datetime(str(created_at))
    if not dt:
        dt = parse_datetime(str(cfg.get("started_at") or cfg.get("campaign_start_at") or ""))
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    author = normalize_handle(
        metric_latest.get("author_username")
        or item.get("normalized_author")
        or item.get("username")
        or item.get("handle")
        or item.get("author")
        or item.get("screen_name")
        or ""
    )
    has_tracker_metrics = bool(metric_latest or derived_latest)
    metric_status = str(item.get("metric_status") or item.get("metrics_status") or "").strip().lower()
    if not has_tracker_metrics and error_status in terminal_error_statuses:
        metric_status = error_status
    metrics = {
        "views": first_metric_value(metric_latest.get("view_count"), derived_latest.get("view_count"), seed_metrics.get("views")),
        "likes": first_metric_value(metric_latest.get("favorite_count"), derived_latest.get("favorite_count"), seed_metrics.get("likes")),
        "retweets": first_metric_value(metric_latest.get("retweet_count"), derived_latest.get("retweet_count"), seed_metrics.get("retweets")),
        "replies": first_metric_value(metric_latest.get("reply_count"), derived_latest.get("reply_count"), seed_metrics.get("replies")),
        "quotes": first_metric_value(metric_latest.get("quote_count"), derived_latest.get("quote_count"), seed_metrics.get("quotes")),
        "bookmarks": first_metric_value(metric_latest.get("bookmark_count"), derived_latest.get("bookmark_count"), seed_metrics.get("bookmarks")),
    }
    affinity_reason = [PAID_DELIVERABLE_SEED_SOURCE]
    if has_tracker_metrics:
        affinity_reason.append("tracked_metric")
    elif any(metrics.values()):
        affinity_reason.append("seed_metric")
    elif metric_status in terminal_error_statuses:
        affinity_reason.append("root_metric_unavailable")
    else:
        affinity_reason.append("pending_metric_fetch")
    return {
        "tweet_id": tid,
        "node_id": tid,
        "type": "tweet",
        "author": author,
        "author_followers": safe_int(metric_latest.get("author_followers")),
        "text": metric_latest.get("text") or item.get("label") or item.get("tweet_url") or item.get("url") or "",
        "created_at": dt.isoformat(timespec="seconds"),
        "metrics": metrics,
        "views": metrics["views"],
        "likes": metrics["likes"],
        "retweets": metrics["retweets"],
        "reposts": metrics["retweets"],
        "replies": metrics["replies"],
        "quotes": metrics["quotes"],
        "bookmarks": metrics["bookmarks"],
        "campaign_affinity": 1.0,
        "affinity_reason": affinity_reason,
        "metric_status": metric_status or ("tracked_metric" if has_tracker_metrics else ("seed_metric" if any(metrics.values()) else "pending_metric_fetch")),
        "metric_error": error_latest.get("message") or error_latest.get("error") or "",
        "source": "paid_deliverable_tracker" if has_tracker_metrics else "paid_deliverable_seed",
        "source_handle": author,
        "tweet_url": item.get("tweet_url") or item.get("tweetUrl") or item.get("url") or "",
    }


def load_campaign_graph_nodes(cfg: dict, *, ignore_window: bool = False) -> list[dict]:
    graph_paths = campaign_graph_paths(cfg)
    cache_paths = [*graph_paths, *paid_deliverable_tracker_paths(cfg)]
    cache_key = campaign_graph_nodes_cache_key(cfg, cache_paths, ignore_window=ignore_window)
    with _campaign_graph_nodes_cache_lock:
        cached = _campaign_graph_nodes_cache.get(cache_key)
        if cached is not None:
            return cached
    snapshot_nodes = load_campaign_nodes_snapshot(cfg, cache_paths, ignore_window=ignore_window)
    if snapshot_nodes is not None:
        with _campaign_graph_nodes_cache_lock:
            _campaign_graph_nodes_cache[cache_key] = snapshot_nodes
            while len(_campaign_graph_nodes_cache) > CAMPAIGN_GRAPH_NODES_CACHE_MAX_ITEMS:
                _campaign_graph_nodes_cache.pop(next(iter(_campaign_graph_nodes_cache)))
        return snapshot_nodes

    deduped: dict[str, dict] = {}
    started_at = None if ignore_window else parse_datetime(str(cfg.get("started_at") or ""))
    ended_at = None if ignore_window else parse_datetime(str(cfg.get("ended_at") or ""))
    if started_at and started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    if ended_at and ended_at.tzinfo is None:
        ended_at = ended_at.replace(tzinfo=timezone.utc)
    for path in graph_paths:
        for row in iter_jsonl_objects(path):
            node = normalize_graph_node(row)
            if not node:
                continue
            if not should_keep_campaign_graph_node(node):
                continue
            node_dt = parse_datetime(node.get("created_at", ""))
            if node_dt and node_dt.tzinfo is None:
                node_dt = node_dt.replace(tzinfo=timezone.utc)
            if started_at and node_dt and node_dt < started_at:
                continue
            if ended_at and node_dt and node_dt > ended_at:
                continue
            tid = node["tweet_id"]
            existing = deduped.get(tid)
            if not existing:
                deduped[tid] = node
                continue
            existing_views = safe_int((existing.get("metrics") or {}).get("views"))
            node_views = safe_int((node.get("metrics") or {}).get("views"))
            if node["campaign_affinity"] > existing["campaign_affinity"] or node_views > existing_views:
                deduped[tid] = node

    for item in load_paid_deliverables(str(cfg.get("campaign_id") or ""), cfg, DATA_DIR):
        node = paid_deliverable_tracker_node(item, cfg)
        if not node:
            continue
        tid = node["tweet_id"]
        existing = deduped.get(tid)
        if not existing or graph_node_attention(node) >= graph_node_attention(existing):
            deduped[tid] = node
    nodes = sorted(deduped.values(), key=lambda n: n["created_at"])
    write_campaign_nodes_snapshot(cfg, cache_paths, nodes, ignore_window=ignore_window)
    with _campaign_graph_nodes_cache_lock:
        _campaign_graph_nodes_cache[cache_key] = nodes
        while len(_campaign_graph_nodes_cache) > CAMPAIGN_GRAPH_NODES_CACHE_MAX_ITEMS:
            _campaign_graph_nodes_cache.pop(next(iter(_campaign_graph_nodes_cache)))
    return nodes


def median_positive(values: list[float], default: float = 1.0) -> float:
    positives = sorted(v for v in values if v > 0)
    if not positives:
        return default
    mid = len(positives) // 2
    if len(positives) % 2:
        return positives[mid]
    return (positives[mid - 1] + positives[mid]) / 2


def graph_node_cluster(node: dict, cfg: dict) -> tuple[str, str]:
    author = normalize_handle(node.get("author", ""))
    watch_handles = set(cfg.get("watch_handles") or [])
    text = (node.get("text") or "").strip().lower()
    followers = safe_int(node.get("author_followers"))
    if author in watch_handles:
        return "core_handles", "官方/核心账号"
    if text.startswith("rt @"):
        return "reshare_evidence", "转发/再分发"
    if followers >= 10000:
        return "external_kol", "外部 KOL / 大号"
    return "organic_longtail", "自然讨论长尾"


def compute_graph_model(nodes: list[dict], cfg: dict, bucket_minutes: int) -> tuple[list[dict], dict]:
    buckets: dict[int, dict] = {}
    for node in nodes:
        bts = bucket_timestamp(node.get("created_at", ""), bucket_minutes)
        if bts is None:
            continue
        metrics = node.get("metrics") or {}
        attention = graph_node_attention(node)
        bucket = buckets.setdefault(bts, {
            "nodes": [],
            "authors": set(),
            "attention_raw": 0.0,
            "top_attention": 0.0,
            "views": 0,
            "likes": 0,
            "retweets": 0,
            "replies": 0,
            "quotes": 0,
            "bookmarks": 0,
        })
        bucket["nodes"].append({**node, "_attention": attention})
        if node.get("author"):
            bucket["authors"].add(node["author"])
        bucket["attention_raw"] += attention
        bucket["top_attention"] = max(bucket["top_attention"], attention)
        bucket["views"] += safe_int(metrics.get("views"))
        bucket["likes"] += safe_int(metrics.get("likes"))
        bucket["retweets"] += safe_int(metrics.get("retweets"))
        bucket["replies"] += safe_int(metrics.get("replies"))
        bucket["quotes"] += safe_int(metrics.get("quotes"))
        bucket["bookmarks"] += safe_int(metrics.get("bookmarks"))

    if not buckets:
        return [], {}

    baseline_attention = median_positive([b["attention_raw"] for b in buckets.values()], 1.0)
    baseline_authors = median_positive([len(b["authors"]) for b in buckets.values()], 1.0)
    baseline_discussion = median_positive([b["replies"] + b["quotes"] for b in buckets.values()], 1.0)
    baseline_er = median_positive([
        (b["likes"] + b["retweets"] + b["replies"] + b["quotes"]) / max(1, b["views"])
        for b in buckets.values()
    ], 0.005)

    series = []
    cumulative = {
        "heat_score": 0.0,
        "attention_raw": 0.0,
        "views": 0,
        "likes": 0,
        "retweets": 0,
        "replies": 0,
        "quotes": 0,
        "bookmarks": 0,
    }
    cumulative_authors: set[str] = set()
    author_potential: dict[str, float] = {}
    streak = 0

    for bts in sorted(buckets):
        bucket = buckets[bts]
        engagement = bucket["likes"] + bucket["retweets"] + bucket["replies"] + bucket["quotes"]
        discussion = bucket["replies"] + bucket["quotes"]
        author_count = len(bucket["authors"])
        top_share = bucket["top_attention"] / bucket["attention_raw"] if bucket["attention_raw"] > 0 else 0.0
        concentration_factor = max(0.55, 1.0 - 0.35 * max(0.0, top_share - 0.45))
        streak = streak + 1 if bucket["attention_raw"] >= baseline_attention * 0.5 else 0
        persistence = 1.0 + min(3, max(0, streak - 1)) * 0.12
        engagement_rate = engagement / max(1, bucket["views"])

        attention_surprise = log1p(bucket["attention_raw"] / max(1.0, baseline_attention))
        breadth_surprise = log1p(author_count / max(1.0, baseline_authors))
        discussion_surprise = log1p(discussion / max(1.0, baseline_discussion))
        quality_surprise = max(0.0, log1p(engagement_rate / max(0.0001, baseline_er)) - log1p(1.0))

        heat_delta = 100.0 * (
            attention_surprise * 0.55
            + breadth_surprise * 0.25
            + discussion_surprise * 0.15
            + quality_surprise * 0.05
        ) * concentration_factor * persistence

        cumulative["heat_score"] += heat_delta
        cumulative["attention_raw"] += bucket["attention_raw"]
        cumulative["views"] += bucket["views"]
        cumulative["likes"] += bucket["likes"]
        cumulative["retweets"] += bucket["retweets"]
        cumulative["replies"] += bucket["replies"]
        cumulative["quotes"] += bucket["quotes"]
        cumulative["bookmarks"] += bucket["bookmarks"]
        cumulative_authors.update(bucket["authors"])

        for node in bucket["nodes"]:
            author = node.get("author")
            if not author:
                continue
            potential = sqrt(max(0, safe_int(node.get("author_followers")))) * 25.0 * safe_float(node.get("campaign_affinity"), 1.0)
            author_potential[author] = max(author_potential.get(author, 0.0), potential)

        series.append({
            "ts": iso_from_timestamp(bts),
            "campaign_heat_raw": round(cumulative["attention_raw"], 2),
            "campaign_heat_score": round(cumulative["heat_score"], 2),
            "campaign_heat_delta": round(heat_delta, 2),
            "campaign_heat_velocity_per_min": round(heat_delta / max(1, bucket_minutes), 4),
            "attention_surprise": round(attention_surprise, 4),
            "breadth_surprise": round(breadth_surprise, 4),
            "discussion_surprise": round(discussion_surprise, 4),
            "quality_surprise": round(quality_surprise, 4),
            "concentration_risk": round(top_share, 4),
            "total_views": cumulative["views"],
            "total_likes": cumulative["likes"],
            "total_retweets": cumulative["retweets"],
            "total_replies": cumulative["replies"],
            "total_quotes": cumulative["quotes"],
            "total_bookmarks": cumulative["bookmarks"],
            "view_delta": bucket["views"],
            "engagement_delta": engagement,
            "active_nodes": len(bucket["nodes"]),
            "active_tweets": len(bucket["nodes"]),
            "new_direct_nodes": len(bucket["nodes"]),
            "unique_engagers_approx": len(cumulative_authors),
            "distribution_potential_score": int(sum(author_potential.values())),
            "avg_structural_virality_wiener": 0,
        })

    latest = series[-1]
    total_engagements = cumulative["likes"] + cumulative["retweets"] + cumulative["replies"] + cumulative["quotes"]
    node_attentions = sorted([graph_node_attention(n) for n in nodes], reverse=True)
    concentration_risk = node_attentions[0] / sum(node_attentions) if node_attentions and sum(node_attentions) > 0 else 0.0
    summary = {
        "model": "entity_graph_v1",
        "observed_node_count": len(nodes),
        "tracked_tweet_count": len(nodes),
        "tweet_count": len(nodes),
        "campaign_heat_score": safe_float(latest.get("campaign_heat_score")),
        "campaign_heat_raw": round(cumulative["attention_raw"], 2),
        "concentration_risk": round(concentration_risk, 4),
        "total_views": cumulative["views"],
        "total_engagements": total_engagements,
        "campaign_momentum_per_min": safe_float(latest.get("campaign_heat_velocity_per_min")),
        "active_nodes": safe_int(latest.get("active_nodes")),
        "active_tweets": safe_int(latest.get("active_tweets")),
        "unique_engagers": len(cumulative_authors),
        "unique_engagers_approx": len(cumulative_authors),
        "distribution_potential_score": safe_int(latest.get("distribution_potential_score")),
        "avg_structural_virality_wiener": 0,
        "latest_ts": latest.get("ts", ""),
    }
    return series, summary


def build_graph_contributions(nodes: list[dict], cfg: dict, full_heat: float, bucket_minutes: int) -> list[dict]:
    clusters: dict[str, dict] = {}
    for node in nodes:
        cluster_id, label = graph_node_cluster(node, cfg)
        metrics = node.get("metrics") or {}
        item = clusters.setdefault(cluster_id, {
            "cluster_id": cluster_id,
            "label": label,
            "node_count": 0,
            "view_count": 0,
            "engagement_count": 0,
            "raw_attention": 0.0,
            "top_nodes": [],
        })
        item["node_count"] += 1
        item["view_count"] += safe_int(metrics.get("views"))
        item["engagement_count"] += (
            safe_int(metrics.get("likes"))
            + safe_int(metrics.get("retweets"))
            + safe_int(metrics.get("replies"))
            + safe_int(metrics.get("quotes"))
        )
        item["raw_attention"] += graph_node_attention(node)
        item["top_nodes"].append({
            "tweet_id": node["tweet_id"],
            "author": node.get("author"),
            "views": safe_int(metrics.get("views")),
            "attention": round(graph_node_attention(node), 2),
        })

    for cluster_id, item in clusters.items():
        remaining = [node for node in nodes if graph_node_cluster(node, cfg)[0] != cluster_id]
        _, summary_without = compute_graph_model(remaining, cfg, bucket_minutes)
        item["attributed_heat"] = round(max(0.0, full_heat - safe_float(summary_without.get("campaign_heat_score"))), 2)
        item["top_nodes"] = sorted(item["top_nodes"], key=lambda n: n["attention"], reverse=True)[:3]

    attribution_total = sum(item["attributed_heat"] for item in clusters.values())
    for item in clusters.values():
        item["heat_share"] = round(item["attributed_heat"] / attribution_total, 4) if attribution_total else 0
    return sorted(clusters.values(), key=lambda item: item["attributed_heat"], reverse=True)


def load_entity_campaign_data(
    cfg: dict,
    *,
    compact: bool,
    bucket_minutes: int,
    target_points: int,
) -> dict:
    nodes = load_campaign_graph_nodes(cfg)
    if not nodes:
        return {"error": "no campaign graph nodes"}

    series, model_summary = compute_graph_model(nodes, cfg, bucket_minutes)
    if not series:
        return {"error": "no bucketed campaign graph nodes"}

    stage = classify_campaign_stage(series, len(nodes))
    health = load_campaign_health(cfg["campaign_id"])
    summary = {
        "campaign_id": cfg["campaign_id"],
        "name": cfg["name"],
        "stage": stage,
        "identity_terms": cfg.get("identity_terms") or [],
        "watch_handles": cfg.get("watch_handles") or [],
        "started_at": cfg.get("started_at") or (series[0]["ts"] if series else ""),
        "ended_at": cfg.get("ended_at") or "",
        "health": health,
        "health_risk_count": health.get("risk_count", 0),
        **model_summary,
    }
    contributions = build_graph_contributions(nodes, cfg, summary["campaign_heat_score"], bucket_minutes)
    top_node = max(nodes, key=graph_node_attention) if nodes else None
    if top_node:
        summary["top_observation_node"] = {
            "tweet_id": top_node["tweet_id"],
            "author": top_node.get("author"),
            "attention": round(graph_node_attention(top_node), 2),
        }

    output_series = compact_series(series, keep_recent=96, target_points=target_points) if compact else series
    return {
        "config": cfg,
        "summary": summary,
        "series": output_series,
        "contributions": contributions,
        "meta": {
            "bucket_minutes": bucket_minutes,
            "compact": compact,
            "series_points": len(output_series),
            "series_points_raw": len(series),
            "model": "entity_graph_v1",
            "graph_nodes": len(nodes),
        },
    }


def load_campaign_nodes_data(
    campaign_id: str,
    *,
    offset: int = 0,
    limit: int | None = None,
    ignore_window: bool = False,
) -> dict:
    cfg = get_campaign_config(campaign_id)
    if not cfg:
        return {"error": "not found"}
    nodes = load_campaign_graph_nodes(cfg, ignore_window=ignore_window)
    start = max(0, int(offset or 0))
    if limit is None:
        page_nodes = nodes[start:]
    else:
        page_nodes = nodes[start:start + max(0, int(limit or 0))]
    return {
        "campaign_id": cfg["campaign_id"],
        "config": cfg,
        "node_count": len(nodes),
        "offset": start,
        "limit": limit,
        "ignore_window": ignore_window,
        "returned": len(page_nodes),
        "has_more": start + len(page_nodes) < len(nodes),
        "nodes": page_nodes,
    }


def add_bucket_delta(bucket: dict, row: dict) -> None:
    bucket["heat_delta"] += safe_float(row.get("heat_delta"))
    bucket["view_delta"] += safe_float(row.get("view_delta"))
    bucket["favorite_delta"] += safe_float(row.get("favorite_delta"))
    bucket["retweet_delta"] += safe_float(row.get("retweet_delta"))
    bucket["reply_delta"] += safe_float(row.get("reply_delta"))
    bucket["quote_delta"] += safe_float(row.get("quote_delta"))
    bucket["bookmark_delta"] += safe_float(row.get("bookmark_delta"))
    bucket["engagement_delta"] += (
        safe_float(row.get("favorite_delta"))
        + safe_float(row.get("retweet_delta"))
        + safe_float(row.get("reply_delta"))
        + safe_float(row.get("quote_delta"))
    )
    bucket["new_direct_nodes"] += safe_int(row.get("new_replies_this_cycle")) + safe_int(row.get("new_quotes_this_cycle"))
    bucket["latest"] = row


def latest_row_at_or_before(rows_by_bucket: dict[int, dict], bucket_ts: int, previous: dict | None) -> dict | None:
    row = rows_by_bucket.get(bucket_ts)
    if row and "latest" in row:
        return row.get("latest") or previous
    return row or previous


def collect_campaign_engagers(tweet_ids: list[str]) -> int:
    handles: set[str] = set()
    for tid in tweet_ids:
        tdir = DATA_DIR / tid
        for filename in ("replies.jsonl", "quotes.jsonl", "cascade_nodes.jsonl"):
            for row in load_jsonl(tdir / filename):
                handle = normalize_handle(row.get("author_username", ""))
                if handle:
                    handles.add(handle)
    return len(handles)


def classify_campaign_stage(series: list[dict], tweet_count: int) -> str:
    if len(series) < 3:
        return "launch"
    recent = series[-4:]
    previous = series[-8:-4] if len(series) >= 8 else series[:-4]
    recent_velocity = sum(s.get("campaign_heat_velocity_per_min", 0) for s in recent) / max(1, len(recent))
    previous_velocity = sum(s.get("campaign_heat_velocity_per_min", 0) for s in previous) / max(1, len(previous))
    active = series[-1].get("active_tweets", 0)
    slope = recent_velocity - previous_velocity
    if recent_velocity < 0.1 and active == 0:
        return "dead"
    if slope > 0.5 and active >= max(1, min(2, tweet_count)):
        return "amplification"
    if slope > 0.1:
        return "discovery"
    if recent_velocity > 1 and slope < -0.2:
        return "saturation"
    return "decay"


def invalidate_campaign_summary_cache(campaign_id: str | None = None) -> None:
    global _campaign_configs_cache
    with _campaign_configs_cache_lock:
        _campaign_configs_cache = None
    with _campaign_summary_cache_lock:
        if campaign_id:
            _campaign_summary_cache.pop(str(campaign_id), None)
        else:
            _campaign_summary_cache.clear()
    with _campaign_graph_nodes_cache_lock:
        if campaign_id:
            prefix = str(campaign_id)
            for key in list(_campaign_graph_nodes_cache):
                if str(key[0]) == prefix:
                    _campaign_graph_nodes_cache.pop(key, None)
        else:
            _campaign_graph_nodes_cache.clear()


def slot_date_key(value) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    dt = parse_datetime(raw)
    if dt:
        return dt.date().isoformat()
    if len(raw) >= 10 and raw[4:5] == "-" and raw[7:8] == "-":
        return raw[:10]
    return raw


def extract_tweet_id(value: str) -> str:
    return extract_paid_tweet_id(value)


def paid_deliverable_tweet_id(item: dict) -> str:
    if not isinstance(item, dict):
        return ""
    tid = str(item.get("tweet_id") or item.get("tweetId") or item.get("tid") or "").strip()
    if tid:
        return tid
    return extract_tweet_id(str(item.get("tweet_url") or item.get("tweetUrl") or item.get("url") or ""))


def campaign_delivery_summary(cfg: dict, tracked_tweet_ids: set[str]) -> dict:
    slots = cfg.get("dispatch_slots") if isinstance(cfg.get("dispatch_slots"), list) else []
    paid_rows = load_paid_deliverables(str(cfg.get("campaign_id") or ""), cfg, DATA_DIR)
    by_slot: dict[str, dict] = {}

    for slot in slots:
        if not isinstance(slot, dict):
            continue
        date = slot_date_key(slot.get("date") or slot.get("slot_date") or slot.get("slotDate"))
        if not date:
            continue
        by_slot.setdefault(date, {
            "date": date,
            "planned_seats": safe_int(slot.get("seats") or slot.get("capacity")),
            "submitted_count": 0,
            "tracked_tweet_count": 0,
            "pending_count": 0,
        })

    submitted_deliverables = 0
    for item in paid_rows:
        tid = str(item.get("tweet_id") or paid_deliverable_tweet_id(item))
        if tid:
            submitted_deliverables += 1
        raw = item.get("raw") if isinstance(item.get("raw"), dict) else {}
        date = slot_date_key(raw.get("slot_date") or raw.get("slotDate") or item.get("slot_date") or item.get("slotDate") or item.get("expected_at") or item.get("delivered_at"))
        if not date:
            date = "unassigned"
        slot = by_slot.setdefault(date, {
            "date": date,
            "planned_seats": 0,
            "submitted_count": 0,
            "tracked_tweet_count": 0,
            "pending_count": 0,
        })
        if tid:
            slot["submitted_count"] += 1
            if tid in tracked_tweet_ids:
                slot["tracked_tweet_count"] += 1

    for slot in by_slot.values():
        planned = safe_int(slot.get("planned_seats"))
        slot["pending_count"] = max(0, planned - safe_int(slot.get("submitted_count"))) if planned else 0

    if not paid_rows:
        submitted_deliverables = len([tid for tid in cfg.get("tweet_ids") or [] if str(tid).strip()])

    slot_summary = sorted(
        by_slot.values(),
        key=lambda item: (item["date"] == "unassigned", item["date"]),
    )
    planned_deliverables = sum(safe_int(item.get("planned_seats")) for item in slot_summary)
    if not planned_deliverables:
        planned_deliverables = len(paid_rows) or len(cfg.get("tweet_ids") or [])

    return {
        "deliverable_count": len(paid_rows) or len(cfg.get("tweet_ids") or []),
        "planned_deliverable_count": planned_deliverables,
        "submitted_deliverable_count": submitted_deliverables,
        "pending_deliverable_count": max(0, planned_deliverables - submitted_deliverables) if planned_deliverables else 0,
        "slot_count": len([item for item in slot_summary if item["date"] != "unassigned"]),
        "slot_summary": slot_summary,
    }


def most_common_stage(stages: list[str]) -> str:
    counts: dict[str, int] = {}
    for stage in stages:
        key = str(stage or "").strip()
        if not key:
            continue
        counts[key] = counts.get(key, 0) + 1
    if not counts:
        return "unknown"
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def latest_ts_max(current: str, candidate: str) -> str:
    if not candidate:
        return current
    if not current:
        return candidate
    current_dt = parse_datetime(current)
    candidate_dt = parse_datetime(candidate)
    if current_dt and candidate_dt:
        return candidate if candidate_dt > current_dt else current
    return max(current, candidate)


def load_entity_campaign_summary_data(cfg: dict, *, bucket_minutes: int = 15) -> dict:
    nodes = load_campaign_graph_nodes(cfg)
    if not nodes:
        return {"error": "no campaign graph nodes"}

    series, model_summary = compute_graph_model(nodes, cfg, bucket_minutes)
    if not series:
        return {"error": "no bucketed campaign graph nodes"}

    health = load_campaign_health(cfg["campaign_id"])
    tracked_ids = {str(node.get("tweet_id")) for node in nodes if node.get("tweet_id")}
    delivery = campaign_delivery_summary(cfg, tracked_ids)
    summary = {
        "campaign_id": cfg["campaign_id"],
        "name": cfg["name"],
        "source_mode": "entity_graph",
        "stage": classify_campaign_stage(series, len(nodes)),
        "identity_terms": cfg.get("identity_terms") or [],
        "watch_handles": cfg.get("watch_handles") or [],
        "started_at": cfg.get("started_at") or (series[0]["ts"] if series else ""),
        "ended_at": cfg.get("ended_at") or "",
        "health": health,
        "health_risk_count": health.get("risk_count", 0),
        **model_summary,
        **delivery,
    }
    return {
        "summary": summary,
        "meta": {
            "bucket_minutes": bucket_minutes,
            "compact": True,
            "summary_only": True,
            "summary_mode": "entity_summary",
            "series_points": 0,
            "series_points_raw": len(series),
            "model": "entity_graph_v1",
            "graph_nodes": len(nodes),
        },
    }


def load_legacy_campaign_summary_data(cfg: dict, *, bucket_minutes: int = 15) -> dict:
    tweet_ids = [tid for tid in cfg["tweet_ids"] if (DATA_DIR / tid).is_dir()]
    contributions = []
    total_views = 0
    total_likes = 0
    total_retweets = 0
    total_replies = 0
    total_quotes = 0
    total_bookmarks = 0
    active_tweets = 0
    unique_engagers_approx = 0
    distribution_potential_score = 0
    wiener_weighted_sum = 0.0
    wiener_weight = 0
    momentum = 0.0
    latest_ts = ""
    stages = []

    for tid in tweet_ids:
        tdir = DATA_DIR / tid
        latest = load_jsonl_latest(tdir / "derived.jsonl")
        if not latest:
            continue

        cascade_latest = load_jsonl_latest(tdir / "cascade_metrics.jsonl")
        metric_latest = load_jsonl_latest(tdir / "metrics.jsonl")
        author_name = (
            latest.get("author_username")
            or metric_latest.get("author_username")
            or metric_latest.get("username")
            or ""
        )
        engagement_count = (
            safe_int(latest.get("favorite_count"))
            + safe_int(latest.get("retweet_count"))
            + safe_int(latest.get("reply_count"))
            + safe_int(latest.get("quote_count"))
        )
        heat_score = safe_float(latest.get("heat_score"))
        contributions.append({
            "tweet_id": tid,
            "author": author_name,
            "heat_score": round(heat_score, 2),
            "view_count": safe_int(latest.get("view_count")),
            "engagement_count": engagement_count,
            "quote_count": safe_int(latest.get("quote_count")),
            "reply_count": safe_int(latest.get("reply_count")),
            "unique_engager_count": safe_int(cascade_latest.get("unique_engager_count")),
            "distribution_potential_score": safe_int(
                cascade_latest.get("distribution_potential_score") or cascade_latest.get("reach_adjusted")
            ),
            "stage": latest.get("stage") or "unknown",
        })
        total_views += safe_int(latest.get("view_count"))
        total_likes += safe_int(latest.get("favorite_count"))
        total_retweets += safe_int(latest.get("retweet_count"))
        total_replies += safe_int(latest.get("reply_count"))
        total_quotes += safe_int(latest.get("quote_count"))
        total_bookmarks += safe_int(latest.get("bookmark_count"))
        latest_velocity = safe_float(latest.get("heat_velocity_per_min"))
        momentum += latest_velocity
        if safe_float(latest.get("heat_delta")) > 0 or latest_velocity > 0:
            active_tweets += 1
        engagers = safe_int(cascade_latest.get("unique_engager_count"))
        unique_engagers_approx += engagers
        distribution_potential_score += safe_int(
            cascade_latest.get("distribution_potential_score") or cascade_latest.get("reach_adjusted")
        )
        if engagers > 0:
            wiener_weighted_sum += safe_float(cascade_latest.get("structural_virality_wiener")) * engagers
            wiener_weight += engagers
        latest_ts = latest_ts_max(latest_ts, str(latest.get("ts") or ""))
        stages.append(str(latest.get("stage") or ""))

    contributions.sort(key=lambda item: item["heat_score"], reverse=True)
    raw_heat_total = sum(item["heat_score"] for item in contributions)
    top_heat = contributions[0]["heat_score"] if contributions else 0
    concentration_risk = (top_heat / raw_heat_total) if raw_heat_total > 0 and len(contributions) > 1 else 0.0
    concentration_penalty = 1.0 - 0.4 * concentration_risk
    tracked_ids = {item["tweet_id"] for item in contributions}
    delivery = campaign_delivery_summary(cfg, tracked_ids)

    summary = {
        "campaign_id": cfg["campaign_id"],
        "name": cfg["name"],
        "source_mode": "legacy_tweets",
        "summary_mode": "latest_only",
        "tweet_count": len(tweet_ids),
        "tracked_tweet_count": len(contributions),
        "observed_node_count": len(contributions),
        "stage": most_common_stage(stages),
        "campaign_heat_score": round(raw_heat_total * concentration_penalty, 2),
        "campaign_heat_raw": round(raw_heat_total, 2),
        "concentration_risk": round(concentration_risk, 4),
        "top_contributing_tweet": contributions[0] if contributions else None,
        "total_views": total_views,
        "total_engagements": total_likes + total_retweets + total_replies + total_quotes,
        "total_bookmarks": total_bookmarks,
        "campaign_momentum_per_min": round(momentum, 4),
        "active_tweets": active_tweets,
        "unique_engagers_approx": unique_engagers_approx,
        "distribution_potential_score": distribution_potential_score,
        "avg_structural_virality_wiener": round(wiener_weighted_sum / wiener_weight, 3) if wiener_weight else 0,
        "started_at": cfg.get("started_at") or "",
        "ended_at": cfg.get("ended_at") or "",
        "latest_ts": latest_ts,
        **delivery,
    }
    return {
        "summary": summary,
        "meta": {
            "bucket_minutes": bucket_minutes,
            "compact": True,
            "summary_only": True,
            "summary_mode": "legacy_latest_only",
            "series_points": 0,
            "series_points_raw": None,
            "model": "legacy_tweets",
        },
    }


def load_campaign_summary_data(campaign_id: str, *, bucket_minutes: int = 15) -> dict:
    now = time.monotonic()
    cache_key = str(campaign_id)
    if CAMPAIGN_SUMMARY_CACHE_TTL_SECONDS:
        with _campaign_summary_cache_lock:
            cached = _campaign_summary_cache.get(cache_key)
            if cached and now - cached[0] <= CAMPAIGN_SUMMARY_CACHE_TTL_SECONDS:
                return cached[1]

    cfg = get_campaign_config(campaign_id)
    if not cfg:
        payload = {"error": "not found"}
    elif has_campaign_graph_nodes(cfg):
        payload = load_entity_campaign_summary_data(cfg, bucket_minutes=bucket_minutes)
    else:
        if cfg.get("source_mode") == "entity_graph":
            cfg = {**cfg, "source_mode": "legacy_tweets"}
        payload = load_legacy_campaign_summary_data(cfg, bucket_minutes=bucket_minutes)

    if CAMPAIGN_SUMMARY_CACHE_TTL_SECONDS and not payload.get("error"):
        with _campaign_summary_cache_lock:
            _campaign_summary_cache[cache_key] = (now, payload)
    return payload


def load_campaign_data(
    campaign_id: str,
    *,
    compact: bool = True,
    bucket_minutes: int = 15,
    target_points: int = 288,
) -> dict:
    cfg = get_campaign_config(campaign_id)
    if not cfg:
        return {"error": "not found"}
    graph_nodes_available = has_campaign_graph_nodes(cfg)
    if graph_nodes_available:
        return load_entity_campaign_data(
            cfg,
            compact=compact,
            bucket_minutes=bucket_minutes,
            target_points=target_points,
        )
    if cfg.get("source_mode") == "entity_graph":
        cfg = {**cfg, "source_mode": "legacy_tweets"}

    tweet_ids = [tid for tid in cfg["tweet_ids"] if (DATA_DIR / tid).is_dir()]
    tweet_buckets: dict[str, dict[int, dict]] = {}
    tweet_cascade_buckets: dict[str, dict[int, dict]] = {}
    all_buckets: set[int] = set()
    contributions = []

    for tid in tweet_ids:
        tdir = DATA_DIR / tid
        derived_rows = load_jsonl(tdir / "derived.jsonl")
        if not derived_rows:
            continue

        buckets: dict[int, dict] = {}
        for row in derived_rows:
            bts = bucket_timestamp(row.get("ts", ""), bucket_minutes)
            if bts is None:
                continue
            bucket = buckets.setdefault(bts, {
                "heat_delta": 0.0,
                "view_delta": 0.0,
                "favorite_delta": 0.0,
                "retweet_delta": 0.0,
                "reply_delta": 0.0,
                "quote_delta": 0.0,
                "bookmark_delta": 0.0,
                "engagement_delta": 0.0,
                "new_direct_nodes": 0,
                "latest": row,
            })
            add_bucket_delta(bucket, row)
            all_buckets.add(bts)
        tweet_buckets[tid] = buckets

        cascade_buckets: dict[int, dict] = {}
        for row in load_jsonl(tdir / "cascade_metrics.jsonl"):
            bts = bucket_timestamp(row.get("ts", ""), bucket_minutes)
            if bts is None:
                continue
            cascade_buckets[bts] = row
            all_buckets.add(bts)
        tweet_cascade_buckets[tid] = cascade_buckets

        latest = derived_rows[-1]
        latest_cascade = load_jsonl(tdir / "cascade_metrics.jsonl")
        cascade_latest = latest_cascade[-1] if latest_cascade else {}
        author = load_jsonl(tdir / "metrics.jsonl")
        author_name = (author[-1].get("author_username") if author else "") or ""
        contributions.append({
            "tweet_id": tid,
            "author": author_name,
            "heat_score": round(safe_float(latest.get("heat_score")), 2),
            "view_count": safe_int(latest.get("view_count")),
            "engagement_count": (
                safe_int(latest.get("favorite_count"))
                + safe_int(latest.get("retweet_count"))
                + safe_int(latest.get("reply_count"))
                + safe_int(latest.get("quote_count"))
            ),
            "quote_count": safe_int(latest.get("quote_count")),
            "reply_count": safe_int(latest.get("reply_count")),
            "unique_engager_count": safe_int(cascade_latest.get("unique_engager_count")),
            "distribution_potential_score": safe_int(cascade_latest.get("distribution_potential_score") or cascade_latest.get("reach_adjusted")),
            "stage": latest.get("stage") or "unknown",
        })

    sorted_buckets = sorted(all_buckets)
    last_by_tweet: dict[str, dict] = {}
    last_cascade_by_tweet: dict[str, dict] = {}
    series = []

    for bts in sorted_buckets:
        total_heat = 0.0
        total_views = 0
        total_likes = 0
        total_rts = 0
        total_replies = 0
        total_quotes = 0
        total_bookmarks = 0
        heat_delta = 0.0
        view_delta = 0.0
        engagement_delta = 0.0
        new_direct_nodes = 0
        active_tweets = 0
        unique_engagers_approx = 0
        potential = 0
        wiener_weighted_sum = 0.0
        wiener_weight = 0

        for tid in tweet_ids:
            bucket = tweet_buckets.get(tid, {}).get(bts)
            if bucket:
                last_by_tweet[tid] = bucket["latest"]
                heat_delta += bucket["heat_delta"]
                view_delta += bucket["view_delta"]
                engagement_delta += bucket["engagement_delta"]
                new_direct_nodes += bucket["new_direct_nodes"]
                if bucket["view_delta"] > 0 or bucket["engagement_delta"] > 0 or bucket["heat_delta"] > 0:
                    active_tweets += 1

            latest = last_by_tweet.get(tid)
            if latest:
                total_heat += safe_float(latest.get("heat_score"))
                total_views += safe_int(latest.get("view_count"))
                total_likes += safe_int(latest.get("favorite_count"))
                total_rts += safe_int(latest.get("retweet_count"))
                total_replies += safe_int(latest.get("reply_count"))
                total_quotes += safe_int(latest.get("quote_count"))
                total_bookmarks += safe_int(latest.get("bookmark_count"))

            cascade_row = latest_row_at_or_before(tweet_cascade_buckets.get(tid, {}), bts, last_cascade_by_tweet.get(tid))
            if cascade_row:
                last_cascade_by_tweet[tid] = cascade_row
                engagers = safe_int(cascade_row.get("unique_engager_count"))
                unique_engagers_approx += engagers
                potential += safe_int(cascade_row.get("distribution_potential_score") or cascade_row.get("reach_adjusted"))
                if engagers > 0:
                    wiener_weighted_sum += safe_float(cascade_row.get("structural_virality_wiener")) * engagers
                    wiener_weight += engagers

        if not last_by_tweet:
            continue

        series.append({
            "ts": iso_from_timestamp(bts),
            "campaign_heat_raw": round(total_heat, 2),
            "campaign_heat_delta": round(heat_delta, 2),
            "campaign_heat_velocity_per_min": round(heat_delta / max(1, bucket_minutes), 4),
            "total_views": total_views,
            "total_likes": total_likes,
            "total_retweets": total_rts,
            "total_replies": total_replies,
            "total_quotes": total_quotes,
            "total_bookmarks": total_bookmarks,
            "view_delta": round(view_delta, 2),
            "engagement_delta": round(engagement_delta, 2),
            "active_tweets": active_tweets,
            "new_direct_nodes": new_direct_nodes,
            "unique_engagers_approx": unique_engagers_approx,
            "distribution_potential_score": potential,
            "avg_structural_virality_wiener": round(wiener_weighted_sum / wiener_weight, 3) if wiener_weight else 0,
        })

    if compact:
        series = compact_series(series, keep_recent=96, target_points=target_points)

    contributions.sort(key=lambda item: item["heat_score"], reverse=True)
    raw_heat_total = sum(item["heat_score"] for item in contributions)
    top_heat = contributions[0]["heat_score"] if contributions else 0
    concentration_risk = (top_heat / raw_heat_total) if raw_heat_total > 0 and len(contributions) > 1 else 0.0
    concentration_penalty = 1.0 - 0.4 * concentration_risk
    latest_series = series[-1] if series else {}
    unique_engagers = collect_campaign_engagers(tweet_ids)
    campaign_heat_score = raw_heat_total * concentration_penalty

    for item in contributions:
        item["heat_share"] = round(item["heat_score"] / raw_heat_total, 4) if raw_heat_total else 0

    stage = classify_campaign_stage(series, len(tweet_ids))
    summary = {
        "campaign_id": cfg["campaign_id"],
        "name": cfg["name"],
        "tweet_count": len(tweet_ids),
        "tracked_tweet_count": len(contributions),
        "stage": stage,
        "campaign_heat_score": round(campaign_heat_score, 2),
        "campaign_heat_raw": round(raw_heat_total, 2),
        "concentration_risk": round(concentration_risk, 4),
        "top_contributing_tweet": contributions[0] if contributions else None,
        "total_views": safe_int(latest_series.get("total_views")),
        "total_engagements": (
            safe_int(latest_series.get("total_likes"))
            + safe_int(latest_series.get("total_retweets"))
            + safe_int(latest_series.get("total_replies"))
            + safe_int(latest_series.get("total_quotes"))
        ),
        "campaign_momentum_per_min": safe_float(latest_series.get("campaign_heat_velocity_per_min")),
        "active_tweets": safe_int(latest_series.get("active_tweets")),
        "unique_engagers": unique_engagers,
        "unique_engagers_approx": safe_int(latest_series.get("unique_engagers_approx")),
        "distribution_potential_score": safe_int(latest_series.get("distribution_potential_score")),
        "avg_structural_virality_wiener": safe_float(latest_series.get("avg_structural_virality_wiener")),
        "started_at": cfg.get("started_at") or (series[0]["ts"] if series else ""),
        "ended_at": cfg.get("ended_at") or "",
        "latest_ts": latest_series.get("ts", ""),
    }

    return {
        "config": cfg,
        "summary": summary,
        "series": series,
        "contributions": contributions,
        "meta": {
            "bucket_minutes": bucket_minutes,
            "compact": compact,
            "series_points": len(series),
        },
    }


def list_campaigns() -> list:
    campaigns = []
    for cfg in load_campaign_configs():
        graph_nodes_available = has_campaign_graph_nodes(cfg)
        if graph_nodes_available:
            health = load_campaign_health(cfg["campaign_id"])
            campaigns.append({
                "campaign_id": cfg["campaign_id"],
                "name": cfg["name"],
                "source_mode": "entity_graph",
                "model": "entity_graph_v1",
                "tweet_count": len(cfg["tweet_ids"]),
                "observed_node_count": health.get("unique_tweets", 0),
                "tracked_tweet_count": health.get("unique_tweets", 0),
                "total_views": 0,
                "campaign_heat_score": 0,
                "health_risk_count": health.get("risk_count", 0),
                "stage": "unknown",
                "latest_ts": health.get("generated_at", ""),
            })
            continue

        data = load_campaign_summary_data(cfg["campaign_id"])
        summary = data.get("summary", {})
        model = summary.get("model") or data.get("meta", {}).get("model") or ""
        campaigns.append({
            "campaign_id": cfg["campaign_id"],
            "name": cfg["name"],
            "source_mode": "legacy_tweets",
            "model": model,
            "tweet_count": len(cfg["tweet_ids"]),
            "observed_node_count": summary.get("observed_node_count", 0),
            "tracked_tweet_count": summary.get("tracked_tweet_count", 0),
            "total_views": summary.get("total_views", 0),
            "campaign_heat_score": summary.get("campaign_heat_score", 0),
            "health_risk_count": summary.get("health_risk_count", 0),
            "stage": summary.get("stage", "unknown"),
            "latest_ts": summary.get("latest_ts", ""),
        })
    return campaigns


def extract_track_token(headers) -> str:
    auth = headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return headers.get("X-Track-Token", "").strip() or headers.get("X-Attribution-Token", "").strip()


def track_request_authorized(headers) -> bool:
    if not TRACK_ADMIN_TOKEN:
        return ALLOW_UNAUTH_TRACK
    return extract_track_token(headers) == TRACK_ADMIN_TOKEN


def read_request_authorized(headers) -> bool:
    if not REQUIRE_READ_TOKEN:
        return True
    return track_request_authorized(headers)


def campaign_summary_payload(payload: dict) -> dict:
    if not isinstance(payload, dict) or payload.get("error"):
        return payload
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else payload
    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    return {
        "summary": summary,
        "meta": {
            "compact": meta.get("compact"),
            "model": meta.get("model") or summary.get("model"),
            "series_points": meta.get("series_points"),
            "series_points_raw": meta.get("series_points_raw"),
            "summary_mode": meta.get("summary_mode") or summary.get("summary_mode"),
            "latest_ts": summary.get("latest_ts"),
            "summary_only": True,
        },
    }


# ──────────────────────────────────────────────────────────────
# HTTP handler
# ──────────────────────────────────────────────────────────────
class Handler(SimpleHTTPRequestHandler):
    # Silence default access log — we log on-demand via print()
    def log_message(self, format, *args):
        pass

    def _send_json(self, obj, status: int = 200):
        body = json.dumps(obj, default=str, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, body: str, status: int = 200):
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _send_file(self, path: Path, status: int = 200):
        data = path.read_bytes()
        mime_type, _ = mimetypes.guess_type(str(path))
        self.send_response(status)
        self.send_header("Content-Type", mime_type or "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "public, max-age=31536000, immutable")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = {}
        if parsed.query:
            for part in parsed.query.split("&"):
                key, _, value = part.partition("=")
                query[key] = value

        try:
            if path == "/" or path == "/index.html":
                self._send_html(render_html())
            elif path.startswith("/vendor/"):
                rel = path[len("/vendor/"):].strip("/")
                asset = (VENDOR_DIR / rel).resolve()
                if not rel or VENDOR_DIR.resolve() not in asset.parents:
                    self.send_error(404)
                    return
                if not asset.exists() or not asset.is_file():
                    self.send_error(404)
                    return
                self._send_file(asset)
            elif path == "/api/tweets":
                if not read_request_authorized(self.headers):
                    self._send_json({"error": "unauthorized"}, status=401)
                    return
                self._send_json(list_tweets())
            elif path == "/api/campaigns":
                if not read_request_authorized(self.headers):
                    self._send_json({"error": "unauthorized"}, status=401)
                    return
                self._send_json(list_campaigns())
            elif path.startswith("/api/campaigns/"):
                if not read_request_authorized(self.headers):
                    self._send_json({"error": "unauthorized"}, status=401)
                    return
                campaign_id = path[len("/api/campaigns/"):].strip("/")
                compact = query.get("compact", "1").lower() not in {"0", "false", "no"}
                summary_only = query.get("summary", "0").lower() not in {"", "0", "false", "no"}
                payload = load_campaign_summary_data(campaign_id) if summary_only else load_campaign_data(campaign_id, compact=compact)
                self._send_json(campaign_summary_payload(payload) if summary_only else payload)
            elif path.startswith("/api/data/"):
                if not read_request_authorized(self.headers):
                    self._send_json({"error": "unauthorized"}, status=401)
                    return
                tid = path[len("/api/data/"):].strip("/")
                # Basic sanity: tweet IDs are all-digits
                if not tid.isdigit():
                    self._send_json({"error": "invalid tweet_id"}, status=400)
                    return
                self._send_json(load_tweet_data(tid))
            elif path == "/api/running":
                if not track_request_authorized(self.headers):
                    self._send_json({"error": "unauthorized"}, status=401)
                    return
                self._send_json(list_running())
            elif path == "/health":
                self._send_json({"status": "ok"})
            else:
                self.send_error(404)
        except Exception as e:
            print(f"[frontend] ERROR {path}: {e}", file=sys.stderr, flush=True)
            self._send_json({"error": str(e)}, status=500)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        try:
            if path == "/api/track":
                if not track_request_authorized(self.headers):
                    self._send_json({"error": "unauthorized"}, status=401)
                    return
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length)
                data = json.loads(body) if body else {}
                tweet_id = str(data.get("tweet_id", "")).strip()
                if not tweet_id or not tweet_id.isdigit():
                    self._send_json({"error": "invalid tweet_id"}, status=400)
                    return
                result = start_tracker(tweet_id)
                self._send_json(result)
            else:
                self.send_error(404)
        except Exception as e:
            print(f"[frontend] ERROR POST {path}: {e}", file=sys.stderr, flush=True)
            self._send_json({"error": str(e)}, status=500)


def main():
    print(f"[frontend] x-heat-index serving on http://{BIND}:{PORT}", flush=True)
    print(f"[frontend] DATA_DIR={DATA_DIR}", flush=True)
    httpd = ThreadingHTTPServer((BIND, PORT), Handler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("[frontend] shutting down", flush=True)
        httpd.server_close()


if __name__ == "__main__":
    main()
