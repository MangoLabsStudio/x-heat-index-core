"""Identity and campaign-signal helpers shared by attribution scripts."""

from __future__ import annotations

import re
from typing import Any, Iterable


SIGNAL_PREFIXES = (
    "identity_term",
    "identity_url",
    "handle_mention",
    "entity_mention",
    "article_identity_term",
    "paid_deliverable_seed",
    "paid_delivery_seed",
)


def normalize_handle(value: Any) -> str:
    return str(value or "").strip().lower().lstrip("@")


def unique_strings(values: Iterable[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in values:
        value = str(item or "").strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def has_identity_signal(row: dict[str, Any], prefixes: tuple[str, ...] = SIGNAL_PREFIXES) -> bool:
    reasons = row.get("affinity_reason") or []
    return any(str(reason).startswith(prefix) for reason in reasons for prefix in prefixes)


def node_conversation_id(row: dict[str, Any]) -> str:
    return str((row.get("relations") or {}).get("conversation_id") or "")


def is_reply_node(row: dict[str, Any]) -> bool:
    return bool(str((row.get("relations") or {}).get("in_reply_to_status_id") or "").strip())


def is_retweet_node(row: dict[str, Any]) -> bool:
    return str(row.get("text") or "").lstrip().startswith("RT @")


def is_article_url(url: Any) -> bool:
    return "i/article" in str(url or "").lower()


def term_in_text(text: str, term: str) -> bool:
    if not text or not term:
        return False
    text_low = text.lower()
    term_low = term.lower().strip()
    if not term_low:
        return False
    # Very short alphanumeric terms must match token boundaries to reduce noise.
    if len(re.sub(r"[^a-z0-9]", "", term_low)) <= 3 and term_low.isalnum():
        return bool(re.search(rf"(?<![a-z0-9_]){re.escape(term_low)}(?![a-z0-9_])", text_low))
    return term_low in text_low
