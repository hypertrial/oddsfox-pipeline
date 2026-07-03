"""Polymarket / HTTP tuning and CLOB env vars."""

from __future__ import annotations

import os

from oddsfox.config._env import (
    _env_bool,
    _env_float,
    _env_int,
    _optional_env_int,
)

GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"

MARKETS_REQUESTS_PER_SECOND = _env_int("MARKETS_REQUESTS_PER_SECOND", 10)
ODDS_REQUESTS_PER_SECOND = _env_int("ODDS_REQUESTS_PER_SECOND", 40)
HTTP_CONNECT_TIMEOUT_SECONDS = _env_float("HTTP_CONNECT_TIMEOUT_SECONDS", 5.0)
HTTP_READ_TIMEOUT_SECONDS = _env_float("HTTP_READ_TIMEOUT_SECONDS", 60.0)
HTTP_REQUEST_TIMEOUT = (
    HTTP_CONNECT_TIMEOUT_SECONDS,
    HTTP_READ_TIMEOUT_SECONDS,
)

MIN_ODDS_FIDELITY_MINUTES = 1
DEFAULT_ODDS_FIDELITY_MINUTES = 1440
WHALE_MIN_VOLUME_USD = 100_000.0
DEFAULT_POLYMARKET_MARKET_SCOPE = "wc2026"


def _parse_market_scopes_csv(raw: str | None) -> tuple[str, ...]:
    if not raw or not str(raw).strip():
        return ()
    seen: set[str] = set()
    result: list[str] = []
    for part in str(raw).split(","):
        scope = part.strip().lower()
        if scope and scope not in seen:
            seen.add(scope)
            result.append(scope)
    return tuple(result)


POLYMARKET_MARKET_SCOPES = _parse_market_scopes_csv(
    os.getenv("POLYMARKET_MARKET_SCOPES", DEFAULT_POLYMARKET_MARKET_SCOPE)
) or (DEFAULT_POLYMARKET_MARKET_SCOPE,)
POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED = _env_bool(
    "POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED",
    False,
)
POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED = _env_bool(
    "POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED",
    False,
)
POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED = _env_bool(
    "POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED",
    False,
)

POLYMARKET_SCOPE_EVENT_SLUGS = os.getenv("POLYMARKET_SCOPE_EVENT_SLUGS", "").strip()
POLYMARKET_SCOPE_EVENT_SLUG_PREFIXES = os.getenv(
    "POLYMARKET_SCOPE_EVENT_SLUG_PREFIXES", ""
).strip()
POLYMARKET_SCOPE_EVENT_TAGS = os.getenv("POLYMARKET_SCOPE_EVENT_TAGS", "").strip()
POLYMARKET_SCOPE_MARKET_IDS = os.getenv("POLYMARKET_SCOPE_MARKET_IDS", "").strip()
POLYMARKET_SCOPE_REGISTRY_MAX_EVENT_PAGES = _optional_env_int(
    "POLYMARKET_SCOPE_REGISTRY_MAX_EVENT_PAGES"
)


def _parse_scope_keyset_closed_env() -> bool | None:
    """Default False (open only); empty/any/none omits closed on /events/keyset."""
    raw = os.getenv("POLYMARKET_SCOPE_KEYSET_CLOSED")
    if raw is None:
        return False
    normalized = str(raw).strip().lower()
    if not normalized or normalized in {"any", "all", "none", "null"}:
        return None
    if normalized in {"false", "0", "open"}:
        return False
    if normalized in {"true", "1", "closed"}:
        return True
    return _env_bool("POLYMARKET_SCOPE_KEYSET_CLOSED", False)


def _parse_scope_keyset_volume_min_env() -> float | None:
    """Default 10000; empty/none/null omits volume_min on /events/keyset."""
    raw = os.getenv("POLYMARKET_SCOPE_KEYSET_VOLUME_MIN")
    if raw is None:
        return 10000.0
    normalized = str(raw).strip().lower()
    if not normalized or normalized in {"none", "null"}:
        return None
    try:
        return float(normalized)
    except ValueError:
        return 10000.0


POLYMARKET_SCOPE_KEYSET_VOLUME_MIN = _parse_scope_keyset_volume_min_env()
POLYMARKET_SCOPE_KEYSET_CLOSED = _parse_scope_keyset_closed_env()
POLYMARKET_SCOPE_KEYSET_RELATED_TAGS = _env_bool(
    "POLYMARKET_SCOPE_KEYSET_RELATED_TAGS",
    True,
)
POLYMARKET_SCOPE_TAG_DISCOVERY = _env_bool("POLYMARKET_SCOPE_TAG_DISCOVERY", True)
POLYMARKET_SCOPE_TAG_DISCOVERY_KEYWORDS = os.getenv(
    "POLYMARKET_SCOPE_TAG_DISCOVERY_KEYWORDS",
    "",
).strip()
POLYMARKET_SCOPE_TAG_CLOSURE_ROUNDS = _env_int(
    "POLYMARKET_SCOPE_TAG_CLOSURE_ROUNDS",
    2,
)
POLYMARKET_SCOPE_TAG_CRAWL_MAX = _env_int("POLYMARKET_SCOPE_TAG_CRAWL_MAX", 100)
POLYMARKET_SCOPE_TAG_CLOSURE_KEYWORD_GATE = _env_bool(
    "POLYMARKET_SCOPE_TAG_CLOSURE_KEYWORD_GATE",
    True,
)


def _parse_scope_tag_crawl_denylist() -> tuple[str, ...]:
    raw = os.getenv(
        "POLYMARKET_SCOPE_TAG_CRAWL_DENYLIST",
        "",
    ).strip()
    if not raw:
        return ()
    return tuple(part.strip().lower() for part in raw.split(",") if part.strip())


POLYMARKET_SCOPE_TAG_CRAWL_DENYLIST = _parse_scope_tag_crawl_denylist()
CLOB_API_KEY = os.getenv("CLOB_API_KEY")
CLOB_API_SECRET = os.getenv("CLOB_API_SECRET")
CLOB_API_PASSPHRASE = os.getenv("CLOB_API_PASSPHRASE")

__all__ = [
    "CLOB_API_KEY",
    "CLOB_API_PASSPHRASE",
    "CLOB_API_SECRET",
    "CLOB_API_URL",
    "GAMMA_API_URL",
    "HTTP_CONNECT_TIMEOUT_SECONDS",
    "HTTP_READ_TIMEOUT_SECONDS",
    "HTTP_REQUEST_TIMEOUT",
    "MARKETS_REQUESTS_PER_SECOND",
    "POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED",
    "POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED",
    "POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED",
    "WHALE_MIN_VOLUME_USD",
    "DEFAULT_ODDS_FIDELITY_MINUTES",
    "DEFAULT_POLYMARKET_MARKET_SCOPE",
    "MIN_ODDS_FIDELITY_MINUTES",
    "ODDS_REQUESTS_PER_SECOND",
    "POLYMARKET_MARKET_SCOPES",
    "POLYMARKET_SCOPE_EVENT_SLUG_PREFIXES",
    "POLYMARKET_SCOPE_EVENT_SLUGS",
    "POLYMARKET_SCOPE_EVENT_TAGS",
    "POLYMARKET_SCOPE_KEYSET_CLOSED",
    "POLYMARKET_SCOPE_KEYSET_RELATED_TAGS",
    "POLYMARKET_SCOPE_KEYSET_VOLUME_MIN",
    "POLYMARKET_SCOPE_MARKET_IDS",
    "POLYMARKET_SCOPE_REGISTRY_MAX_EVENT_PAGES",
    "POLYMARKET_SCOPE_TAG_DISCOVERY",
    "POLYMARKET_SCOPE_TAG_DISCOVERY_KEYWORDS",
    "POLYMARKET_SCOPE_TAG_CLOSURE_ROUNDS",
    "POLYMARKET_SCOPE_TAG_CLOSURE_KEYWORD_GATE",
    "POLYMARKET_SCOPE_TAG_CRAWL_DENYLIST",
    "POLYMARKET_SCOPE_TAG_CRAWL_MAX",
]
