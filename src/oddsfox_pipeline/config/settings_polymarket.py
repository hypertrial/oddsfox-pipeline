"""Polymarket / HTTP tuning and CLOB env vars."""

from __future__ import annotations

import os

from oddsfox_pipeline.config._env import (
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
WC2026_POLYMARKET_WHALE_MIN_VOLUME_USD = 100_000.0
DEFAULT_WC2026_POLYMARKET_MARKET_SCOPE = "wc2026"
WC2026_POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED = _env_bool(
    "WC2026_POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED",
    False,
)
WC2026_POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED = _env_bool(
    "WC2026_POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED",
    False,
)
WC2026_POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED = _env_bool(
    "WC2026_POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED",
    False,
)

WC2026_POLYMARKET_SCOPE_EVENT_SLUGS = os.getenv(
    "WC2026_POLYMARKET_SCOPE_EVENT_SLUGS",
    "",
).strip()
WC2026_POLYMARKET_SCOPE_EVENT_SLUG_PREFIXES = os.getenv(
    "WC2026_POLYMARKET_SCOPE_EVENT_SLUG_PREFIXES", ""
).strip()
WC2026_POLYMARKET_SCOPE_EVENT_TAGS = os.getenv(
    "WC2026_POLYMARKET_SCOPE_EVENT_TAGS",
    "",
).strip()
WC2026_POLYMARKET_SCOPE_MARKET_IDS = os.getenv(
    "WC2026_POLYMARKET_SCOPE_MARKET_IDS",
    "",
).strip()
WC2026_POLYMARKET_SCOPE_REGISTRY_MAX_EVENT_PAGES = _optional_env_int(
    "WC2026_POLYMARKET_SCOPE_REGISTRY_MAX_EVENT_PAGES"
)


def _parse_scope_keyset_closed_env() -> bool | None:
    """Default False (open only); empty/any/none omits closed on /events/keyset."""
    raw = os.getenv("WC2026_POLYMARKET_SCOPE_KEYSET_CLOSED")
    if raw is None:
        return False
    normalized = str(raw).strip().lower()
    if not normalized or normalized in {"any", "all", "none", "null"}:
        return None
    if normalized in {"false", "0", "open"}:
        return False
    if normalized in {"true", "1", "closed"}:
        return True
    return _env_bool("WC2026_POLYMARKET_SCOPE_KEYSET_CLOSED", False)


def _parse_scope_keyset_volume_min_env() -> float | None:
    """Default 10000; empty/none/null omits volume_min on /events/keyset."""
    raw = os.getenv("WC2026_POLYMARKET_SCOPE_KEYSET_VOLUME_MIN")
    if raw is None:
        return 10000.0
    normalized = str(raw).strip().lower()
    if not normalized or normalized in {"none", "null"}:
        return None
    try:
        return float(normalized)
    except ValueError:
        return 10000.0


WC2026_POLYMARKET_SCOPE_KEYSET_VOLUME_MIN = _parse_scope_keyset_volume_min_env()
WC2026_POLYMARKET_SCOPE_KEYSET_CLOSED = _parse_scope_keyset_closed_env()
WC2026_POLYMARKET_SCOPE_KEYSET_RELATED_TAGS = _env_bool(
    "WC2026_POLYMARKET_SCOPE_KEYSET_RELATED_TAGS",
    True,
)
WC2026_POLYMARKET_SCOPE_TAG_DISCOVERY = _env_bool(
    "WC2026_POLYMARKET_SCOPE_TAG_DISCOVERY",
    True,
)
WC2026_POLYMARKET_SCOPE_TAG_DISCOVERY_KEYWORDS = os.getenv(
    "WC2026_POLYMARKET_SCOPE_TAG_DISCOVERY_KEYWORDS",
    "",
).strip()
WC2026_POLYMARKET_SCOPE_TAG_CLOSURE_ROUNDS = _env_int(
    "WC2026_POLYMARKET_SCOPE_TAG_CLOSURE_ROUNDS",
    2,
)
WC2026_POLYMARKET_SCOPE_TAG_CRAWL_MAX = _env_int(
    "WC2026_POLYMARKET_SCOPE_TAG_CRAWL_MAX",
    100,
)
WC2026_POLYMARKET_SCOPE_TAG_CLOSURE_KEYWORD_GATE = _env_bool(
    "WC2026_POLYMARKET_SCOPE_TAG_CLOSURE_KEYWORD_GATE",
    True,
)


def _parse_scope_tag_crawl_denylist() -> tuple[str, ...]:
    raw = os.getenv(
        "WC2026_POLYMARKET_SCOPE_TAG_CRAWL_DENYLIST",
        "",
    ).strip()
    if not raw:
        return ()
    return tuple(part.strip().lower() for part in raw.split(",") if part.strip())


WC2026_POLYMARKET_SCOPE_TAG_CRAWL_DENYLIST = _parse_scope_tag_crawl_denylist()
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
    "WC2026_POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED",
    "WC2026_POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED",
    "WC2026_POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED",
    "WC2026_POLYMARKET_WHALE_MIN_VOLUME_USD",
    "DEFAULT_ODDS_FIDELITY_MINUTES",
    "DEFAULT_WC2026_POLYMARKET_MARKET_SCOPE",
    "MIN_ODDS_FIDELITY_MINUTES",
    "ODDS_REQUESTS_PER_SECOND",
    "WC2026_POLYMARKET_SCOPE_EVENT_SLUG_PREFIXES",
    "WC2026_POLYMARKET_SCOPE_EVENT_SLUGS",
    "WC2026_POLYMARKET_SCOPE_EVENT_TAGS",
    "WC2026_POLYMARKET_SCOPE_KEYSET_CLOSED",
    "WC2026_POLYMARKET_SCOPE_KEYSET_RELATED_TAGS",
    "WC2026_POLYMARKET_SCOPE_KEYSET_VOLUME_MIN",
    "WC2026_POLYMARKET_SCOPE_MARKET_IDS",
    "WC2026_POLYMARKET_SCOPE_REGISTRY_MAX_EVENT_PAGES",
    "WC2026_POLYMARKET_SCOPE_TAG_DISCOVERY",
    "WC2026_POLYMARKET_SCOPE_TAG_DISCOVERY_KEYWORDS",
    "WC2026_POLYMARKET_SCOPE_TAG_CLOSURE_ROUNDS",
    "WC2026_POLYMARKET_SCOPE_TAG_CLOSURE_KEYWORD_GATE",
    "WC2026_POLYMARKET_SCOPE_TAG_CRAWL_DENYLIST",
    "WC2026_POLYMARKET_SCOPE_TAG_CRAWL_MAX",
]
