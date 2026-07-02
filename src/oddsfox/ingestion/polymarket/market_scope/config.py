"""Generic Polymarket market-scope configuration and seed loading."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import yaml

from oddsfox.config.settings import (
    DEFAULT_POLYMARKET_MARKET_SCOPE,
    POLYMARKET_MARKET_SCOPES,
    POLYMARKET_SCOPE_EVENT_SLUG_PREFIXES,
    POLYMARKET_SCOPE_EVENT_SLUGS,
    POLYMARKET_SCOPE_EVENT_TAGS,
    POLYMARKET_SCOPE_KEYSET_CLOSED,
    POLYMARKET_SCOPE_KEYSET_RELATED_TAGS,
    POLYMARKET_SCOPE_KEYSET_VOLUME_MIN,
    POLYMARKET_SCOPE_MARKET_IDS,
    POLYMARKET_SCOPE_REGISTRY_MAX_EVENT_PAGES,
    POLYMARKET_SCOPE_TAG_CLOSURE_KEYWORD_GATE,
    POLYMARKET_SCOPE_TAG_CLOSURE_ROUNDS,
    POLYMARKET_SCOPE_TAG_CRAWL_DENYLIST,
    POLYMARKET_SCOPE_TAG_CRAWL_MAX,
    POLYMARKET_SCOPE_TAG_DISCOVERY,
    POLYMARKET_SCOPE_TAG_DISCOVERY_KEYWORDS,
)


def default_market_scopes_seed_path() -> Path:
    return Path(__file__).resolve().parent.parent / "seeds" / "market_scopes.yml"


def _parse_csv_list(raw: str | None) -> tuple[str, ...]:
    if not raw or not str(raw).strip():
        return ()
    return tuple(s.strip() for s in str(raw).split(",") if s.strip())


def _validate_slug_token(slug: str) -> str:
    s = slug.strip()
    if not s or not re.fullmatch(r"[a-z0-9][a-z0-9-]*", s, flags=re.IGNORECASE):
        raise ValueError(f"Invalid event slug token: {slug!r}")
    return s.lower()


def _optional_list(raw: Any, *, key: str, path: Path) -> list[str]:
    if raw is None:
        return []
    if not isinstance(raw, list) or not all(isinstance(v, str) for v in raw):
        raise ValueError(f"{key} must be a list of strings in {path}")
    return [v.strip() for v in raw if v.strip()]


def _optional_int(raw: Any, *, key: str, path: Path) -> int | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, bool):
        raise ValueError(f"{key} must be an integer or null in {path}")
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be an integer or null in {path}") from exc


def _optional_float(raw: Any, *, key: str, path: Path) -> float | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, bool):
        raise ValueError(f"{key} must be a number or null in {path}")
    try:
        return float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be a number or null in {path}") from exc


def _optional_bool(raw: Any, *, key: str, path: Path) -> bool | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, bool):
        return raw
    value = str(raw).strip().lower()
    if value in {"true", "1", "yes", "on"}:
        return True
    if value in {"false", "0", "no", "off"}:
        return False
    raise ValueError(f"{key} must be a boolean or null in {path}")


def _read_seed(path: Path) -> tuple[str, dict[str, Any]]:
    with path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid YAML root in {path}")
    default_scope = (
        str(raw.get("default_scope") or DEFAULT_POLYMARKET_MARKET_SCOPE).strip().lower()
    )
    scopes = raw.get("scopes")
    if not isinstance(scopes, dict) or not scopes:
        raise ValueError(f"scopes must be a non-empty mapping in {path}")
    return _validate_slug_token(default_scope), scopes


def _scope_payload(
    scopes: dict[str, Any], scope_name: str, *, path: Path
) -> dict[str, Any]:
    raw = scopes.get(scope_name)
    if raw is None:
        raise ValueError(f"Unknown Polymarket market scope {scope_name!r} in {path}")
    if not isinstance(raw, dict):
        raise ValueError(f"scopes.{scope_name} must be a mapping in {path}")
    return raw


@dataclass(frozen=True)
class MarketScopeConfig:
    event_slugs: tuple[str, ...]
    event_slug_prefixes: tuple[str, ...]
    market_ids: tuple[str, ...]
    registry_max_event_pages: int | None
    event_tags: tuple[str, ...] = ()
    scope_name: str = DEFAULT_POLYMARKET_MARKET_SCOPE
    keyset_closed: bool | None = None
    keyset_volume_min: float | None = None
    keyset_related_tags: bool = True
    tag_discovery: bool = True
    tag_discovery_keywords: tuple[str, ...] = ()
    tag_closure_rounds: int = 0
    tag_crawl_max: int | None = None
    tag_closure_keyword_gate: bool = True
    tag_crawl_denylist: tuple[str, ...] = ()

    @property
    def default_event_slug(self) -> str | None:
        return self.event_slugs[0] if self.event_slugs else None

    @property
    def default_keyset_tag_slugs(self) -> tuple[str, ...]:
        return self.event_tags


def load_market_scope_config(
    *,
    scope_name: str | None = None,
    seed_path: Path | None = None,
    event_slugs_override: Sequence[str] | None = None,
    event_slug_prefixes_override: Sequence[str] | None = None,
    event_tags_override: Sequence[str] | None = None,
    market_ids_override: Sequence[str] | None = None,
) -> MarketScopeConfig:
    path = seed_path or default_market_scopes_seed_path()
    default_scope, scopes = _read_seed(path)
    selected_scope = _validate_slug_token(
        scope_name or POLYMARKET_MARKET_SCOPES[0] or default_scope
    )
    payload = _scope_payload(scopes, selected_scope, path=path)

    seed_slugs = _optional_list(
        payload.get("event_slugs"), key="event_slugs", path=path
    )
    seed_prefixes = _optional_list(
        payload.get("event_slug_prefixes"), key="event_slug_prefixes", path=path
    )
    seed_tags = _optional_list(payload.get("event_tags"), key="event_tags", path=path)
    seed_market_ids = _optional_list(
        payload.get("market_ids"), key="market_ids", path=path
    )
    seed_keywords = _optional_list(
        payload.get("tag_discovery_keywords"),
        key="tag_discovery_keywords",
        path=path,
    )
    seed_denylist = _optional_list(
        payload.get("tag_crawl_denylist"), key="tag_crawl_denylist", path=path
    )

    env_slugs = _parse_csv_list(POLYMARKET_SCOPE_EVENT_SLUGS)
    env_prefixes = _parse_csv_list(POLYMARKET_SCOPE_EVENT_SLUG_PREFIXES)
    env_tags = _parse_csv_list(POLYMARKET_SCOPE_EVENT_TAGS)
    env_market_ids = _parse_csv_list(POLYMARKET_SCOPE_MARKET_IDS)
    env_keywords = _parse_csv_list(POLYMARKET_SCOPE_TAG_DISCOVERY_KEYWORDS)

    slugs = list(event_slugs_override or ()) or list(env_slugs) or seed_slugs
    prefixes = (
        list(event_slug_prefixes_override or ()) or list(env_prefixes) or seed_prefixes
    )
    tags = (
        list(event_tags_override)
        if event_tags_override is not None
        else (list(env_tags) or seed_tags)
    )
    market_ids = (
        list(market_ids_override or ()) or list(env_market_ids) or seed_market_ids
    )
    keywords = list(env_keywords) or seed_keywords

    registry_pages = (
        POLYMARKET_SCOPE_REGISTRY_MAX_EVENT_PAGES
        if POLYMARKET_SCOPE_REGISTRY_MAX_EVENT_PAGES is not None
        else _optional_int(
            payload.get("registry_max_event_pages"),
            key="registry_max_event_pages",
            path=path,
        )
    )
    seed_closed = _optional_bool(
        payload.get("keyset_closed"), key="keyset_closed", path=path
    )
    seed_related = _optional_bool(
        payload.get("keyset_related_tags"),
        key="keyset_related_tags",
        path=path,
    )
    seed_discovery = _optional_bool(
        payload.get("tag_discovery"), key="tag_discovery", path=path
    )
    seed_gate = _optional_bool(
        payload.get("tag_closure_keyword_gate"),
        key="tag_closure_keyword_gate",
        path=path,
    )
    seed_volume = _optional_float(
        payload.get("keyset_volume_min"), key="keyset_volume_min", path=path
    )
    tag_crawl_max = _optional_int(
        payload.get("tag_crawl_max"), key="tag_crawl_max", path=path
    )

    return MarketScopeConfig(
        scope_name=selected_scope,
        event_slugs=tuple(_validate_slug_token(s) for s in slugs),
        event_slug_prefixes=tuple(_validate_slug_token(p) for p in prefixes),
        market_ids=tuple(str(m).strip() for m in market_ids if str(m).strip()),
        registry_max_event_pages=registry_pages,
        event_tags=tuple(_validate_slug_token(t) for t in tags),
        keyset_closed=(
            POLYMARKET_SCOPE_KEYSET_CLOSED
            if os.getenv("POLYMARKET_SCOPE_KEYSET_CLOSED") is not None
            else seed_closed
        ),
        keyset_volume_min=(
            POLYMARKET_SCOPE_KEYSET_VOLUME_MIN
            if os.getenv("POLYMARKET_SCOPE_KEYSET_VOLUME_MIN") is not None
            else seed_volume
        ),
        keyset_related_tags=(
            POLYMARKET_SCOPE_KEYSET_RELATED_TAGS
            if os.getenv("POLYMARKET_SCOPE_KEYSET_RELATED_TAGS") is not None
            else (True if seed_related is None else seed_related)
        ),
        tag_discovery=(
            POLYMARKET_SCOPE_TAG_DISCOVERY
            if os.getenv("POLYMARKET_SCOPE_TAG_DISCOVERY") is not None
            else (True if seed_discovery is None else seed_discovery)
        ),
        tag_discovery_keywords=tuple(k.strip().lower() for k in keywords if k.strip()),
        tag_closure_rounds=(
            POLYMARKET_SCOPE_TAG_CLOSURE_ROUNDS
            if os.getenv("POLYMARKET_SCOPE_TAG_CLOSURE_ROUNDS") is not None
            else max(
                0,
                _optional_int(
                    payload.get("tag_closure_rounds"),
                    key="tag_closure_rounds",
                    path=path,
                )
                or 0,
            )
        ),
        tag_crawl_max=(
            POLYMARKET_SCOPE_TAG_CRAWL_MAX
            if os.getenv("POLYMARKET_SCOPE_TAG_CRAWL_MAX") is not None
            else tag_crawl_max
        ),
        tag_closure_keyword_gate=(
            POLYMARKET_SCOPE_TAG_CLOSURE_KEYWORD_GATE
            if os.getenv("POLYMARKET_SCOPE_TAG_CLOSURE_KEYWORD_GATE") is not None
            else (True if seed_gate is None else seed_gate)
        ),
        tag_crawl_denylist=tuple(POLYMARKET_SCOPE_TAG_CRAWL_DENYLIST)
        or tuple(d.strip().lower() for d in seed_denylist if d.strip()),
    )


def scope_config_hash(cfg: MarketScopeConfig) -> str:
    payload = json.dumps(
        {
            "event_slugs": list(cfg.event_slugs),
            "event_slug_prefixes": list(cfg.event_slug_prefixes),
            "event_tags": list(cfg.event_tags),
            "keyset_closed": cfg.keyset_closed,
            "keyset_related_tags": cfg.keyset_related_tags,
            "keyset_volume_min": cfg.keyset_volume_min,
            "market_ids": list(cfg.market_ids),
            "registry_max_event_pages": cfg.registry_max_event_pages,
            "scope_name": cfg.scope_name,
            "tag_closure_keyword_gate": cfg.tag_closure_keyword_gate,
            "tag_closure_rounds": cfg.tag_closure_rounds,
            "tag_crawl_denylist": list(cfg.tag_crawl_denylist),
            "tag_crawl_max": cfg.tag_crawl_max,
            "tag_discovery": cfg.tag_discovery,
            "tag_discovery_keywords": list(cfg.tag_discovery_keywords),
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
