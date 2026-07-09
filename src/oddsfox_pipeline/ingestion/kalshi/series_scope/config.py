"""Kalshi market-scope configuration and seed loading."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from oddsfox_pipeline.config.settings import DEFAULT_KALSHI_WC2026_MARKET_SCOPE


def default_market_scopes_seed_path() -> Path:
    return Path(__file__).resolve().parent.parent / "seeds" / "market_scopes.yml"


@dataclass(frozen=True)
class KalshiMarketScopeConfig:
    scope_name: str
    series_tickers: tuple[str, ...]
    excluded_market_suffixes: dict[str, tuple[str, ...]]


def _normalize_scope(scope_name: str | None) -> str:
    normalized = str(scope_name or DEFAULT_KALSHI_WC2026_MARKET_SCOPE).strip().lower()
    if not normalized:
        raise ValueError("scope_name must not be empty")
    return normalized


def _load_seed(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        parsed = yaml.safe_load(handle)
    if not isinstance(parsed, dict):
        raise ValueError(f"Invalid market scope seed at {path}")
    return parsed


def load_market_scope_config(
    *,
    scope_name: str | None = None,
    seed_path: Path | None = None,
) -> KalshiMarketScopeConfig:
    path = seed_path or default_market_scopes_seed_path()
    seed = _load_seed(path)
    default_scope = _normalize_scope(seed.get("default_scope"))
    target_scope = _normalize_scope(scope_name or default_scope)
    scopes = seed.get("scopes")
    if not isinstance(scopes, dict) or target_scope not in scopes:
        raise ValueError(f"Unknown Kalshi scope {target_scope!r} in {path}")
    raw = scopes[target_scope]
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid scope block for {target_scope!r} in {path}")
    series = raw.get("series_tickers")
    if not isinstance(series, list) or not all(isinstance(s, str) for s in series):
        raise ValueError(f"series_tickers must be a list of strings in {path}")
    series_tickers = tuple(s.strip() for s in series if s.strip())
    if not series_tickers:
        raise ValueError(f"series_tickers must not be empty for {target_scope!r}")
    excluded_raw = raw.get("excluded_market_suffixes") or {}
    excluded: dict[str, tuple[str, ...]] = {}
    if isinstance(excluded_raw, dict):
        for key, values in excluded_raw.items():
            if isinstance(key, str) and isinstance(values, list):
                excluded[key] = tuple(str(v).strip() for v in values if str(v).strip())
    return KalshiMarketScopeConfig(
        scope_name=target_scope,
        series_tickers=series_tickers,
        excluded_market_suffixes=excluded,
    )


def scope_config_hash(cfg: KalshiMarketScopeConfig) -> str:
    payload = {
        "scope_name": cfg.scope_name,
        "series_tickers": list(cfg.series_tickers),
        "excluded_market_suffixes": {
            key: list(values) for key, values in cfg.excluded_market_suffixes.items()
        },
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()


def market_suffix_excluded(
    cfg: KalshiMarketScopeConfig,
    *,
    series_ticker: str,
    market_ticker: str,
) -> bool:
    suffixes = cfg.excluded_market_suffixes.get(series_ticker, ())
    if not suffixes:
        return False
    return any(market_ticker.endswith(f"-{suffix}") for suffix in suffixes)


__all__ = [
    "KalshiMarketScopeConfig",
    "default_market_scopes_seed_path",
    "load_market_scope_config",
    "market_suffix_excluded",
    "scope_config_hash",
]
