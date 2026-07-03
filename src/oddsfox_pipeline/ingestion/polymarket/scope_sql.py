"""Market-scope SQL builders (registry-backed, no HTTP dependencies)."""

from __future__ import annotations

import re

from oddsfox_pipeline.config.settings_polymarket import (
    DEFAULT_WC2026_POLYMARKET_MARKET_SCOPE,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import wc2026_polymarket_ops_tbl

DEFAULT_MARKET_SCOPE = DEFAULT_WC2026_POLYMARKET_MARKET_SCOPE

_SCOPE_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$", re.IGNORECASE)


def _validate_scope_token(scope: str) -> str:
    normalized = scope.strip().lower()
    if not _SCOPE_RE.fullmatch(normalized):
        raise ValueError(f"market_scope must be 'wc2026', got {scope!r}")
    if normalized != DEFAULT_MARKET_SCOPE:
        raise ValueError(f"market_scope must be 'wc2026', got {scope!r}")
    return normalized


def validate_market_scope(market_scope: str | None = None) -> str:
    if market_scope is None:
        return DEFAULT_MARKET_SCOPE
    if not isinstance(market_scope, str):
        raise ValueError(f"market_scope must be 'wc2026', got {market_scope!r}")
    return _validate_scope_token(market_scope)


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _registry_scope_sql(alias: str, scope_name: str) -> str:
    registry = wc2026_polymarket_ops_tbl("market_scope_registry")
    return (
        f"{alias}.id IN ("
        f"SELECT market_id FROM {registry} "
        f"WHERE scope_name = {_quote(scope_name)}"
        ")"
    )


def market_scope_sql(
    market_scope: str | None = None,
    alias: str = "m",
) -> str:
    """Return a WC2026 registry SQL AND-clause fragment."""
    scope_name = validate_market_scope(market_scope)
    return f"AND {_registry_scope_sql(alias, scope_name)}"


def market_scope_predicate_sql(
    market_scope: str | None = None,
    alias: str = "m",
) -> str:
    """Return a bare WC2026 registry boolean SQL predicate."""
    scope_name = validate_market_scope(market_scope)
    return _registry_scope_sql(alias, scope_name)


__all__ = [
    "DEFAULT_MARKET_SCOPE",
    "market_scope_predicate_sql",
    "market_scope_sql",
    "validate_market_scope",
]
