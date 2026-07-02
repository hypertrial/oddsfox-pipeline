"""Market-scope SQL builders (registry-backed, no HTTP dependencies)."""

from __future__ import annotations

import re

from oddsfox.config.settings_polymarket import DEFAULT_POLYMARKET_MARKET_SCOPE
from oddsfox.storage.duckdb.schemas.constants import polymarket_ops_tbl

DEFAULT_MARKET_SCOPE = DEFAULT_POLYMARKET_MARKET_SCOPE
MARKET_SCOPE_ALL = "all"

_SCOPE_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$", re.IGNORECASE)


def validate_market_scope(market_scope: str | None) -> str:
    from oddsfox.config import settings

    configured_scope = settings.POLYMARKET_MARKET_SCOPE
    scope = (market_scope or configured_scope or DEFAULT_MARKET_SCOPE).strip()
    scope = scope.lower()
    if scope == MARKET_SCOPE_ALL:
        return scope
    if not _SCOPE_RE.fullmatch(scope):
        raise ValueError(
            "market_scope must be 'all' or a slug-like scope name, "
            f"got {market_scope!r}"
        )
    return scope


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _registry_scope_sql(alias: str, scope_name: str) -> str:
    registry = polymarket_ops_tbl("market_scope_registry")
    return (
        f"{alias}.id IN ("
        f"SELECT market_id FROM {registry} WHERE scope_name = {_quote(scope_name)}"
        ")"
    )


def market_scope_sql(
    market_scope: str | None,
    alias: str = "m",
) -> str:
    """Return SQL AND-clause fragment (includes leading AND) or empty for `all`."""
    scope = validate_market_scope(market_scope)
    if scope == MARKET_SCOPE_ALL:
        return ""
    return f"AND {_registry_scope_sql(alias, scope)}"


def market_scope_predicate_sql(
    market_scope: str | None,
    alias: str = "m",
) -> str:
    """Return bare boolean SQL (no leading AND) for scoped/excluded counts."""
    scope = validate_market_scope(market_scope)
    if scope == MARKET_SCOPE_ALL:
        return "TRUE"
    return _registry_scope_sql(alias, scope)


__all__ = [
    "DEFAULT_MARKET_SCOPE",
    "MARKET_SCOPE_ALL",
    "market_scope_predicate_sql",
    "market_scope_sql",
    "validate_market_scope",
]
