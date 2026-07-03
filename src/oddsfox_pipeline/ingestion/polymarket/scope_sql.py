"""Market-scope SQL builders (registry-backed, no HTTP dependencies)."""

from __future__ import annotations

import re
from collections.abc import Sequence

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


def validate_market_scopes(
    market_scopes: str | Sequence[str] | None = None,
) -> tuple[str, ...]:
    from oddsfox_pipeline.config import settings

    if market_scopes is None:
        return settings.WC2026_POLYMARKET_MARKET_SCOPES
    if isinstance(market_scopes, str):
        raw_parts = [part.strip() for part in market_scopes.split(",") if part.strip()]
    else:
        raw_parts = [str(part).strip() for part in market_scopes if str(part).strip()]
    if not raw_parts:
        raise ValueError("market_scopes must contain at least one scope")
    seen: set[str] = set()
    validated: list[str] = []
    for part in raw_parts:
        scope = _validate_scope_token(part)
        if scope not in seen:
            seen.add(scope)
            validated.append(scope)
    return tuple(validated)


def validate_market_scope(market_scope: str | None) -> str:
    scopes = validate_market_scopes(
        [market_scope] if market_scope is not None else None
    )
    return scopes[0]


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _registry_scope_sql(alias: str, scope_names: Sequence[str]) -> str:
    registry = wc2026_polymarket_ops_tbl("market_scope_registry")
    return (
        f"{alias}.id IN ("
        f"SELECT market_id FROM {registry} "
        f"WHERE scope_name = {_quote(scope_names[0])}"
        ")"
    )


def market_scope_sql(
    market_scope: str | Sequence[str] | None,
    alias: str = "m",
) -> str:
    """Return a WC2026 registry SQL AND-clause fragment."""
    scopes = validate_market_scopes(market_scope)
    return f"AND {_registry_scope_sql(alias, scopes)}"


def market_scope_predicate_sql(
    market_scope: str | Sequence[str] | None,
    alias: str = "m",
) -> str:
    """Return a bare WC2026 registry boolean SQL predicate."""
    scopes = validate_market_scopes(market_scope)
    return _registry_scope_sql(alias, scopes)


__all__ = [
    "DEFAULT_MARKET_SCOPE",
    "market_scope_predicate_sql",
    "market_scope_sql",
    "validate_market_scope",
    "validate_market_scopes",
]
