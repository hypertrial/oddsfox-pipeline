"""Shared source/scope naming helpers."""

from __future__ import annotations

from dagster import AssetKey

SOURCE_POLYMARKET = "polymarket"
SCOPE_WC2026 = "wc2026"


def flat_name(source: str, scope: str, *parts: str) -> str:
    return "_".join((source, scope, *parts))


def schema_name(source: str, scope: str, layer: str) -> str:
    return flat_name(source, scope, layer)


def asset_key(source: str, scope: str, layer: str, *parts: str) -> AssetKey:
    return AssetKey([source, scope, layer, *parts])


POLYMARKET_WC2026 = flat_name(SOURCE_POLYMARKET, SCOPE_WC2026)

__all__ = [
    "POLYMARKET_WC2026",
    "SCOPE_WC2026",
    "SOURCE_POLYMARKET",
    "asset_key",
    "flat_name",
    "schema_name",
]
