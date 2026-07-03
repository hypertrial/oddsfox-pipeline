"""DuckDB schema names and qualified table helpers."""

from __future__ import annotations

WC2026_POLYMARKET_RAW_SCHEMA = "wc2026_polymarket_raw"
WC2026_POLYMARKET_OPS_SCHEMA = "wc2026_polymarket_ops"


def wc2026_polymarket_q(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


def wc2026_polymarket_raw_tbl(name: str) -> str:
    return wc2026_polymarket_q(WC2026_POLYMARKET_RAW_SCHEMA, name)


def wc2026_polymarket_ops_tbl(name: str) -> str:
    return wc2026_polymarket_q(WC2026_POLYMARKET_OPS_SCHEMA, name)


__all__ = [
    "WC2026_POLYMARKET_OPS_SCHEMA",
    "WC2026_POLYMARKET_RAW_SCHEMA",
    "wc2026_polymarket_ops_tbl",
    "wc2026_polymarket_q",
    "wc2026_polymarket_raw_tbl",
]
