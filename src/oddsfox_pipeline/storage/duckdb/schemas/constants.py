"""DuckDB schema names and qualified table helpers."""

from __future__ import annotations

from oddsfox_pipeline.naming import (
    SCOPE_WC2026,
    SOURCE_INTERNATIONAL_RESULTS,
    SOURCE_POLYMARKET,
    schema_name,
)

POLYMARKET_WC2026_RAW_SCHEMA = schema_name(SOURCE_POLYMARKET, SCOPE_WC2026, "raw")
POLYMARKET_WC2026_OPS_SCHEMA = schema_name(SOURCE_POLYMARKET, SCOPE_WC2026, "ops")
INTERNATIONAL_RESULTS_WC2026_RAW_SCHEMA = schema_name(
    SOURCE_INTERNATIONAL_RESULTS, SCOPE_WC2026, "raw"
)


def polymarket_wc2026_q(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


def polymarket_wc2026_raw_tbl(name: str) -> str:
    return polymarket_wc2026_q(POLYMARKET_WC2026_RAW_SCHEMA, name)


def polymarket_wc2026_ops_tbl(name: str) -> str:
    return polymarket_wc2026_q(POLYMARKET_WC2026_OPS_SCHEMA, name)


def international_results_wc2026_raw_tbl(name: str) -> str:
    return polymarket_wc2026_q(INTERNATIONAL_RESULTS_WC2026_RAW_SCHEMA, name)


__all__ = [
    "INTERNATIONAL_RESULTS_WC2026_RAW_SCHEMA",
    "POLYMARKET_WC2026_OPS_SCHEMA",
    "POLYMARKET_WC2026_RAW_SCHEMA",
    "international_results_wc2026_raw_tbl",
    "polymarket_wc2026_ops_tbl",
    "polymarket_wc2026_q",
    "polymarket_wc2026_raw_tbl",
]
