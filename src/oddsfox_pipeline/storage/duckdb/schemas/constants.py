"""DuckDB schema names and qualified table helpers."""

from __future__ import annotations

from oddsfox_pipeline.naming import (
    SCOPE_US_MIDTERMS_2026,
    SCOPE_WC2026,
    SOURCE_INTERNATIONAL_RESULTS,
    SOURCE_KALSHI,
    SOURCE_POLYMARKET,
    schema_name,
)

POLYMARKET_WC2026_RAW_SCHEMA = schema_name(SOURCE_POLYMARKET, SCOPE_WC2026, "raw")
POLYMARKET_WC2026_OPS_SCHEMA = schema_name(SOURCE_POLYMARKET, SCOPE_WC2026, "ops")
POLYMARKET_US_MIDTERMS_2026_RAW_SCHEMA = schema_name(
    SOURCE_POLYMARKET, SCOPE_US_MIDTERMS_2026, "raw"
)
POLYMARKET_US_MIDTERMS_2026_OPS_SCHEMA = schema_name(
    SOURCE_POLYMARKET, SCOPE_US_MIDTERMS_2026, "ops"
)
KALSHI_WC2026_RAW_SCHEMA = schema_name(SOURCE_KALSHI, SCOPE_WC2026, "raw")
KALSHI_WC2026_OPS_SCHEMA = schema_name(SOURCE_KALSHI, SCOPE_WC2026, "ops")
INTERNATIONAL_RESULTS_WC2026_RAW_SCHEMA = schema_name(
    SOURCE_INTERNATIONAL_RESULTS, SCOPE_WC2026, "raw"
)

_POLYMARKET_RAW_SCHEMAS: dict[str, str] = {
    SCOPE_WC2026: POLYMARKET_WC2026_RAW_SCHEMA,
    SCOPE_US_MIDTERMS_2026: POLYMARKET_US_MIDTERMS_2026_RAW_SCHEMA,
}
_POLYMARKET_OPS_SCHEMAS: dict[str, str] = {
    SCOPE_WC2026: POLYMARKET_WC2026_OPS_SCHEMA,
    SCOPE_US_MIDTERMS_2026: POLYMARKET_US_MIDTERMS_2026_OPS_SCHEMA,
}


def polymarket_q(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


def polymarket_wc2026_q(schema: str, table: str) -> str:
    return polymarket_q(schema, table)


def polymarket_raw_schema(scope_name: str = SCOPE_WC2026) -> str:
    normalized = scope_name.strip().lower()
    return _POLYMARKET_RAW_SCHEMAS.get(normalized) or schema_name(
        SOURCE_POLYMARKET, normalized, "raw"
    )


def polymarket_ops_schema(scope_name: str = SCOPE_WC2026) -> str:
    normalized = scope_name.strip().lower()
    return _POLYMARKET_OPS_SCHEMAS.get(normalized) or schema_name(
        SOURCE_POLYMARKET, normalized, "ops"
    )


def polymarket_raw_tbl(scope_name: str, table: str) -> str:
    return polymarket_q(polymarket_raw_schema(scope_name), table)


def polymarket_ops_tbl(scope_name: str, table: str) -> str:
    return polymarket_q(polymarket_ops_schema(scope_name), table)


def polymarket_wc2026_raw_tbl(name: str) -> str:
    return polymarket_raw_tbl(SCOPE_WC2026, name)


def polymarket_wc2026_ops_tbl(name: str) -> str:
    return polymarket_ops_tbl(SCOPE_WC2026, name)


def polymarket_us_midterms_2026_raw_tbl(name: str) -> str:
    return polymarket_raw_tbl(SCOPE_US_MIDTERMS_2026, name)


def polymarket_us_midterms_2026_ops_tbl(name: str) -> str:
    return polymarket_ops_tbl(SCOPE_US_MIDTERMS_2026, name)


def kalshi_q(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


def kalshi_raw_schema(scope_name: str = SCOPE_WC2026) -> str:
    normalized = scope_name.strip().lower()
    if normalized == SCOPE_WC2026:
        return KALSHI_WC2026_RAW_SCHEMA
    return schema_name(SOURCE_KALSHI, normalized, "raw")


def kalshi_ops_schema(scope_name: str = SCOPE_WC2026) -> str:
    normalized = scope_name.strip().lower()
    if normalized == SCOPE_WC2026:
        return KALSHI_WC2026_OPS_SCHEMA
    return schema_name(SOURCE_KALSHI, normalized, "ops")


def kalshi_raw_tbl(scope_name: str, table: str) -> str:
    return kalshi_q(kalshi_raw_schema(scope_name), table)


def kalshi_ops_tbl(scope_name: str, table: str) -> str:
    return kalshi_q(kalshi_ops_schema(scope_name), table)


def kalshi_wc2026_raw_tbl(name: str) -> str:
    return kalshi_raw_tbl(SCOPE_WC2026, name)


def kalshi_wc2026_ops_tbl(name: str) -> str:
    return kalshi_ops_tbl(SCOPE_WC2026, name)


def international_results_wc2026_raw_tbl(name: str) -> str:
    return polymarket_q(INTERNATIONAL_RESULTS_WC2026_RAW_SCHEMA, name)


__all__ = [
    "INTERNATIONAL_RESULTS_WC2026_RAW_SCHEMA",
    "KALSHI_WC2026_OPS_SCHEMA",
    "KALSHI_WC2026_RAW_SCHEMA",
    "POLYMARKET_US_MIDTERMS_2026_OPS_SCHEMA",
    "POLYMARKET_US_MIDTERMS_2026_RAW_SCHEMA",
    "POLYMARKET_WC2026_OPS_SCHEMA",
    "POLYMARKET_WC2026_RAW_SCHEMA",
    "international_results_wc2026_raw_tbl",
    "kalshi_ops_schema",
    "kalshi_ops_tbl",
    "kalshi_q",
    "kalshi_raw_schema",
    "kalshi_raw_tbl",
    "kalshi_wc2026_ops_tbl",
    "kalshi_wc2026_raw_tbl",
    "polymarket_ops_schema",
    "polymarket_ops_tbl",
    "polymarket_q",
    "polymarket_raw_schema",
    "polymarket_raw_tbl",
    "polymarket_us_midterms_2026_ops_tbl",
    "polymarket_us_midterms_2026_raw_tbl",
    "polymarket_wc2026_ops_tbl",
    "polymarket_wc2026_q",
    "polymarket_wc2026_raw_tbl",
]
