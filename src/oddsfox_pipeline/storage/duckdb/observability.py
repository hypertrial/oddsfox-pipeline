"""Read-only DuckDB snapshots for Dagster metadata and logs."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import duckdb

from oddsfox_pipeline.storage.duckdb.connection import (
    INTERNATIONAL_RESULTS_WC2026_RAW_SCHEMA,
    KALSHI_WC2026_OPS_SCHEMA,
    KALSHI_WC2026_RAW_SCHEMA,
    POLYMARKET_WC2026_OPS_SCHEMA,
    POLYMARKET_WC2026_RAW_SCHEMA,
    get_connection,
    polymarket_wc2026_ops_tbl,
    polymarket_wc2026_raw_tbl,
)
from oddsfox_pipeline.storage.duckdb.schemas.dbt_schemas import DBT_EXPECTED_RELATIONS

logger = logging.getLogger(__name__)

_RAW_OPS_TABLES: tuple[tuple[str, str], ...] = (
    (INTERNATIONAL_RESULTS_WC2026_RAW_SCHEMA, "match_results"),
    (POLYMARKET_WC2026_RAW_SCHEMA, "markets"),
    (POLYMARKET_WC2026_RAW_SCHEMA, "market_tokens"),
    (POLYMARKET_WC2026_RAW_SCHEMA, "odds_history"),
    (POLYMARKET_WC2026_RAW_SCHEMA, "match_minute_odds_history"),
    (POLYMARKET_WC2026_RAW_SCHEMA, "token_odds_daily"),
    (POLYMARKET_WC2026_OPS_SCHEMA, "market_scope_registry"),
    (POLYMARKET_WC2026_OPS_SCHEMA, "match_minute_odds_fetch_audit"),
    (POLYMARKET_WC2026_OPS_SCHEMA, "token_sync_ledger"),
    (POLYMARKET_WC2026_OPS_SCHEMA, "token_sync_skips"),
    (POLYMARKET_WC2026_OPS_SCHEMA, "ingestion_run_events"),
    (POLYMARKET_WC2026_OPS_SCHEMA, "sync_run_metrics"),
    (KALSHI_WC2026_RAW_SCHEMA, "events"),
    (KALSHI_WC2026_RAW_SCHEMA, "markets"),
    (KALSHI_WC2026_RAW_SCHEMA, "market_candlesticks_hourly"),
    (KALSHI_WC2026_OPS_SCHEMA, "market_scope_registry"),
    (KALSHI_WC2026_OPS_SCHEMA, "candlestick_sync_ledger"),
    (KALSHI_WC2026_OPS_SCHEMA, "ingestion_run_events"),
    (KALSHI_WC2026_OPS_SCHEMA, "sync_run_metrics"),
)
_WC2026_POLY_SCHEMAS = (POLYMARKET_WC2026_RAW_SCHEMA, POLYMARKET_WC2026_OPS_SCHEMA)
_RAW_SHORT_LOG_TABLES: tuple[str, ...] = tuple(
    table for schema, table in _RAW_OPS_TABLES if schema in _WC2026_POLY_SCHEMAS
)

_TAB_MT = polymarket_wc2026_raw_tbl("market_tokens")
_TAB_OH = polymarket_wc2026_raw_tbl("odds_history")
_TAB_TOD = polymarket_wc2026_raw_tbl("token_odds_daily")
_TAB_LED = polymarket_wc2026_ops_tbl("token_sync_ledger")
_TAB_SKP = polymarket_wc2026_ops_tbl("token_sync_skips")

_MARKET_TOKEN_IDS_CTE = f"""
WITH market_token_ids AS (
    SELECT DISTINCT json_extract_string(je.value, '$') AS token_id
    FROM {_TAB_MT} mt
    CROSS JOIN LATERAL json_each(mt.clobTokenIds) AS je
    WHERE mt.clobTokenIds IS NOT NULL
      AND mt.clobTokenIds != '[]'
      AND LEFT(LTRIM(mt.clobTokenIds), 1) = '['
      AND json_extract_string(je.value, '$') IS NOT NULL
)
"""


def _scalar_int(conn, sql: str) -> int | None:
    try:
        row = conn.execute(sql).fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])
    except duckdb.Error:
        return None
    except (TypeError, ValueError) as exc:
        logger.warning("unexpected value in _scalar_int: %s", exc)
        return None


def _table_row_count(conn, table: str) -> tuple[bool, int | None]:
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        if row is None or row[0] is None:
            return True, 0
        return True, int(row[0])
    except duckdb.Error:
        return False, None
    except (TypeError, ValueError) as exc:
        logger.warning("unexpected value in _table_row_count for %s: %s", table, exc)
        return False, None


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _batch_table_row_counts(
    conn,
    tables: tuple[tuple[str, str], ...],
) -> dict[tuple[str, str], tuple[bool, int | None]]:
    if not tables:
        return {}
    values_sql = ",\n".join(
        f"({_sql_literal(schema)}, {_sql_literal(table)})" for schema, table in tables
    )
    try:
        exists_rows = conn.execute(
            f"""
            WITH wanted(schema_name, table_name) AS (
                VALUES {values_sql}
            )
            SELECT
                w.schema_name,
                w.table_name,
                (t.table_name IS NOT NULL) AS table_exists
            FROM wanted AS w
            LEFT JOIN information_schema.tables AS t
                ON
                    lower(t.table_schema) = lower(w.schema_name)
                    AND lower(t.table_name) = lower(w.table_name)
            """
        ).fetchall()
    except duckdb.Error:
        return {
            (schema, table): _table_row_count(conn, _qualified(schema, table))
            for schema, table in tables
        }
    out: dict[tuple[str, str], tuple[bool, int | None]] = {}
    existing: list[tuple[str, str]] = []
    for schema, table, table_exists in exists_rows:
        key = (str(schema), str(table))
        if not table_exists:
            out[key] = (False, None)
        else:
            existing.append(key)
    if not existing:
        return out
    count_parts = [
        f"SELECT {_sql_literal(schema)} AS schema_name, "
        f"{_sql_literal(table)} AS table_name, "
        f"COUNT(*)::BIGINT AS row_count "
        f"FROM {_qualified(schema, table)}"
        for schema, table in existing
    ]
    try:
        for schema, table, row_count in conn.execute(
            "\nUNION ALL\n".join(count_parts)
        ).fetchall():
            out[(str(schema), str(table))] = (True, int(row_count))
    except duckdb.Error:
        for schema, table in existing:
            out[(schema, table)] = _table_row_count(conn, _qualified(schema, table))
    except (TypeError, ValueError) as exc:
        logger.warning("unexpected value in _batch_table_row_counts: %s", exc)
        for schema, table in existing:
            out[(schema, table)] = _table_row_count(conn, _qualified(schema, table))
    return out


def _dict_rows(conn, sql: str) -> dict[str, int] | None:
    try:
        rows = conn.execute(sql).fetchall()
    except duckdb.Error:
        return None
    except Exception as exc:
        logger.warning("unexpected error in _dict_rows: %s", exc)
        return None
    out: dict[str, int] = {}
    for key, value in rows:
        if key is None or value is None:
            continue
        out[str(key)] = int(value)
    return out


def _normalize_dt(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def _scalar_max_timestamp(conn, sql: str) -> str | None:
    try:
        row = conn.execute(sql).fetchone()
        if row is None:
            return None
        return _normalize_dt(row[0])
    except duckdb.Error:
        return None


def snapshot_raw_layer(conn=None, *, level: str = "full") -> dict[str, Any]:
    """Aggregate row counts and freshness markers for raw + operational tables."""
    snapshot_level = str(level or "full").strip().lower()
    if snapshot_level not in ("basic", "full"):
        raise ValueError("snapshot_raw_layer level must be 'basic' or 'full'")
    out: dict[str, Any] = {}

    def _fill(c) -> None:
        counts = _batch_table_row_counts(c, _RAW_OPS_TABLES)
        for schema, table in _RAW_OPS_TABLES:
            exists, n = counts.get((schema, table), (False, None))
            qualified = f"{schema}.{table}"
            out[f"{qualified}_rows"] = n
            out[f"{qualified}_missing"] = not exists
            if schema in _WC2026_POLY_SCHEMAS:
                out[f"{table}_rows"] = n
                out[f"{table}_missing"] = not exists

        if snapshot_level == "basic":
            return

        out["market_tokens_distinct_tokens"] = _scalar_int(
            c,
            _MARKET_TOKEN_IDS_CTE + "SELECT COUNT(*) FROM market_token_ids",
        )
        out["ledger_distinct_tokens"] = _scalar_int(
            c,
            f"SELECT COUNT(DISTINCT clobTokenId) FROM {_TAB_LED}",
        )
        out["token_sync_skips_distinct_tokens"] = _scalar_int(
            c,
            f"SELECT COUNT(DISTINCT clobTokenId) FROM {_TAB_SKP}",
        )
        out["odds_history_distinct_tokens"] = _scalar_int(
            c,
            f"SELECT COUNT(DISTINCT clobTokenId) FROM {_TAB_OH}",
        )
        out["odds_history_max_ts"] = _scalar_max_timestamp(
            c,
            f"SELECT MAX(timestamp) FROM {_TAB_OH}",
        )
        out["token_odds_daily_distinct_tokens"] = _scalar_int(
            c,
            f"SELECT COUNT(DISTINCT clobTokenId) FROM {_TAB_TOD}",
        )
        out["ledger_fully_checked_tokens"] = _scalar_int(
            c,
            f"SELECT COUNT(*) FROM {_TAB_LED} WHERE fully_checked = TRUE",
        )
        out["market_tokens_without_history"] = _scalar_int(
            c,
            _MARKET_TOKEN_IDS_CTE
            + f"""
            SELECT COUNT(*)
            FROM market_token_ids m
            LEFT JOIN (
                SELECT DISTINCT clobTokenId AS token_id
                FROM {_TAB_OH}
                WHERE clobTokenId IS NOT NULL
            ) h ON h.token_id = m.token_id
            WHERE h.token_id IS NULL
            """,
        )
        out["history_tokens_without_market_tokens"] = _scalar_int(
            c,
            _MARKET_TOKEN_IDS_CTE
            + f"""
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT clobTokenId AS token_id
                FROM {_TAB_OH}
                WHERE clobTokenId IS NOT NULL
            ) h
            LEFT JOIN market_token_ids m ON m.token_id = h.token_id
            WHERE m.token_id IS NULL
            """,
        )
        out["token_sync_skips_by_reason"] = _dict_rows(
            c,
            f"""
            SELECT COALESCE(reason, 'unknown') AS reason, COUNT(*) AS token_count
            FROM {_TAB_SKP}
            GROUP BY 1
            ORDER BY token_count DESC, reason ASC
            """,
        )

    if conn is not None:
        _fill(conn)
    else:
        with get_connection() as c:
            _fill(c)
    return out


def delta_raw_layer(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Return only keys whose values changed between two snapshots."""
    delta: dict[str, Any] = {}
    for key in set(before) | set(after):
        if key.endswith("_missing"):
            continue
        if before.get(key) != after.get(key):
            delta[key] = {"before": before.get(key), "after": after.get(key)}
    return delta


def _qualified(schema: str, name: str) -> str:
    return f'"{schema}"."{name}"'


def _selector_groups(selector: str | None) -> tuple[frozenset[str], ...]:
    if not selector:
        return ()
    groups: list[frozenset[str]] = []
    for part in selector.split():
        token = part.strip().lstrip("+")
        if not token:
            continue
        and_tokens = frozenset(
            piece.strip() for piece in token.split(",") if piece.strip()
        )
        if and_tokens:
            groups.append(and_tokens)
    return tuple(groups)


def _infer_dbt_model_tags(schema: str, model: str) -> frozenset[str]:
    tags: set[str] = set()
    if schema.startswith("polymarket_wc2026_") or model.startswith(
        ("stg_polymarket_wc2026_", "int_polymarket_wc2026_", "polymarket_wc2026_")
    ):
        tags.update({"polymarket", "wc2026"})
    if schema.startswith("kalshi_wc2026_") or model.startswith(
        ("stg_kalshi_wc2026_", "int_kalshi_wc2026_", "kalshi_wc2026_")
    ):
        tags.update({"kalshi", "wc2026"})
    if schema.startswith("international_results_wc2026_"):
        tags.update({"wc2026", "international_results"})
    if schema.startswith("openfootball_wc2026_"):
        tags.update({"wc2026", "openfootball"})
    if schema.startswith("wc2026_") or model.startswith(("int_wc2026_", "wc2026_")):
        tags.add("wc2026")
    if "polygon_settlement" in model:
        tags.add("polygon_settlement")
    if (
        "match_order_book" in model
        or "match_trade" in model
        or model.startswith("stg_polymarket_wc2026_match_order_book")
    ):
        tags.add("pmxt_order_book")
    return frozenset(tags)


def _token_matches_model(
    token: str, schema: str, model: str, tags: frozenset[str]
) -> bool:
    if token.startswith("tag:"):
        return token[4:] in tags
    return token == model or model.startswith(token) or schema.startswith(token)


def _relation_matches_selector_groups(
    schema: str,
    model: str,
    groups: tuple[frozenset[str], ...],
) -> bool:
    if not groups:
        return True
    tags = _infer_dbt_model_tags(schema, model)
    return any(
        all(_token_matches_model(token, schema, model, tags) for token in group)
        for group in groups
    )


def _scoped_dbt_relations(
    dbt_select: str | None = None,
    dbt_exclude: str | None = None,
) -> tuple[tuple[str, str], ...]:
    select_groups = _selector_groups(dbt_select)
    exclude_tokens = [
        token for group in _selector_groups(dbt_exclude) for token in group
    ]
    relations: list[tuple[str, str]] = []
    for schema, model in DBT_EXPECTED_RELATIONS:
        tags = _infer_dbt_model_tags(schema, model)
        if exclude_tokens and any(
            _token_matches_model(token, schema, model, tags) for token in exclude_tokens
        ):
            continue
        if not _relation_matches_selector_groups(schema, model, select_groups):
            continue
        relations.append((schema, model))
    return tuple(relations)


def snapshot_dbt_models(
    conn=None,
    *,
    dbt_select: str | None = None,
    dbt_exclude: str | None = None,
) -> dict[str, Any]:
    """Return row counts for modeled dbt relations selected by the build scope."""
    out: dict[str, Any] = {}
    relations = _scoped_dbt_relations(dbt_select, dbt_exclude)

    def _fill(c) -> None:
        counts = _batch_table_row_counts(c, relations)
        for schema, model in relations:
            key = f"{schema}.{model}"
            exists, row_count = counts.get((schema, model), (False, None))
            if exists:
                out[key] = {
                    "exists": True,
                    "rows": row_count if row_count is not None else 0,
                }
            else:
                out[key] = {"exists": False, "rows": None}

    if conn is not None:
        _fill(conn)
    else:
        with get_connection() as c:
            _fill(c)
    return out


def delta_dbt_models(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Return dbt model keys whose exists/rows changed."""
    delta: dict[str, Any] = {}
    for key in set(before) | set(after):
        if before.get(key) != after.get(key):
            delta[key] = {"before": before.get(key), "after": after.get(key)}
    return delta


def format_raw_snapshot_log(snapshot: dict[str, Any]) -> str:
    """Single-line key=value summary for raw layer operator logs."""
    parts: list[str] = []
    for table in _RAW_SHORT_LOG_TABLES:
        parts.append(f"{table}={snapshot.get(f'{table}_rows')}")
    for schema, table in _RAW_OPS_TABLES:
        qualified = f"{schema}.{table}"
        parts.append(f"{qualified}={snapshot.get(f'{qualified}_rows')}")
    for extra in (
        "market_tokens_distinct_tokens",
        "odds_history_distinct_tokens",
        "token_odds_daily_distinct_tokens",
        "ledger_distinct_tokens",
        "odds_history_max_ts",
        "ledger_fully_checked_tokens",
        "market_tokens_without_history",
        "history_tokens_without_market_tokens",
        "token_sync_skips_distinct_tokens",
    ):
        if extra in snapshot:
            parts.append(f"{extra}={snapshot[extra]}")
    skip_reasons = snapshot.get("token_sync_skips_by_reason")
    if isinstance(skip_reasons, dict):
        rendered = ",".join(
            f"{reason}:{count}" for reason, count in skip_reasons.items()
        )
        parts.append(f"token_sync_skips_by_reason={{{rendered}}}")
    return " ".join(parts)


def format_dbt_snapshot_log(snapshot: dict[str, Any]) -> str:
    """Single-line summary for dbt model row counts."""
    parts: list[str] = []
    for key in sorted(snapshot):
        value = snapshot[key]
        if isinstance(value, dict):
            parts.append(f"{key}:exists={value.get('exists')},rows={value.get('rows')}")
        else:
            parts.append(f"{key}={value}")
    return "; ".join(parts)


__all__ = [
    "snapshot_raw_layer",
    "delta_raw_layer",
    "snapshot_dbt_models",
    "delta_dbt_models",
    "format_raw_snapshot_log",
    "format_dbt_snapshot_log",
]
