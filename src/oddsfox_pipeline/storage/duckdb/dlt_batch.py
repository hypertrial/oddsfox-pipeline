"""dlt batch landing helpers for DuckDB canonical table finalizers."""

from __future__ import annotations

import os
from collections.abc import Sequence
from hashlib import blake2b
from typing import Any

import dlt
import duckdb

from oddsfox_pipeline.naming import SCOPE_WC2026
from oddsfox_pipeline.storage.duckdb import connection as duckdb_connection
from oddsfox_pipeline.storage.duckdb.polymarket_scope import get_active_polymarket_scope
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    polymarket_ops_schema,
    polymarket_ops_tbl,
    polymarket_q,
    polymarket_raw_schema,
    polymarket_raw_tbl,
)

DLT_STRICT_SCHEMA_CONTRACT = {
    "tables": "evolve",
    "columns": "freeze",
    "data_type": "freeze",
}

_TAB_MARKET_TOKENS = polymarket_raw_tbl(SCOPE_WC2026, "market_tokens")
_TAB_ODDS_HISTORY = polymarket_raw_tbl(SCOPE_WC2026, "odds_history")
_TAB_PIPELINE_RUN_EVENTS = polymarket_ops_tbl(SCOPE_WC2026, "pipeline_run_events")
_TAB_MARKET_SCOPE_REGISTRY = polymarket_ops_tbl(SCOPE_WC2026, "market_scope_registry")

_PIPELINES: dict[tuple[str, str], dlt.Pipeline] = {}
_BATCH_PIPELINE_RUN_ID = f"{os.getpid():x}"

MARKET_TOKEN_COLUMNS = {
    "market_id": {"data_type": "text"},
    "clobTokenIds": {"data_type": "text"},
    "updated_at": {"data_type": "timestamp"},
    "row_order": {"data_type": "bigint"},
}

ODDS_HISTORY_COLUMNS = {
    "clobTokenId": {"data_type": "text"},
    "timestamp": {"data_type": "bigint"},
    "price": {"data_type": "double"},
    "ingested_at": {"data_type": "timestamp"},
    "row_order": {"data_type": "bigint"},
}

MATCH_MINUTE_ODDS_HISTORY_COLUMNS = {
    "market_id": {"data_type": "text"},
    "clobTokenId": {"data_type": "text"},
    "timestamp": {"data_type": "bigint"},
    "price": {"data_type": "double"},
    "fidelity_minutes": {"data_type": "bigint"},
    "window_start_at": {"data_type": "timestamp"},
    "window_end_at": {"data_type": "timestamp"},
    "ingested_at": {"data_type": "timestamp"},
    "row_order": {"data_type": "bigint"},
}

PIPELINE_RUN_EVENT_COLUMNS = {
    "run_id": {"data_type": "text"},
    "task_name": {"data_type": "text"},
    "recorded_at": {"data_type": "timestamp"},
    "metrics_json": {"data_type": "text"},
}

MARKET_SCOPE_REGISTRY_COLUMNS = {
    "scope_name": {"data_type": "text"},
    "market_id": {"data_type": "text"},
    "event_slug": {"data_type": "text", "nullable": True},
    "event_id": {"data_type": "text", "nullable": True},
    "source": {"data_type": "text"},
    "refreshed_at": {"data_type": "timestamp"},
    "row_order": {"data_type": "bigint"},
}


def reset_dlt_batch_pipelines() -> None:
    """Clear cached pipelines; useful when tests swap DUCKDB_NAME."""
    _PIPELINES.clear()


def _pipeline(schema: str) -> dlt.Pipeline:
    duckdb_connection.ensure_duck_db()
    db_path = str(duckdb_connection.active_duckdb_path())
    key = (schema, db_path)
    if key not in _PIPELINES:
        # dlt persists pipeline state outside DuckDB; these stage tables are
        # replace-only scratch space, so avoid cross-process stale schemas.
        path_hash = blake2b(db_path.encode("utf-8"), digest_size=12).hexdigest()
        _PIPELINES[key] = dlt.pipeline(
            pipeline_name=(
                f"polymarket_{schema}_batch_v1_{path_hash}_{_BATCH_PIPELINE_RUN_ID}"
            ),
            destination=dlt.destinations.duckdb(credentials=db_path),
            dataset_name=schema,
        )
    return _PIPELINES[key]


def load_stage_rows(
    *,
    schema: str,
    stage_table: str,
    rows: Sequence[dict[str, Any]],
    columns: dict[str, dict[str, Any]],
) -> str:
    """Replace a dlt stage table and return its fully qualified DuckDB name."""
    if not rows:
        raise ValueError("rows must not be empty")
    pipe = _pipeline(schema)
    if pipe.has_pending_data:
        pipe.drop_pending_packages()
    pipe.run(
        list(rows),
        table_name=stage_table,
        write_disposition="replace",
        columns=columns,
        schema_contract=DLT_STRICT_SCHEMA_CONTRACT,
    )
    return polymarket_q(schema, stage_table)


def _with_row_order(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{**row, "row_order": idx} for idx, row in enumerate(rows)]


def load_market_tokens_stage(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str = SCOPE_WC2026,
) -> None:
    raw_schema = polymarket_raw_schema(scope_name)
    target = polymarket_raw_tbl(scope_name, "market_tokens")
    stage = load_stage_rows(
        schema=raw_schema,
        stage_table="stage_market_tokens_v1",
        rows=_with_row_order(rows),
        columns=MARKET_TOKEN_COLUMNS,
    )
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {target}
        (market_id, clobTokenIds, updated_at)
        SELECT market_id, clob_token_ids, updated_at
        FROM (
            SELECT
                market_id,
                clob_token_ids,
                updated_at,
                row_number() OVER (
                    PARTITION BY market_id
                    ORDER BY updated_at DESC, row_order DESC
                ) AS rn
            FROM {stage}
        )
        WHERE rn = 1
        """
    )


def load_odds_history_stage(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str | None = None,
) -> None:
    scope = scope_name or get_active_polymarket_scope()
    stage = prepare_odds_history_stage(rows, scope_name=scope)
    merge_odds_history_stage(conn, stage, scope_name=scope)


def prepare_odds_history_stage(
    rows: Sequence[dict[str, Any]],
    *,
    scope_name: str | None = None,
) -> str:
    """Load odds rows into a dlt stage table; call before ``BEGIN`` on ``conn``."""
    scope = scope_name or get_active_polymarket_scope()
    return load_stage_rows(
        schema=polymarket_raw_schema(scope),
        stage_table="stage_odds_history_v1",
        rows=_with_row_order(rows),
        columns=ODDS_HISTORY_COLUMNS,
    )


def merge_odds_history_stage(
    conn: duckdb.DuckDBPyConnection,
    stage: str,
    *,
    scope_name: str | None = None,
) -> None:
    target = polymarket_raw_tbl(
        scope_name or get_active_polymarket_scope(), "odds_history"
    )
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {target}
        (clobTokenId, timestamp, price, ingested_at)
        SELECT clob_token_id, timestamp, price, ingested_at
        FROM (
            SELECT
                clob_token_id,
                timestamp,
                price,
                ingested_at,
                row_number() OVER (
                    PARTITION BY clob_token_id, timestamp
                    ORDER BY ingested_at DESC, row_order DESC
                ) AS rn
            FROM {stage}
        )
        WHERE rn = 1
        """
    )


def load_match_minute_odds_history_stage(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Deterministically upsert the bounded WC2026 match-minute observations."""
    target = polymarket_raw_tbl(SCOPE_WC2026, "match_minute_odds_history")
    stage = load_stage_rows(
        schema=polymarket_raw_schema(SCOPE_WC2026),
        stage_table="stage_match_minute_odds_history_v1",
        rows=_with_row_order(rows),
        columns=MATCH_MINUTE_ODDS_HISTORY_COLUMNS,
    )
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {target}
        (market_id, clobTokenId, timestamp, price, fidelity_minutes,
         window_start_at, window_end_at, ingested_at)
        SELECT market_id, clob_token_id, timestamp, price, fidelity_minutes,
               window_start_at, window_end_at, ingested_at
        FROM (
            SELECT *, row_number() OVER (
                PARTITION BY clob_token_id, timestamp
                ORDER BY ingested_at DESC, row_order DESC
            ) AS rn
            FROM {stage}
        )
        WHERE rn = 1
        """
    )


def append_pipeline_run_event_stage(
    row: dict[str, Any],
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str | None = None,
) -> None:
    scope = scope_name or get_active_polymarket_scope()
    ops_schema = polymarket_ops_schema(scope)
    target = polymarket_ops_tbl(scope, "pipeline_run_events")
    stage = load_stage_rows(
        schema=ops_schema,
        stage_table="stage_pipeline_run_events_v1",
        rows=[row],
        columns=PIPELINE_RUN_EVENT_COLUMNS,
    )
    conn.execute(
        f"""
        INSERT INTO {target}
        (run_id, task_name, recorded_at, metrics_json)
        SELECT run_id, task_name, recorded_at, metrics_json
        FROM {stage}
        """
    )


def load_market_scope_registry_stage(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str = SCOPE_WC2026,
) -> None:
    ops_schema = polymarket_ops_schema(scope_name)
    target = polymarket_ops_tbl(scope_name, "market_scope_registry")
    stage = load_stage_rows(
        schema=ops_schema,
        stage_table="stage_market_scope_registry_v1",
        rows=_with_row_order(rows),
        columns=MARKET_SCOPE_REGISTRY_COLUMNS,
    )
    conn.execute(
        f"""
        INSERT INTO {target}
        (scope_name, market_id, event_slug, event_id, source, refreshed_at)
        SELECT scope_name, market_id, event_slug, event_id, source, refreshed_at
        FROM (
            SELECT
                scope_name,
                market_id,
                event_slug,
                event_id,
                source,
                refreshed_at,
                row_number() OVER (
                    PARTITION BY scope_name, market_id
                    ORDER BY refreshed_at DESC, row_order DESC
                ) AS rn
            FROM {stage}
        )
        WHERE rn = 1
        ON CONFLICT(scope_name, market_id) DO UPDATE SET
          event_slug=COALESCE(
              excluded.event_slug,
              {target}.event_slug
          ),
          event_id=COALESCE(
              excluded.event_id,
              {target}.event_id
          ),
          source=excluded.source,
          refreshed_at=excluded.refreshed_at
        """
    )


__all__ = [
    "DLT_STRICT_SCHEMA_CONTRACT",
    "MARKET_TOKEN_COLUMNS",
    "ODDS_HISTORY_COLUMNS",
    "PIPELINE_RUN_EVENT_COLUMNS",
    "MARKET_SCOPE_REGISTRY_COLUMNS",
    "MATCH_MINUTE_ODDS_HISTORY_COLUMNS",
    "append_pipeline_run_event_stage",
    "load_market_tokens_stage",
    "load_odds_history_stage",
    "load_stage_rows",
    "load_market_scope_registry_stage",
    "load_match_minute_odds_history_stage",
    "merge_odds_history_stage",
    "prepare_odds_history_stage",
    "reset_dlt_batch_pipelines",
]
