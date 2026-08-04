"""dlt batch landing helpers for DuckDB canonical table finalizers."""

from __future__ import annotations

import os
from collections.abc import Sequence
from hashlib import blake2b
from typing import Any

import dlt
import duckdb
import pyarrow as pa

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
from oddsfox_pipeline.storage.duckdb.schemas.polymarket_raw_columns import (
    EVENT_MARKET_PAYLOAD_SNAPSHOT_COLUMNS,
    EVENT_MARKET_SNAPSHOT_COLUMNS,
    EVENT_SNAPSHOT_COLUMNS,
    EVENT_TAG_SNAPSHOT_COLUMNS,
    INGESTION_RUN_EVENT_COLUMNS,
    MARKET_SCOPE_REGISTRY_COLUMNS,
    MARKET_TOKEN_COLUMNS,
    MATCH_MINUTE_ODDS_HISTORY_COLUMNS,
    MATCH_ORDER_BOOK_SNAPSHOT_COLUMNS,
    ODDS_HISTORY_COLUMNS,
)

DLT_STRICT_SCHEMA_CONTRACT = {
    "tables": "evolve",
    "columns": "freeze",
    "data_type": "freeze",
}

_PIPELINES: dict[tuple[str, str], dlt.Pipeline] = {}
_BATCH_PIPELINE_RUN_ID = f"{os.getpid():x}"


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


def _load_odds_history_stage_arrow(
    conn: duckdb.DuckDBPyConnection,
    rows: Sequence[dict[str, Any]],
    *,
    schema: str,
    stage_table: str,
) -> str:
    """Replace an odds stage table on ``conn`` without a dlt pipeline round-trip."""
    if not rows:
        raise ValueError("rows must not be empty")
    ordered = _with_row_order(rows)
    # Explicit types: empty Python lists otherwise infer Arrow null columns.
    table = pa.table(
        {
            "clob_token_id": pa.array(
                [row["clobTokenId"] for row in ordered], type=pa.string()
            ),
            "timestamp": pa.array(
                [row["timestamp"] for row in ordered], type=pa.int64()
            ),
            "price": pa.array([row["price"] for row in ordered], type=pa.float64()),
            "ingested_at": [row["ingested_at"] for row in ordered],
            "row_order": pa.array(
                [row["row_order"] for row in ordered], type=pa.int64()
            ),
        }
    )
    qualified = polymarket_q(schema, stage_table)
    conn.register("_oddsfox_odds_stage_arrow", table)
    try:
        conn.execute(
            f"CREATE OR REPLACE TABLE {qualified} AS SELECT * FROM _oddsfox_odds_stage_arrow"
        )
    finally:
        conn.unregister("_oddsfox_odds_stage_arrow")
    return qualified


def load_odds_history_stage(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str | None = None,
) -> None:
    scope = scope_name or get_active_polymarket_scope()
    stage = prepare_odds_history_stage(rows, conn, scope_name=scope)
    merge_odds_history_stage(conn, stage, scope_name=scope)


def prepare_odds_history_stage(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str | None = None,
) -> str:
    """Load odds rows into a stage table on ``conn``; call before ``BEGIN``."""
    scope = scope_name or get_active_polymarket_scope()
    return _load_odds_history_stage_arrow(
        conn,
        rows,
        schema=polymarket_raw_schema(scope),
        stage_table="stage_odds_history_v1",
    )


def merge_odds_history_stage(
    conn: duckdb.DuckDBPyConnection,
    stage: str,
    *,
    scope_name: str | None = None,
) -> None:
    """Append new source points without rewriting an observed token/timestamp."""
    target = polymarket_raw_tbl(
        scope_name or get_active_polymarket_scope(), "odds_history"
    )
    conn.execute(
        f"""
        INSERT INTO {target}
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
        ON CONFLICT DO NOTHING
        """
    )


def load_match_minute_fetch_audit(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Append one immutable operational audit row per run and token."""
    if not rows:
        return
    target = polymarket_ops_tbl(SCOPE_WC2026, "match_minute_odds_fetch_audit")
    columns = (
        "fetch_run_id",
        "market_id",
        "clobTokenId",
        "fetch_status",
        "raw_published",
        "fidelity_minutes",
        "exact_window_start_at",
        "exact_window_end_at",
        "request_start_epoch",
        "request_end_epoch",
        "source_row_count",
        "in_game_row_count",
        "in_game_history_sha256",
        "source_endpoint",
        "fetch_started_at",
        "fetch_finished_at",
        "error_type",
        "error_message",
    )
    placeholders = ", ".join(["?"] * len(columns))
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.executemany(
            f"INSERT INTO {target} ({', '.join(columns)}) VALUES ({placeholders})",
            [tuple(row.get(column) for column in columns) for row in rows],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def load_match_minute_odds_history_stage(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
    *,
    fetch_run_id: str,
) -> None:
    """Atomically replace the complete bounded WC2026 minute snapshot."""
    target = polymarket_raw_tbl(SCOPE_WC2026, "match_minute_odds_history")
    stage = load_stage_rows(
        schema=polymarket_raw_schema(SCOPE_WC2026),
        stage_table="stage_match_minute_odds_history_v1",
        rows=_with_row_order(rows),
        columns=MATCH_MINUTE_ODDS_HISTORY_COLUMNS,
    )
    audit = polymarket_ops_tbl(SCOPE_WC2026, "match_minute_odds_fetch_audit")
    conn.execute("BEGIN TRANSACTION")
    try:
        stage_tokens = int(
            conn.execute(
                f"SELECT count(DISTINCT clob_token_id) FROM {stage}"
            ).fetchone()[0]
        )
        audit_inventory = conn.execute(
            f"""
            SELECT
                count(*),
                count(*) FILTER (
                    WHERE fetch_status = 'success' AND NOT raw_published
                )
            FROM {audit}
            WHERE fetch_run_id = ?
            """,
            [fetch_run_id],
        ).fetchone()
        if audit_inventory != (stage_tokens, stage_tokens):
            raise RuntimeError(
                f"Fetch audit inventory does not match {stage_tokens} staged tokens "
                f"for run {fetch_run_id}: {audit_inventory}"
            )
        conn.execute(f"DELETE FROM {target}")
        conn.execute(
            f"""
            INSERT INTO {target}
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
        updated = conn.execute(
            f"UPDATE {audit} SET raw_published = TRUE WHERE fetch_run_id = ?",
            [fetch_run_id],
        ).fetchone()[0]
        if int(updated) != stage_tokens:  # pragma: no cover - guarded above
            raise RuntimeError(
                f"Published {updated} audit rows for {stage_tokens} staged tokens "
                f"in run {fetch_run_id}"
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def merge_match_order_book_snapshots(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Land a bounded dlt batch, then merge it into the canonical raw table.

    dlt owns the replaceable staging relation and may add its internal columns
    there. The canonical relation remains an explicit project contract for dbt
    and recovery logic.
    """
    if not rows:
        return
    normalized_rows = []
    for source in rows:
        row = dict(source)
        if not row.get("landscape_role"):
            label = str(row.get("outcome_label") or "")
            if label.casefold() == str(row.get("home_team") or "").casefold():
                row["landscape_role"] = "home"
            elif label.casefold() == str(row.get("away_team") or "").casefold():
                row["landscape_role"] = "away"
            else:
                raise ValueError("snapshot row requires an explicit landscape_role")
        row.setdefault("provider_sequence", 0)
        normalized_rows.append(row)
    raw_schema = polymarket_raw_schema(SCOPE_WC2026)
    target = polymarket_raw_tbl(SCOPE_WC2026, "match_order_book_snapshots")
    stage = load_stage_rows(
        schema=raw_schema,
        stage_table="stage_match_order_book_snapshots_v1",
        rows=normalized_rows,
        columns=MATCH_ORDER_BOOK_SNAPSHOT_COLUMNS,
    )
    target_columns = ", ".join(MATCH_ORDER_BOOK_SNAPSHOT_COLUMNS)
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {target} ({target_columns})
        SELECT {target_columns}
        FROM {stage}
        """
    )


def append_ingestion_run_event_stage(
    row: dict[str, Any],
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str | None = None,
) -> None:
    scope = scope_name or get_active_polymarket_scope()
    ops_schema = polymarket_ops_schema(scope)
    target = polymarket_ops_tbl(scope, "ingestion_run_events")
    stage = load_stage_rows(
        schema=ops_schema,
        stage_table="stage_ingestion_run_events_v1",
        rows=[row],
        columns=INGESTION_RUN_EVENT_COLUMNS,
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
        (
            scope_name,
            market_id,
            event_slug,
            event_id,
            source,
            refreshed_at,
            event_volume_usd_lifetime_reported,
            is_event_volume_eligible,
            first_eligible_at
        )
        SELECT
            scope_name,
            market_id,
            event_slug,
            event_id,
            source,
            refreshed_at,
            event_volume_usd_lifetime_reported,
            is_event_volume_eligible,
            first_eligible_at
        FROM (
            SELECT
                scope_name,
                market_id,
                event_slug,
                event_id,
                source,
                refreshed_at,
                event_volume_usd_lifetime_reported,
                is_event_volume_eligible,
                first_eligible_at,
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
          refreshed_at=excluded.refreshed_at,
          event_volume_usd_lifetime_reported=COALESCE(
              excluded.event_volume_usd_lifetime_reported,
              {target}.event_volume_usd_lifetime_reported
          ),
          is_event_volume_eligible=(
              coalesce({target}.is_event_volume_eligible, false)
              OR coalesce(excluded.is_event_volume_eligible, false)
          ),
          first_eligible_at=COALESCE(
              {target}.first_eligible_at,
              excluded.first_eligible_at
          )
        """
    )


__all__ = [
    "DLT_STRICT_SCHEMA_CONTRACT",
    "MARKET_TOKEN_COLUMNS",
    "ODDS_HISTORY_COLUMNS",
    "INGESTION_RUN_EVENT_COLUMNS",
    "MARKET_SCOPE_REGISTRY_COLUMNS",
    "EVENT_SNAPSHOT_COLUMNS",
    "EVENT_TAG_SNAPSHOT_COLUMNS",
    "EVENT_MARKET_SNAPSHOT_COLUMNS",
    "EVENT_MARKET_PAYLOAD_SNAPSHOT_COLUMNS",
    "MATCH_MINUTE_ODDS_HISTORY_COLUMNS",
    "MATCH_ORDER_BOOK_SNAPSHOT_COLUMNS",
    "append_ingestion_run_event_stage",
    "load_market_tokens_stage",
    "load_odds_history_stage",
    "load_stage_rows",
    "load_market_scope_registry_stage",
    "load_match_minute_fetch_audit",
    "load_match_minute_odds_history_stage",
    "merge_match_order_book_snapshots",
    "merge_odds_history_stage",
    "prepare_odds_history_stage",
    "reset_dlt_batch_pipelines",
]
