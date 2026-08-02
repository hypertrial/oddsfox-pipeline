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
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    bootstrap_polymarket_tables,
)

DLT_STRICT_SCHEMA_CONTRACT = {
    "tables": "evolve",
    "columns": "freeze",
    "data_type": "freeze",
}

_TAB_MARKET_TOKENS = polymarket_raw_tbl(SCOPE_WC2026, "market_tokens")
_TAB_ODDS_HISTORY = polymarket_raw_tbl(SCOPE_WC2026, "odds_history")
_TAB_INGESTION_RUN_EVENTS = polymarket_ops_tbl(SCOPE_WC2026, "ingestion_run_events")
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

MATCH_ORDER_BOOK_SNAPSHOT_COLUMNS = {
    "scan_id": {"data_type": "text"},
    "manifest_sha256": {"data_type": "text"},
    "fifa_match_id": {"data_type": "bigint"},
    "stage": {"data_type": "text"},
    "home_team": {"data_type": "text"},
    "away_team": {"data_type": "text"},
    "event_id": {"data_type": "text"},
    "event_slug": {"data_type": "text"},
    "market_id": {"data_type": "text"},
    "market_slug": {"data_type": "text"},
    "market_type": {"data_type": "text"},
    "condition_id": {"data_type": "text"},
    "outcome_label": {"data_type": "text"},
    "landscape_role": {"data_type": "text"},
    "clob_token_id": {"data_type": "text"},
    "window_start_ms": {"data_type": "bigint"},
    "window_end_ms": {"data_type": "bigint"},
    "snapshot_timestamp_ms": {"data_type": "bigint"},
    "snapshot_at": {"data_type": "timestamp"},
    "snapshot_sha256": {"data_type": "text"},
    "provider_sequence": {"data_type": "bigint"},
    "bids_json": {"data_type": "text"},
    "asks_json": {"data_type": "text"},
    "is_neg_risk": {"data_type": "bool", "nullable": True},
    "last_trade_price": {"data_type": "text", "nullable": True},
    "source_endpoint": {"data_type": "text"},
    "ingested_at": {"data_type": "timestamp"},
}

INGESTION_RUN_EVENT_COLUMNS = {
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

EVENT_SNAPSHOT_COLUMNS = {
    "event_id": {"data_type": "text"},
    "event_slug": {"data_type": "text", "nullable": True},
    "event_title": {"data_type": "text", "nullable": True},
    "event_subtitle": {"data_type": "text", "nullable": True},
    "event_description": {"data_type": "text", "nullable": True},
    "resolution_source": {"data_type": "text", "nullable": True},
    "event_volume_usd_lifetime_reported": {"data_type": "double", "nullable": True},
    "volume_24h_usd": {"data_type": "double", "nullable": True},
    "volume_1w_usd": {"data_type": "double", "nullable": True},
    "volume_1m_usd": {"data_type": "double", "nullable": True},
    "volume_1y_usd": {"data_type": "double", "nullable": True},
    "liquidity_usd": {"data_type": "double", "nullable": True},
    "open_interest_usd": {"data_type": "double", "nullable": True},
    "is_active": {"data_type": "bool", "nullable": True},
    "is_closed": {"data_type": "bool", "nullable": True},
    "is_archived": {"data_type": "bool", "nullable": True},
    "created_at": {"data_type": "timestamp", "nullable": True},
    "source_updated_at": {"data_type": "timestamp", "nullable": True},
    "start_at": {"data_type": "timestamp", "nullable": True},
    "end_at": {"data_type": "timestamp", "nullable": True},
    "closed_at": {"data_type": "timestamp", "nullable": True},
    "event_start_at": {"data_type": "timestamp", "nullable": True},
    "finished_at": {"data_type": "timestamp", "nullable": True},
    "game_id": {"data_type": "text", "nullable": True},
    "parent_event_id": {"data_type": "text", "nullable": True},
    "neg_risk": {"data_type": "bool", "nullable": True},
    "enable_neg_risk": {"data_type": "bool", "nullable": True},
    "neg_risk_market_id": {"data_type": "text", "nullable": True},
    "show_all_outcomes": {"data_type": "bool", "nullable": True},
    "tags_json": {"data_type": "text"},
    "series_slugs_json": {"data_type": "text"},
    "candidate_sources_json": {"data_type": "text"},
    "source_market_count": {"data_type": "bigint"},
    "observed_at": {"data_type": "timestamp"},
    "source_endpoint": {"data_type": "text"},
    "row_order": {"data_type": "bigint"},
}

EVENT_TAG_SNAPSHOT_COLUMNS = {
    "event_id": {"data_type": "text"},
    "tag_key": {"data_type": "text"},
    "tag_id": {"data_type": "text", "nullable": True},
    "tag_slug": {"data_type": "text", "nullable": True},
    "tag_label": {"data_type": "text", "nullable": True},
    "observed_at": {"data_type": "timestamp"},
    "row_order": {"data_type": "bigint"},
}

EVENT_MARKET_SNAPSHOT_COLUMNS = {
    "event_id": {"data_type": "text"},
    "market_id": {"data_type": "text"},
    "source_ordinal": {"data_type": "bigint"},
    "is_enclosing_event": {"data_type": "bool"},
    "observed_at": {"data_type": "timestamp"},
    "row_order": {"data_type": "bigint"},
}

EVENT_CATALOG_MARKET_COLUMNS = {
    "id": {"data_type": "text"},
    "question": {"data_type": "text"},
    "category": {"data_type": "text", "nullable": True},
    "description": {"data_type": "text", "nullable": True},
    "market_resolution_source": {"data_type": "text", "nullable": True},
    "outcomes": {"data_type": "text"},
    "volume": {"data_type": "double"},
    "active": {"data_type": "bool", "nullable": True},
    "closed": {"data_type": "bool", "nullable": True},
    "created_at": {"data_type": "timestamp", "nullable": True},
    "scraped_at": {"data_type": "timestamp"},
    "end_date": {"data_type": "timestamp", "nullable": True},
    "slug": {"data_type": "text", "nullable": True},
    "event_slug": {"data_type": "text", "nullable": True},
    "event_id": {"data_type": "text", "nullable": True},
    "event_title": {"data_type": "text", "nullable": True},
    "event_start_time": {"data_type": "timestamp", "nullable": True},
    "event_finished_time": {"data_type": "timestamp", "nullable": True},
    "event_game_id": {"data_type": "text", "nullable": True},
    "event_ended": {"data_type": "bool", "nullable": True},
    "condition_id": {"data_type": "text", "nullable": True},
    "sports_market_type": {"data_type": "text", "nullable": True},
    "game_start_time": {"data_type": "timestamp", "nullable": True},
    "group_item_title": {"data_type": "text", "nullable": True},
    "group_item_threshold": {"data_type": "text", "nullable": True},
    "line": {"data_type": "double", "nullable": True},
    "tags": {"data_type": "text", "nullable": True},
    "clob_token_ids": {"data_type": "text", "nullable": True},
    "is_resolved": {"data_type": "bool", "nullable": True},
    "winning_outcome": {"data_type": "text", "nullable": True},
    "winning_clob_token_id": {"data_type": "text", "nullable": True},
    "neg_risk_market_id": {"data_type": "text", "nullable": True},
    "neg_risk_request_id": {"data_type": "text", "nullable": True},
    "neg_risk_other": {"data_type": "bool", "nullable": True},
    "row_order": {"data_type": "bigint"},
}

EVENT_MARKET_PAYLOAD_SNAPSHOT_COLUMNS = {
    "market_id": EVENT_CATALOG_MARKET_COLUMNS["id"],
    **{
        column: contract
        for column, contract in EVENT_CATALOG_MARKET_COLUMNS.items()
        if column not in {"id", "row_order"}
    },
    "observed_at": {"data_type": "timestamp"},
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


def _assert_append_only_snapshot(
    conn: duckdb.DuckDBPyConnection,
    *,
    stage: str,
    target: str,
    columns: tuple[str, ...],
    key_columns: tuple[str, ...],
    order_by: str,
    label: str,
) -> None:
    """Reject a reused snapshot key unless every persisted value is identical."""
    compared_columns = tuple(column for column in columns if column not in key_columns)
    stage_key_match = " AND ".join(
        f'a."{column}" IS NOT DISTINCT FROM b."{column}"' for column in key_columns
    )
    stage_value_diff = " OR ".join(
        f'a."{column}" IS DISTINCT FROM b."{column}"' for column in compared_columns
    )
    staged_divergence = int(
        conn.execute(
            f"""
            SELECT count(*)
            FROM {stage} AS a
            INNER JOIN {stage} AS b
                ON {stage_key_match}
                AND a.row_order < b.row_order
            WHERE {stage_value_diff}
            """
        ).fetchone()[0]
    )
    if staged_divergence:
        raise RuntimeError(f"Divergent append-only {label} rows share one snapshot key")

    quoted_columns = ", ".join(f'"{column}"' for column in columns)
    partition_by = ", ".join(f'"{column}"' for column in key_columns)
    target_key_match = " AND ".join(
        f'target."{column}" IS NOT DISTINCT FROM candidate."{column}"'
        for column in key_columns
    )
    difference_counts = ", ".join(
        "count(*) filter (where "
        f'target."{column}" is distinct from candidate."{column}") '
        f'as "{column}"'
        for column in compared_columns
    )
    result = conn.execute(
        f"""
            WITH candidate AS (
                SELECT {quoted_columns}
                FROM {stage}
                QUALIFY row_number() OVER (
                    PARTITION BY {partition_by} ORDER BY {order_by}
                ) = 1
            )
            SELECT {difference_counts}
            FROM candidate
            INNER JOIN {target} AS target
                ON {target_key_match}
            """
    )
    row = result.fetchone()
    persisted_divergence = {
        column[0]: int(count)
        for column, count in zip(result.description, row, strict=True)
        if count
    }
    if persisted_divergence:
        raise RuntimeError(
            f"Divergent append-only replay for {label} at an existing snapshot key: "
            f"{persisted_divergence}"
        )


def _assert_exact_observation_replay(
    conn: duckdb.DuckDBPyConnection,
    *,
    observed_at: Any,
    events_target: str,
    relations: tuple[
        tuple[
            str,
            str | None,
            str,
            tuple[str, ...],
            tuple[str, ...],
            str,
        ],
        ...,
    ],
) -> None:
    """Make reuse of a complete catalog observation exactly idempotent."""
    persisted_event_count = int(
        conn.execute(
            f"SELECT count(*) FROM {events_target} WHERE observed_at = ?",
            [observed_at],
        ).fetchone()[0]
    )
    if persisted_event_count == 0:
        return

    for label, stage, target, columns, key_columns, order_by in relations:
        quoted_columns = ", ".join(f'"{column}"' for column in columns)
        if stage is None:
            candidate_query = f"SELECT {quoted_columns} FROM {target} WHERE FALSE"
        else:
            partition_by = ", ".join(f'"{column}"' for column in key_columns)
            candidate_query = f"""
                SELECT {quoted_columns}
                FROM {stage}
                QUALIFY row_number() OVER (
                    PARTITION BY {partition_by} ORDER BY {order_by}
                ) = 1
            """
        difference_count = int(
            conn.execute(
                f"""
                WITH candidate AS ({candidate_query}),
                persisted AS (
                    SELECT {quoted_columns}
                    FROM {target}
                    WHERE observed_at = ?
                ),
                differences AS (
                    (SELECT * FROM candidate EXCEPT ALL SELECT * FROM persisted)
                    UNION ALL
                    (SELECT * FROM persisted EXCEPT ALL SELECT * FROM candidate)
                )
                SELECT count(*) FROM differences
                """,
                [observed_at],
            ).fetchone()[0]
        )
        if difference_count:
            raise RuntimeError(
                "Divergent append-only replay for complete "
                f"{label} relation at observed_at; differences={difference_count}"
            )


def merge_event_catalog_batch(
    *,
    event_rows: Sequence[dict[str, Any]],
    tag_rows: Sequence[dict[str, Any]],
    event_market_rows: Sequence[dict[str, Any]],
    market_rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Stage and atomically append one complete WC2026 event catalog observation."""
    if not event_rows:
        raise ValueError("event_rows must not be empty")
    raw_schema = polymarket_raw_schema(SCOPE_WC2026)
    events_target = polymarket_raw_tbl(SCOPE_WC2026, "event_snapshots")
    tags_target = polymarket_raw_tbl(SCOPE_WC2026, "event_tag_snapshots")
    bridge_target = polymarket_raw_tbl(SCOPE_WC2026, "event_market_snapshots")
    market_payloads_target = polymarket_raw_tbl(
        SCOPE_WC2026, "event_market_payload_snapshots"
    )
    observed_at_values = {row.get("observed_at") for row in event_rows}
    if None in observed_at_values or len(observed_at_values) != 1:
        raise ValueError("event_rows must share one non-null observed_at")
    observed_at = next(iter(observed_at_values))
    for label, rows in (
        ("tag_rows", tag_rows),
        ("event_market_rows", event_market_rows),
    ):
        relation_observed_at_values = {row.get("observed_at") for row in rows}
        if relation_observed_at_values and relation_observed_at_values != {observed_at}:
            raise ValueError(f"{label} must share event_rows observed_at")
    market_payload_rows: list[dict[str, Any]] = []
    for row in market_rows:
        market_id = str(row.get("id") or "").strip()
        if not market_id:
            raise ValueError("market_rows must contain non-empty id values")
        market_payload_rows.append(
            {
                "market_id": market_id,
                **{
                    column: row.get(column)
                    for column in EVENT_CATALOG_MARKET_COLUMNS
                    if column not in {"id", "row_order"}
                },
                "observed_at": observed_at,
            }
        )

    # Existing warehouses predate the dedicated payload snapshot table. Keep
    # dlt-owned ``markets`` untouched and migrate only project-owned raw tables.
    bootstrap_polymarket_tables(conn, scope_name=SCOPE_WC2026)

    events_stage = load_stage_rows(
        schema=raw_schema,
        stage_table="stage_event_snapshots_v1",
        rows=_with_row_order(event_rows),
        columns=EVENT_SNAPSHOT_COLUMNS,
    )
    tags_stage = (
        load_stage_rows(
            schema=raw_schema,
            stage_table="stage_event_tag_snapshots_v1",
            rows=_with_row_order(tag_rows),
            columns=EVENT_TAG_SNAPSHOT_COLUMNS,
        )
        if tag_rows
        else None
    )
    bridge_stage = (
        load_stage_rows(
            schema=raw_schema,
            stage_table="stage_event_market_snapshots_v1",
            rows=_with_row_order(event_market_rows),
            columns=EVENT_MARKET_SNAPSHOT_COLUMNS,
        )
        if event_market_rows
        else None
    )
    market_payloads_stage = (
        load_stage_rows(
            schema=raw_schema,
            stage_table="stage_event_market_payload_snapshots_v1",
            rows=_with_row_order(market_payload_rows),
            columns=EVENT_MARKET_PAYLOAD_SNAPSHOT_COLUMNS,
        )
        if market_payload_rows
        else None
    )

    market_payload_columns = tuple(
        column
        for column in EVENT_MARKET_PAYLOAD_SNAPSHOT_COLUMNS
        if column != "row_order"
    )
    event_columns = tuple(
        column for column in EVENT_SNAPSHOT_COLUMNS if column != "row_order"
    )
    tag_columns = tuple(
        column for column in EVENT_TAG_SNAPSHOT_COLUMNS if column != "row_order"
    )
    bridge_columns = tuple(
        column for column in EVENT_MARKET_SNAPSHOT_COLUMNS if column != "row_order"
    )
    quoted_market_payload_columns = ", ".join(market_payload_columns)
    quoted_event_columns = ", ".join(event_columns)
    quoted_tag_columns = ", ".join(tag_columns)
    quoted_bridge_columns = ", ".join(bridge_columns)
    conn.execute("BEGIN TRANSACTION")
    try:
        _assert_append_only_snapshot(
            conn,
            stage=events_stage,
            target=events_target,
            columns=event_columns,
            key_columns=("event_id", "observed_at"),
            order_by="row_order DESC",
            label="event snapshots",
        )
        if tags_stage is not None:
            _assert_append_only_snapshot(
                conn,
                stage=tags_stage,
                target=tags_target,
                columns=tag_columns,
                key_columns=("event_id", "tag_key", "observed_at"),
                order_by="row_order DESC",
                label="event tag snapshots",
            )
        if bridge_stage is not None:
            _assert_append_only_snapshot(
                conn,
                stage=bridge_stage,
                target=bridge_target,
                columns=bridge_columns,
                key_columns=("event_id", "market_id", "observed_at"),
                order_by="row_order DESC",
                label="event market snapshots",
            )
        if market_payloads_stage is not None:
            _assert_append_only_snapshot(
                conn,
                stage=market_payloads_stage,
                target=market_payloads_target,
                columns=market_payload_columns,
                key_columns=("market_id", "observed_at"),
                order_by="scraped_at DESC, row_order DESC",
                label="event market payload snapshots",
            )
        _assert_exact_observation_replay(
            conn,
            observed_at=observed_at,
            events_target=events_target,
            relations=(
                (
                    "event snapshots",
                    events_stage,
                    events_target,
                    event_columns,
                    ("event_id", "observed_at"),
                    "row_order DESC",
                ),
                (
                    "event tag snapshots",
                    tags_stage,
                    tags_target,
                    tag_columns,
                    ("event_id", "tag_key", "observed_at"),
                    "row_order DESC",
                ),
                (
                    "event market snapshots",
                    bridge_stage,
                    bridge_target,
                    bridge_columns,
                    ("event_id", "market_id", "observed_at"),
                    "row_order DESC",
                ),
                (
                    "event market payload snapshots",
                    market_payloads_stage,
                    market_payloads_target,
                    market_payload_columns,
                    ("market_id", "observed_at"),
                    "scraped_at DESC, row_order DESC",
                ),
            ),
        )
        conn.execute(
            f"""
            INSERT INTO {events_target} ({quoted_event_columns})
            SELECT {quoted_event_columns}
            FROM {events_stage}
            QUALIFY row_number() OVER (
                PARTITION BY event_id, observed_at ORDER BY row_order DESC
            ) = 1
            ON CONFLICT (event_id, observed_at) DO NOTHING
            """
        )
        if tags_stage is not None:
            conn.execute(
                f"""
                INSERT INTO {tags_target} ({quoted_tag_columns})
                SELECT {quoted_tag_columns}
                FROM {tags_stage}
                QUALIFY row_number() OVER (
                    PARTITION BY event_id, tag_key, observed_at ORDER BY row_order DESC
                ) = 1
                ON CONFLICT (event_id, tag_key, observed_at) DO NOTHING
                """
            )
        if bridge_stage is not None:
            conn.execute(
                f"""
                INSERT INTO {bridge_target} ({quoted_bridge_columns})
                SELECT {quoted_bridge_columns}
                FROM {bridge_stage}
                QUALIFY row_number() OVER (
                    PARTITION BY event_id, market_id, observed_at ORDER BY row_order DESC
                ) = 1
                ON CONFLICT (event_id, market_id, observed_at) DO NOTHING
                """
            )
        if market_payloads_stage is not None:
            conn.execute(
                f"""
                INSERT INTO {market_payloads_target} ({quoted_market_payload_columns})
                SELECT {quoted_market_payload_columns}
                FROM {market_payloads_stage}
                QUALIFY row_number() OVER (
                    PARTITION BY market_id, observed_at
                    ORDER BY scraped_at DESC, row_order DESC
                ) = 1
                ON CONFLICT (market_id, observed_at) DO NOTHING
                """
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


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
    "merge_event_catalog_batch",
    "merge_match_order_book_snapshots",
    "merge_odds_history_stage",
    "prepare_odds_history_stage",
    "reset_dlt_batch_pipelines",
]
