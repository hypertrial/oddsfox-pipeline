from typing import Dict, List, Tuple

import duckdb

from oddsfox_pipeline.storage.duckdb.connection import ensure_duck_db, get_connection
from oddsfox_pipeline.storage.duckdb.dlt_batch import (
    load_odds_history_stage,
    merge_odds_history_stage,
    prepare_odds_history_stage,
)
from oddsfox_pipeline.storage.duckdb.odds._common import (
    _TAB_ODDS_HISTORY,
    _utc_now,
    logger,
)


def save_odds_batch(records: List[Tuple[str, int, float]]):
    """Save a batch of odds history records into DuckDB."""
    if not records:
        return
    ensure_duck_db()
    ingested = _utc_now()
    rows = [
        {
            "clobTokenId": token_id,
            "timestamp": int(timestamp),
            "price": float(price),
            "ingested_at": ingested,
        }
        for token_id, timestamp, price in records
    ]
    with get_connection() as conn:
        load_odds_history_stage(rows, conn)
    logger.debug("Saved %d odds records to DuckDB", len(records))


def save_odds_bulk_appender(
    records: List[Tuple[str, int, float]], conn: duckdb.DuckDBPyConnection
):
    """Compatibility wrapper for bulk odds-history upserts on an open connection."""
    if not records:
        return
    save_odds_bulk_upsert(records, conn, assume_deduped=False)
    logger.debug("Saved %d odds records to DuckDB", len(records))


def _ensure_odds_history_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_TAB_ODDS_HISTORY} (
            clobTokenId TEXT,
            timestamp BIGINT,
            price DOUBLE,
            ingested_at TIMESTAMP,
            PRIMARY KEY (clobTokenId, timestamp)
        )
        """
    )
    conn.execute(
        f"ALTER TABLE {_TAB_ODDS_HISTORY} ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP"
    )


def _odds_history_stage_rows(
    records: List[Tuple[str, int, float]],
    *,
    assume_deduped: bool,
) -> list[dict[str, object]]:
    ingested = _utc_now()
    if assume_deduped:
        rows = [
            (token_id, int(timestamp), float(price), ingested)
            for token_id, timestamp, price in records
        ]
    else:
        dedup: Dict[Tuple[str, int], float] = {}
        for token_id, timestamp, price in records:
            dedup[(token_id, int(timestamp))] = float(price)
        rows = [
            (token_id, timestamp, price, ingested)
            for (token_id, timestamp), price in dedup.items()
        ]
    return [
        {
            "clobTokenId": token_id,
            "timestamp": timestamp,
            "price": price,
            "ingested_at": ing,
        }
        for token_id, timestamp, price, ing in rows
    ]


def prepare_odds_bulk_upsert(
    records: List[Tuple[str, int, float]],
    conn: duckdb.DuckDBPyConnection,
    *,
    assume_deduped: bool = False,
) -> str | None:
    """Load a dlt odds stage table; call before ``BEGIN`` on ``conn``."""
    if not records:
        return None
    _ensure_odds_history_schema(conn)
    return prepare_odds_history_stage(
        _odds_history_stage_rows(records, assume_deduped=assume_deduped)
    )


def merge_odds_bulk_upsert(conn: duckdb.DuckDBPyConnection, stage: str) -> None:
    merge_odds_history_stage(conn, stage)


def save_odds_bulk_upsert(
    records: List[Tuple[str, int, float]],
    conn: duckdb.DuckDBPyConnection,
    *,
    assume_deduped: bool = False,
):
    """
    Bulk upsert odds rows using a dlt-managed stage table.

    This path is resilient to overlap-driven duplicates (same token/timestamp)
    and generally performs better than row-wise inserts for large bulk loads.
    """
    if not records:
        return
    stage = prepare_odds_bulk_upsert(records, conn, assume_deduped=assume_deduped)
    if stage is None:
        return
    merge_odds_bulk_upsert(conn, stage)
    logger.debug("Upserted %d odds records to DuckDB", len(records))
