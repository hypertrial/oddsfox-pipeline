from typing import Dict, List, Tuple

import duckdb

from oddsfox.storage.duckdb.connection import ensure_duck_db, get_connection
from oddsfox.storage.duckdb.dlt_batch import load_odds_history_stage
from oddsfox.storage.duckdb.odds._common import (
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


def save_odds_bulk_upsert(
    records: List[Tuple[str, int, float]],
    conn: duckdb.DuckDBPyConnection,
    *,
    assume_deduped: bool = False,
):
    """
    Bulk upsert odds rows using a dlt-managed stage table.

    This path is resilient to overlap-driven duplicates (same token/timestamp)
    and generally performs better than row-wise inserts for large minutely loads.
    """
    if not records:
        return

    # Defensive schema guard: callers may provide connections from mixed test/runtime
    # setups where odds_history wasn't initialized on this file yet.
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

    ingested = _utc_now()
    if assume_deduped:
        rows = [
            (token_id, int(timestamp), float(price), ingested)
            for token_id, timestamp, price in records
        ]
    else:
        # Ensure deterministic conflict behavior even if callers pass duplicates.
        dedup: Dict[Tuple[str, int], float] = {}
        for token_id, timestamp, price in records:
            dedup[(token_id, int(timestamp))] = float(price)
        rows = [
            (token_id, timestamp, price, ingested)
            for (token_id, timestamp), price in dedup.items()
        ]

    load_odds_history_stage(
        [
            {
                "clobTokenId": token_id,
                "timestamp": timestamp,
                "price": price,
                "ingested_at": ing,
            }
            for token_id, timestamp, price, ing in rows
        ],
        conn,
    )
    logger.debug("Upserted %d odds records to DuckDB", len(rows))
