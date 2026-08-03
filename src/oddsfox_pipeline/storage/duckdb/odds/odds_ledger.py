from datetime import datetime
from typing import Dict, List, Tuple

import duckdb

from oddsfox_pipeline.storage.duckdb.connection import ensure_duck_db, get_connection
from oddsfox_pipeline.storage.duckdb.odds._common import (
    logger,
    odds_history_tbl,
    sql_upsert_ledger_last_sync,
    sql_upsert_ledger_state,
    sql_upsert_token_sync_skip,
    token_sync_ledger_tbl,
)


def upsert_ledger_last_sync_batch(
    token_timestamps: List[Tuple[str, int]],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Advance per-token sync cursors without clobbering other ledger columns (e.g. fully_checked)."""
    if not token_timestamps:
        return
    conn.executemany(sql_upsert_ledger_last_sync(), token_timestamps)


def upsert_token_sync_state_batch(
    token_states: List[
        Tuple[
            str,
            int | None,
            datetime | None,
            datetime | None,
            int | None,
            bool,
        ]
    ],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Persist per-token scheduler state without regressing existing ledger progress."""
    if not token_states:
        return
    conn.executemany(sql_upsert_ledger_state(), token_states)


def upsert_skipped_tokens_batch(
    token_reasons: List[Tuple[str, str]],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Persist or update skip reasons without resetting created_at on existing rows."""
    if not token_reasons:
        return
    conn.executemany(sql_upsert_token_sync_skip(), token_reasons)


def save_skipped_tokens(token_reasons: List[Tuple[str, str]]):
    """Persist tokens that should be skipped (permanent client-side errors)."""
    if not token_reasons:
        return
    ensure_duck_db()
    with get_connection() as conn:
        upsert_skipped_tokens_batch(token_reasons, conn)
    logger.debug("Saved %d skipped tokens to DuckDB", len(token_reasons))


def reconcile_token_sync_ledger_from_history() -> Dict[str, int]:
    """
    Reconcile stale/missing ledger cursors from odds_history maxima.

    Returns:
      dict with keys:
        scanned_tokens: number of tokens with any odds history
        repaired_tokens: number of tokens with stale/missing ledger cursors
    """
    ensure_duck_db()
    with get_connection() as conn:
        scanned_row = conn.execute(
            f"SELECT COUNT(DISTINCT clobTokenId) FROM {odds_history_tbl()}"
        ).fetchone()
        scanned_tokens = (
            int(scanned_row[0]) if scanned_row and scanned_row[0] is not None else 0
        )

        repaired_row = conn.execute(
            f"""
            WITH history AS (
                SELECT clobTokenId, MAX(timestamp) AS max_history_ts
                FROM {odds_history_tbl()}
                GROUP BY clobTokenId
            )
            SELECT COUNT(*)
            FROM history h
            LEFT JOIN {token_sync_ledger_tbl()} l ON l.clobTokenId = h.clobTokenId
            WHERE l.last_sync_timestamp IS NULL OR h.max_history_ts > l.last_sync_timestamp
            """
        ).fetchone()
        repaired_tokens = (
            int(repaired_row[0]) if repaired_row and repaired_row[0] is not None else 0
        )

        conn.execute(
            f"""
            INSERT INTO {token_sync_ledger_tbl()} (clobTokenId, last_sync_timestamp)
            SELECT h.clobTokenId, h.max_history_ts
            FROM (
                SELECT clobTokenId, MAX(timestamp) AS max_history_ts
                FROM {odds_history_tbl()}
                GROUP BY clobTokenId
            ) h
            ON CONFLICT(clobTokenId) DO UPDATE SET
                last_sync_timestamp = GREATEST(
                    COALESCE(token_sync_ledger.last_sync_timestamp, CAST(-9223372036854775808 AS BIGINT)),
                    COALESCE(excluded.last_sync_timestamp, CAST(-9223372036854775808 AS BIGINT))
                )
            """
        )

    logger.debug(
        "Reconciled odds ledger from history: scanned_tokens=%s repaired_tokens=%s",
        scanned_tokens,
        repaired_tokens,
    )
    return {
        "scanned_tokens": scanned_tokens,
        "repaired_tokens": repaired_tokens,
    }


def save_sync_status_batch(token_timestamps: List[Tuple[str, int]]):
    """Update last sync timestamps for a batch of tokens in DuckDB."""
    if not token_timestamps:
        return
    ensure_duck_db()
    with get_connection() as conn:
        upsert_ledger_last_sync_batch(token_timestamps, conn)
    logger.debug("Saved sync status for %d tokens to DuckDB", len(token_timestamps))


def save_token_sync_state_batch(
    token_states: List[
        Tuple[
            str,
            int | None,
            datetime | None,
            datetime | None,
            int | None,
            bool,
        ]
    ],
):
    """Persist per-token scheduler state for routine odds syncing."""
    if not token_states:
        return
    ensure_duck_db()
    with get_connection() as conn:
        upsert_token_sync_state_batch(token_states, conn)
    logger.debug(
        "Saved token scheduler state for %d tokens to DuckDB", len(token_states)
    )
