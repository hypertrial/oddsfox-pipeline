from datetime import datetime, timezone

import duckdb

from oddsfox_pipeline.storage.duckdb.dlt_batch import (
    load_match_minute_odds_history_stage,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    bootstrap_all_polymarket_tables,
)


def test_match_minute_raw_table_is_wc2026_only():
    with duckdb.connect(":memory:") as conn:
        conn.execute("create schema polymarket_wc2026_raw")
        conn.execute("create schema polymarket_wc2026_ops")
        conn.execute("create schema polymarket_us_midterms_2026_raw")
        conn.execute("create schema polymarket_us_midterms_2026_ops")
        bootstrap_all_polymarket_tables(conn)

        rows = conn.execute(
            """
            select table_schema
            from information_schema.tables
            where table_name = 'match_minute_odds_history'
            """
        ).fetchall()

    assert rows == [("polymarket_wc2026_raw",)]


def test_match_minute_raw_upsert_is_idempotent_and_isolated(duck):
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    row = {
        "market_id": "market",
        "clobTokenId": "token",
        "timestamp": 100,
        "price": 0.4,
        "fidelity_minutes": 1,
        "window_start_at": now,
        "window_end_at": now,
        "ingested_at": now,
    }
    with duck.get_connection() as conn:
        load_match_minute_odds_history_stage([row], conn)
        load_match_minute_odds_history_stage([{**row, "price": 0.5}], conn)
        minute_rows = conn.execute(
            "select clobTokenId, timestamp, price "
            "from polymarket_wc2026_raw.match_minute_odds_history"
        ).fetchall()
        hourly_rows = conn.execute(
            "select count(*) from polymarket_wc2026_raw.odds_history"
        ).fetchone()[0]
        ledger_rows = conn.execute(
            "select count(*) from polymarket_wc2026_ops.token_sync_ledger"
        ).fetchone()[0]

    assert minute_rows == [("token", 100, 0.5)]
    assert hourly_rows == 0
    assert ledger_rows == 0
