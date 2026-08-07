from datetime import datetime, timezone

import duckdb
import pytest

from oddsfox_pipeline.storage.duckdb.dlt_batch import (
    load_futures_minute_fetch_audit,
    load_futures_minute_odds_history_stage,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    bootstrap_all_polymarket_tables,
)


def test_futures_minute_raw_table_is_wc2026_only():
    with duckdb.connect(":memory:") as conn:
        conn.execute("create schema polymarket_wc2026_raw")
        conn.execute("create schema polymarket_wc2026_ops")
        bootstrap_all_polymarket_tables(conn)

        rows = conn.execute(
            """
            select table_schema
            from information_schema.tables
            where table_name = 'futures_minute_odds_history'
            """
        ).fetchall()

    assert rows == [("polymarket_wc2026_raw",)]


def test_futures_minute_raw_replace_is_exact_idempotent_and_isolated(duck):
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

    def audit(run_id: str) -> dict[str, object]:
        return {
            "fetch_run_id": run_id,
            "market_id": "market",
            "clobTokenId": "token",
            "fetch_status": "success",
            "raw_published": False,
            "fidelity_minutes": 1,
            "exact_window_start_at": now,
            "exact_window_end_at": now,
            "request_start_epoch": 100,
            "request_end_epoch": 100,
            "source_row_count": 1,
            "window_row_count": 1,
            "window_history_sha256": "a" * 64,
            "source_endpoint": "https://clob.polymarket.com/prices-history",
            "fetch_started_at": now,
            "fetch_finished_at": now,
            "error_type": None,
            "error_message": None,
        }

    with duck.get_connection() as conn:
        load_futures_minute_fetch_audit([audit("run-1")], conn)
        load_futures_minute_odds_history_stage(
            [row, {**row, "timestamp": 101}], conn, fetch_run_id="run-1"
        )
        load_futures_minute_fetch_audit([audit("run-2")], conn)
        load_futures_minute_odds_history_stage(
            [{**row, "price": 0.5}], conn, fetch_run_id="run-2"
        )
        with pytest.raises(RuntimeError, match="Fetch audit inventory"):
            load_futures_minute_odds_history_stage(
                [{**row, "price": 0.9}], conn, fetch_run_id="missing-audit"
            )

        prices = conn.execute(
            """
            select price
            from polymarket_wc2026_raw.futures_minute_odds_history
            order by timestamp
            """
        ).fetchall()
        assert prices == [(0.5,)]

        hourly = conn.execute(
            "select count(*) from polymarket_wc2026_raw.odds_history"
        ).fetchone()[0]
        assert hourly == 0
