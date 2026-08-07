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


def test_futures_minute_publish_allows_empty_audit_siblings(duck):
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    success_row = {
        "market_id": "market-a",
        "clobTokenId": "token-a",
        "timestamp": 100,
        "price": 0.4,
        "fidelity_minutes": 1,
        "window_start_at": now,
        "window_end_at": now,
        "ingested_at": now,
    }

    def audit(token_id: str, status: str) -> dict[str, object]:
        return {
            "fetch_run_id": "run-empty-ok",
            "market_id": f"market-{token_id[-1]}",
            "clobTokenId": token_id,
            "fetch_status": status,
            "raw_published": False,
            "fidelity_minutes": 1,
            "exact_window_start_at": now,
            "exact_window_end_at": now,
            "request_start_epoch": 100,
            "request_end_epoch": 100,
            "source_row_count": 1 if status == "success" else 0,
            "window_row_count": 1 if status == "success" else 0,
            "window_history_sha256": "a" * 64 if status == "success" else None,
            "source_endpoint": "https://clob.polymarket.com/prices-history",
            "fetch_started_at": now,
            "fetch_finished_at": now,
            "error_type": None if status == "success" else "EmptyHistory",
            "error_message": None
            if status == "success"
            else f"Empty in-window CLOB history for token {token_id}",
        }

    with duck.get_connection() as conn:
        load_futures_minute_fetch_audit(
            [audit("token-a", "success"), audit("token-b", "empty")], conn
        )
        load_futures_minute_odds_history_stage(
            [success_row], conn, fetch_run_id="run-empty-ok"
        )
        published = conn.execute(
            """
            select clobTokenId, fetch_status, raw_published
            from polymarket_wc2026_ops.futures_minute_odds_fetch_audit
            where fetch_run_id = 'run-empty-ok'
            order by clobTokenId
            """
        ).fetchall()
        assert published == [
            ("token-a", "success", True),
            ("token-b", "empty", False),
        ]
        assert (
            conn.execute(
                "select count(*) from polymarket_wc2026_raw.futures_minute_odds_history"
            ).fetchone()[0]
            == 1
        )


def test_futures_minute_raw_replace_accepts_arrow_table(duck):
    import pyarrow as pa

    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    table = pa.table(
        {
            "market_id": pa.array(["market"], type=pa.string()),
            "clob_token_id": pa.array(["token"], type=pa.string()),
            "timestamp": pa.array([100], type=pa.int64()),
            "price": pa.array([0.4], type=pa.float64()),
            "fidelity_minutes": pa.array([1], type=pa.int32()),
            "window_start_at": pa.array(
                [now], type=pa.timestamp("us", tz="UTC")
            ),
            "window_end_at": pa.array([now], type=pa.timestamp("us", tz="UTC")),
            "ingested_at": pa.array([now], type=pa.timestamp("us", tz="UTC")),
            "row_order": pa.array([0], type=pa.int64()),
        }
    )
    audit = {
        "fetch_run_id": "run-arrow",
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
        load_futures_minute_fetch_audit([audit], conn)
        load_futures_minute_odds_history_stage(
            table, conn, fetch_run_id="run-arrow"
        )
        prices = conn.execute(
            """
            select price
            from polymarket_wc2026_raw.futures_minute_odds_history
            order by timestamp
            """
        ).fetchall()
        published = conn.execute(
            """
            select raw_published
            from polymarket_wc2026_ops.futures_minute_odds_fetch_audit
            where fetch_run_id = 'run-arrow'
            """
        ).fetchone()[0]

    assert prices == [(0.4,)]
    assert published is True
