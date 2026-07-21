from datetime import datetime, timezone

import duckdb
import pytest

from oddsfox_pipeline.storage.duckdb.dlt_batch import (
    load_match_minute_fetch_audit,
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


def test_match_minute_raw_replace_is_exact_idempotent_and_isolated(duck):
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
            "in_game_row_count": 1,
            "in_game_history_sha256": "a" * 64,
            "source_endpoint": "https://clob.polymarket.com/prices-history",
            "fetch_started_at": now,
            "fetch_finished_at": now,
            "error_type": None,
            "error_message": None,
        }

    with duck.get_connection() as conn:
        load_match_minute_fetch_audit([audit("run-1")], conn)
        load_match_minute_odds_history_stage(
            [row, {**row, "timestamp": 101}], conn, fetch_run_id="run-1"
        )
        load_match_minute_fetch_audit([audit("run-2")], conn)
        load_match_minute_odds_history_stage(
            [{**row, "price": 0.5}], conn, fetch_run_id="run-2"
        )
        try:
            load_match_minute_odds_history_stage(
                [{**row, "price": 0.9}], conn, fetch_run_id="missing-audit"
            )
        except RuntimeError as exc:
            assert "Fetch audit inventory" in str(exc)
        else:  # pragma: no cover - assertion helper
            raise AssertionError("missing audit must block raw publication")
        load_match_minute_fetch_audit([audit("run-3")], conn)
        with pytest.raises(duckdb.ConstraintException):
            load_match_minute_odds_history_stage(
                [{**row, "price": 0.9, "fidelity_minutes": 2}],
                conn,
                fetch_run_id="run-3",
            )
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
        published = conn.execute(
            "select count(*) from polymarket_wc2026_ops.match_minute_odds_fetch_audit "
            "where raw_published"
        ).fetchone()[0]
        unpublished_run_3 = conn.execute(
            "select count(*) "
            "from polymarket_wc2026_ops.match_minute_odds_fetch_audit "
            "where fetch_run_id = 'run-3' and not raw_published"
        ).fetchone()[0]

    assert minute_rows == [("token", 100, 0.5)]
    assert hourly_rows == 0
    assert ledger_rows == 0
    assert published == 2
    assert unpublished_run_3 == 1


def test_match_minute_fetch_audit_append_is_atomic(duck):
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)

    def row(token: str, error_message: str | None = None):
        return {
            "fetch_run_id": "run",
            "market_id": "market",
            "clobTokenId": token,
            "fetch_status": "error",
            "raw_published": False,
            "fidelity_minutes": 1,
            "exact_window_start_at": now,
            "exact_window_end_at": now,
            "request_start_epoch": 100,
            "request_end_epoch": 100,
            "source_row_count": 0,
            "in_game_row_count": 0,
            "in_game_history_sha256": None,
            "source_endpoint": "https://clob.polymarket.com/prices-history",
            "fetch_started_at": now,
            "fetch_finished_at": now,
            "error_type": "RuntimeError",
            "error_message": error_message,
        }

    with duck.get_connection() as conn:
        load_match_minute_fetch_audit([], conn)
        with pytest.raises(duckdb.ConstraintException):
            load_match_minute_fetch_audit(
                [row("valid", "ok"), row("invalid", "x" * 501)], conn
            )
        assert (
            conn.execute(
                "select count(*) "
                "from polymarket_wc2026_ops.match_minute_odds_fetch_audit"
            ).fetchone()[0]
            == 0
        )
