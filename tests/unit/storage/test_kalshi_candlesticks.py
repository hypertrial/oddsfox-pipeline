"""Storage tests for Kalshi candlestick persistence."""

from __future__ import annotations

from datetime import datetime

import pytest

from oddsfox_pipeline.storage.duckdb import kalshi_candlesticks
from oddsfox_pipeline.storage.duckdb.kalshi_market_scope_registry import (
    KalshiRegistryRow,
    upsert_registry_rows,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    kalshi_ops_tbl,
    kalshi_raw_tbl,
)
from oddsfox_pipeline.storage.duckdb.schemas.kalshi import create_test_kalshi_raw_tables


def test_empty_candlestick_ledger_state_batch_is_noop():
    assert kalshi_candlesticks.upsert_candlestick_ledger_states_batch([]) is None


def _seed_registry_and_market(duck, *, market_ticker="KXWC-MKT1", open_time=None):
    with duck.get_connection() as conn:
        create_test_kalshi_raw_tables(conn)
    upsert_registry_rows(
        [
            KalshiRegistryRow(
                market_ticker=market_ticker,
                event_ticker="KXWC-EVT1",
                series_ticker="KXWC",
                source="test",
            )
        ]
    )
    with duck.get_connection() as conn:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {kalshi_raw_tbl("wc2026", "markets")} (
                market_ticker, event_ticker, series_ticker, open_time, scraped_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                market_ticker,
                "KXWC-EVT1",
                "KXWC",
                open_time,
                datetime(2026, 1, 1),
            ],
        )


def test_get_registry_markets_for_sync_respects_ledger_due_filter(duck):
    _seed_registry_and_market(duck, market_ticker="KXWC-DUE")
    _seed_registry_and_market(duck, market_ticker="KXWC-SKIP")
    ledger = kalshi_ops_tbl("wc2026", "candlestick_sync_ledger")
    with duck.get_connection() as conn:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {ledger} (
                market_ticker, fully_checked, last_checked_at, next_check_at
            )
            VALUES (?, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + INTERVAL '2 hours')
            """,
            ["KXWC-SKIP"],
        )

    due = kalshi_candlesticks.get_registry_markets_for_sync(scope_name="wc2026")
    forced = kalshi_candlesticks.get_registry_markets_for_sync(
        scope_name="wc2026", force=True
    )

    assert [row["market_ticker"] for row in due] == ["KXWC-DUE"]
    assert {row["market_ticker"] for row in forced} == {"KXWC-DUE", "KXWC-SKIP"}


@pytest.mark.parametrize(
    "session_tz",
    ["UTC", "America/Los_Angeles", "Asia/Tokyo", "Europe/Warsaw"],
)
def test_get_registry_markets_for_sync_due_filter_uses_utc_walls(
    duck, session_tz, monkeypatch
):
    from datetime import timedelta, timezone

    import oddsfox_pipeline.storage.duckdb.connection as duck_connection

    _seed_registry_and_market(duck, market_ticker="KXWC-PAST")
    _seed_registry_and_market(duck, market_ticker="KXWC-FUT")
    ledger = kalshi_ops_tbl("wc2026", "candlestick_sync_ledger")
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    with duck.get_connection() as conn:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {ledger} (
                market_ticker, fully_checked, last_checked_at, next_check_at
            )
            VALUES
                (?, FALSE, ?, ?),
                (?, FALSE, ?, ?)
            """,
            [
                "KXWC-PAST",
                now,
                now - timedelta(hours=1),
                "KXWC-FUT",
                now,
                now + timedelta(hours=2),
            ],
        )

    # get_connection() opens a fresh session each time; force the session TZ on
    # every new writable connection so this actually exercises non-UTC hosts.
    real_open = duck_connection.open_writable_duckdb_connection

    def open_with_session_tz(path, *args, **kwargs):
        conn = real_open(path, *args, **kwargs)
        conn.execute(f"SET TimeZone='{session_tz}'")
        return conn

    monkeypatch.setattr(
        duck_connection, "open_writable_duckdb_connection", open_with_session_tz
    )

    due = kalshi_candlesticks.get_registry_markets_for_sync(scope_name="wc2026")
    assert [row["market_ticker"] for row in due] == ["KXWC-PAST"]


def test_save_candlesticks_batch_noop_and_upsert(duck):
    assert kalshi_candlesticks.save_candlesticks_batch([]) == 0

    rows = [
        {
            "market_ticker": "KXWC-MKT1",
            "hour_start_utc": datetime(2026, 1, 1, 0, 0, 0),
            "close_price": 0.4,
            "refreshed_at": datetime(2026, 1, 1, 1, 0, 0),
        }
    ]
    assert kalshi_candlesticks.save_candlesticks_batch(rows) == 1
    with duck.get_connection() as conn:
        count = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {kalshi_raw_tbl("wc2026", "market_candlesticks_hourly")}
            """
        ).fetchone()[0]
    assert count == 1


def test_upsert_candlestick_ledger_state_tracks_empty_runs(duck):
    kalshi_candlesticks.upsert_candlestick_ledger_state(
        market_ticker="KXWC-MKT1",
        fully_checked=True,
        empty_run=True,
    )
    kalshi_candlesticks.upsert_candlestick_ledger_state(
        market_ticker="KXWC-MKT1",
        fully_checked=True,
        empty_run=False,
    )
    ledger = kalshi_ops_tbl("wc2026", "candlestick_sync_ledger")
    with duck.get_connection() as conn:
        streak = conn.execute(
            f"SELECT empty_run_streak FROM {ledger} WHERE market_ticker = ?",
            ["KXWC-MKT1"],
        ).fetchone()[0]
    assert streak == 0


def test_upsert_candlestick_ledger_states_batch_tracks_empty_runs(duck):
    kalshi_candlesticks.upsert_candlestick_ledger_states_batch(
        [("KXWC-MKT1", True, True), ("KXWC-MKT2", True, True)],
        routine_interval_hours=2,
    )
    kalshi_candlesticks.upsert_candlestick_ledger_states_batch(
        [("KXWC-MKT1", True, True), ("KXWC-MKT2", True, False)],
    )
    ledger = kalshi_ops_tbl("wc2026", "candlestick_sync_ledger")
    with duck.get_connection() as conn:
        rows = {
            row[0]: row[1]
            for row in conn.execute(
                f"""
                SELECT market_ticker, empty_run_streak
                FROM {ledger}
                WHERE market_ticker IN (?, ?)
                """,
                ["KXWC-MKT1", "KXWC-MKT2"],
            ).fetchall()
        }
    assert rows == {"KXWC-MKT1": 2, "KXWC-MKT2": 0}


def test_upsert_candlestick_ledger_state_persists_last_sync_hour_start(duck):
    kalshi_candlesticks.upsert_candlestick_ledger_state(
        market_ticker="KXWC-MKT1",
        fully_checked=True,
        empty_run=False,
        last_sync_hour_start=1_735_689_600,
    )
    ledger = kalshi_ops_tbl("wc2026", "candlestick_sync_ledger")
    with duck.get_connection() as conn:
        row = conn.execute(
            f"""
            SELECT last_sync_hour_start, fully_checked
            FROM {ledger}
            WHERE market_ticker = ?
            """,
            ["KXWC-MKT1"],
        ).fetchone()
    assert row == (1_735_689_600, True)
