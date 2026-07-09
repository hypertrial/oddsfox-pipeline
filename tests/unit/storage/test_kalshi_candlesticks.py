"""Storage tests for Kalshi candlestick persistence."""

from __future__ import annotations

from datetime import datetime

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
