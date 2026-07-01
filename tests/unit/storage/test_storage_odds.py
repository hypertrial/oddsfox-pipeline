"""Unit tests for storage/duckdb odds module."""

from __future__ import annotations

import duckdb
import pytest
from tests.unit.storage.duckdb_storage_test_support import T_OH, T_TOD

import oddsfox.storage.duckdb.odds as odds_mod


def test_odds_chunked_raises():
    with pytest.raises(ValueError):
        list(odds_mod._chunked(["a"], 0))


def test_odds_latest_and_tokens(duck):
    odds_mod.save_odds_batch([("tok", 100, 0.5)])
    assert "tok" in odds_mod.get_latest_timestamps()
    assert "tok" in odds_mod.get_tokens_with_data()
    odds_mod.mark_tokens_fully_checked(["tok"])
    assert "tok" in odds_mod.get_fully_checked_tokens()
    odds_mod.save_skipped_tokens([("t2", "reason")])
    assert odds_mod.get_skipped_tokens()["t2"] == "reason"


def test_refresh_token_odds_daily_and_backfill(duck):
    odds_mod.save_odds_batch(
        [
            ("tok", 1710000000, 0.4),
            ("tok", 1710000300, 0.6),
            ("tok", 1710086400, 0.2),
        ]
    )
    with odds_mod.get_connection() as conn:
        odds_mod.refresh_token_odds_daily(
            [
                ("tok", odds_mod._epoch_to_utc_date(1710000000)),
                ("tok", odds_mod._epoch_to_utc_date(1710086400)),
            ],
            conn,
        )
        rows = conn.execute(
            f"""
            SELECT odds_date_utc, open_price, high_price, low_price, close_price, avg_price, observed_points
            FROM {T_TOD}
            WHERE clobTokenId = 'tok'
            ORDER BY odds_date_utc
            """
        ).fetchall()
    assert rows[0][1:] == (0.4, 0.6, 0.4, 0.6, 0.5, 2)
    assert rows[1][1:] == (0.2, 0.2, 0.2, 0.2, 0.2, 1)

    count = odds_mod.backfill_token_odds_daily_from_history()
    assert count >= 2

    odds_mod.save_odds_batch(
        [
            ("drift", 1720000000, 0.9995),
            ("drift", 1720000300, 0.9995),
            ("drift", 1720000600, 0.9995),
        ]
    )
    with odds_mod.get_connection() as conn:
        odds_mod.refresh_token_odds_daily(
            [("drift", odds_mod._epoch_to_utc_date(1720000000))],
            conn,
        )
        low_price, high_price, avg_price = conn.execute(
            f"""
            SELECT low_price, high_price, avg_price
            FROM {T_TOD}
            WHERE clobTokenId = 'drift'
            """
        ).fetchone()
    assert low_price <= avg_price <= high_price


def test_save_odds_bulk_appender_fallback_without_appender(duck, monkeypatch):
    with odds_mod.get_connection() as conn:
        monkeypatch.delattr(duckdb, "Appender", raising=False)
        odds_mod.save_odds_bulk_appender([("z", 1, 0.1)], conn)


def test_save_odds_bulk_upsert_paths(duck):
    with odds_mod.get_connection() as conn:
        odds_mod.save_odds_bulk_upsert(
            [("z", 1, 0.1), ("z", 1, 0.2)], conn, assume_deduped=False
        )
        odds_mod.save_odds_bulk_upsert([("z", 2, 0.3)], conn, assume_deduped=True)
        rows = conn.execute(
            f"""
            SELECT timestamp, price
            FROM {T_OH}
            WHERE clobTokenId = 'z'
            ORDER BY timestamp
            """
        ).fetchall()
    assert rows == [(1, 0.2), (2, 0.3)]


def test_reconcile_ledger(duck):
    odds_mod.save_odds_batch([("r", 50, 0.4)])
    summary = odds_mod.reconcile_token_sync_ledger_from_history()
    assert "scanned_tokens" in summary


def test_get_token_sync_snapshot_empty():
    assert odds_mod.get_token_sync_snapshot([]) == ({}, set(), {})


def test_get_token_sync_snapshot_empty_with_scheduler_state():
    assert odds_mod.get_token_sync_snapshot([], include_scheduler_state=True) == (
        {},
        set(),
        {},
        {},
    )


def test_get_token_sync_snapshot_with_reconcile(duck):
    odds_mod.save_odds_batch([("snap", 10, 0.5)])
    odds_mod.save_sync_status_batch([("snap", 5)])
    a, b, c = odds_mod.get_token_sync_snapshot(
        ["snap"], reconcile_with_history=True, repair_ledger=True
    )
    assert "snap" in a
