"""Unit tests for DuckDB market mutation helpers."""

from __future__ import annotations

from contextlib import contextmanager

import pytest
from tests.unit.storage.duckdb_storage_test_support import (
    T_M,
    T_MT,
    _insert_minimal_market,
)

import oddsfox_pipeline.storage.duckdb.markets as markets
from oddsfox_pipeline.storage.duckdb import _market_mutations as mutations
from oddsfox_pipeline.storage.duckdb.connection import get_connection


def test_save_market_tokens_batch_persists_tokens(duck):
    with duck.get_connection() as conn:
        _insert_minimal_market(conn, "m-tok")

    markets.save_market_tokens_batch([("m-tok", '["old"]')])
    markets.save_market_tokens_batch([("m-tok", '["tok-a", "tok-b"]')])

    with duck.get_connection() as conn:
        row = conn.execute(
            f"SELECT clobTokenIds FROM {T_MT} WHERE market_id = 'm-tok'"
        ).fetchone()
    assert row == ('["tok-a", "tok-b"]',)


def test_save_market_tokens_batch_noop_without_tokens(duck):
    with duck.get_connection() as conn:
        before = conn.execute(f"SELECT COUNT(*) FROM {T_MT}").fetchone()[0]
    markets.save_market_tokens_batch([])
    with duck.get_connection() as conn:
        after = conn.execute(f"SELECT COUNT(*) FROM {T_MT}").fetchone()[0]
    assert before == after == 0


def test_persist_market_tokens_empty_guard():
    mutations._persist_market_tokens(None, [])


def test_get_markets_without_tokens_and_save_tokens(duck):
    with markets.get_connection() as conn:
        _insert_minimal_market(conn, "nm")
        mids = markets.get_markets_without_tokens(limit=5)
        assert "nm" in mids
    markets.save_tokens_batch([("nm", '["tok"]')])
    assert markets.get_markets_without_tokens(limit=5) == []


def test_delete_orphan_market_tokens(duck):
    assert markets.delete_orphan_market_tokens() == 0
    with markets.get_connection() as conn:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {T_MT} (market_id, clobTokenIds, updated_at)
            VALUES ('orphan_only', '["t"]', CURRENT_TIMESTAMP)
            """
        )
    assert markets.delete_orphan_market_tokens() == 1
    assert markets.delete_orphan_market_tokens() == 0


def test_delete_orphan_market_tokens_rolls_back_on_error(monkeypatch):
    calls = []

    class Conn:
        def execute(self, sql):
            calls.append(sql)
            if sql == "BEGIN" or sql == "ROLLBACK":
                return self
            if "SELECT COUNT" in sql:
                return self
            if "DELETE FROM" in sql:
                raise RuntimeError("delete failed")
            return self

        def fetchone(self):
            return (1,)

    @contextmanager
    def connection():
        yield Conn()

    monkeypatch.setattr(mutations, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(mutations, "get_connection", connection)

    with pytest.raises(RuntimeError, match="delete failed"):
        mutations.delete_orphan_market_tokens()

    assert "ROLLBACK" in calls


def test_save_tokens_batch_skips_unknown_market_id(duck, caplog):
    caplog.set_level("WARNING")
    markets.save_tokens_batch([("unknown_mid", '["x"]')])
    with markets.get_connection() as conn:
        n = conn.execute(
            f"SELECT COUNT(*) FROM {T_MT} WHERE market_id = 'unknown_mid'"
        ).fetchone()[0]
    assert int(n) == 0
    assert any("skipping" in r.message for r in caplog.records)


def test_extract_slug_record_order_matches_save_slugs_batch(duck):
    """_extract_slug_record returns (slug, market_id) for save_slugs_batch."""
    from oddsfox_pipeline.ingestion.polymarket.markets.backfill._extract import (
        _extract_slug_record,
    )

    with get_connection() as conn:
        _insert_minimal_market(conn, mid="m-slug", slug=None)
    record = _extract_slug_record("m-slug", {"slug": "my-slug"})
    assert record == ("my-slug", "m-slug")
    markets.save_slugs_batch([record])
    with get_connection() as conn:
        row = conn.execute(f"SELECT slug FROM {T_M} WHERE id = 'm-slug'").fetchone()
    assert row[0] == "my-slug"


def test_save_event_slugs_batch_rolls_back_on_error(monkeypatch):
    calls = []

    class Conn:
        def execute(self, sql):
            calls.append(sql)
            return self

        def executemany(self, sql, rows):
            calls.append(sql)
            raise RuntimeError("update failed")

    @contextmanager
    def connection():
        yield Conn()

    monkeypatch.setattr(mutations, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(mutations, "get_connection", connection)

    with pytest.raises(RuntimeError, match="update failed"):
        mutations.save_event_slugs_batch([("event", "market")])

    assert "ROLLBACK" in calls


def test_markets_empty_saves_noop(duck):
    markets.save_tokens_batch([])
    markets.save_slugs_batch([])
    markets.save_event_slugs_batch([])
    markets.save_end_dates_batch([])
