from __future__ import annotations

import pytest

from oddsfox_pipeline.storage.duckdb import dlt_batch as dlt_batch_mod
from oddsfox_pipeline.storage.duckdb.dlt_batch import (
    load_market_tokens_stage,
    load_stage_rows,
    merge_match_order_book_snapshots,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import polymarket_wc2026_raw_tbl


def _order_book_row(**overrides):
    row = {
        "scan_id": "scan-1",
        "manifest_sha256": "a" * 64,
        "fifa_match_id": 95,
        "stage": "round_of_16",
        "home_team": "Argentina",
        "away_team": "Egypt",
        "event_id": "665733",
        "event_slug": "fifwc-arg-egy-2026-07-07-more-markets",
        "market_id": "2793969",
        "market_slug": "fifwc-arg-egy-2026-07-07-team-to-advance",
        "market_type": "soccer_team_to_advance",
        "condition_id": "0x" + ("1" * 64),
        "outcome_label": "Argentina",
        "clob_token_id": "123",
        "window_start_ms": 1_000,
        "window_end_ms": 2_000,
        "snapshot_timestamp_ms": 1_500,
        "snapshot_at": "1970-01-01T00:00:01.500000+00:00",
        "snapshot_sha256": "b" * 64,
        "bids_json": '[{"price":"0.4","size":"10"}]',
        "asks_json": '[{"price":"0.6","size":"5"}]',
        "is_neg_risk": False,
        "last_trade_price": "0.5",
        "source_endpoint": "pmxt",
        "ingested_at": "2026-07-28T00:00:00+00:00",
    }
    row.update(overrides)
    return row


def test_dlt_batch_loads_stage_and_finalizes_market_tokens(duck):
    with duck.get_connection() as conn:
        conn.execute(
            f"""
            CREATE TABLE {polymarket_wc2026_raw_tbl("stage_market_tokens")} (
                market_id TEXT,
                clob_token_ids TEXT,
                updated_at TIMESTAMP
            )
            """
        )
        load_market_tokens_stage(
            [
                {
                    "market_id": "m1",
                    "clobTokenIds": '["tok-a"]',
                    "updated_at": "2026-01-01T00:00:00",
                }
            ],
            conn,
        )
        canonical = conn.execute(
            f"""
            SELECT market_id, clobTokenIds
            FROM {polymarket_wc2026_raw_tbl("market_tokens")}
            """
        ).fetchall()
        staged = conn.execute(
            f"""
            SELECT market_id, clob_token_ids
            FROM {polymarket_wc2026_raw_tbl("stage_market_tokens_v1")}
            """
        ).fetchall()

    assert canonical == [("m1", '["tok-a"]')]
    assert staged == canonical


def test_load_stage_rows_rejects_empty_rows():
    with pytest.raises(ValueError, match="rows must not be empty"):
        load_stage_rows(
            schema="polymarket_wc2026_raw", stage_table="stage", rows=[], columns={}
        )


def test_match_order_book_dlt_stage_merges_canonical_rows_idempotently(duck):
    row = _order_book_row()
    away_row = _order_book_row(
        outcome_label="Egypt",
        clob_token_id="456",
        snapshot_timestamp_ms=1_600,
        snapshot_at="1970-01-01T00:00:01.600000+00:00",
        snapshot_sha256="c" * 64,
    )
    with duck.get_connection() as conn:
        merge_match_order_book_snapshots([row], conn)
        merge_match_order_book_snapshots([row], conn)
        merge_match_order_book_snapshots([away_row], conn)
        canonical_count = conn.execute(
            """
            SELECT count(*)
            FROM polymarket_wc2026_raw.match_order_book_snapshots
            """
        ).fetchone()[0]
        staged_count = conn.execute(
            """
            SELECT count(*)
            FROM polymarket_wc2026_raw.stage_match_order_book_snapshots_v1
            """
        ).fetchone()[0]

        roles = conn.execute(
            """
            SELECT landscape_role
            FROM polymarket_wc2026_raw.match_order_book_snapshots
            ORDER BY clob_token_id
            """
        ).fetchall()

    assert canonical_count == 2
    assert staged_count == 1
    assert roles == [("home",), ("away",)]


def test_match_order_book_dlt_stage_rejects_unknown_role(duck):
    with duck.get_connection() as conn:
        with pytest.raises(ValueError, match="explicit landscape_role"):
            merge_match_order_book_snapshots(
                [_order_book_row(outcome_label="Draw")],
                conn,
            )


def test_load_stage_rows_drops_pending_packages(monkeypatch):
    class Pipe:
        has_pending_data = True

        def __init__(self):
            self.dropped = False
            self.runs = []

        def drop_pending_packages(self):
            self.dropped = True

        def run(self, rows, **kwargs):
            self.runs.append((rows, kwargs))

    pipe = Pipe()
    monkeypatch.setattr(dlt_batch_mod, "_pipeline", lambda _schema: pipe)

    stage = load_stage_rows(
        schema="polymarket_wc2026_raw",
        stage_table="stage_probe",
        rows=[{"id": "1"}],
        columns={"id": {"data_type": "text"}},
    )

    assert pipe.dropped is True
    assert pipe.runs
    assert stage == '"polymarket_wc2026_raw"."stage_probe"'


def test_match_order_book_merge_ignores_empty_batch(duck):
    with duck.get_connection() as conn:
        merge_match_order_book_snapshots([], conn)
        count = conn.execute(
            """
            select count(*)
            from polymarket_wc2026_raw.match_order_book_snapshots
            """
        ).fetchone()[0]

    assert count == 0


def test_dlt_pipeline_uses_public_active_duckdb_path(monkeypatch):
    created = {}

    class FakeDlt:
        class destinations:
            @staticmethod
            def duckdb(*, credentials):
                return {"credentials": credentials}

        @staticmethod
        def pipeline(**kwargs):
            created.update(kwargs)
            return object()

    dlt_batch_mod._PIPELINES.clear()
    monkeypatch.setattr(dlt_batch_mod, "dlt", FakeDlt)
    monkeypatch.setattr(dlt_batch_mod.duckdb_connection, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        dlt_batch_mod.duckdb_connection,
        "active_duckdb_path",
        lambda: "/tmp/public.duckdb",
    )

    dlt_batch_mod._pipeline("polymarket_wc2026_raw")

    assert created["destination"] == {"credentials": "/tmp/public.duckdb"}
    dlt_batch_mod._PIPELINES.clear()
