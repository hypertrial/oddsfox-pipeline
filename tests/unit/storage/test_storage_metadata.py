"""Unit tests for storage/duckdb metadata module."""

from __future__ import annotations

import json
from contextlib import contextmanager

import duckdb
from tests.unit.storage.duckdb_storage_test_support import T_PRE

import oddsfox_pipeline.storage.duckdb.metadata as metadata


def test_save_sync_run_metrics_history_json_not_list(duck):
    """Parsed history is valid JSON but not a list — skip list branch (143-144)."""
    metadata._metadata_set("sync_metrics:nl2:history", json.dumps({"a": 1}))
    metadata.save_sync_run_metrics("nl2", {"x": 1}, history_limit=5)


def test_save_sync_run_metrics_history_list_mixed_types(duck):
    """History JSON is a list: keep only dict items (lines 143-144)."""
    metadata._metadata_set(
        "sync_metrics:mix:history",
        json.dumps([{"ok": 1}, "not-a-dict", {"ok": 2}]),
    )
    metadata.save_sync_run_metrics("mix", {"n": 1}, history_limit=5)
    raw = metadata._metadata_get("sync_metrics:mix:history")
    assert raw and '"n"' in raw


def test_save_sync_run_metrics_corrupt_history_json(duck):
    metadata._metadata_set("sync_metrics:hist:last", json.dumps({"a": 1}))
    metadata._metadata_set("sync_metrics:hist:history", "{not-json")
    metadata.save_sync_run_metrics("hist", {"b": 2}, history_limit=5)
    raw = metadata._metadata_get("sync_metrics:hist:history")
    assert raw and "b" in raw


def test_get_sync_run_metrics_non_dict_payload(duck):
    metadata._metadata_set("sync_metrics:nd:last", json.dumps([1, 2, 3]))
    assert metadata.get_sync_run_metrics("nd") is None


def test_save_sync_run_metrics_zero_history_limit(duck):
    metadata.save_sync_run_metrics("zlim", {"x": 1}, history_limit=0)


def test_append_ingestion_run_event_inserts_row(duck):
    rid = metadata.append_ingestion_run_event("sync_odds", {"rows": 1})
    with metadata.get_connection() as conn:
        row = conn.execute(
            f"""
            SELECT task_name, metrics_json
            FROM {T_PRE}
            WHERE run_id = ?
            """,
            [rid],
        ).fetchone()
    assert row is not None
    assert row[0] == "sync_odds"
    assert json.loads(row[1])["rows"] == 1
    assert "timestamp" in json.loads(row[1])


def test_save_sync_run_metrics_scoped_to_kalshi_wc2026(duck):
    from oddsfox_pipeline.storage.duckdb.schemas.constants import kalshi_ops_tbl

    kalshi_pre = kalshi_ops_tbl("wc2026", "ingestion_run_events")
    metadata.save_sync_run_metrics(
        "sync_markets",
        {"total_fetched": 3},
        source="kalshi",
        scope_name="wc2026",
    )
    with metadata.get_connection() as conn:
        kalshi_count = conn.execute(
            f"SELECT count(*) FROM {kalshi_pre} WHERE task_name = 'sync_markets'"
        ).fetchone()[0]
        wc2026_count = conn.execute(
            f"SELECT count(*) FROM {T_PRE} WHERE task_name = 'sync_markets'"
        ).fetchone()[0]
    assert kalshi_count == 1
    assert wc2026_count == 0


def test_save_sync_run_metrics_kalshi_does_not_clobber_polymarket_scrape_metadata(duck):
    metadata.save_sync_run_metrics("sync_markets", {"pm": 1}, source="polymarket")
    metadata.save_sync_run_metrics(
        "sync_markets",
        {"kalshi": 1},
        source="kalshi",
        scope_name="wc2026",
    )
    raw = metadata._metadata_get("sync_metrics:sync_markets:last")
    assert raw is not None
    assert json.loads(raw)["pm"] == 1
    assert "kalshi" not in json.loads(raw)


def test_save_sync_run_metrics_kalshi_keeps_rolling_history_in_ops_table(duck):
    metadata.save_sync_run_metrics(
        "sync_markets",
        {"kalshi": 1},
        source="kalshi",
        scope_name="wc2026",
        history_limit=3,
    )
    metadata.save_sync_run_metrics(
        "sync_markets",
        {"kalshi": 2},
        source="kalshi",
        scope_name="wc2026",
        history_limit=3,
    )
    with metadata.get_connection() as conn:
        row = conn.execute(
            f"""
            SELECT history_json
            FROM {metadata._ops_tbl("wc2026", "sync_run_metrics", source="kalshi")}
            WHERE task_name = 'sync_markets'
            """
        ).fetchone()
    history = json.loads(row[0])
    assert len(history) == 2
    assert history[0]["kalshi"] == 1
    assert history[1]["kalshi"] == 2


def test_save_sync_run_metrics_preserves_nested_planning_payload(duck):
    payload = {
        "planning": {"plans": 2, "closed_done": 1},
        "planning_context": {
            "market_tokens_distinct_tokens": 10,
            "planned_vs_market_tokens": 0.2,
        },
        "invalid_tokens": 1,
    }
    metadata.save_sync_run_metrics("sync_odds", payload, history_limit=2)
    saved = metadata.get_sync_run_metrics("sync_odds")
    assert saved is not None
    assert saved["planning"]["plans"] == 2
    assert saved["planning_context"]["market_tokens_distinct_tokens"] == 10
    assert saved["invalid_tokens"] == 1


def test_save_sync_run_metrics_pipeline_append_failure_continues(monkeypatch, duck):
    def boom(*_a, **_k):
        raise RuntimeError("simulated append failure")

    monkeypatch.setattr(metadata, "append_ingestion_run_event", boom)
    metadata.save_sync_run_metrics("append_fail", {"x": 1})
    saved = metadata.get_sync_run_metrics("append_fail")
    assert saved is not None
    assert saved["ingestion_run_event_append_failed"] is True
    assert saved["ingestion_run_event_append_error"] == (
        "RuntimeError: simulated append failure"
    )


def test_metadata_helpers(duck):
    assert metadata._metadata_get("missing") is None
    metadata._metadata_set("k", "v")
    assert metadata._metadata_get("k") == "v"

    assert metadata.get_backfill_fully_checked("t") is None
    metadata.set_backfill_fully_checked("t", True)
    assert metadata.get_backfill_fully_checked("t") is True
    metadata.set_backfill_fully_checked("t", False)
    assert metadata.get_backfill_fully_checked("t") is False

    metadata.save_sync_run_metrics("job", {"a": 1}, history_limit=2)
    m = metadata.get_sync_run_metrics("job")
    assert m is not None and m.get("a") == 1

    metadata._metadata_set("sync_metrics:bad:last", "not-json")
    assert metadata.get_sync_run_metrics("bad") is None

    metadata._metadata_set(
        "sync_metrics:badh:last",
        json.dumps([1, 2, 3]),
    )
    assert metadata.get_sync_run_metrics("badh") is None


def test_get_sync_run_metrics_missing_returns_none(duck):
    assert metadata.get_sync_run_metrics("missing") is None


def test_get_sync_run_metrics_handles_corrupt_table_payloads(duck):
    with metadata.get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO polymarket_wc2026_ops.sync_run_metrics
            (task_name, recorded_at, metrics_json, history_json)
            VALUES ('bad_table_json', CURRENT_TIMESTAMP, '{not-json', '[]')
            """
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO polymarket_wc2026_ops.sync_run_metrics
            (task_name, recorded_at, metrics_json, history_json)
            VALUES ('table_list_payload', CURRENT_TIMESTAMP, '[1, 2, 3]', '[]')
            """
        )

    metadata._metadata_set(
        "sync_metrics:table_list_payload:last",
        json.dumps({"fallback": True}),
    )

    assert metadata.get_sync_run_metrics("bad_table_json") is None
    assert metadata.get_sync_run_metrics("table_list_payload") == {"fallback": True}


def test_get_sync_run_metrics_query_exception_falls_back(monkeypatch):
    class Conn:
        def execute(self, *_args, **_kwargs):
            raise RuntimeError("query failed")

    @contextmanager
    def use_conn(conn=None):
        yield conn if conn is not None else Conn()

    monkeypatch.setattr(metadata, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(metadata, "_use_conn", use_conn)
    monkeypatch.setattr(metadata, "_metadata_get", lambda _key, _conn=None: None)

    assert metadata.get_sync_run_metrics("query_error") is None


def test_append_ingestion_run_event_kalshi_source(duck):
    from oddsfox_pipeline.storage.duckdb.schemas.constants import kalshi_ops_tbl

    run_id = metadata.append_ingestion_run_event(
        "sync_kalshi_candlesticks",
        {"rows_written": 2},
        source="kalshi",
        scope_name="wc2026",
    )
    pre = kalshi_ops_tbl("wc2026", "ingestion_run_events")
    with metadata.get_connection() as conn:
        row = conn.execute(
            f"SELECT task_name, metrics_json FROM {pre} WHERE run_id = ?",
            [run_id],
        ).fetchone()
    assert row[0] == "sync_kalshi_candlesticks"
    assert json.loads(row[1])["rows_written"] == 2


def test_save_and_get_sync_run_metrics_kalshi_source(duck):
    metadata.save_sync_run_metrics(
        "sync_kalshi_markets",
        {"total_markets": 4},
        source="kalshi",
        scope_name="wc2026",
    )
    saved = metadata.get_sync_run_metrics(
        "sync_kalshi_markets",
        source="kalshi",
        scope_name="wc2026",
    )
    assert saved is not None
    assert saved["total_markets"] == 4


def test_metadata_helpers_reuse_supplied_connection(duck, monkeypatch):
    from oddsfox_pipeline.storage.duckdb.connection import get_connection

    opened = 0
    real_get_connection = get_connection

    @contextmanager
    def counting_get_connection():
        nonlocal opened
        opened += 1
        with real_get_connection() as conn:
            yield conn

    monkeypatch.setattr(
        "oddsfox_pipeline.storage.duckdb.connection.get_connection",
        counting_get_connection,
    )

    with real_get_connection() as conn:
        metadata._metadata_set("conn-thread", "1", conn)
        assert metadata._metadata_get("conn-thread", conn) == "1"
        metadata.save_sync_run_metrics("conn-thread", {"ok": True}, conn=conn)
        saved = metadata.get_sync_run_metrics("conn-thread", conn=conn)

    assert saved is not None
    assert saved["ok"] is True
    assert opened == 0


def test_event_catalog_partition_checkpoint_round_trip(duck):
    with metadata.get_connection() as conn:
        metadata.save_event_catalog_partition_checkpoint(
            conn,
            "exact_2026_tag:open",
            {"1": {"id": "1", "slug": "fifwc-event-1"}},
            {"event_count": 1, "complete": True},
        )
        loaded = metadata.load_event_catalog_partition_checkpoints(conn)
        assert set(loaded) == {"exact_2026_tag:open"}
        assert loaded["exact_2026_tag:open"]["stable_events"]["1"]["slug"] == (
            "fifwc-event-1"
        )
        assert loaded["exact_2026_tag:open"]["scan_summary"]["event_count"] == 1

        metadata.save_event_catalog_partition_checkpoint(
            conn,
            "exact_2026_tag:open",
            {"1": {"id": "1", "slug": "updated"}},
            {"event_count": 1, "complete": False},
        )
        loaded = metadata.load_event_catalog_partition_checkpoints(conn)
        assert loaded["exact_2026_tag:open"]["stable_events"]["1"]["slug"] == "updated"
        assert loaded["exact_2026_tag:open"]["scan_summary"]["complete"] is False

        metadata.clear_event_catalog_partition_checkpoints(conn)
        assert metadata.load_event_catalog_partition_checkpoints(conn) == {}


def test_event_catalog_partition_checkpoint_rejects_blank_key(duck):
    import pytest

    with metadata.get_connection() as conn:
        with pytest.raises(ValueError, match="partition_key"):
            metadata.save_event_catalog_partition_checkpoint(conn, "  ", {}, {})


def test_save_kalshi_metrics_tolerates_missing_history_table(monkeypatch):
    class Conn:
        def execute(self, sql, *_args, **_kwargs):
            if "SELECT history_json" in sql:
                raise RuntimeError("missing")
            return self

    monkeypatch.setattr(metadata, "ensure_duck_db", lambda: None)
    metadata.save_sync_run_metrics(
        "task",
        {"ok": True},
        source="kalshi",
        scope_name="wc2026",
        conn=Conn(),
    )


def test_save_kalshi_metrics_parses_and_repairs_history(monkeypatch):
    class Result:
        def __init__(self, row=None):
            self.row = row

        def fetchone(self):
            return self.row

    class Conn:
        def __init__(self, history):
            self.history = history
            self.inserted = None

        def execute(self, sql, params=None):
            if "SELECT history_json" in sql:
                return Result((self.history,))
            self.inserted = params
            return Result()

    monkeypatch.setattr(metadata, "ensure_duck_db", lambda: None)
    valid = Conn('[{"old": 1}, "drop"]')
    metadata.save_sync_run_metrics(
        "task",
        {"new": 2},
        source="kalshi",
        scope_name="wc2026",
        history_limit=1,
        conn=valid,
    )
    assert json.loads(valid.inserted[-1]) == [
        {"new": 2, "timestamp": valid.inserted[1].isoformat()}
    ]

    corrupt = Conn("{")
    metadata.save_sync_run_metrics(
        "task",
        {"new": 3},
        source="kalshi",
        scope_name="wc2026",
        history_limit=0,
        conn=corrupt,
    )
    assert len(json.loads(corrupt.inserted[-1])) == 1

    non_list = Conn("{}")
    metadata.save_sync_run_metrics(
        "task",
        {"new": 4},
        source="kalshi",
        scope_name="wc2026",
        conn=non_list,
    )
    assert len(json.loads(non_list.inserted[-1])) == 1


def test_event_catalog_checkpoint_load_skips_bad_rows_and_missing_table():
    with duckdb.connect(":memory:") as conn:
        assert metadata.load_event_catalog_partition_checkpoints(conn) == {}
        metadata.clear_event_catalog_partition_checkpoints(conn)

        conn.execute("CREATE SCHEMA polymarket_wc2026_ops")
        conn.execute(
            """
            CREATE TABLE polymarket_wc2026_ops.event_catalog_scan_checkpoint (
                partition_key VARCHAR,
                stable_events_json VARCHAR,
                scan_summary_json VARCHAR
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO polymarket_wc2026_ops.event_catalog_scan_checkpoint
            VALUES (?, ?, ?)
            """,
            [
                ("bad-json", "{", "{}"),
                ("wrong-shape", "[]", "{}"),
                ("valid", '{"event": {"id": "event"}}', '{"complete": true}'),
            ],
        )

        assert metadata.load_event_catalog_partition_checkpoints(conn) == {
            "valid": {
                "stable_events": {"event": {"id": "event"}},
                "scan_summary": {"complete": True},
            }
        }
