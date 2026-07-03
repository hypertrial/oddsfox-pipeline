"""Unit tests for storage/duckdb metadata module."""

from __future__ import annotations

import json
from contextlib import contextmanager

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


def test_append_pipeline_run_event_inserts_row(duck):
    rid = metadata.append_pipeline_run_event("sync_odds", {"rows": 1})
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

    monkeypatch.setattr(metadata, "append_pipeline_run_event", boom)
    metadata.save_sync_run_metrics("append_fail", {"x": 1})
    saved = metadata.get_sync_run_metrics("append_fail")
    assert saved is not None
    assert saved["pipeline_run_event_append_failed"] is True
    assert saved["pipeline_run_event_append_error"] == (
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

    assert metadata.get_backfill_progress("p") == 0
    metadata.set_backfill_progress("p", 3)
    assert metadata.get_backfill_progress("p") == 3

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


def test_get_backfill_progress_invalid(duck):
    metadata._metadata_set("backfill:x:progress", "x")
    assert metadata.get_backfill_progress("x") == 0


def test_get_sync_run_metrics_missing_returns_none(duck):
    assert metadata.get_sync_run_metrics("missing") is None


def test_get_sync_run_metrics_handles_corrupt_table_payloads(duck):
    with metadata.get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO wc2026_polymarket_ops.sync_run_metrics
            (task_name, recorded_at, metrics_json, history_json)
            VALUES ('bad_table_json', CURRENT_TIMESTAMP, '{not-json', '[]')
            """
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO wc2026_polymarket_ops.sync_run_metrics
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
    def connection():
        yield Conn()

    monkeypatch.setattr(metadata, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(metadata, "get_connection", connection)
    monkeypatch.setattr(metadata, "_metadata_get", lambda _key: None)

    assert metadata.get_sync_run_metrics("query_error") is None
