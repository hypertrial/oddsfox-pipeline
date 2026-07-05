"""Unit tests for Polymarket odds sync writer and auto-tune."""

from __future__ import annotations

import importlib
from contextlib import contextmanager
from queue import Empty, Queue
from unittest.mock import MagicMock

import duckdb
import pytest

pytest.importorskip("duckdb")

from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules
from oddsfox_pipeline.ingestion.polymarket.odds import sync as odds_sync
from oddsfox_pipeline.storage.duckdb.connection import (
    polymarket_wc2026_ops_tbl,
    polymarket_wc2026_raw_tbl,
)

_TOD = polymarket_wc2026_raw_tbl("token_odds_daily")
_T_LED = polymarket_wc2026_ops_tbl("token_sync_ledger")


def test_flush_writer_buffers_writes_all_buffer_types(monkeypatch):
    class FakeConn:
        def __init__(self):
            self.executed = []
            self.executemany_calls = []

        def execute(self, sql):
            self.executed.append(sql)

        def executemany(self, sql, rows):
            self.executemany_calls.append((sql, list(rows)))

    fake_conn = FakeConn()
    stats = {
        "saved": 0,
        "saved_daily_rows": 0,
        "sync_rows": 0,
        "full_rows": 0,
        "skip_rows": 0,
    }
    buffers = odds_sync.WriterBuffers(
        odds_map={("tok", 1): 0.1},
        state_buffer=[("tok", 1, None, None, 0, True), ("tok", 2, None, None, 0, True)],
        skip_buffer=[("tok", "a"), ("tok", "b")],
    )
    saved = []
    monkeypatch.setattr(
        odds_sync,
        "save_odds_bulk_upsert",
        lambda rows, conn, assume_deduped: saved.extend(rows),
    )
    monkeypatch.setattr(odds_sync, "refresh_token_odds_daily", lambda keys, conn: None)

    odds_sync._flush_writer_buffers(fake_conn, buffers, stats, 1, force=True)

    assert saved == [("tok", 1, 0.1)]
    assert stats == {
        "saved": 1,
        "saved_daily_rows": 0,
        "sync_rows": 1,
        "full_rows": 1,
        "skip_rows": 1,
    }
    assert len(buffers.dirty_daily_keys) == 1
    assert buffers == odds_sync.WriterBuffers(
        odds_map={},
        state_buffer=[],
        skip_buffer=[],
        dirty_daily_keys=buffers.dirty_daily_keys,
    )
    assert fake_conn.executed[0] == "BEGIN"
    assert fake_conn.executed[-1] == "COMMIT"


def test_refresh_dirty_daily_keys_dedupes_until_final_refresh(monkeypatch):
    class FakeConn:
        def execute(self, sql):
            return None

    stats = {
        "saved": 0,
        "saved_daily_rows": 0,
        "sync_rows": 0,
        "full_rows": 0,
        "skip_rows": 0,
    }
    buffers = odds_sync.WriterBuffers(
        odds_map={("tok", 1): 0.1, ("tok", 2): 0.2},
        state_buffer=[],
        skip_buffer=[],
    )
    monkeypatch.setattr(
        odds_sync,
        "save_odds_bulk_upsert",
        lambda rows, conn, assume_deduped: None,
    )
    odds_sync._flush_writer_buffers(FakeConn(), buffers, stats, 1, force=True)
    buffers.odds_map[("tok", 3)] = 0.3
    odds_sync._flush_writer_buffers(FakeConn(), buffers, stats, 1, force=True)

    refreshed = []
    monkeypatch.setattr(
        odds_sync,
        "refresh_token_odds_daily",
        lambda keys, conn: refreshed.extend(keys),
    )
    odds_sync._refresh_dirty_daily_keys(FakeConn(), buffers, stats)

    assert len(refreshed) == 1
    assert stats["saved_daily_rows"] == 1
    assert buffers.dirty_daily_keys == set()


def test_flush_writer_buffers_sync_branch_keeps_max_timestamp(monkeypatch):
    class FakeConn:
        def execute(self, sql):
            return None

        def executemany(self, sql, rows):
            self.rows = list(rows)

    fake_conn = FakeConn()
    stats = {
        "saved": 0,
        "saved_daily_rows": 0,
        "sync_rows": 0,
        "full_rows": 0,
        "skip_rows": 0,
    }
    buffers = odds_sync.WriterBuffers(
        odds_map={},
        state_buffer=[
            ("tok", 5, None, None, 0, False),
            ("tok", 2, None, None, 0, False),
        ],
        skip_buffer=[],
    )
    monkeypatch.setattr(
        odds_sync, "save_odds_bulk_upsert", lambda *args, **kwargs: None
    )
    odds_sync._flush_writer_buffers(fake_conn, buffers, stats, 1, force=True)
    assert fake_conn.rows == [("tok", 5, None, None, 0, False)]
    assert stats["sync_rows"] == 1


def test_flush_writer_buffers_state_only_without_skips(monkeypatch):
    class FakeConn:
        def execute(self, sql):
            return None

        def executemany(self, sql, rows):
            self.rows = list(rows)

    fake_conn = FakeConn()
    stats = {
        "saved": 0,
        "saved_daily_rows": 0,
        "sync_rows": 0,
        "full_rows": 0,
        "skip_rows": 0,
    }
    buffers = odds_sync.WriterBuffers(
        odds_map={},
        state_buffer=[("tok", 9, None, None, 1, False)],
        skip_buffer=[],
    )
    monkeypatch.setattr(
        odds_sync, "save_odds_bulk_upsert", lambda *args, **kwargs: None
    )
    odds_sync._flush_writer_buffers(fake_conn, buffers, stats, 1, force=True)
    assert fake_conn.rows == [("tok", 9, None, None, 1, False)]
    assert stats["sync_rows"] == 1
    assert stats["skip_rows"] == 0


def test_flush_writer_buffers_skip_only(monkeypatch):
    class FakeConn:
        def execute(self, sql):
            return None

    fake_conn = FakeConn()
    stats = {
        "saved": 0,
        "saved_daily_rows": 0,
        "sync_rows": 0,
        "full_rows": 0,
        "skip_rows": 0,
    }
    buffers = odds_sync.WriterBuffers(
        odds_map={},
        state_buffer=[],
        skip_buffer=[("tok", "why")],
    )
    captured = []
    monkeypatch.setattr(
        odds_sync, "save_odds_bulk_upsert", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        odds_sync,
        "upsert_skipped_tokens_batch",
        lambda rows, conn: captured.extend(rows),
    )
    odds_sync._flush_writer_buffers(fake_conn, buffers, stats, 1, force=True)
    assert captured == [("tok", "why")]
    assert stats["skip_rows"] == 1


def test_flush_writer_buffers_fully_checked_only(monkeypatch):
    class FakeConn:
        def execute(self, sql):
            return None

        def executemany(self, sql, rows):
            self.rows = list(rows)

    fake_conn = FakeConn()
    stats = {
        "saved": 0,
        "saved_daily_rows": 0,
        "sync_rows": 0,
        "full_rows": 0,
        "skip_rows": 0,
    }
    buffers = odds_sync.WriterBuffers(
        odds_map={},
        state_buffer=[("tok", None, None, None, 0, True)],
        skip_buffer=[],
    )
    monkeypatch.setattr(
        odds_sync, "save_odds_bulk_upsert", lambda *args, **kwargs: None
    )
    odds_sync._flush_writer_buffers(fake_conn, buffers, stats, 1, force=True)
    assert fake_conn.rows == [("tok", None, None, None, 0, True)]
    assert stats["full_rows"] == 1


def test_apply_writer_item_drops_non_finite_prices():
    stats = {"invalid_ts_dropped": 0, "invalid_price_dropped": 0, "deduped": 0}
    buffers = odds_sync.WriterBuffers(odds_map={}, state_buffer=[], skip_buffer=[])
    odds_sync._apply_writer_item(
        (
            "odds",
            [
                ("t", 1, float("nan")),
                ("t", 2, float("inf")),
                ("t", 3, float("-inf")),
                ("t", 4, 0.5),
            ],
        ),
        buffers,
        stats,
    )
    assert stats["invalid_price_dropped"] == 3
    assert buffers.odds_map == {("t", 4): 0.5}


def test_apply_writer_item_non_odds_buffers():
    stats = {"invalid_ts_dropped": 0, "invalid_price_dropped": 0, "deduped": 0}
    buffers = odds_sync.WriterBuffers(odds_map={}, state_buffer=[], skip_buffer=[])
    odds_sync._apply_writer_item(
        ("token_state", [("tok", 1, None, None, 0, True)]),
        buffers,
        stats,
    )
    odds_sync._apply_writer_item(("skipped_tokens", [("tok", "why")]), buffers, stats)
    assert buffers.state_buffer == [("tok", 1, None, None, 0, True)]
    assert buffers.skip_buffer == [("tok", "why")]


def test_apply_writer_item_ignores_unknown_op():
    stats = {"invalid_ts_dropped": 0, "invalid_price_dropped": 0, "deduped": 0}
    buffers = odds_sync.WriterBuffers(odds_map={}, state_buffer=[], skip_buffer=[])
    odds_sync._apply_writer_item(("unknown", [("tok", 1)]), buffers, stats)
    assert buffers == odds_sync.WriterBuffers(
        odds_map={}, state_buffer=[], skip_buffer=[]
    )


def test_writer_loop_empty_poll_then_final_flush(monkeypatch):
    class FakeQueue:
        def __init__(self):
            self.calls = 0
            self.maxsize = 10

        def get(self, timeout=None):
            self.calls += 1
            if self.calls == 1:
                raise Empty()
            return None

        def qsize(self):
            return 0

        def task_done(self):
            return None

    class FakeConn:
        pass

    calls = []

    @contextmanager
    def fake_connection():
        yield FakeConn()

    monkeypatch.setattr(odds_sync, "get_connection", fake_connection)
    monkeypatch.setattr(
        odds_sync, "_dynamic_writer_flush_rows", lambda *args, **kwargs: 123
    )
    monkeypatch.setattr(
        odds_sync,
        "_flush_writer_buffers",
        lambda conn, buffers, stats, rows, force=False: calls.append((rows, force)),
    )

    odds_sync._writer_loop(
        FakeQueue(),
        100,
        {
            "saved": 0,
            "deduped": 0,
            "sync_rows": 0,
            "skip_rows": 0,
            "full_rows": 0,
            "invalid_ts_dropped": 0,
            "invalid_price_dropped": 0,
            "queue_high_watermark": 0,
        },
        [],
    )

    assert calls == [(123, False), (100, True)]


def test_writer_loop_marks_fatal_after_empty_flush_failure(monkeypatch):
    class FakeQueue:
        def __init__(self):
            self.items = [("odds", [("tok", 1, 0.1)]), None]
            self.maxsize = 10
            self.task_done_calls = 0
            self.get_calls = 0

        def get(self, timeout=None):
            self.get_calls += 1
            if self.get_calls == 1:
                raise Empty()
            return self.items.pop(0)

        def qsize(self):
            return 1

        def task_done(self):
            self.task_done_calls += 1

    @contextmanager
    def fake_connection():
        yield object()

    monkeypatch.setattr(odds_sync, "get_connection", fake_connection)
    monkeypatch.setattr(
        odds_sync, "_dynamic_writer_flush_rows", lambda *args, **kwargs: 1
    )
    monkeypatch.setattr(
        odds_sync,
        "_flush_writer_buffers",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("flush fail")),
    )
    applied = []
    monkeypatch.setattr(
        odds_sync, "_apply_writer_item", lambda *args, **kwargs: applied.append(args)
    )
    failures = []
    odds_sync._writer_loop(
        FakeQueue(),
        100,
        {
            "saved": 0,
            "deduped": 0,
            "sync_rows": 0,
            "skip_rows": 0,
            "full_rows": 0,
            "invalid_ts_dropped": 0,
            "invalid_price_dropped": 0,
            "queue_high_watermark": 0,
        },
        failures,
    )
    assert failures
    assert applied == []


def test_writer_loop_empty_poll_after_fatal_error_skips_flush(monkeypatch):
    class FakeQueue:
        def __init__(self):
            self.responses = [Empty(), Empty(), None]
            self.maxsize = 10
            self.task_done_calls = 0

        def get(self, timeout=None):
            response = self.responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response

        def qsize(self):
            return 0

        def task_done(self):
            self.task_done_calls += 1

    @contextmanager
    def fake_connection():
        yield object()

    calls = []
    monkeypatch.setattr(odds_sync, "get_connection", fake_connection)
    monkeypatch.setattr(
        odds_sync, "_dynamic_writer_flush_rows", lambda *args, **kwargs: 1
    )

    def bad_flush(*args, **kwargs):
        calls.append(kwargs.get("force", False))
        raise RuntimeError("flush fail")

    monkeypatch.setattr(odds_sync, "_flush_writer_buffers", bad_flush)
    failures = []
    odds_sync._writer_loop(
        FakeQueue(),
        100,
        {
            "saved": 0,
            "deduped": 0,
            "sync_rows": 0,
            "skip_rows": 0,
            "full_rows": 0,
            "invalid_ts_dropped": 0,
            "invalid_price_dropped": 0,
            "queue_high_watermark": 0,
        },
        failures,
    )
    assert len(failures) == 1
    assert calls == [False]


def test_maybe_auto_tune_rate_zero_and_small_delta():
    class ZeroLimiter:
        def get_rate(self):
            raise RuntimeError("bad")

    odds_sync._maybe_auto_tune_rps(
        limiter=ZeroLimiter(),
        runtime_status={"total": 100, "429": 0, "error": 0},
        tune_state={"last_total": 0, "last_429": 0, "last_error": 0},
        window_requests=1,
        threshold_429=0.0,
        threshold_error=0.0,
        min_rps=1,
        max_rps=10,
    )

    class SmallLimiter:
        def __init__(self):
            self.rate = 1.0
            self.set_calls = 0

        def get_rate(self):
            return self.rate

        def set_rate(self, value):
            self.set_calls += 1
            self.rate = value

    limiter = SmallLimiter()
    odds_sync._maybe_auto_tune_rps(
        limiter=limiter,
        runtime_status={"total": 100, "429": 0, "error": 0},
        tune_state={"last_total": 0, "last_429": 0, "last_error": 0},
        window_requests=1,
        threshold_429=0.99,
        threshold_error=0.99,
        min_rps=1,
        max_rps=2,
    )
    assert limiter.set_calls == 0


def test_maybe_auto_tune_increase_sets_rate():
    class Limiter:
        def __init__(self):
            self.rate = 10.0

        def get_rate(self):
            return self.rate

        def set_rate(self, value):
            self.rate = value

    limiter = Limiter()
    odds_sync._maybe_auto_tune_rps(
        limiter=limiter,
        runtime_status={"total": 100, "429": 0, "error": 0},
        tune_state={"last_total": 0, "last_429": 0, "last_error": 0},
        window_requests=1,
        threshold_429=1.0,
        threshold_error=1.0,
        min_rps=1,
        max_rps=50,
    )
    assert limiter.rate == 11.0


def test_maybe_auto_tune_delta_429_nonzero_keeps_rate(monkeypatch):
    class Limiter:
        def __init__(self):
            self.rate = 10.0
            self.set_calls = 0

        def get_rate(self):
            return self.rate

        def set_rate(self, value):
            self.set_calls += 1
            self.rate = value

    limiter = Limiter()
    odds_sync._maybe_auto_tune_rps(
        limiter=limiter,
        runtime_status={"total": 100, "429": 1, "error": 0},
        tune_state={"last_total": 0, "last_429": 0, "last_error": 0},
        window_requests=1,
        threshold_429=1.0,
        threshold_error=1.0,
        min_rps=1,
        max_rps=50,
    )
    assert limiter.set_calls == 0


def test_dynamic_writer_flush_rows():
    q = Queue(maxsize=10)
    for i in range(9):
        q.put(i)
    assert odds_sync._dynamic_writer_flush_rows(4000, q) < 4000


def test_dynamic_writer_flush_rows_no_maxsize():
    q = Queue()
    assert odds_sync._dynamic_writer_flush_rows(2000, q) == 2000


def test_writer_buffers_apply_and_flush(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "w.duckdb"))
    import oddsfox_pipeline.storage.duckdb.connection as connection

    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    buf = odds_sync.WriterBuffers(odds_map={}, state_buffer=[], skip_buffer=[])
    st = {
        "saved": 0,
        "saved_daily_rows": 0,
        "sync_rows": 0,
        "skip_rows": 0,
        "full_rows": 0,
        "deduped": 0,
        "invalid_ts_dropped": 0,
        "invalid_price_dropped": 0,
    }
    odds_sync._apply_writer_item(
        ("odds", [("t", 1, 0.5), ("t", 0, 0.5), ("t", 2, 2.0)]), buf, st
    )
    odds_sync._apply_writer_item(
        ("token_state", [("t", 1, None, None, 0, True)]),
        buf,
        st,
    )
    odds_sync._apply_writer_item(("skipped_tokens", [("t", "r")]), buf, st)

    with connection.get_connection() as conn:
        odds_sync._flush_writer_buffers(conn, buf, st, 1, force=True)
        odds_sync._refresh_dirty_daily_keys(conn, buf, st)
        daily_rows = conn.execute(f"select count(*) from {_TOD}").fetchone()[0]
    assert daily_rows == 1


def test_flush_writer_preserves_fully_checked_on_cursor_update(monkeypatch, tmp_path):
    """Cursor-only flushes must not clear fully_checked (operational upsert, not row replace)."""
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "fc.duckdb"))
    import oddsfox_pipeline.storage.duckdb.connection as connection

    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    tid = "j" * 33 + "12"
    with connection.get_connection() as conn:
        conn.execute(
            f"INSERT INTO {_T_LED} (clobTokenId, last_sync_timestamp, fully_checked) VALUES (?, 10, TRUE)",
            [tid],
        )

    buf = odds_sync.WriterBuffers(
        odds_map={},
        state_buffer=[(tid, 99, None, None, 0, False)],
        skip_buffer=[],
    )
    st = {
        "saved": 0,
        "saved_daily_rows": 0,
        "sync_rows": 0,
        "skip_rows": 0,
        "full_rows": 0,
    }
    with connection.get_connection() as conn:
        odds_sync._flush_writer_buffers(conn, buf, st, 1, force=True)

    with connection.get_connection() as conn:
        row = conn.execute(
            f"SELECT last_sync_timestamp, fully_checked FROM {_T_LED} WHERE clobTokenId = ?",
            [tid],
        ).fetchone()
    assert row[0] == 99
    assert row[1] is True


def test_dynamic_writer_flush_rows_qsize_exception():
    class BadQueue:
        maxsize = 100

        def qsize(self):
            raise RuntimeError("no size")

    assert odds_sync._dynamic_writer_flush_rows(2000, BadQueue()) == 2000


def test_dynamic_writer_flush_rows_utilization_branches():
    q = Queue(maxsize=20)
    for _ in range(16):  # 16/20 = 0.8
        q.put(1)
    out_high = odds_sync._dynamic_writer_flush_rows(8000, q)
    assert out_high == max(1000, 8000 // 4)

    q2 = Queue(maxsize=20)
    for _ in range(11):  # 11/20 = 0.55
        q2.put(1)
    out_mid = odds_sync._dynamic_writer_flush_rows(8000, q2)
    assert out_mid == max(1000, 8000 // 2)

    q3 = Queue(maxsize=100)
    for _ in range(5):  # 5/100 = 0.05
        q3.put(1)
    out_low = odds_sync._dynamic_writer_flush_rows(5000, q3)
    assert out_low == min(odds_sync.MAX_FLUSH_ROWS_CAP, 5000 * 2)


def test_maybe_auto_tune_rps_increase_and_getattr_rate(monkeypatch):
    class Lim:
        rate = 10.0

        def get_rate(self):
            return self.rate

        def set_rate(self, r):
            self.rate = r

    lim = Lim()
    st = {"last_total": 0, "last_429": 0, "last_error": 0}
    odds_sync._maybe_auto_tune_rps(
        limiter=lim,
        runtime_status={"total": 500, "429": 0, "error": 0},
        tune_state=st,
        window_requests=1,
        threshold_429=0.99,
        threshold_error=0.99,
        min_rps=1,
        max_rps=50,
    )


def test_maybe_auto_tune_get_rate_exception_uses_attr():
    class Lim:
        rate = 5.0

        def get_rate(self):
            raise RuntimeError("x")

        def set_rate(self, r):
            self.rate = r

    lim = Lim()
    odds_sync._maybe_auto_tune_rps(
        limiter=lim,
        runtime_status={"total": 50, "429": 10, "error": 0},
        tune_state={"last_total": 0, "last_429": 0, "last_error": 0},
        window_requests=1,
        threshold_429=0.0,
        threshold_error=0.99,
        min_rps=1,
        max_rps=20,
    )


def test_writer_loop_fatal_flush_and_final_error(monkeypatch, tmp_path):
    q: Queue = Queue()
    stats = {
        "saved": 0,
        "deduped": 0,
        "sync_rows": 0,
        "skip_rows": 0,
        "full_rows": 0,
        "invalid_ts_dropped": 0,
        "invalid_price_dropped": 0,
        "queue_high_watermark": 0,
    }
    fails: list = []

    class BadConn:
        def execute(self, *a, **k):
            raise RuntimeError("flush")

        def executemany(self, *a, **k):
            raise RuntimeError("x")

    @contextmanager
    def bad_gc():
        yield BadConn()

    monkeypatch.setattr(odds_sync, "get_connection", bad_gc)
    q.put(("odds", [("t" * 35, 1, 0.5)]))
    q.put(None)
    odds_sync._writer_loop(q, 1, stats, fails)
    assert fails


def test_flush_writer_buffers_early_exits():
    buf = odds_sync.WriterBuffers(odds_map={}, state_buffer=[], skip_buffer=[])
    st = {
        k: 0
        for k in (
            "saved",
            "deduped",
            "sync_rows",
            "skip_rows",
            "full_rows",
            "invalid_ts_dropped",
            "invalid_price_dropped",
        )
    }
    odds_sync._flush_writer_buffers(MagicMock(), buf, st, 1000, force=False)


def test_dynamic_writer_flush_rows_default_branch():
    q = Queue(maxsize=20)
    for _ in range(8):
        q.put(1)
    assert odds_sync._dynamic_writer_flush_rows(3000, q) == 3000


def test_flush_writer_buffers_empty_buffers_noop():
    buf = odds_sync.WriterBuffers(odds_map={}, state_buffer=[], skip_buffer=[])
    odds_sync._flush_writer_buffers(
        MagicMock(),
        buf,
        {
            "saved": 0,
            "saved_daily_rows": 0,
            "sync_rows": 0,
            "full_rows": 0,
            "skip_rows": 0,
        },
        100,
        force=False,
    )


def test_flush_writer_buffers_merge_error_rolls_back(monkeypatch):
    buf = odds_sync.WriterBuffers(
        odds_map={("t", 1): 0.5},
        state_buffer=[],
        skip_buffer=[],
    )
    bad = MagicMock()
    bad.execute.return_value = None
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.odds.writer.prepare_odds_bulk_upsert",
        lambda *a, **k: "stage",
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.odds.writer.merge_odds_bulk_upsert",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("fail")),
    )
    with pytest.raises(RuntimeError):
        odds_sync._flush_writer_buffers(
            bad,
            buf,
            {
                "saved": 0,
                "saved_daily_rows": 0,
                "sync_rows": 0,
                "full_rows": 0,
                "skip_rows": 0,
            },
            1,
            force=True,
        )
    assert "ROLLBACK" in [call.args[0] for call in bad.execute.call_args_list]


def test_apply_writer_invalid_ts_price_dedupe():
    st = {"invalid_ts_dropped": 0, "invalid_price_dropped": 0, "deduped": 0}
    buf = odds_sync.WriterBuffers(odds_map={}, state_buffer=[], skip_buffer=[])
    odds_sync._apply_writer_item(
        ("odds", [("t", 0, 0.5), ("t", 5, -1), ("t", 5, 1.5), ("t", 5, 0.5)]), buf, st
    )
    odds_sync._apply_writer_item(("odds", [("t", 5, 0.5)]), buf, st)
    assert st["invalid_ts_dropped"] >= 1
    assert st["invalid_price_dropped"] >= 2
    assert st["deduped"] >= 1


def test_maybe_auto_tune_rps_branches():
    class Limiter:
        def __init__(self):
            self.rate = 10.0

        def get_rate(self):
            return self.rate

        def set_rate(self, r):
            self.rate = r

    lim = Limiter()
    tune = {"last_total": 0, "last_429": 0, "last_error": 0}
    odds_sync._maybe_auto_tune_rps(
        limiter=None,
        runtime_status={"total": 0},
        tune_state=tune,
        window_requests=10,
        threshold_429=0.01,
        threshold_error=0.01,
        min_rps=1,
        max_rps=20,
    )
    odds_sync._maybe_auto_tune_rps(
        limiter=lim,
        runtime_status={"total": 5},
        tune_state=tune,
        window_requests=200,
        threshold_429=0.01,
        threshold_error=0.01,
        min_rps=1,
        max_rps=20,
    )
    odds_sync._maybe_auto_tune_rps(
        limiter=lim,
        runtime_status={"total": 250, "429": 50, "error": 0},
        tune_state={"last_total": 0, "last_429": 0, "last_error": 0},
        window_requests=50,
        threshold_429=0.01,
        threshold_error=0.01,
        min_rps=1,
        max_rps=20,
    )
    odds_sync._maybe_auto_tune_rps(
        limiter=lim,
        runtime_status={"total": 500, "429": 0, "error": 40},
        tune_state={"last_total": 250, "last_429": 50, "last_error": 0},
        window_requests=50,
        threshold_429=0.5,
        threshold_error=0.01,
        min_rps=1,
        max_rps=20,
    )
    odds_sync._maybe_auto_tune_rps(
        limiter=lim,
        runtime_status={"total": 800, "429": 50, "error": 40},
        tune_state={"last_total": 500, "last_429": 50, "last_error": 40},
        window_requests=50,
        threshold_429=0.99,
        threshold_error=0.99,
        min_rps=1,
        max_rps=100,
    )


def test_maybe_auto_tune_get_rate_fallback():
    class Lim:
        rate = 5.0

    tune = {"last_total": 0, "last_429": 0, "last_error": 0}
    odds_sync._maybe_auto_tune_rps(
        limiter=Lim(),
        runtime_status={"total": 300, "429": 0, "error": 0},
        tune_state=tune,
        window_requests=50,
        threshold_429=0.99,
        threshold_error=0.99,
        min_rps=1,
        max_rps=50,
    )


def test_writer_loop_fatal_flush_and_final(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "wl.duckdb"))

    reload_all_settings_modules()
    import oddsfox_pipeline.storage.duckdb.connection as conn

    conn.reset_duckdb_connection_state()
    importlib.reload(conn)
    conn.ensure_duck_db()

    q: Queue = Queue()

    def bad_flush(*a, **k):
        raise RuntimeError("flush")

    monkeypatch.setattr(odds_sync, "_dynamic_writer_flush_rows", lambda *a, **k: 1)
    monkeypatch.setattr(odds_sync, "_flush_writer_buffers", bad_flush)
    failures: list = []
    stats = {
        "saved": 0,
        "sync_rows": 0,
        "full_rows": 0,
        "skip_rows": 0,
        "deduped": 0,
        "invalid_ts_dropped": 0,
        "invalid_price_dropped": 0,
        "queue_high_watermark": 0,
    }
    t = odds_sync.Thread(
        target=odds_sync._writer_loop,
        args=(q, 100, stats, failures),
    )
    t.start()
    q.put(("odds", [("t", 1, 0.5)]))
    q.put(None)
    t.join(timeout=5)
    assert failures


def test_save_odds_bulk_appender_with_appender(monkeypatch, tmp_path):
    from oddsfox_pipeline.storage.duckdb import odds as odds_mod

    if not hasattr(duckdb, "Appender"):
        pytest.skip("DuckDB Appender not available")
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "app.duckdb"))

    reload_all_settings_modules()
    import oddsfox_pipeline.storage.duckdb.connection as conn

    conn.reset_duckdb_connection_state()
    importlib.reload(conn)
    conn.ensure_duck_db()
    with odds_mod.get_connection() as c:
        odds_mod.save_odds_bulk_appender([("app", 3, 0.4)], c)


def test_save_odds_bulk_upsert_appender_staging(monkeypatch, tmp_path):
    from oddsfox_pipeline.storage.duckdb import odds as odds_mod

    if not hasattr(duckdb, "Appender"):
        pytest.skip("Appender required")
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "stg.duckdb"))

    reload_all_settings_modules()
    import oddsfox_pipeline.storage.duckdb.connection as conn

    conn.reset_duckdb_connection_state()
    importlib.reload(conn)
    conn.ensure_duck_db()
    with odds_mod.get_connection() as c:
        odds_mod.save_odds_bulk_upsert([("stg", 9, 0.7)] * 3, c, assume_deduped=False)
