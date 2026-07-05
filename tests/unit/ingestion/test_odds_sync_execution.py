"""Odds sync main-loop tests with executor and writer fakes."""

from __future__ import annotations

import importlib
from queue import Queue
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("duckdb")

from tests.support.odds_sync_harness import (
    FakeClock,
    ImmediatePoolNoShutdown,
    NeverDonePool,
    NoShutdownNeverDonePool,
    NoThread,
    immediate_executor,
    make_runtime,
    make_token_plan,
    patch_guardrail_clock,
    raw_snapshot,
)

from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules
from oddsfox_pipeline.ingestion.polymarket.odds import sync as odds_sync
from oddsfox_pipeline.storage.duckdb.connection import polymarket_wc2026_raw_tbl

_T_OH = polymarket_wc2026_raw_tbl("odds_history")


def _plan():
    return make_token_plan()


def test_sync_odds_runs_main_loop(monkeypatch):
    plans = [_plan(), _plan()]

    def plan_iter(**kwargs):
        for p in plans:
            yield p

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "snapshot_raw_layer", raw_snapshot)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)

    def fake_sync_plan(plan, *a, **k):
        return {
            "rows": 0,
            "windows": 1,
            "empty": True,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        }

    monkeypatch.setattr(odds_sync, "_sync_token_plan", fake_sync_plan)

    monkeypatch.setattr(odds_sync, "Thread", NoThread)

    def fake_writer_loop(*a, **k):
        return None

    monkeypatch.setattr(odds_sync, "_writer_loop", fake_writer_loop)
    # Fake writer thread never drains the queue; real sync_odds calls join() which
    # would deadlock without task_done — skip join when testing the executor path.
    monkeypatch.setattr(Queue, "join", lambda self: None)

    with immediate_executor():
        odds_sync.sync_odds(
            max_workers=2,
            batch_size=1000,
            writer_flush_rows=1000,
            market_page_size=100,
            persist_run_metrics=True,
            auto_tune_rps=False,
            requests_per_second=5,
        )


def test_sync_odds_pool_without_shutdown_completes(monkeypatch):
    plans = [_plan()]

    def plan_iter(**kwargs):
        del kwargs
        for p in plans:
            yield p

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "snapshot_raw_layer", raw_snapshot)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(
        odds_sync,
        "_sync_token_plan",
        lambda *a, **k: {
            "rows": 0,
            "windows": 1,
            "empty": True,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        },
    )
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)
    monkeypatch.setattr(Queue, "join", lambda self: None)

    with patch.object(odds_sync, "ThreadPoolExecutor", ImmediatePoolNoShutdown):
        summary = odds_sync.sync_odds(
            max_workers=1,
            auto_tune_rps=False,
            persist_run_metrics=False,
        )

    assert summary["aborted"] is False


def test_writer_loop_smoke(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "wl.duckdb"))
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection

    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    q = Queue()
    q.put(None)
    stats = {
        "saved": 0,
        "saved_daily_rows": 0,
        "deduped": 0,
        "sync_rows": 0,
        "skip_rows": 0,
        "full_rows": 0,
        "invalid_ts_dropped": 0,
        "invalid_price_dropped": 0,
        "queue_high_watermark": 0,
    }
    odds_sync._writer_loop(q, 1000, stats, [])


def test_flush_writer_buffers_rollback(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "rb.duckdb"))
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection

    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    buf = odds_sync.WriterBuffers(
        odds_map={("t", 1): 0.5},
        state_buffer=[("t", 1, None, None, 0, False)],
        skip_buffer=[],
    )
    st = {
        "saved": 0,
        "saved_daily_rows": 0,
        "deduped": 0,
        "sync_rows": 0,
        "skip_rows": 0,
        "full_rows": 0,
        "invalid_ts_dropped": 0,
        "invalid_price_dropped": 0,
    }

    class BadConn:
        def execute(self, *a, **k):
            raise RuntimeError("rollback path")

        def executemany(self, *a, **k):
            raise RuntimeError("x")

    with pytest.raises(RuntimeError):
        odds_sync._flush_writer_buffers(BadConn(), buf, st, 1, force=True)


def test_flush_writer_buffers_atomic_rollback_odds_and_state(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "atomic.duckdb"))
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection

    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    token_id = "k" * 33 + "12"
    buf = odds_sync.WriterBuffers(
        odds_map={(token_id, 1): 0.5},
        state_buffer=[(token_id, 1, None, None, 0, False)],
        skip_buffer=[],
    )
    st = {
        "saved": 0,
        "saved_daily_rows": 0,
        "deduped": 0,
        "sync_rows": 0,
        "skip_rows": 0,
        "full_rows": 0,
        "invalid_ts_dropped": 0,
        "invalid_price_dropped": 0,
    }

    def fail_state_upsert(_rows, _conn):
        raise RuntimeError("state upsert failed")

    def simple_odds_upsert(records, conn, assume_deduped=False):
        del assume_deduped
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {_T_OH}
            (clobTokenId, timestamp, price, ingested_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [(token_id, ts, price) for token_id, ts, price in records],
        )

    with connection.get_connection() as conn:
        with pytest.raises(RuntimeError, match="state upsert failed"):
            odds_sync._flush_writer_buffers(
                conn,
                buf,
                st,
                1,
                force=True,
                save_odds_bulk_upsert_fn=simple_odds_upsert,
                upsert_token_sync_state_batch_fn=fail_state_upsert,
            )

    with connection.get_connection() as conn:
        odds_count = conn.execute(f"SELECT count(*) FROM {_T_OH}").fetchone()[0]
    assert odds_count == 0
    assert st["sync_rows"] == 0


def test_sync_odds_no_progress_hard_timeout_aborts_and_persists_metrics(monkeypatch):
    plan = _plan()
    captured_metrics = {}
    clock = FakeClock()

    def plan_iter(**kwargs):
        del kwargs
        yield plan
        return (odds_sync.PlanningState(plans=1), {})

    def fake_wait(*args, **kwargs):
        del args, kwargs
        clock.advance(1.1)
        return set(), set()

    patch_guardrail_clock(monkeypatch, clock)
    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "snapshot_raw_layer", raw_snapshot)
    monkeypatch.setattr(odds_sync.time, "monotonic", clock)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(
        Queue,
        "put_nowait",
        lambda self, item: (_ for _ in ()).throw(RuntimeError("queue full")),
    )
    monkeypatch.setattr(odds_sync, "ThreadPoolExecutor", NeverDonePool)
    monkeypatch.setattr(odds_sync, "wait", fake_wait)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(
        odds_sync,
        "save_sync_run_metrics",
        lambda task, metrics: captured_metrics.update(metrics),
    )

    with pytest.raises(odds_sync.NoProgressTimeoutError):
        odds_sync.sync_odds(
            max_workers=1,
            auto_tune_rps=False,
            persist_run_metrics=True,
            progress_poll_seconds=1,
            no_progress_soft_timeout_seconds=None,
            no_progress_hard_timeout_seconds=1,
            client_factory=lambda **_kwargs: object(),
            rate_limiter_factory=lambda r: None,
        )

    assert captured_metrics.get("aborted") is True
    assert captured_metrics.get("abort_reason")
    assert captured_metrics.get("max_idle_seconds", 0) >= 1
    assert "soft_warning_count" in captured_metrics


def test_sync_odds_no_progress_timeout_without_shutdown_method(monkeypatch):
    plan = _plan()
    clock = FakeClock()

    def plan_iter(**kwargs):
        del kwargs
        yield plan
        return (odds_sync.PlanningState(plans=1), {})

    def fake_wait(*args, **kwargs):
        del args, kwargs
        clock.advance(1.1)
        return set(), set()

    patch_guardrail_clock(monkeypatch, clock)
    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "snapshot_raw_layer", raw_snapshot)
    monkeypatch.setattr(odds_sync.time, "monotonic", clock)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(odds_sync, "ThreadPoolExecutor", NoShutdownNeverDonePool)
    monkeypatch.setattr(odds_sync, "wait", fake_wait)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)

    with pytest.raises(odds_sync.NoProgressTimeoutError):
        odds_sync.sync_odds(
            max_workers=1,
            auto_tune_rps=False,
            persist_run_metrics=True,
            progress_poll_seconds=1,
            no_progress_soft_timeout_seconds=None,
            no_progress_hard_timeout_seconds=1,
            client_factory=lambda **_kwargs: object(),
            rate_limiter_factory=lambda r: None,
        )


def test_sync_odds_timeout_transient_finishes_without_abort(monkeypatch):
    plan = _plan()

    def plan_iter(**kwargs):
        del kwargs
        yield plan
        return (odds_sync.PlanningState(plans=1), {})

    def fake_sync_plan(*args, **kwargs):
        del args, kwargs
        return {
            "rows": 0,
            "windows": 1,
            "empty": True,
            "error": 1,
            "permanent_error": 0,
            "fully_checked": False,
        }

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "snapshot_raw_layer", raw_snapshot)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "_sync_token_plan", fake_sync_plan)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)

    with immediate_executor():
        summary = odds_sync.sync_odds(
            max_workers=1,
            auto_tune_rps=False,
            persist_run_metrics=True,
            progress_poll_seconds=1,
            client_factory=lambda **_kwargs: object(),
            rate_limiter_factory=lambda r: None,
        )

    assert summary["aborted"] is False
    assert summary["totals"]["processed_tokens"] == 1
    assert summary["totals"]["error"] == 1
    assert summary["totals"]["permanent_error"] == 0


def test_sync_odds_waiting_heartbeat_includes_oldest_inflight(monkeypatch):
    plan = _plan()
    seen_payloads = []
    wait_state = {"calls": 0}
    clock = FakeClock()

    def plan_iter(**kwargs):
        del kwargs
        yield plan
        return (odds_sync.PlanningState(plans=1), {})

    def fake_wait(futures, return_when=None, timeout=None):
        del return_when, timeout
        wait_state["calls"] += 1
        if wait_state["calls"] == 1:
            clock.advance(1.1)
            return set(), set()
        return set(futures), set()

    def fake_sync_plan(*args, **kwargs):
        del args, kwargs
        return {
            "rows": 3,
            "windows": 1,
            "empty": False,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        }

    patch_guardrail_clock(monkeypatch, clock)
    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "snapshot_raw_layer", raw_snapshot)
    monkeypatch.setattr(odds_sync.time, "monotonic", clock)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "_sync_token_plan", fake_sync_plan)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(odds_sync, "wait", fake_wait)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)

    with immediate_executor():
        odds_sync.sync_odds(
            max_workers=1,
            auto_tune_rps=False,
            persist_run_metrics=True,
            progress_poll_seconds=1,
            progress_log_interval_seconds=1,
            progress_callback=lambda phase, payload: seen_payloads.append(
                (phase, payload)
            ),
            client_factory=lambda **_kwargs: object(),
            rate_limiter_factory=lambda r: None,
        )

    heartbeat_payloads = [
        payload
        for phase, payload in seen_payloads
        if phase == "guardrail" and payload.get("phase") == "waiting_for_token_futures"
    ]
    assert heartbeat_payloads
    diagnostics = heartbeat_payloads[0]["diagnostics"]
    assert diagnostics["oldest_inflight_seconds"] >= 1.0
    assert diagnostics["oldest_inflight"][0]["market_id"] == "m"
    assert diagnostics["oldest_inflight"][0]["token_id_prefix"] == plan.token_id[:24]


def test_build_inflight_future_diagnostics_empty():
    diagnostics = odds_sync._build_inflight_future_diagnostics({})
    assert diagnostics == {
        "oldest_inflight_seconds": 0.0,
        "oldest_inflight": [],
    }


def _run_sync_odds_with_tqdm_capture(monkeypatch, *, is_tty: bool) -> list[dict]:
    from oddsfox_pipeline.ingestion.polymarket.odds.engine import pool as pool_mod

    monkeypatch.setattr(pool_mod.sys.stderr, "isatty", lambda: is_tty)
    kwargs_seen: list[dict] = []

    def fake_tqdm(*args, **kwargs):
        del args
        kwargs_seen.append(kwargs)
        from unittest.mock import MagicMock

        return MagicMock(
            __enter__=lambda s: s,
            __exit__=lambda *x: None,
            update=lambda *a, **k: None,
            set_postfix=lambda *a, **k: None,
        )

    monkeypatch.setattr(odds_sync, "tqdm", fake_tqdm)

    plans = [_plan()]

    def plan_iter(**kwargs):
        del kwargs
        for plan in plans:
            yield plan

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "snapshot_raw_layer", raw_snapshot)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(
        odds_sync,
        "_sync_token_plan",
        lambda *a, **k: {
            "rows": 0,
            "windows": 1,
            "empty": True,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        },
    )
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)
    monkeypatch.setattr(Queue, "join", lambda self: None)

    with immediate_executor():
        odds_sync.sync_odds(
            max_workers=1,
            auto_tune_rps=False,
            persist_run_metrics=False,
        )

    return kwargs_seen


@pytest.mark.parametrize("is_tty,expected_disable", [(False, True), (True, False)])
def test_sync_odds_tqdm_disable_follows_stderr_tty(
    monkeypatch, is_tty, expected_disable
):
    kwargs_seen = _run_sync_odds_with_tqdm_capture(monkeypatch, is_tty=is_tty)
    assert kwargs_seen
    assert kwargs_seen[0]["disable"] is expected_disable


def test_parse_created_at_variants():
    from datetime import datetime, timezone

    assert odds_sync._parse_created_at(None) is None
    assert (
        odds_sync._parse_created_at(datetime(2020, 1, 1, tzinfo=timezone.utc))
        is not None
    )
    assert odds_sync._parse_created_at("2020-01-01T12:30:45.123Z") is not None
    assert odds_sync._parse_created_at("2020-01-01 12:30:45") is not None


def test_iter_windows_and_rate_factory():
    assert list(odds_sync.iter_windows(0, 100, 40))[:2]
    assert odds_sync._default_rate_limiter_factory(None) is None
    assert odds_sync._default_rate_limiter_factory(5) is not None


def test_reconcile_odds_ledger_mocked(monkeypatch):
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.odds.engine.ledger.ensure_duck_db",
        lambda: None,
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.odds.engine.ledger.snapshot_raw_layer",
        lambda: {"markets_rows": 1},
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.odds.engine.ledger.reconcile_token_sync_ledger_from_history",
        lambda: {"scanned_tokens": 1, "repaired_tokens": 0},
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.odds.engine.ledger.save_sync_run_metrics",
        lambda *a, **k: None,
    )
    odds_sync.reconcile_odds_ledger(persist_run_metrics=True)
    odds_sync.reconcile_odds_ledger(persist_run_metrics=False)


def test_init_db(monkeypatch):
    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    odds_sync.init_db()


def test_sync_odds_no_tokens_path(monkeypatch):
    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", lambda **kw: iter(()))
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    odds_sync.sync_odds(max_workers=1, market_page_size=100, persist_run_metrics=True)


def test_sync_odds_persists_planning_context(monkeypatch):
    captured = {}

    def empty_plans():
        if False:
            yield None
        return (odds_sync.PlanningState(plans=0), {"bad": "reason"})

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        odds_sync,
        "snapshot_raw_layer",
        lambda: {
            "market_tokens_distinct_tokens": 4,
            "odds_history_distinct_tokens": 3,
            "token_odds_daily_distinct_tokens": 2,
            "ledger_distinct_tokens": 3,
            "ledger_fully_checked_tokens": 1,
            "token_sync_skips_distinct_tokens": 0,
            "market_tokens_without_history": 1,
            "history_tokens_without_market_tokens": 0,
            "token_sync_skips_by_reason": {},
        },
    )
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", lambda **kw: empty_plans())
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(
        odds_sync,
        "save_sync_run_metrics",
        lambda task, metrics, **kwargs: captured.update(
            {"task": task, "metrics": metrics}
        ),
    )
    result = odds_sync.sync_odds(
        max_workers=1, market_page_size=100, persist_run_metrics=True
    )
    assert result["planning_context"]["market_tokens_distinct_tokens"] == 4
    assert captured["metrics"]["planning_context"]["planned_tokens"] == 0
    assert captured["metrics"]["invalid_tokens"] == 1


def test_sync_odds_rejects_fidelity_below_one():
    with pytest.raises(ValueError, match="at least 1"):
        odds_sync.sync_odds(max_workers=1, fidelity=0, persist_run_metrics=False)


def test_sync_odds_rejects_unknown_keyword():
    with pytest.raises(TypeError, match="unexpected keyword"):
        odds_sync.sync_odds(max_workers=1, unknown_option=object())


def test_sync_odds_runtime_instances_do_not_mutate_shared_modules():
    from oddsfox_pipeline.ingestion.polymarket.odds import planning

    original_iter_due = planning.iter_due_market_tokens
    seen: list[str] = []

    def runtime_for(label: str):
        return make_runtime(
            ensure_duck_db=lambda: seen.append(f"{label}:ensure"),
            snapshot_raw_layer=lambda: {"label": label},
            iter_due_market_tokens=lambda **_kwargs: iter(()),
            iter_markets_with_tokens=lambda **_kwargs: iter(()),
            count_due_market_token_exclusions=lambda **_kwargs: {
                "scope_skip": 0,
                "ended_market_skip": 0,
            },
            save_sync_run_metrics=lambda *args, **_kwargs: seen.append(
                f"{label}:metrics:{args[0]}"
            ),
        )

    odds_sync.sync_odds(max_workers=1, runtime=runtime_for("a"))
    odds_sync.sync_odds(max_workers=1, runtime=runtime_for("b"))

    assert seen == [
        "a:ensure",
        "a:metrics:sync_odds",
        "b:ensure",
        "b:metrics:sync_odds",
    ]
    assert planning.iter_due_market_tokens is original_iter_due


def test_sync_odds_accepts_explicit_plan_iterator_with_runtime():
    seen: list[str] = []
    runtime = make_runtime(
        ensure_duck_db=lambda: seen.append("ensure"),
        snapshot_raw_layer=lambda: {"ok": True},
        save_sync_run_metrics=lambda *args, **_kwargs: seen.append(args[0]),
    )

    def empty_plan_iterator(**_kwargs):
        if False:
            yield None
        return (odds_sync.PlanningState(plans=0), {})

    result = odds_sync.sync_odds(
        max_workers=1,
        runtime=runtime,
        plan_iterator_factory=empty_plan_iterator,
    )

    assert result["noop"] is True
    assert seen == ["ensure", "sync_odds"]


def test_odds_sync_all_excludes_private_helpers():
    assert odds_sync.__all__ == [
        "EngineRuntime",
        "ExecutionRuntime",
        "OddsSyncOptions",
        "OddsSyncRuntime",
        "PlanningRuntime",
        "WriterRuntime",
        "default_odds_sync_runtime",
        "init_db",
        "reconcile_odds_ledger",
        "sync_odds",
    ]


def test_odds_sync_runtime_flat_property_accessors():
    from dataclasses import replace

    runtime = make_runtime()
    flat_to_nested = {
        "fetch_window_with_auto_split_impl": runtime.execution.fetch_window_with_auto_split_impl,
        "fetch_token_history_with_retry": runtime.execution.fetch_token_history_with_retry,
        "default_rate_limiter_factory": runtime.execution.default_rate_limiter_factory,
        "sync_token_plan": runtime.execution.sync_token_plan,
        "count_due_market_token_exclusions": (
            runtime.planning.count_due_market_token_exclusions
        ),
        "get_connection": runtime.writer.get_connection,
        "refresh_token_odds_daily": runtime.writer.refresh_token_odds_daily,
        "save_odds_bulk_upsert": runtime.writer.save_odds_bulk_upsert,
        "upsert_skipped_tokens_batch": runtime.writer.upsert_skipped_tokens_batch,
        "upsert_token_sync_state_batch": runtime.writer.upsert_token_sync_state_batch,
        "dynamic_writer_flush_rows": runtime.writer.dynamic_writer_flush_rows,
        "flush_writer_buffers": runtime.writer.flush_writer_buffers,
        "apply_writer_item": runtime.writer.apply_writer_item,
        "writer_loop": runtime.writer.writer_loop,
        "reconcile_token_sync_ledger_from_history": (
            runtime.engine.reconcile_token_sync_ledger_from_history
        ),
    }
    for name, nested_value in flat_to_nested.items():
        assert getattr(runtime, name) is nested_value

    sentinel = object()
    updated = replace(
        runtime,
        execution=replace(runtime.execution, sync_token_plan=sentinel),
    )
    assert updated.execution.sync_token_plan is sentinel
    updated = replace(runtime, writer=replace(runtime.writer, writer_loop=sentinel))
    assert updated.writer.writer_loop is sentinel


def test_sync_odds_worker_cap_warning(monkeypatch):
    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        odds_sync,
        "snapshot_raw_layer",
        lambda: {
            "market_tokens_distinct_tokens": 0,
            "odds_history_distinct_tokens": 0,
            "token_odds_daily_distinct_tokens": 0,
            "ledger_distinct_tokens": 0,
            "ledger_fully_checked_tokens": 0,
            "token_sync_skips_distinct_tokens": 0,
            "market_tokens_without_history": 0,
            "history_tokens_without_market_tokens": 0,
            "token_sync_skips_by_reason": {},
        },
    )
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", lambda **kw: iter(()))
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(Queue, "join", lambda self: None)
    odds_sync.sync_odds(
        max_workers=9999,
        market_page_size=100,
        persist_run_metrics=False,
        auto_tune_rps=False,
        requests_per_second=5,
    )


def test_sync_odds_effective_max_rps_respects_configured_rps_when_auto_tune_max_none(
    monkeypatch,
):
    plans = [
        odds_sync.TokenPlan(
            token_id="b" * 33 + "12",
            market_id="m",
            is_closed=False,
            created_at_ts=1,
            start_ts=1,
            end_ts=100,
            fidelity=1440,
        )
    ]

    def plan_iter(**kw):
        for p in plans:
            yield p

    captured_max_rps: list[int] = []

    def capture_tune(*, max_rps, **kwargs):
        captured_max_rps.append(int(max_rps))

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(
        odds_sync,
        "_sync_token_plan",
        lambda *a, **k: {
            "rows": 0,
            "windows": 1,
            "empty": True,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        },
    )
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.odds.engine.pool.maybe_auto_tune_rps",
        capture_tune,
    )

    class Lim:
        rate = 40.0

        def get_rate(self):
            return self.rate

        def set_rate(self, r):
            self.rate = r

    with immediate_executor():
        odds_sync.sync_odds(
            max_workers=10,
            market_page_size=100,
            persist_run_metrics=False,
            auto_tune_rps=True,
            requests_per_second=40,
            auto_tune_max_rps=None,
            rate_limiter_factory=lambda rps: Lim(),
        )

    assert captured_max_rps
    assert captured_max_rps[0] >= 40


def test_sync_odds_auto_tune_max_rps_branch(monkeypatch):
    plans = [
        odds_sync.TokenPlan(
            token_id="a" * 33 + "12",
            market_id="m",
            is_closed=False,
            created_at_ts=1,
            start_ts=1,
            end_ts=100,
            fidelity=1440,
        )
    ]

    def plan_iter(**kw):
        for p in plans:
            yield p

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(
        odds_sync,
        "_sync_token_plan",
        lambda *a, **k: {
            "rows": 0,
            "windows": 1,
            "empty": True,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        },
    )
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)

    class Lim:
        rate = 5.0

        def get_rate(self):
            return self.rate

        def set_rate(self, r):
            self.rate = r

    with immediate_executor():
        odds_sync.sync_odds(
            max_workers=1,
            market_page_size=100,
            persist_run_metrics=False,
            auto_tune_rps=True,
            requests_per_second=3,
            auto_tune_max_rps=20,
            rate_limiter_factory=lambda rps: Lim(),
        )


def test_sync_odds_run_summary_includes_planning_context(monkeypatch):
    captured = {}
    plan = odds_sync.TokenPlan(
        token_id="p" * 33 + "12",
        market_id="m",
        is_closed=False,
        created_at_ts=1,
        start_ts=1,
        end_ts=10,
        fidelity=1440,
    )

    def plan_iter(**kw):
        yield plan
        return (odds_sync.PlanningState(plans=1), {})

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        odds_sync,
        "snapshot_raw_layer",
        lambda: {
            "market_tokens_distinct_tokens": 2,
            "odds_history_distinct_tokens": 1,
            "token_odds_daily_distinct_tokens": 1,
            "ledger_distinct_tokens": 1,
            "ledger_fully_checked_tokens": 0,
            "token_sync_skips_distinct_tokens": 0,
            "market_tokens_without_history": 1,
            "history_tokens_without_market_tokens": 0,
            "token_sync_skips_by_reason": {},
        },
    )
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(
        odds_sync,
        "_sync_token_plan",
        lambda *a, **k: {
            "rows": 1,
            "windows": 1,
            "empty": False,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        },
    )
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(
        odds_sync,
        "save_sync_run_metrics",
        lambda task, metrics, **kwargs: captured.update(
            {"task": task, "metrics": metrics}
        ),
    )
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)

    with immediate_executor():
        result = odds_sync.sync_odds(
            max_workers=1,
            market_page_size=100,
            persist_run_metrics=True,
            auto_tune_rps=False,
            requests_per_second=5,
            client_factory=lambda: MagicMock(),
            rate_limiter_factory=lambda r: None,
        )

    assert result["planning_context"]["planned_vs_market_tokens"] == 0.5
    assert captured["metrics"]["planning"]["plans"] == 1
    assert captured["metrics"]["planning_context"]["market_tokens_distinct_tokens"] == 2


def test_sync_odds_no_plans_stopiteration(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "so.duckdb"))

    reload_all_settings_modules()
    import oddsfox_pipeline.storage.duckdb.connection as conn

    conn.reset_duckdb_connection_state()
    importlib.reload(conn)
    conn.ensure_duck_db()

    def empty_plans():
        for _ in []:
            yield None
        return (odds_sync.PlanningState(), {"bad": "token"})

    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", lambda **k: empty_plans())
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda x: None)
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)
    odds_sync.sync_odds(
        max_workers=1,
        persist_run_metrics=True,
        client_factory=lambda: MagicMock(),
        rate_limiter_factory=lambda r: None,
    )


def test_sync_odds_worker_cap_and_rps_branches(monkeypatch):
    """Worker cap log + RPS branches without real writer/queue (avoids join deadlock)."""
    tid = "f" * 33 + "12"
    plan = odds_sync.TokenPlan(
        token_id=tid,
        market_id="m",
        is_closed=False,
        created_at_ts=1,
        start_ts=1,
        end_ts=10,
        fidelity=1440,
    )

    def plans():
        yield plan

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", lambda **k: plans())
    monkeypatch.setattr(
        odds_sync,
        "_sync_token_plan",
        lambda *a, **k: {
            "rows": 0,
            "windows": 1,
            "empty": True,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        },
    )
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)

    with immediate_executor():
        odds_sync.sync_odds(
            max_workers=odds_sync.MAX_WORKERS_CAP + 50,
            requests_per_second=None,
            auto_tune_max_rps=100,
            persist_run_metrics=True,
            empty_token_skip_runs=2,
            client_factory=lambda: MagicMock(),
            rate_limiter_factory=lambda r: odds_sync.RateLimiter(5),
        )


def test_sync_odds_soft_warning_then_progress_reset(monkeypatch):
    token_id = "f" * 33 + "12"
    clock = FakeClock()
    plan = odds_sync.TokenPlan(
        token_id=token_id,
        market_id="m",
        is_closed=False,
        created_at_ts=1,
        start_ts=1,
        end_ts=10,
        fidelity=1440,
    )

    def plans():
        yield plan
        return (odds_sync.PlanningState(plans=1), {})

    wait_state = {"calls": 0}

    def fake_wait(futures, return_when=None, timeout=None):
        del return_when, timeout
        wait_state["calls"] += 1
        if wait_state["calls"] == 1:
            clock.advance(1.1)
            return set(), set()
        return set(futures), set()

    patch_guardrail_clock(monkeypatch, clock)
    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync.time, "monotonic", clock)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", lambda **k: plans())
    monkeypatch.setattr(
        odds_sync,
        "_sync_token_plan",
        lambda *a, **k: {
            "rows": 3,
            "windows": 1,
            "empty": False,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        },
    )
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "wait", fake_wait)

    with immediate_executor():
        summary = odds_sync.sync_odds(
            max_workers=1,
            auto_tune_rps=False,
            persist_run_metrics=True,
            progress_poll_seconds=1,
            no_progress_soft_timeout_seconds=1,
            no_progress_hard_timeout_seconds=5,
            client_factory=lambda **_kwargs: object(),
            rate_limiter_factory=lambda r: None,
        )

    assert summary["aborted"] is False
    assert summary["soft_warning_count"] >= 1
    assert summary["totals"]["processed_tokens"] == 1
