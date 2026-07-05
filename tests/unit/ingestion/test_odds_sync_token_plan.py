"""Unit tests for per-token odds sync execution."""

from __future__ import annotations

from queue import Queue
from unittest.mock import MagicMock

import pytest

pytest.importorskip("duckdb")

from tests.support.odds_sync_harness import make_plan

from oddsfox_pipeline.ingestion.polymarket.odds import sync as odds_sync
from oddsfox_pipeline.ingestion.polymarket.odds.fetch import BadRequestError


def test_sync_token_plan_empty_then_rows_flushes_and_marks_closed():
    token_id = "x" * 33 + "12"
    queue = Queue()
    plan = make_plan(token_id, closed=True)
    calls = iter([[], [(token_id, 20, 0.4), (token_id, 21, 0.5)]])
    result = odds_sync._sync_token_plan(
        plan,
        object(),
        queue,
        window_seconds=11,
        writer_chunk_rows=1,
        min_split_window_seconds=1,
        fetch_window_fn=lambda *args, **kwargs: next(calls),
    )

    items = []
    while not queue.empty():
        items.append(queue.get_nowait())

    assert result["rows"] == 2
    assert any(item[0] == "odds" for item in items)
    assert any(
        item[0] == "token_state"
        and item[1][0][0] == token_id
        and item[1][0][1] == 21
        and item[1][0][5] is True
        for item in items
    )


def test_sync_token_plan_transient_uses_contiguous_checked_until():
    token_id = "y" * 33 + "12"
    queue = Queue()
    plan = make_plan(token_id)
    calls = iter([[], None])
    result = odds_sync._sync_token_plan(
        plan,
        object(),
        queue,
        window_seconds=11,
        writer_chunk_rows=100,
        min_split_window_seconds=1,
        fetch_window_fn=lambda *args, **kwargs: next(calls),
    )

    assert result["error"] == 1
    item = queue.get_nowait()
    assert item[0] == "token_state"
    assert item[1][0][0] == token_id
    assert item[1][0][1] == 21


def test_sync_token_plan_transient_after_rows_uses_max_contiguous():
    token_id = "z" * 33 + "12"
    queue = Queue()
    plan = make_plan(token_id)
    calls = iter([[(token_id, 20, 0.4)], None])
    result = odds_sync._sync_token_plan(
        plan,
        object(),
        queue,
        window_seconds=11,
        writer_chunk_rows=100,
        min_split_window_seconds=1,
        fetch_window_fn=lambda *args, **kwargs: next(calls),
    )

    items = []
    while not queue.empty():
        items.append(queue.get_nowait())
    assert result["error"] == 1
    assert any(
        item[0] == "token_state" and item[1][0][0] == token_id and item[1][0][1] == 20
        for item in items
    )


def test_sync_token_plan_rows_after_transient_skip_contiguous_tracking():
    token_id = "n" * 33 + "12"
    queue = Queue()
    plan = make_plan(token_id)
    calls = iter([None, [(token_id, 25, 0.4)]])
    result = odds_sync._sync_token_plan(
        plan,
        object(),
        queue,
        window_seconds=11,
        writer_chunk_rows=100,
        min_split_window_seconds=1,
        fetch_window_fn=lambda *args, **kwargs: next(calls),
    )

    items = []
    while not queue.empty():
        items.append(queue.get_nowait())
    assert result["rows"] == 1
    assert not any(
        item[0] == "token_state"
        and item[1][0][0] == token_id
        and item[1][0][1] == plan.end_ts
        for item in items
    )


def test_sync_token_plan_empty_without_errors_uses_end_cursor():
    token_id = "q" * 33 + "12"
    queue = Queue()
    plan = make_plan(token_id)
    result = odds_sync._sync_token_plan(
        plan,
        object(),
        queue,
        window_seconds=100,
        writer_chunk_rows=100,
        min_split_window_seconds=1,
        fetch_window_fn=lambda *args, **kwargs: [],
    )

    assert result["empty"] is True
    item = queue.get_nowait()
    assert item[0] == "token_state"
    assert item[1][0][0] == token_id
    assert item[1][0][1] == plan.end_ts


def test_sync_token_plan_mocked():
    q = Queue()
    tid = "v" * 33 + "12"
    plan = odds_sync.TokenPlan(
        token_id=tid,
        market_id="m",
        is_closed=False,
        created_at_ts=1,
        start_ts=1,
        end_ts=100,
        fidelity=1440,
    )

    def fetch_stub(*a, **k):
        return [(tid, 50, 0.4)]

    odds_sync._sync_token_plan(
        plan,
        MagicMock(),
        q,
        window_seconds=200,
        writer_chunk_rows=10,
        min_split_window_seconds=1,
        fetch_window_fn=fetch_stub,
    )


def test_sync_token_plan_typeerror_propagates():
    def ft(*a, **k):
        raise TypeError("bad signature")

    q = Queue()
    plan = odds_sync.TokenPlan(
        token_id="q" * 33 + "12",
        market_id="m",
        is_closed=False,
        created_at_ts=1,
        start_ts=1,
        end_ts=50,
        fidelity=1440,
    )
    with pytest.raises(TypeError, match="bad signature"):
        odds_sync._sync_token_plan(
            plan,
            MagicMock(),
            q,
            window_seconds=100,
            writer_chunk_rows=1000,
            min_split_window_seconds=1,
            fetch_window_fn=ft,
        )


def test_sync_token_plan_permanent_error_puts_skip():
    def ft(*a, **k):
        raise BadRequestError("bad", status=400, body="not interval")

    q = Queue()
    plan = odds_sync.TokenPlan(
        token_id="p" * 33 + "12",
        market_id="m",
        is_closed=True,
        created_at_ts=1,
        start_ts=1,
        end_ts=50,
        fidelity=1440,
    )
    odds_sync._sync_token_plan(
        plan,
        MagicMock(),
        q,
        window_seconds=100,
        writer_chunk_rows=1000,
        min_split_window_seconds=1,
        fetch_window_fn=ft,
    )
    items = []
    while not q.empty():
        items.append(q.get_nowait())
    assert items


def test_sync_token_plan_transient_and_cursor_branches():
    """had_transient_error True with max_contiguous; empty chunk continuation."""

    def ft(*a, **k):
        return None  # transient

    q = Queue()
    plan = odds_sync.TokenPlan(
        token_id="r" * 33 + "12",
        market_id="m",
        is_closed=False,
        created_at_ts=1,
        start_ts=1,
        end_ts=4000,
        fidelity=1440,
    )
    odds_sync._sync_token_plan(
        plan,
        MagicMock(),
        q,
        window_seconds=2000,
        writer_chunk_rows=10000,
        min_split_window_seconds=1,
        fetch_window_fn=ft,
    )


def test_sync_token_plan_passes_extended_fetch_signature():
    seen = {}

    def hook(status):
        return None

    def ft(*args):
        seen["args"] = args
        return [(args[1], 10, 0.5)]

    q = Queue()
    plan = odds_sync.TokenPlan(
        token_id="c" * 33 + "12",
        market_id="m",
        is_closed=False,
        created_at_ts=1,
        start_ts=1,
        end_ts=20,
        fidelity=1440,
    )
    odds_sync._sync_token_plan(
        plan,
        MagicMock(),
        q,
        window_seconds=100,
        writer_chunk_rows=1000,
        min_split_window_seconds=1,
        status_hook=hook,
        fetch_window_fn=ft,
    )
    assert seen["args"][6] == odds_sync.DEFAULT_TRANSIENT_RETRIES
    assert seen["args"][7] == odds_sync.DEFAULT_TRANSIENT_BACKOFF_SECONDS
    assert seen["args"][8] is hook


def test_sync_token_plan_badrequest_skip():
    def ft(*a, **k):
        raise BadRequestError("x", status=400, body="")

    q = Queue()
    plan = odds_sync.TokenPlan(
        token_id="d" * 33 + "12",
        market_id="m",
        is_closed=False,
        created_at_ts=1,
        start_ts=1,
        end_ts=30,
        fidelity=1440,
    )
    odds_sync._sync_token_plan(
        plan,
        MagicMock(),
        q,
        window_seconds=50,
        writer_chunk_rows=1000,
        min_split_window_seconds=1,
        fetch_window_fn=ft,
    )


def test_sync_token_plan_cursor_transient_empty_rows():
    def ft(*a, **k):
        return None

    q = Queue()
    plan = odds_sync.TokenPlan(
        token_id="e" * 33 + "12",
        market_id="m",
        is_closed=False,
        created_at_ts=1,
        start_ts=1,
        end_ts=100,
        fidelity=1440,
    )
    result = odds_sync._sync_token_plan(
        plan,
        MagicMock(),
        q,
        window_seconds=40,
        writer_chunk_rows=10000,
        min_split_window_seconds=1,
        fetch_window_fn=ft,
    )
    assert result["error"] == 1
    assert result["permanent_error"] == 0
    item = q.get_nowait()
    assert item[0] == "token_state"
    token_id, cursor_ts, checked_at, next_check_at, empty_run_streak, fully_checked = (
        item[1][0]
    )
    assert token_id == plan.token_id
    assert cursor_ts is None
    assert next_check_at is not None
    assert next_check_at > checked_at
    assert empty_run_streak == 0
    assert fully_checked is False
