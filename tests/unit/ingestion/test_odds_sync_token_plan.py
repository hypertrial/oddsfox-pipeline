"""Unit tests for per-token odds sync execution."""

from __future__ import annotations

from queue import Queue

import pytest

pytest.importorskip("duckdb")

from tests.integration.ingestion._odds_sync_harness import make_plan

from oddsfox_pipeline.ingestion.polymarket.odds import sync as odds_sync


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
