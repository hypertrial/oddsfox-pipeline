"""Unit tests for batch CLOB prices-history fetch and group execution."""

from __future__ import annotations

from queue import Queue
from unittest.mock import MagicMock

import pytest
import requests

from oddsfox_pipeline.ingestion.polymarket.odds import execution as odds_exec
from oddsfox_pipeline.ingestion.polymarket.odds import fetch as odds_fetch
from oddsfox_pipeline.ingestion.polymarket.odds.planning import group_token_plans
from oddsfox_pipeline.ingestion.polymarket.odds.support import GroupPlan, TokenPlan
from tests.support.odds_sync_harness import make_group_plan, make_token_plan, valid_token_id


def _history_point(t: int, p: float = 0.5) -> dict:
    return {"t": t, "p": p}


def test_fetch_batch_token_history_success_and_missing_token():
    tid_a = valid_token_id("a")
    tid_b = valid_token_id("b")
    client = MagicMock()
    client.post.return_value = {
        "history": {
            tid_a: [_history_point(10), _history_point(20)],
            # tid_b missing -> empty list
        }
    }
    out = odds_fetch.fetch_batch_token_history(
        client, [tid_a, tid_b], start_ts=1, end_ts=100, fidelity=60
    )
    assert out is not None
    assert out[tid_a] == [(tid_a, 10, 0.5), (tid_a, 20, 0.5)]
    assert out[tid_b] == []
    kwargs = client.post.call_args.kwargs
    assert kwargs["json"]["markets"] == [tid_a, tid_b]
    assert kwargs["json"]["start_ts"] == 1
    assert kwargs["json"]["end_ts"] == 100
    assert kwargs["json"]["fidelity"] == 60


def test_fetch_batch_token_history_rejects_over_max():
    client = MagicMock()
    with pytest.raises(ValueError, match="maxItems"):
        odds_fetch.fetch_batch_token_history(
            client,
            [valid_token_id(str(i)) for i in range(21)],
            start_ts=1,
            end_ts=2,
        )


def test_fetch_batch_token_history_transient_429():
    client = MagicMock()
    resp = MagicMock(status_code=429, text="slow down")
    err = requests.HTTPError(response=resp)
    client.post.side_effect = err
    assert (
        odds_fetch.fetch_batch_token_history(
            client, [valid_token_id()], start_ts=1, end_ts=2
        )
        is None
    )


def test_fetch_batch_token_history_400_interval_too_long():
    client = MagicMock()
    resp = MagicMock(status_code=400, text="interval is too long")
    err = requests.HTTPError(response=resp)
    client.post.side_effect = err
    with pytest.raises(odds_fetch.BadRequestError) as exc:
        odds_fetch.fetch_batch_token_history(
            client, [valid_token_id()], start_ts=1, end_ts=2
        )
    assert "interval is too long" in (exc.value.body or "").lower()


def test_fetch_batch_token_history_404_permanent():
    client = MagicMock()
    resp = MagicMock(status_code=404, text="not found")
    err = requests.HTTPError(response=resp)
    client.post.side_effect = err
    with pytest.raises(odds_fetch.PermanentAPIError):
        odds_fetch.fetch_batch_token_history(
            client, [valid_token_id()], start_ts=1, end_ts=2
        )


def test_fetch_batch_token_history_with_retry_empty_range():
    out = odds_fetch.fetch_batch_token_history_with_retry(
        MagicMock(),
        [valid_token_id()],
        start_ts=100,
        end_ts=50,
    )
    assert out == {valid_token_id(): []}


def test_group_token_plans_caps_and_preserves_count():
    plans = [make_token_plan(valid_token_id(str(i))) for i in range(45)]
    groups = group_token_plans(plans, group_size=20)
    assert len(groups) == 3
    assert all(len(g.token_plans) <= 20 for g in groups)
    assert sum(len(g.token_plans) for g in groups) == 45
    assert len({p.token_id for g in groups for p in g.token_plans}) == 45


def test_group_token_plans_preserve_order_when_requested():
    plans = [
        TokenPlan("a" * 33 + "12", "m", False, 1, 50, 100, 60),
        TokenPlan("b" * 33 + "12", "m", False, 1, 10, 100, 60),
    ]
    groups = group_token_plans(plans, group_size=20, sort_for_batch=False)
    assert [p.token_id for p in groups[0].token_plans] == [
        "a" * 33 + "12",
        "b" * 33 + "12",
    ]


@pytest.mark.parametrize(
    "batch_exc",
    [
        odds_fetch.PermanentAPIError("poison batch"),
        odds_fetch.BadRequestError("bad market", body="invalid market", status=400),
    ],
)
def test_fetch_group_window_fallback_isolates_bad_token(batch_exc):
    tid_ok = valid_token_id("1")
    tid_bad = valid_token_id("2")
    calls = {"single": 0}

    def batch_fn(client, token_ids, **kwargs):
        del client, token_ids, kwargs
        raise batch_exc

    def single_fn(client, token_id, **kwargs):
        del client
        calls["single"] += 1
        if token_id == tid_bad:
            raise odds_fetch.PermanentAPIError("bad token")
        return [(token_id, kwargs["end_ts"] - 1, 0.7)]

    out = odds_exec.fetch_group_window_with_auto_split(
        MagicMock(),
        [tid_ok, tid_bad],
        0,
        50,
        60,
        10,
        fetch_batch_token_history_fn=batch_fn,
        fetch_token_history_fn=single_fn,
    )
    assert isinstance(out[tid_bad], odds_fetch.PermanentAPIError)
    assert out[tid_ok] == [(tid_ok, 49, 0.7)]
    assert calls["single"] == 2


def test_fetch_group_window_auto_split_interval_too_long():
    tid = valid_token_id("1")
    calls = []

    def batch_fn(client, token_ids, **kwargs):
        del client, token_ids
        start_ts = kwargs["start_ts"]
        end_ts = kwargs["end_ts"]
        calls.append((start_ts, end_ts))
        if end_ts - start_ts > 60:
            raise odds_fetch.BadRequestError(
                "long", body="interval is too long", status=400
            )
        return {tid: [(tid, end_ts - 1, 0.4)]}

    out = odds_exec.fetch_group_window_with_auto_split(
        MagicMock(),
        [tid],
        0,
        200,
        60,
        60,
        fetch_batch_token_history_fn=batch_fn,
    )
    assert isinstance(out[tid], list)
    assert len(calls) >= 2
    assert out[tid]


def test_sync_token_group_plan_per_token_results_and_writer_rows():
    tid_a = valid_token_id("a")
    tid_b = valid_token_id("b")
    plan_a = TokenPlan(tid_a, "m1", False, 1, 10, 31, 60)
    plan_b = TokenPlan(tid_b, "m2", True, 1, 10, 31, 60)
    group = GroupPlan(
        token_plans=(plan_a, plan_b),
        group_start_ts=10,
        group_end_ts=31,
        fidelity=60,
    )
    q: Queue = Queue()

    def fetch_group_window(
        client,
        token_ids,
        start_ts,
        end_ts,
        fidelity,
        min_window_seconds,
        *rest,
    ):
        del client, fidelity, min_window_seconds, rest
        return {
            tid_a: [(tid_a, start_ts + 1, 0.2)],
            tid_b: [],
        }

    results = odds_exec.sync_token_group_plan(
        group,
        MagicMock(),
        q,
        window_seconds=30,
        writer_chunk_rows=100,
        min_split_window_seconds=10,
        fetch_group_window_fn=fetch_group_window,
    )
    assert results[tid_a]["rows"] == 1
    assert results[tid_a]["empty"] is False
    assert results[tid_b]["rows"] == 0
    assert results[tid_b]["empty"] is True
    assert results[tid_b]["fully_checked"] is True
    ops = []
    while not q.empty():
        ops.append(q.get())
    assert any(op[0] == "odds" for op in ops)
    assert any(op[0] == "token_state" for op in ops)


def test_sync_token_group_plan_isolates_permanent_token():
    tid_ok = valid_token_id("ok")
    tid_bad = valid_token_id("bad")
    group = make_group_plan(
        TokenPlan(tid_ok, "m", False, 1, 10, 31, 60),
        TokenPlan(tid_bad, "m", False, 1, 10, 31, 60),
    )
    q: Queue = Queue()

    def fetch_group_window(*args, **kwargs):
        del args, kwargs
        return {
            tid_ok: [(tid_ok, 15, 0.5)],
            tid_bad: odds_fetch.PermanentAPIError("gone"),
        }

    results = odds_exec.sync_token_group_plan(
        group,
        MagicMock(),
        q,
        window_seconds=30,
        writer_chunk_rows=100,
        min_split_window_seconds=10,
        fetch_group_window_fn=fetch_group_window,
    )
    assert results[tid_ok]["permanent_error"] == 0
    assert results[tid_ok]["rows"] == 1
    assert results[tid_bad]["permanent_error"] == 1
    skips = [op for op in list(q.queue) if op[0] == "skipped_tokens"]
    assert skips and tid_bad in {row[0] for row in skips[0][1]}


def test_batch_and_single_paths_produce_identical_rows():
    """Ponytail check: batched window fetch matches per-token fetch row sets."""
    tid_a = valid_token_id("x")
    tid_b = valid_token_id("y")
    store = {
        tid_a: [(tid_a, 11, 0.1), (tid_a, 21, 0.2)],
        tid_b: [(tid_b, 12, 0.3)],
    }

    def single_fn(client, token_id, **kwargs):
        del client, kwargs
        return list(store[token_id])

    def batch_fn(client, token_ids, **kwargs):
        del client, kwargs
        return {tid: list(store[tid]) for tid in token_ids}

    single_rows = []
    for tid in (tid_a, tid_b):
        chunk = odds_exec.fetch_window_with_auto_split(
            MagicMock(),
            tid,
            10,
            30,
            60,
            10,
            fetch_token_history_fn=single_fn,
        )
        assert chunk is not None
        single_rows.extend(chunk)

    batch_map = odds_exec.fetch_group_window_with_auto_split(
        MagicMock(),
        [tid_a, tid_b],
        10,
        30,
        60,
        10,
        fetch_batch_token_history_fn=batch_fn,
        fetch_token_history_fn=single_fn,
    )
    batch_rows = []
    for tid in (tid_a, tid_b):
        value = batch_map[tid]
        assert isinstance(value, list)
        batch_rows.extend(value)
    assert sorted(single_rows) == sorted(batch_rows)
