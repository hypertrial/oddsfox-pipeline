"""Unit tests for shared minute-batch CLOB fetch helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from oddsfox_pipeline.ingestion.polymarket.odds import minute_batch


@dataclass(frozen=True)
class _Plan:
    market_id: str
    token_id: str
    started_at: datetime
    finished_at: datetime


def _plan(token_id: str, *, start: datetime, end: datetime) -> _Plan:
    return _Plan(
        market_id=f"m-{token_id}",
        token_id=token_id,
        started_at=start,
        finished_at=end,
    )


def test_group_minute_plans_clusters_by_window_and_respects_batch_size():
    start_a = datetime(2026, 7, 1, tzinfo=timezone.utc)
    end_a = start_a + timedelta(hours=1)
    start_b = start_a + timedelta(hours=2)
    end_b = start_b + timedelta(hours=1)
    plans = [
        _plan("a1", start=start_a, end=end_a),
        _plan("a2", start=start_a, end=end_a),
        _plan("a3", start=start_a, end=end_a),
        _plan("b1", start=start_b, end=end_b),
        _plan("b2", start=start_b, end=end_b),
    ]

    groups = minute_batch.group_minute_plans(plans, batch_group_size=2)

    assert [tuple(p.token_id for p in group) for group in groups] == [
        ("a1", "a2"),
        ("a3",),
        ("b1", "b2"),
    ]


def test_fetch_minute_plan_group_uses_group_fetch_and_returns_success_rows():
    start = datetime(2026, 7, 1, 12, tzinfo=timezone.utc)
    end = start + timedelta(minutes=2)
    plans = [
        _plan("tok-a", start=start, end=end),
        _plan("tok-b", start=start, end=end),
    ]
    mid = int(start.timestamp()) + 30
    calls: list[tuple] = []

    def fetch_group(
        _client,
        token_ids,
        window_start,
        window_end,
        *_args,
    ):
        calls.append((tuple(token_ids), window_start, window_end))
        return {
            token_id: [(token_id, mid, 0.42)]
            for token_id in token_ids
        }

    results = minute_batch.fetch_minute_plan_group(
        plans,
        object(),
        fetch_group,
        transient_retries=0,
        transient_backoff_seconds=0,
        window_seconds=86_400,
    )

    assert len(calls) == 1
    assert calls[0][0] == ("tok-a", "tok-b")
    assert [result.fetch_status for result in results] == ["success", "success"]
    assert all(result.history_sha256 for result in results)
    assert all(len(result.history) == 1 for result in results)


def test_execute_minute_fetches_with_batch_calls_fetch_group_window_fn():
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    end = start + timedelta(minutes=1)
    plans = [
        _plan("tok-1", start=start, end=end),
        _plan("tok-2", start=start, end=end),
    ]
    group_calls: list[tuple[str, ...]] = []
    single_calls: list[str] = []
    mid = int(start.timestamp())

    def fetch_group(_client, token_ids, *_args):
        group_calls.append(tuple(token_ids))
        return {
            token_id: [(token_id, mid, 0.5)]
            for token_id in token_ids
        }

    def fetch_single(_client, token_id, *_args, **_kwargs):
        single_calls.append(token_id)
        return [(token_id, mid, 0.5)]

    results = minute_batch.execute_minute_fetches(
        plans,
        asset_name="test_minute_batch",
        workers=1,
        requests_per_second=1000,
        batch_group_size=20,
        auto_tune_rps=False,
        client_factory=lambda: object(),
        fetch_window_fn=fetch_single,
        fetch_group_window_fn=fetch_group,
        no_progress_soft_timeout_seconds=None,
        no_progress_hard_timeout_seconds=None,
    )

    assert group_calls == [("tok-1", "tok-2")]
    assert single_calls == []
    assert [result.fetch_status for result in results] == ["success", "success"]
    assert [result.plan.token_id for result in results] == ["tok-1", "tok-2"]
