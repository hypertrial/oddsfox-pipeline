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


def test_borrow_duckdb_connection_requires_exactly_one_source():
    from contextlib import nullcontext

    import pytest

    with pytest.raises(ValueError, match="exactly one"):
        with minute_batch.borrow_duckdb_connection():
            pass
    with pytest.raises(ValueError, match="exactly one"):
        with minute_batch.borrow_duckdb_connection(
            object(), connection_factory=lambda: nullcontext(object())
        ):
            pass


def test_borrow_duckdb_connection_releases_factory_between_phases():
    from contextlib import contextmanager

    open_depth = {"n": 0}
    observed: list[int] = []

    @contextmanager
    def factory():
        open_depth["n"] += 1
        try:
            yield "conn"
        finally:
            open_depth["n"] -= 1

    with minute_batch.borrow_duckdb_connection(connection_factory=factory) as active:
        assert active == "conn"
        assert open_depth["n"] == 1
    observed.append(open_depth["n"])
    with minute_batch.borrow_duckdb_connection(connection_factory=factory) as active:
        assert active == "conn"
        assert open_depth["n"] == 1
    observed.append(open_depth["n"])
    assert observed == [0, 0]


def test_borrow_duckdb_connection_yields_passed_conn_without_closing():
    sentinel = object()
    with minute_batch.borrow_duckdb_connection(sentinel) as active:
        assert active is sentinel


def test_build_minute_history_arrow_table_flattens_and_broadcasts():
    import pyarrow as pa
    import pytest

    start_a = datetime(2026, 6, 11, tzinfo=timezone.utc)
    end_a = start_a + timedelta(days=1)
    start_b = datetime(2026, 6, 12, tzinfo=timezone.utc)
    end_b = start_b + timedelta(hours=6)
    ingested_at = datetime(2026, 7, 1, tzinfo=timezone.utc)
    results = [
        minute_batch.MinuteFetchResult(
            plan=_plan("tok-a", start=start_a, end=end_a),
            fetch_status="success",
            history=(("tok-a", 100, 0.1), ("tok-a", 160, 0.2)),
            request_start_epoch=100,
            request_end_epoch=200,
            source_row_count=2,
            history_sha256="a" * 64,
            fetch_started_at=ingested_at,
            fetch_finished_at=ingested_at,
        ),
        minute_batch.MinuteFetchResult(
            plan=_plan("tok-empty", start=start_a, end=end_a),
            fetch_status="empty",
            history=(),
            request_start_epoch=100,
            request_end_epoch=200,
            source_row_count=0,
            history_sha256=None,
            fetch_started_at=ingested_at,
            fetch_finished_at=ingested_at,
            error_type="EmptyHistory",
            error_message="empty",
        ),
        minute_batch.MinuteFetchResult(
            plan=_plan("tok-b", start=start_b, end=end_b),
            fetch_status="success",
            history=(("tok-b", 200, 0.9),),
            request_start_epoch=200,
            request_end_epoch=300,
            source_row_count=1,
            history_sha256="b" * 64,
            fetch_started_at=ingested_at,
            fetch_finished_at=ingested_at,
        ),
    ]

    table = minute_batch.build_minute_history_arrow_table(
        results, ingested_at=ingested_at
    )

    assert table.num_rows == 3
    assert table.column_names == [
        "market_id",
        "clob_token_id",
        "timestamp",
        "price",
        "fidelity_minutes",
        "window_start_at",
        "window_end_at",
        "ingested_at",
        "row_order",
    ]
    assert table.schema.field("clob_token_id").type == pa.string()
    assert table.schema.field("timestamp").type == pa.int64()
    assert table.schema.field("price").type == pa.float64()
    assert table.schema.field("fidelity_minutes").type == pa.int32()
    assert table.schema.field("window_start_at").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("window_end_at").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("ingested_at").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("row_order").type == pa.int64()
    assert table["clob_token_id"].to_pylist() == ["tok-a", "tok-a", "tok-b"]
    assert table["timestamp"].to_pylist() == [100, 160, 200]
    assert table["price"].to_pylist() == [0.1, 0.2, 0.9]
    assert table["market_id"].to_pylist() == ["m-tok-a", "m-tok-a", "m-tok-b"]
    assert table["fidelity_minutes"].to_pylist() == [1, 1, 1]
    assert table["row_order"].to_pylist() == [0, 1, 2]
    assert table["window_start_at"].to_pylist() == [start_a, start_a, start_b]
    assert table["window_end_at"].to_pylist() == [end_a, end_a, end_b]
    assert table["ingested_at"].to_pylist() == [ingested_at, ingested_at, ingested_at]

    with pytest.raises(ValueError, match="rows must not be empty"):
        minute_batch.build_minute_history_arrow_table(
            [results[1]], ingested_at=ingested_at
        )


def test_build_minute_history_arrow_table_matches_dict_rows_at_moderate_scale():
    ingested_at = datetime(2026, 7, 1, tzinfo=timezone.utc)
    results = []
    expected_rows = []
    for token_idx in range(40):
        start = datetime(2026, 6, 11, tzinfo=timezone.utc) + timedelta(hours=token_idx)
        end = start + timedelta(hours=2)
        history = tuple(
            (f"tok-{token_idx}", 1_000 + token_idx * 100 + point, 0.01 * point)
            for point in range(50)
        )
        results.append(
            minute_batch.MinuteFetchResult(
                plan=_plan(f"tok-{token_idx}", start=start, end=end),
                fetch_status="success",
                history=history,
                request_start_epoch=int(start.timestamp()),
                request_end_epoch=int(end.timestamp()),
                source_row_count=len(history),
                history_sha256="c" * 64,
                fetch_started_at=ingested_at,
                fetch_finished_at=ingested_at,
            )
        )
        for token_id, timestamp, price in history:
            expected_rows.append(
                {
                    "market_id": f"m-tok-{token_idx}",
                    "clob_token_id": token_id,
                    "timestamp": timestamp,
                    "price": price,
                    "fidelity_minutes": 1,
                    "window_start_at": start,
                    "window_end_at": end,
                    "ingested_at": ingested_at,
                }
            )

    table = minute_batch.build_minute_history_arrow_table(
        results, ingested_at=ingested_at
    )
    assert table.num_rows == 2000
    assert table["row_order"].to_pylist() == list(range(2000))
    actual = table.to_pylist()
    for idx, expected in enumerate(expected_rows):
        row = actual[idx]
        assert row["market_id"] == expected["market_id"]
        assert row["clob_token_id"] == expected["clob_token_id"]
        assert row["timestamp"] == expected["timestamp"]
        assert row["price"] == expected["price"]
        assert row["fidelity_minutes"] == expected["fidelity_minutes"]
        assert row["window_start_at"] == expected["window_start_at"]
        assert row["window_end_at"] == expected["window_end_at"]
        assert row["ingested_at"] == expected["ingested_at"]
        assert row["row_order"] == idx
