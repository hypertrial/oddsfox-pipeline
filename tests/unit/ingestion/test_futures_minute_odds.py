from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import duckdb
import pytest

from oddsfox_pipeline.ingestion.polymarket import futures_minute
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    bootstrap_all_polymarket_tables,
    create_all_scope_test_markets_tables,
)


def _futures_inventory_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute("create schema polymarket_wc2026_raw")
    conn.execute("create schema polymarket_wc2026_ops")
    bootstrap_all_polymarket_tables(conn)
    create_all_scope_test_markets_tables(conn)
    conn.execute(
        """
        insert into polymarket_wc2026_raw.markets (
            id, question, closed, created_at, end_date, sports_market_type,
            clob_token_ids
        ) values
        (
            'futures-1', 'Who wins WC2026?', true,
            timestamp '2026-05-01 00:00:00',
            timestamp '2026-07-10 12:00:00',
            'tournament_winner',
            ?
        ),
        (
            'money-1', 'Team A win?', true,
            timestamp '2026-06-12 00:00:00',
            timestamp '2026-06-12 02:00:00',
            'moneyline',
            ?
        ),
        (
            'futures-empty', 'Too early?', true,
            timestamp '2026-08-01 00:00:00',
            timestamp '2026-08-02 00:00:00',
            'golden_boot',
            ?
        )
        """,
        [
            json.dumps(["futures-1-yes", "futures-1-no"]),
            json.dumps(["money-1-yes", "money-1-no"]),
            json.dumps(["empty-yes", "empty-no"]),
        ],
    )
    conn.execute(
        """
        insert into polymarket_wc2026_ops.market_scope_registry (
            scope_name, market_id, source, refreshed_at, is_event_volume_eligible
        ) values
        ('wc2026', 'futures-1', 'test', current_timestamp, true),
        ('wc2026', 'money-1', 'test', current_timestamp, true),
        ('wc2026', 'futures-empty', 'test', current_timestamp, true)
        """
    )
    return conn


def test_tournament_window_bounds_parse_contract_defaults():
    start, end = futures_minute.tournament_window_bounds()
    assert start == datetime(2026, 6, 11, tzinfo=timezone.utc)
    assert end == datetime(2026, 7, 19, 23, 59, 59, tzinfo=timezone.utc)


def test_resolve_futures_token_window_caps_by_market_close():
    tournament_start = datetime(2026, 6, 11, tzinfo=timezone.utc)
    tournament_end = datetime(2026, 7, 19, 23, 59, 59, tzinfo=timezone.utc)
    window = futures_minute.resolve_futures_token_window(
        created_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        end_date=datetime(2026, 7, 10, 12, tzinfo=timezone.utc),
        tournament_start=tournament_start,
        tournament_end=tournament_end,
    )
    assert window == (
        tournament_start,
        datetime(2026, 7, 10, 12, tzinfo=timezone.utc),
    )


def test_resolve_futures_token_window_rejects_empty_span():
    tournament_start = datetime(2026, 6, 11, tzinfo=timezone.utc)
    tournament_end = datetime(2026, 7, 19, tzinfo=timezone.utc)
    assert (
        futures_minute.resolve_futures_token_window(
            created_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
            end_date=datetime(2026, 8, 2, tzinfo=timezone.utc),
            tournament_start=tournament_start,
            tournament_end=tournament_end,
        )
        is None
    )


def test_select_futures_minute_token_plans_skips_match_types_and_empty_windows():
    conn = _futures_inventory_connection()
    try:
        plans = futures_minute.select_futures_minute_token_plans(conn)
    finally:
        conn.close()

    assert {plan.market_id for plan in plans} == {"futures-1"}
    assert len(plans) == 2
    assert all(plan.token_id.startswith("futures-1-") for plan in plans)
    assert all(
        plan.started_at == datetime(2026, 6, 11, tzinfo=timezone.utc) for plan in plans
    )
    assert all(
        plan.finished_at == datetime(2026, 7, 10, 12, tzinfo=timezone.utc)
        for plan in plans
    )


def test_select_futures_minute_token_plans_requires_registry_eligible_futures():
    conn = _futures_inventory_connection()
    conn.execute(
        "delete from polymarket_wc2026_ops.market_scope_registry where market_id = 'futures-1'"
    )
    conn.execute(
        "delete from polymarket_wc2026_raw.markets where id = 'futures-empty'"
    )
    try:
        with pytest.raises(ValueError, match="No registry-eligible WC2026 futures"):
            futures_minute.select_futures_minute_token_plans(conn)
    finally:
        conn.close()


def test_sync_futures_minute_samples_markets_and_caps_window(monkeypatch, tmp_path):
    conn = _futures_inventory_connection()
    for index in range(2, 22):
        conn.execute(
            """
            insert into polymarket_wc2026_raw.markets (
                id, question, closed, created_at, end_date, sports_market_type,
                clob_token_ids
            ) values (
                ?, 'Winner?', true,
                timestamp '2026-05-01 00:00:00',
                timestamp '2026-07-10 12:00:00',
                'tournament_winner',
                ?
            )
            """,
            [
                f"futures-{index}",
                json.dumps([f"futures-{index}-yes", f"futures-{index}-no"]),
            ],
        )
        conn.execute(
            """
            insert into polymarket_wc2026_ops.market_scope_registry (
                scope_name, market_id, source, refreshed_at, is_event_volume_eligible
            ) values (
                'wc2026', ?, 'test', current_timestamp, true
            )
            """,
            [f"futures-{index}"],
        )
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path))
    captured_plans: list = []

    def fetch_window(_client, token_id, start_ts, end_ts, *_args, **_kwargs):
        return [(token_id, int(end_ts) - 60, 0.42)]

    real_execute = futures_minute.execute_minute_fetches

    def wrap_execute(plans, *args, **kwargs):
        captured_plans.extend(plans)
        return real_execute(plans, *args, **kwargs)

    monkeypatch.setattr(futures_minute, "execute_minute_fetches", wrap_execute)
    try:
        summary = futures_minute.sync_futures_minute_odds_history(
            conn,
            workers=1,
            batch_group_size=1,
            client_factory=lambda: object(),
            fetch_window_fn=fetch_window,
            persist_fn=lambda *_a, **_k: None,
            audit_persist_fn=lambda *_a, **_k: None,
            market_sample_fraction=0.05,
            market_sample_seed="futures-smoke",
            sample_window_hours=24,
        )
    finally:
        conn.close()

    assert summary["status"] == "published"
    assert summary["sample_enabled"] is True
    assert summary["population_markets"] == 21
    assert summary["selected_markets"] == 2
    assert summary["sample_window_hours"] == 24
    assert captured_plans
    for plan in captured_plans:
        assert plan.finished_at == datetime(2026, 7, 10, 12, tzinfo=timezone.utc)
        assert plan.started_at == plan.finished_at - timedelta(hours=24)
        assert (plan.finished_at - plan.started_at).total_seconds() == 24 * 3600


def test_sync_futures_minute_odds_history_releases_duckdb_during_fetch():
    conn = _futures_inventory_connection()
    open_counts: list[int] = []
    open_depth = {"n": 0}
    point_ts = int(datetime(2026, 6, 11, tzinfo=timezone.utc).timestamp())

    @contextmanager
    def connection_factory():
        open_depth["n"] += 1
        try:
            yield conn
        finally:
            open_depth["n"] -= 1

    def fetch_window(
        _client,
        token_id,
        start_ts,
        end_ts,
        *_args,
        **_kwargs,
    ):
        open_counts.append(open_depth["n"])
        if int(start_ts) <= point_ts <= int(end_ts):
            return [(token_id, point_ts, 0.55)]
        return []

    try:
        summary = futures_minute.sync_futures_minute_odds_history(
            connection_factory=connection_factory,
            workers=1,
            batch_group_size=1,
            client_factory=lambda: object(),
            fetch_window_fn=fetch_window,
            persist_fn=lambda *_a, **_k: None,
            audit_persist_fn=lambda *_a, **_k: None,
        )
    finally:
        conn.close()

    assert summary["status"] == "published"
    assert open_counts
    assert all(count == 0 for count in open_counts)


def test_sync_futures_minute_releases_histories_before_persist(monkeypatch, tmp_path):
    conn = _futures_inventory_connection()
    point_ts = int(datetime(2026, 6, 11, tzinfo=timezone.utc).timestamp())
    order: list[str] = []
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path))

    def fetch_window(_client, token_id, start_ts, end_ts, *_args, **_kwargs):
        if int(start_ts) <= point_ts <= int(end_ts):
            return [(token_id, point_ts, 0.55)]
        return []

    real_release = futures_minute.release_minute_history_payloads

    def wrap_release(results):
        order.append("release")
        released = real_release(results)
        assert released >= 1
        assert all(result.history == () for result in results)
        return released

    def persist(rows, _conn, *, fetch_run_id):
        order.append("persist")
        assert rows

    monkeypatch.setattr(
        futures_minute, "release_minute_history_payloads", wrap_release
    )
    try:
        summary = futures_minute.sync_futures_minute_odds_history(
            conn,
            workers=1,
            batch_group_size=1,
            client_factory=lambda: object(),
            fetch_window_fn=fetch_window,
            persist_fn=persist,
            audit_persist_fn=lambda *_a, **_k: None,
        )
    finally:
        conn.close()

    assert summary["status"] == "published"
    assert order == ["release", "persist"]


def test_sync_futures_minute_odds_history_separate_audit_and_publish_borrows(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path))
    conn = _futures_inventory_connection()
    open_depth = {"n": 0}
    borrow_count = {"n": 0}
    shard_open_depth: list[int] = []
    point_ts = int(datetime(2026, 6, 11, tzinfo=timezone.utc).timestamp())

    @contextmanager
    def connection_factory():
        open_depth["n"] += 1
        borrow_count["n"] += 1
        try:
            yield conn
        finally:
            open_depth["n"] -= 1

    def fetch_window(_client, token_id, start_ts, end_ts, *_args, **_kwargs):
        if int(start_ts) <= point_ts <= int(end_ts):
            return [(token_id, point_ts, 0.55)]
        return []

    real_write = futures_minute.write_minute_history_parquet_shards

    def wrap_write(*args, **kwargs):
        shard_open_depth.append(open_depth["n"])
        return real_write(*args, **kwargs)

    monkeypatch.setattr(futures_minute, "write_minute_history_parquet_shards", wrap_write)

    try:
        summary = futures_minute.sync_futures_minute_odds_history(
            connection_factory=connection_factory,
            workers=1,
            batch_group_size=1,
            client_factory=lambda: object(),
            fetch_window_fn=fetch_window,
            persist_fn=lambda *_a, **_k: None,
            audit_persist_fn=lambda *_a, **_k: None,
        )
    finally:
        conn.close()

    assert summary["status"] == "published"
    # plan selection, audit write, then publish (shard build is unlocked).
    assert borrow_count["n"] == 3
    assert shard_open_depth == [0]
    assert not (tmp_path / "minute-odds-publish" / summary["fetch_run_id"]).exists()


def test_sync_futures_minute_odds_history_publishes_on_full_success():
    import pyarrow.parquet as pq

    conn = _futures_inventory_connection()
    audit_rows: list[dict] = []
    published: list[tuple] = []
    point_ts = int(datetime(2026, 6, 11, tzinfo=timezone.utc).timestamp())

    def fetch_window(
        _client,
        token_id,
        start_ts,
        end_ts,
        *_args,
        **_kwargs,
    ):
        if int(start_ts) <= point_ts <= int(end_ts):
            return [(token_id, point_ts, 0.55)]
        return []

    def audit_persist(rows, _conn):
        audit_rows.extend(rows)

    def persist(rows, _conn, *, fetch_run_id):
        table = pq.read_table(rows[0])
        published.append((fetch_run_id, table))

    try:
        summary = futures_minute.sync_futures_minute_odds_history(
            conn,
            workers=2,
            batch_group_size=1,
            client_factory=lambda: object(),
            fetch_window_fn=fetch_window,
            persist_fn=persist,
            audit_persist_fn=audit_persist,
        )
    finally:
        conn.close()

    assert summary["status"] == "published"
    assert summary["tokens"] == 2
    assert summary["success_tokens"] == 2
    assert len(audit_rows) == 2
    assert published
    fetch_run_id, table = published[0]
    assert table.num_rows == 2
    assert "clobTokenId" in table.column_names
    assert fetch_run_id == summary["fetch_run_id"]

def test_sync_futures_minute_odds_history_publishes_success_and_skips_empty():
    conn = _futures_inventory_connection()
    audit_rows: list[dict] = []
    published: list[tuple] = []
    log = MagicMock()
    point_ts = int(datetime(2026, 6, 11, tzinfo=timezone.utc).timestamp())

    def fetch_window(
        _client,
        token_id,
        start_ts,
        end_ts,
        *_args,
        **_kwargs,
    ):
        if token_id.endswith("-no"):
            return []
        if int(start_ts) <= point_ts <= int(end_ts):
            return [(token_id, point_ts, 0.55)]
        return []

    try:
        summary = futures_minute.sync_futures_minute_odds_history(
            conn,
            log=log,
            workers=2,
            batch_group_size=1,
            client_factory=lambda: object(),
            fetch_window_fn=fetch_window,
            persist_fn=lambda rows, _conn, *, fetch_run_id: published.append(
                (fetch_run_id, len(rows))
            ),
            audit_persist_fn=lambda rows, _conn: audit_rows.extend(rows),
        )
    finally:
        conn.close()

    assert summary["status"] == "published"
    assert summary["tokens"] == 2
    assert summary["success_tokens"] == 1
    assert summary["empty_tokens"] == 1
    assert summary["raw_published_tokens"] == 1
    assert {row["fetch_status"] for row in audit_rows} == {"success", "empty"}
    assert published and published[0][1] == 1
    info_messages = [call.args[0] % call.args[1:] for call in log.info.call_args_list]
    assert any(
        "Futures CLOB fetch done; entering DuckDB audit/publish" in msg
        for msg in info_messages
    )
    assert any("writing fetch audit" in msg for msg in info_messages)
    assert any("staging/publishing" in msg for msg in info_messages)
    assert any("empty in-window history" in msg for msg in info_messages)
    assert any("Futures-minute published" in msg for msg in info_messages)


def test_sync_futures_minute_odds_history_fail_closed_when_all_empty():
    conn = _futures_inventory_connection()

    def fetch_window(*_args, **_kwargs):
        return []

    try:
        with pytest.raises(futures_minute.FuturesMinuteSyncError) as excinfo:
            futures_minute.sync_futures_minute_odds_history(
                conn,
                workers=1,
                batch_group_size=1,
                client_factory=lambda: object(),
                fetch_window_fn=fetch_window,
                persist_fn=lambda *_a, **_k: None,
                audit_persist_fn=lambda *_a, **_k: None,
            )
    finally:
        conn.close()

    assert excinfo.value.summary["status"] == "fetch_failed"
    assert excinfo.value.summary["empty_tokens"] == 2
    assert "No successful futures-minute" in str(excinfo.value)


def test_sync_futures_minute_odds_history_fail_closed_on_error():
    conn = _futures_inventory_connection()

    def fetch_window(*_args, **_kwargs):
        raise RuntimeError("boom")

    try:
        with pytest.raises(futures_minute.FuturesMinuteSyncError) as excinfo:
            futures_minute.sync_futures_minute_odds_history(
                conn,
                workers=1,
                batch_group_size=1,
                client_factory=lambda: object(),
                fetch_window_fn=fetch_window,
                persist_fn=lambda *_a, **_k: None,
                audit_persist_fn=lambda *_a, **_k: None,
            )
    finally:
        conn.close()

    assert excinfo.value.summary["status"] == "fetch_failed"
    assert excinfo.value.summary["error_tokens"] == 2


def test_sync_futures_minute_odds_history_publishes_via_batch_path():
    import pyarrow.parquet as pq

    conn = _futures_inventory_connection()
    audit_rows: list[dict] = []
    published: list[tuple] = []
    group_calls: list[tuple[str, ...]] = []
    point_ts = int(datetime(2026, 6, 12, tzinfo=timezone.utc).timestamp())
    emitted: set[str] = set()

    def fetch_group(
        _client,
        token_ids,
        window_start,
        window_end,
        *_args,
    ):
        group_calls.append(tuple(token_ids))
        out: dict[str, list] = {}
        for token_id in token_ids:
            if (
                token_id not in emitted
                and int(window_start) <= point_ts <= int(window_end)
            ):
                out[token_id] = [(token_id, point_ts, 0.61)]
                emitted.add(token_id)
            else:
                out[token_id] = []
        return out

    try:
        summary = futures_minute.sync_futures_minute_odds_history(
            conn,
            workers=2,
            batch_group_size=20,
            client_factory=lambda: object(),
            fetch_group_window_fn=fetch_group,
            persist_fn=lambda rows, _conn, *, fetch_run_id: published.append(
                (fetch_run_id, sum(pq.ParquetFile(path).metadata.num_rows for path in rows))
            ),
            audit_persist_fn=lambda rows, _conn: audit_rows.extend(rows),
        )
    finally:
        conn.close()

    assert summary["status"] == "published"
    assert summary["success_tokens"] == 2
    assert group_calls
    assert set(group_calls[0]) == {"futures-1-yes", "futures-1-no"}
    assert len(audit_rows) == 2
    assert published and published[0][1] == 2


def test_sync_futures_minute_odds_history_batch_publishes_with_partial_empty():
    import pyarrow.parquet as pq

    conn = _futures_inventory_connection()
    audit_rows: list[dict] = []
    published: list[tuple] = []
    point_ts = int(datetime(2026, 6, 12, tzinfo=timezone.utc).timestamp())
    emitted: set[str] = set()

    def fetch_group(_client, token_ids, window_start, window_end, *_args):
        out: dict[str, list] = {}
        for token_id in token_ids:
            if (
                token_id.endswith("-yes")
                and token_id not in emitted
                and int(window_start) <= point_ts <= int(window_end)
            ):
                out[token_id] = [(token_id, point_ts, 0.61)]
                emitted.add(token_id)
            else:
                out[token_id] = []
        return out

    try:
        summary = futures_minute.sync_futures_minute_odds_history(
            conn,
            workers=1,
            batch_group_size=20,
            client_factory=lambda: object(),
            fetch_group_window_fn=fetch_group,
            persist_fn=lambda rows, _conn, *, fetch_run_id: published.append(
                (fetch_run_id, sum(pq.ParquetFile(path).metadata.num_rows for path in rows))
            ),
            audit_persist_fn=lambda rows, _conn: audit_rows.extend(rows),
        )
    finally:
        conn.close()

    assert summary["status"] == "published"
    assert summary["success_tokens"] == 1
    assert summary["empty_tokens"] == 1
    assert summary["raw_published_tokens"] == 1
    assert published and published[0][1] == 1
    assert {row["fetch_status"] for row in audit_rows} == {"success", "empty"}


def test_sync_futures_minute_odds_history_batch_fail_closed_when_all_empty():
    conn = _futures_inventory_connection()

    def fetch_group(*_args, **_kwargs):
        return {"futures-1-yes": [], "futures-1-no": []}

    try:
        with pytest.raises(futures_minute.FuturesMinuteSyncError) as excinfo:
            futures_minute.sync_futures_minute_odds_history(
                conn,
                workers=1,
                batch_group_size=20,
                client_factory=lambda: object(),
                fetch_group_window_fn=fetch_group,
                persist_fn=lambda *_a, **_k: None,
                audit_persist_fn=lambda *_a, **_k: None,
            )
    finally:
        conn.close()

    assert excinfo.value.summary["status"] == "fetch_failed"
    assert excinfo.value.summary["empty_tokens"] == 2
