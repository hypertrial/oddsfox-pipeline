from datetime import datetime, timezone

import duckdb
import pytest

from oddsfox_pipeline.storage.duckdb.dlt_batch import (
    load_futures_minute_fetch_audit,
    load_futures_minute_odds_history_stage,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    bootstrap_all_polymarket_tables,
)


def test_futures_minute_raw_table_is_wc2026_only():
    with duckdb.connect(":memory:") as conn:
        conn.execute("create schema polymarket_wc2026_raw")
        conn.execute("create schema polymarket_wc2026_ops")
        bootstrap_all_polymarket_tables(conn)

        rows = conn.execute(
            """
            select table_schema
            from information_schema.tables
            where table_name = 'futures_minute_odds_history'
            """
        ).fetchall()

    assert rows == [("polymarket_wc2026_raw",)]


def test_futures_minute_raw_replace_is_exact_idempotent_and_isolated(duck):
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    row = {
        "market_id": "market",
        "clobTokenId": "token",
        "timestamp": 100,
        "price": 0.4,
        "fidelity_minutes": 1,
        "window_start_at": now,
        "window_end_at": now,
        "ingested_at": now,
    }

    def audit(run_id: str) -> dict[str, object]:
        return {
            "fetch_run_id": run_id,
            "market_id": "market",
            "clobTokenId": "token",
            "fetch_status": "success",
            "raw_published": False,
            "fidelity_minutes": 1,
            "exact_window_start_at": now,
            "exact_window_end_at": now,
            "request_start_epoch": 100,
            "request_end_epoch": 100,
            "source_row_count": 1,
            "window_row_count": 1,
            "window_history_sha256": "a" * 64,
            "source_endpoint": "https://clob.polymarket.com/prices-history",
            "fetch_started_at": now,
            "fetch_finished_at": now,
            "error_type": None,
            "error_message": None,
        }

    with duck.get_connection() as conn:
        load_futures_minute_fetch_audit([audit("run-1")], conn)
        load_futures_minute_odds_history_stage(
            [row, {**row, "timestamp": 101}], conn, fetch_run_id="run-1"
        )
        load_futures_minute_fetch_audit([audit("run-2")], conn)
        load_futures_minute_odds_history_stage(
            [{**row, "price": 0.5}], conn, fetch_run_id="run-2"
        )
        with pytest.raises(RuntimeError, match="Fetch audit inventory"):
            load_futures_minute_odds_history_stage(
                [{**row, "price": 0.9}], conn, fetch_run_id="missing-audit"
            )

        prices = conn.execute(
            """
            select price
            from polymarket_wc2026_raw.futures_minute_odds_history
            order by timestamp
            """
        ).fetchall()
        assert prices == [(0.5,)]

        hourly = conn.execute(
            "select count(*) from polymarket_wc2026_raw.odds_history"
        ).fetchone()[0]
        assert hourly == 0


def test_futures_minute_publish_allows_empty_audit_siblings(duck):
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    success_row = {
        "market_id": "market-a",
        "clobTokenId": "token-a",
        "timestamp": 100,
        "price": 0.4,
        "fidelity_minutes": 1,
        "window_start_at": now,
        "window_end_at": now,
        "ingested_at": now,
    }

    def audit(token_id: str, status: str) -> dict[str, object]:
        return {
            "fetch_run_id": "run-empty-ok",
            "market_id": f"market-{token_id[-1]}",
            "clobTokenId": token_id,
            "fetch_status": status,
            "raw_published": False,
            "fidelity_minutes": 1,
            "exact_window_start_at": now,
            "exact_window_end_at": now,
            "request_start_epoch": 100,
            "request_end_epoch": 100,
            "source_row_count": 1 if status == "success" else 0,
            "window_row_count": 1 if status == "success" else 0,
            "window_history_sha256": "a" * 64 if status == "success" else None,
            "source_endpoint": "https://clob.polymarket.com/prices-history",
            "fetch_started_at": now,
            "fetch_finished_at": now,
            "error_type": None if status == "success" else "EmptyHistory",
            "error_message": None
            if status == "success"
            else f"Empty in-window CLOB history for token {token_id}",
        }

    with duck.get_connection() as conn:
        load_futures_minute_fetch_audit(
            [audit("token-a", "success"), audit("token-b", "empty")], conn
        )
        load_futures_minute_odds_history_stage(
            [success_row], conn, fetch_run_id="run-empty-ok"
        )
        published = conn.execute(
            """
            select clobTokenId, fetch_status, raw_published
            from polymarket_wc2026_ops.futures_minute_odds_fetch_audit
            where fetch_run_id = 'run-empty-ok'
            order by clobTokenId
            """
        ).fetchall()
        assert published == [
            ("token-a", "success", True),
            ("token-b", "empty", False),
        ]
        assert (
            conn.execute(
                "select count(*) from polymarket_wc2026_raw.futures_minute_odds_history"
            ).fetchone()[0]
            == 1
        )


def test_futures_minute_raw_replace_accepts_arrow_table(duck):
    import pyarrow as pa

    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    table = pa.table(
        {
            "market_id": pa.array(["market"], type=pa.string()),
            "clob_token_id": pa.array(["token"], type=pa.string()),
            "timestamp": pa.array([100], type=pa.int64()),
            "price": pa.array([0.4], type=pa.float64()),
            "fidelity_minutes": pa.array([1], type=pa.int32()),
            "window_start_at": pa.array([now], type=pa.timestamp("us", tz="UTC")),
            "window_end_at": pa.array([now], type=pa.timestamp("us", tz="UTC")),
            "ingested_at": pa.array([now], type=pa.timestamp("us", tz="UTC")),
            "row_order": pa.array([0], type=pa.int64()),
        }
    )
    audit = {
        "fetch_run_id": "run-arrow",
        "market_id": "market",
        "clobTokenId": "token",
        "fetch_status": "success",
        "raw_published": False,
        "fidelity_minutes": 1,
        "exact_window_start_at": now,
        "exact_window_end_at": now,
        "request_start_epoch": 100,
        "request_end_epoch": 100,
        "source_row_count": 1,
        "window_row_count": 1,
        "window_history_sha256": "a" * 64,
        "source_endpoint": "https://clob.polymarket.com/prices-history",
        "fetch_started_at": now,
        "fetch_finished_at": now,
        "error_type": None,
        "error_message": None,
    }

    with duck.get_connection() as conn:
        load_futures_minute_fetch_audit([audit], conn)
        load_futures_minute_odds_history_stage(table, conn, fetch_run_id="run-arrow")
        prices = conn.execute(
            """
            select price
            from polymarket_wc2026_raw.futures_minute_odds_history
            order by timestamp
            """
        ).fetchall()
        published = conn.execute(
            """
            select raw_published
            from polymarket_wc2026_ops.futures_minute_odds_fetch_audit
            where fetch_run_id = 'run-arrow'
            """
        ).fetchone()[0]

    assert prices == [(0.4,)]
    assert published is True


def _futures_minute_audit(
    run_id: str,
    *,
    token: str,
    market: str,
    now: datetime,
) -> dict[str, object]:
    return {
        "fetch_run_id": run_id,
        "market_id": market,
        "clobTokenId": token,
        "fetch_status": "success",
        "raw_published": False,
        "fidelity_minutes": 1,
        "exact_window_start_at": now,
        "exact_window_end_at": now,
        "request_start_epoch": 100,
        "request_end_epoch": 100,
        "source_row_count": 1,
        "window_row_count": 1,
        "window_history_sha256": "a" * 64,
        "source_endpoint": "https://clob.polymarket.com/prices-history",
        "fetch_started_at": now,
        "fetch_finished_at": now,
        "error_type": None,
        "error_message": None,
    }


def test_futures_minute_publish_from_parquet_registers_snapshot_views(
    duck, tmp_path, monkeypatch
):
    import pyarrow as pa
    import pyarrow.parquet as pq

    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path))
    start = datetime(2026, 6, 11, tzinfo=timezone.utc)
    end = datetime(2026, 7, 19, tzinfo=timezone.utc)
    ingested = datetime(2026, 7, 20, tzinfo=timezone.utc)
    ts = int(datetime(2026, 7, 1, tzinfo=timezone.utc).timestamp())
    table = pa.table(
        {
            "market_id": ["market"],
            "clob_token_id": ["token"],
            "timestamp": pa.array([ts], type=pa.int64()),
            "price": pa.array([0.4], type=pa.float64()),
            "fidelity_minutes": pa.array([1], type=pa.int32()),
            "window_start_at": pa.array([start], type=pa.timestamp("us", tz="UTC")),
            "window_end_at": pa.array([end], type=pa.timestamp("us", tz="UTC")),
            "ingested_at": pa.array([ingested], type=pa.timestamp("us", tz="UTC")),
        }
    )
    shard = tmp_path / "shard.parquet"
    pq.write_table(table, shard)
    audit = _futures_minute_audit(
        "run-parquet", token="token", market="market", now=ingested
    )
    with duck.get_connection() as conn:
        load_futures_minute_fetch_audit([audit], conn)
        load_futures_minute_odds_history_stage(
            [shard], conn, fetch_run_id="run-parquet"
        )
        assert (
            conn.execute(
                "select count(*) from polymarket_wc2026_raw.futures_minute_odds_history"
            ).fetchone()[0]
            == 1
        )
        assert (
            conn.execute(
                """
                select count(*)
                from polymarket_wc2026_raw.futures_primary_minute_ohlc
                """
            ).fetchone()[0]
            == 1
        )
        assert (
            conn.execute(
                """
                select raw_published
                from polymarket_wc2026_ops.futures_minute_odds_fetch_audit
                where fetch_run_id = 'run-parquet'
                """
            ).fetchone()[0]
            is True
        )


def test_futures_minute_publish_rejects_understated_manifest_token_ids(
    duck, tmp_path, monkeypatch
):
    import json

    import pyarrow as pa
    import pyarrow.parquet as pq

    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path))
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    table = pa.table(
        {
            "market_id": ["market-a", "market-b"],
            "clobTokenId": ["token-a", "token-b"],
            "timestamp": pa.array([100, 100], type=pa.int64()),
            "price": pa.array([0.4, 0.5], type=pa.float64()),
            "fidelity_minutes": pa.array([1, 1], type=pa.int32()),
            "window_start_at": pa.array([now, now], type=pa.timestamp("us", tz="UTC")),
            "window_end_at": pa.array([now, now], type=pa.timestamp("us", tz="UTC")),
            "ingested_at": pa.array([now, now], type=pa.timestamp("us", tz="UTC")),
        }
    )
    shard_dir = tmp_path / "run-understated"
    shard_dir.mkdir()
    shard = shard_dir / "shard-00000.parquet"
    pq.write_table(table, shard)
    (shard_dir / "manifest.json").write_text(
        json.dumps(
            {
                "fetch_run_id": "run-understated",
                "token_count": 1,
                "token_ids": ["token-a"],
                "row_count": 2,
                "shard_count": 1,
            }
        ),
        encoding="utf-8",
    )
    audit = _futures_minute_audit(
        "run-understated", token="token-a", market="market-a", now=now
    )
    with duck.get_connection() as conn:
        load_futures_minute_fetch_audit([audit], conn)
        with pytest.raises(RuntimeError, match="exceeds manifest|token_ids"):
            load_futures_minute_odds_history_stage(
                [shard], conn, fetch_run_id="run-understated"
            )
        assert (
            conn.execute(
                "select count(*) from polymarket_wc2026_raw.futures_minute_odds_history"
            ).fetchone()[0]
            == 0
        )


def test_futures_minute_publish_from_writer_shards_and_manifest(
    duck, tmp_path, monkeypatch
):
    from oddsfox_pipeline.ingestion.polymarket.odds import minute_batch

    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path))
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    start = now
    end = now

    class _Plan:
        def __init__(self, token: str) -> None:
            self.market_id = f"market-{token}"
            self.token_id = token
            self.started_at = start
            self.finished_at = end

    results = [
        minute_batch.MinuteFetchResult(
            plan=_Plan(token),
            fetch_status="success",
            history=((token, 100 + index, 0.4 + index / 10),),
            request_start_epoch=100,
            request_end_epoch=100,
            source_row_count=1,
            history_sha256="a" * 64,
            fetch_started_at=now,
            fetch_finished_at=now,
        )
        for index, token in enumerate(("token-a", "token-b"))
    ]
    shards = minute_batch.write_minute_history_parquet_shards(
        results,
        fetch_run_id="run-writer",
        ingested_at=now,
        max_rows_per_shard=1,
        batch_rows=1,
    )
    assert len(shards) == 2
    audits = [
        _futures_minute_audit(
            "run-writer", token=token, market=f"market-{token}", now=now
        )
        for token in ("token-a", "token-b")
    ]
    with duck.get_connection() as conn:
        load_futures_minute_fetch_audit(audits, conn)
        load_futures_minute_odds_history_stage(shards, conn, fetch_run_id="run-writer")
        tokens = conn.execute(
            """
            select "clobTokenId"
            from polymarket_wc2026_raw.futures_minute_odds_history
            order by 1
            """
        ).fetchall()
        assert tokens == [("token-a",), ("token-b",)]
        published = conn.execute(
            """
            select count(*)
            from polymarket_wc2026_ops.futures_minute_odds_fetch_audit
            where fetch_run_id = 'run-writer' and raw_published
            """
        ).fetchone()[0]
        assert published == 2


def test_futures_minute_publish_rolls_back_on_constraint_failure(duck):
    from oddsfox_pipeline.storage.minute_odds_snapshots import MinuteOddsSnapshotError

    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    good = {
        "market_id": "market",
        "clobTokenId": "token",
        "timestamp": 100,
        "price": 0.4,
        "fidelity_minutes": 1,
        "window_start_at": now,
        "window_end_at": now,
        "ingested_at": now,
    }
    bad = {**good, "fidelity_minutes": 2}

    def audit(run_id: str) -> dict[str, object]:
        return {
            "fetch_run_id": run_id,
            "market_id": "market",
            "clobTokenId": "token",
            "fetch_status": "success",
            "raw_published": False,
            "fidelity_minutes": 1,
            "exact_window_start_at": now,
            "exact_window_end_at": now,
            "request_start_epoch": 100,
            "request_end_epoch": 100,
            "source_row_count": 1,
            "window_row_count": 1,
            "window_history_sha256": "a" * 64,
            "source_endpoint": "https://clob.polymarket.com/prices-history",
            "fetch_started_at": now,
            "fetch_finished_at": now,
            "error_type": None,
            "error_message": None,
        }

    with duck.get_connection() as conn:
        load_futures_minute_fetch_audit([audit("run-good")], conn)
        load_futures_minute_odds_history_stage([good], conn, fetch_run_id="run-good")
        load_futures_minute_fetch_audit([audit("run-bad")], conn)
        with pytest.raises(MinuteOddsSnapshotError, match="fidelity_minutes"):
            load_futures_minute_odds_history_stage([bad], conn, fetch_run_id="run-bad")
        prices = conn.execute(
            "select price from polymarket_wc2026_raw.futures_minute_odds_history"
        ).fetchall()
        assert prices == [(0.4,)]
        published = conn.execute(
            """
            select fetch_run_id, raw_published
            from polymarket_wc2026_ops.futures_minute_odds_fetch_audit
            order by fetch_run_id
            """
        ).fetchall()
        assert published == [("run-bad", False), ("run-good", True)]


def test_futures_minute_publish_dedupes_duplicate_timestamps(
    duck, tmp_path, monkeypatch
):
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path))
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    row = {
        "market_id": "market",
        "clobTokenId": "token",
        "timestamp": 100,
        "price": 0.4,
        "fidelity_minutes": 1,
        "window_start_at": now,
        "window_end_at": now,
        "ingested_at": now,
    }
    audit = {
        "fetch_run_id": "run-dup",
        "market_id": "market",
        "clobTokenId": "token",
        "fetch_status": "success",
        "raw_published": False,
        "fidelity_minutes": 1,
        "exact_window_start_at": now,
        "exact_window_end_at": now,
        "request_start_epoch": 100,
        "request_end_epoch": 100,
        "source_row_count": 2,
        "window_row_count": 2,
        "window_history_sha256": "a" * 64,
        "source_endpoint": "https://clob.polymarket.com/prices-history",
        "fetch_started_at": now,
        "fetch_finished_at": now,
        "error_type": None,
        "error_message": None,
    }
    with duck.get_connection() as conn:
        load_futures_minute_fetch_audit([audit], conn)
        load_futures_minute_odds_history_stage(
            [row, {**row, "price": 0.9}], conn, fetch_run_id="run-dup"
        )
        assert (
            conn.execute(
                "select count(*) from polymarket_wc2026_raw.futures_minute_odds_history"
            ).fetchone()[0]
            == 1
        )
        assert conn.execute(
            "select price from polymarket_wc2026_raw.futures_minute_odds_history"
        ).fetchone() == (0.9,)
        assert (
            conn.execute(
                """
                select raw_published
                from polymarket_wc2026_ops.futures_minute_odds_fetch_audit
                where fetch_run_id = 'run-dup'
                """
            ).fetchone()[0]
            is True
        )
