"""Unit coverage for immutable minute-odds Parquet snapshots + primary OHLC."""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from oddsfox_pipeline.storage.minute_odds_snapshots import (
    MinuteOddsSnapshotError,
    active_snapshot_id,
    backfill_primary_ohlc_table,
    build_and_publish_snapshot_from_shards,
    compute_primary_minute_ohlc,
    minute_odds_snapshot_root,
    primary_mapping_sha256,
    retain_snapshots,
    reusable_token_ids,
    rollback_snapshot_pointer,
    token_bucket,
    validate_minute_odds_snapshot,
)


def test_token_bucket_is_stable():
    assert token_bucket("tok-a", bucket_count=8) == token_bucket("tok-a", bucket_count=8)
    assert 0 <= token_bucket("tok-b", bucket_count=8) < 8


def test_compute_primary_minute_ohlc_filters_window_and_non_primary():
    start = datetime(2026, 7, 1, 12, 0, 30)
    end = datetime(2026, 7, 1, 12, 2, 35)
    rows = [
        {
            "market_id": "m1",
            "clobTokenId": "yes",
            "timestamp": 1782907229,  # before window
            "price": 0.1,
            "window_start_at": start,
            "window_end_at": end,
        },
        {
            "market_id": "m1",
            "clobTokenId": "yes",
            "timestamp": 1782907230,
            "price": 0.2,
            "window_start_at": start,
            "window_end_at": end,
        },
        {
            "market_id": "m1",
            "clobTokenId": "yes",
            "timestamp": 1782907250,
            "price": 0.4,
            "window_start_at": start,
            "window_end_at": end,
        },
        {
            "market_id": "m1",
            "clobTokenId": "no",
            "timestamp": 1782907290,
            "price": 0.6,
            "window_start_at": start,
            "window_end_at": end,
        },
        {
            "market_id": "m1",
            "clobTokenId": "yes",
            "timestamp": 1782907356,  # after window
            "price": 0.9,
            "window_start_at": start,
            "window_end_at": end,
        },
    ]
    out = compute_primary_minute_ohlc(rows, primary_token_ids={"yes"})
    assert out.height == 1
    row = out.to_dicts()[0]
    assert row["clob_token_id"] == "yes"
    assert row["odds_minute_epoch"] == 1782907200
    assert row["open_price"] == 0.2
    assert row["high_price"] == 0.4
    assert row["low_price"] == 0.2
    assert row["close_price"] == 0.4
    assert row["avg_price"] == 0.3
    assert row["observed_points"] == 2


def test_build_and_publish_snapshot_registers_views(tmp_path, monkeypatch):
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path))
    start = datetime(2026, 7, 1, 0, 0, 0)
    end = datetime(2026, 7, 2, 0, 0, 0)
    ingested = datetime(2026, 7, 3, 0, 0, 0)
    ts = int(datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp())
    shard_dir = tmp_path / "shards"
    shard_dir.mkdir()
    table = pa.table(
        {
            "market_id": ["m1", "m1"],
            "clobTokenId": ["yes", "no"],
            "timestamp": pa.array([ts, ts], type=pa.int64()),
            "price": pa.array([0.2, 0.8], type=pa.float64()),
            "fidelity_minutes": pa.array([1, 1], type=pa.int32()),
            "window_start_at": pa.array([start, start], type=pa.timestamp("us")),
            "window_end_at": pa.array([end, end], type=pa.timestamp("us")),
            "ingested_at": pa.array([ingested, ingested], type=pa.timestamp("us")),
        }
    )
    shard = shard_dir / "shard-00000.parquet"
    pq.write_table(table, shard)
    (shard_dir / "manifest.json").write_text(
        '{"fetch_run_id":"run-1","token_count":2,'
        '"token_ids":["no","yes"],"row_count":2,"shard_count":1}\n',
        encoding="utf-8",
    )

    db = tmp_path / "w.duckdb"
    with duckdb.connect(str(db)) as conn:
        conn.execute("CREATE SCHEMA polymarket_wc2026_raw")
        snapshot = build_and_publish_snapshot_from_shards(
            leg="futures",
            fetch_run_id="run-1",
            shard_paths=[shard],
            primary_token_ids={"yes"},
            conn=conn,
            register=True,
        )
        assert snapshot.raw_row_count == 2
        assert snapshot.primary_row_count == 1
        assert (
            conn.execute(
                "select count(*) from polymarket_wc2026_raw.futures_minute_odds_history"
            ).fetchone()[0]
            == 2
        )
        assert (
            conn.execute(
                """
                select clob_token_id
                from polymarket_wc2026_raw.futures_primary_minute_ohlc
                """
            ).fetchone()[0]
            == "yes"
        )

    root = minute_odds_snapshot_root(leg="futures", runtime_root=tmp_path)
    assert (root / "CURRENT").exists()
    validated = validate_minute_odds_snapshot(
        (root / "snapshots" / snapshot.snapshot_id)
    )
    assert validated.primary_mapping_sha256 == primary_mapping_sha256(["yes"])
    kept = retain_snapshots(root, keep=2)
    assert snapshot.snapshot_id in kept


def test_rollback_snapshot_pointer_restores_predecessor(tmp_path, monkeypatch):
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path))
    start = datetime(2026, 7, 1, 0, 0, 0)
    end = datetime(2026, 7, 2, 0, 0, 0)
    ingested = datetime(2026, 7, 3, 0, 0, 0)
    ts = int(datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp())

    def _shard(name: str, token: str) -> object:
        shard_dir = tmp_path / name
        shard_dir.mkdir()
        shard = shard_dir / "shard-00000.parquet"
        pq.write_table(
            pa.table(
                {
                    "market_id": ["m1"],
                    "clobTokenId": [token],
                    "timestamp": pa.array([ts], type=pa.int64()),
                    "price": pa.array([0.2], type=pa.float64()),
                    "fidelity_minutes": pa.array([1], type=pa.int32()),
                    "window_start_at": pa.array([start], type=pa.timestamp("us")),
                    "window_end_at": pa.array([end], type=pa.timestamp("us")),
                    "ingested_at": pa.array([ingested], type=pa.timestamp("us")),
                }
            ),
            shard,
        )
        return shard

    first = build_and_publish_snapshot_from_shards(
        leg="futures",
        fetch_run_id="run-a",
        shard_paths=[_shard("a", "yes")],
        primary_token_ids={"yes"},
        register=False,
        retain=False,
    )
    second = build_and_publish_snapshot_from_shards(
        leg="futures",
        fetch_run_id="run-b",
        shard_paths=[_shard("b", "yes")],
        primary_token_ids={"yes"},
        register=False,
        retain=False,
    )
    root = minute_odds_snapshot_root(leg="futures", runtime_root=tmp_path)
    assert active_snapshot_id(root) == second.snapshot_id
    rollback_snapshot_pointer(
        root,
        failed_snapshot_id=second.snapshot_id,
        previous_snapshot_id=first.snapshot_id,
    )
    assert active_snapshot_id(root) == first.snapshot_id
    assert (root / "snapshots" / second.snapshot_id).is_dir()


def test_reusable_token_ids_requires_matching_window_hash():
    class _Snap:
        token_ids = ("a", "b")

    assert reusable_token_ids(
        previous=_Snap(),  # type: ignore[arg-type]
        requested_token_ids={"a", "b", "c"},
        window_hashes={"a": "x", "b": "y"},
        previous_window_hashes={"a": "x", "b": "z"},
    ) == {"a"}


def test_backfill_primary_ohlc_table_from_history(tmp_path):
    db = tmp_path / "seed.duckdb"
    start = datetime(2026, 7, 1)
    end = datetime(2026, 7, 2)
    ingested = datetime(2026, 7, 3)
    ts = int(datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp())
    with duckdb.connect(str(db)) as conn:
        conn.execute("CREATE SCHEMA polymarket_wc2026_raw")
        conn.execute(
            """
            CREATE TABLE polymarket_wc2026_raw.futures_minute_odds_history (
                market_id TEXT,
                clobTokenId TEXT,
                timestamp BIGINT,
                price DOUBLE,
                fidelity_minutes INTEGER,
                window_start_at TIMESTAMP,
                window_end_at TIMESTAMP,
                ingested_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE polymarket_wc2026_raw.futures_primary_minute_ohlc (
                market_id TEXT,
                clob_token_id TEXT,
                odds_minute_epoch BIGINT,
                odds_minute_utc TIMESTAMP,
                open_price DOUBLE,
                high_price DOUBLE,
                low_price DOUBLE,
                close_price DOUBLE,
                avg_price DOUBLE,
                observed_points BIGINT,
                first_observed_at TIMESTAMP,
                last_observed_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO polymarket_wc2026_raw.futures_minute_odds_history VALUES
            ('m1','m1-yes',?,0.2,1,?,?,?),
            ('m1','m1-no',?,0.8,1,?,?,?)
            """,
            [ts, start, end, ingested, ts, start, end, ingested],
        )
        rows = backfill_primary_ohlc_table(conn, leg="futures")
        assert rows == 1
        assert (
            conn.execute(
                """
                select clob_token_id
                from polymarket_wc2026_raw.futures_primary_minute_ohlc
                """
            ).fetchone()[0]
            == "m1-yes"
        )


def test_reject_bad_fidelity_partition(tmp_path, monkeypatch):
    from oddsfox_pipeline.storage.minute_odds_snapshots import (
        minute_odds_snapshot_root,
        stage_snapshot_dir,
        write_snapshot_partitions_from_raw_parquet,
    )

    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path))
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    root = minute_odds_snapshot_root(leg="futures", runtime_root=tmp_path)
    staged = stage_snapshot_dir(root, "bad-fidelity")
    shard = tmp_path / "bad.parquet"
    pq.write_table(
        pa.table(
            {
                "market_id": ["m"],
                "clobTokenId": ["t"],
                "timestamp": pa.array([1], type=pa.int64()),
                "price": pa.array([0.1], type=pa.float64()),
                "fidelity_minutes": pa.array([5], type=pa.int32()),
                "window_start_at": pa.array([now], type=pa.timestamp("us")),
                "window_end_at": pa.array([now], type=pa.timestamp("us")),
                "ingested_at": pa.array([now], type=pa.timestamp("us")),
            }
        ),
        shard,
    )
    with pytest.raises(MinuteOddsSnapshotError, match="fidelity_minutes"):
        write_snapshot_partitions_from_raw_parquet(
            staged, [shard], primary_token_ids={"t"}
        )
