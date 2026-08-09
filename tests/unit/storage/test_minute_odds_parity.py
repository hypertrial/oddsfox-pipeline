"""Parity + recovery coverage for parquet-first minute-odds snapshots."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from oddsfox_pipeline.storage.minute_odds_snapshots import (
    MinuteOddsSnapshotError,
    PublishedTokenWindow,
    active_snapshot_id,
    build_and_publish_snapshot_from_shards,
    compute_primary_minute_ohlc,
    minute_odds_snapshot_root,
    stage_snapshot_dir,
    tokens_reusable_by_window,
    validate_minute_odds_snapshot,
    write_snapshot_partitions_incremental,
)


def _shard(path: Path, *, token: str, market: str, ts: int, price: float) -> Path:
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    end = datetime(2026, 7, 2, tzinfo=timezone.utc)
    ingested = datetime(2026, 7, 3, tzinfo=timezone.utc)
    table = pa.table(
        {
            "market_id": [market],
            "clobTokenId": [token],
            "timestamp": pa.array([ts], type=pa.int64()),
            "price": pa.array([price], type=pa.float64()),
            "fidelity_minutes": pa.array([1], type=pa.int32()),
            "window_start_at": pa.array(
                [start.replace(tzinfo=None)], type=pa.timestamp("us")
            ),
            "window_end_at": pa.array(
                [end.replace(tzinfo=None)], type=pa.timestamp("us")
            ),
            "ingested_at": pa.array(
                [ingested.replace(tzinfo=None)], type=pa.timestamp("us")
            ),
        }
    )
    pq.write_table(table, path)
    return path


def test_primary_ohlc_matches_arg_min_arg_max_sql():
    start = datetime(2026, 7, 1, 12, 0, 0)
    end = datetime(2026, 7, 1, 12, 5, 0)
    rows = [
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
            "clobTokenId": "yes",
            "timestamp": 1782907255,
            "price": 0.3,
            "window_start_at": start,
            "window_end_at": end,
        },
    ]
    polars_out = compute_primary_minute_ohlc(rows, primary_token_ids={"yes"}).to_dicts()
    with duckdb.connect() as conn:
        conn.register(
            "raw_points",
            pa.Table.from_pylist(
                [
                    {
                        "market_id": row["market_id"],
                        "clob_token_id": row["clobTokenId"],
                        "odds_timestamp_epoch": row["timestamp"],
                        "price": row["price"],
                        "window_start_at": row["window_start_at"],
                        "window_end_at": row["window_end_at"],
                    }
                    for row in rows
                ]
            ),
        )
        sql_rows = conn.execute(
            """
            SELECT
                market_id,
                clob_token_id,
                (odds_timestamp_epoch // 60) * 60 AS odds_minute_epoch,
                arg_min(price, odds_timestamp_epoch) AS open_price,
                max(price) AS high_price,
                min(price) AS low_price,
                arg_max(price, odds_timestamp_epoch) AS close_price,
                round(avg(price), 8) AS avg_price,
                count(*) AS observed_points
            FROM raw_points
            WHERE clob_token_id = 'yes'
              AND odds_timestamp_epoch >= epoch(window_start_at)
              AND odds_timestamp_epoch <= epoch(window_end_at)
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
            """
        ).fetchall()
    assert len(polars_out) == 1
    assert len(sql_rows) == 1
    sql_out = {
        "odds_minute_epoch": sql_rows[0][2],
        "open_price": sql_rows[0][3],
        "high_price": sql_rows[0][4],
        "low_price": sql_rows[0][5],
        "close_price": sql_rows[0][6],
        "avg_price": sql_rows[0][7],
        "observed_points": sql_rows[0][8],
    }
    for key in (
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "avg_price",
        "observed_points",
        "odds_minute_epoch",
    ):
        assert polars_out[0][key] == pytest.approx(sql_out[key])


def test_changed_bucket_rebuild_copies_unchanged_partitions(tmp_path, monkeypatch):
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path))
    ts = int(datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp())
    shard_a = _shard(
        tmp_path / "a.parquet", token="tok-a", market="m1", ts=ts, price=0.2
    )
    first = build_and_publish_snapshot_from_shards(
        leg="futures",
        fetch_run_id="run-1",
        shard_paths=[
            shard_a,
            _shard(
                tmp_path / "b.parquet", token="tok-b", market="m2", ts=ts, price=0.8
            ),
        ],
        primary_token_ids={"tok-a", "tok-b"},
        register=False,
    )
    root = minute_odds_snapshot_root(leg="futures")
    staged = stage_snapshot_dir(root, "run-2-incremental")
    shard_c = _shard(
        tmp_path / "c.parquet", token="tok-b", market="m2", ts=ts + 60, price=0.9
    )
    raw_files, primary_files, dirty = write_snapshot_partitions_incremental(
        staged,
        [shard_c],
        previous=first,
        reuse_token_ids={"tok-a"},
        primary_token_ids={"tok-a", "tok-b"},
    )
    assert dirty
    assert raw_files
    assert primary_files
    tokens = {token for part in raw_files for token in part.token_ids}
    assert tokens == {"tok-a", "tok-b"}


def test_failed_publish_leaves_current_pointer(tmp_path, monkeypatch):
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path))
    ts = int(datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp())
    first = build_and_publish_snapshot_from_shards(
        leg="futures",
        fetch_run_id="run-ok",
        shard_paths=[
            _shard(
                tmp_path / "ok.parquet", token="tok-a", market="m1", ts=ts, price=0.2
            )
        ],
        primary_token_ids={"tok-a"},
        register=False,
    )
    root = minute_odds_snapshot_root(leg="futures")
    assert active_snapshot_id(root) == first.snapshot_id
    bad = tmp_path / "bad.parquet"
    _shard(bad, token="tok-b", market="m2", ts=ts, price=0.3)
    # Corrupt fidelity after write.
    table = pq.read_table(bad)
    fidelity_idx = table.schema.get_field_index("fidelity_minutes")
    bad_table = table.set_column(
        fidelity_idx,
        "fidelity_minutes",
        pa.array([2], type=pa.int32()),
    )
    pq.write_table(bad_table, bad)
    with pytest.raises(MinuteOddsSnapshotError, match="fidelity_minutes"):
        build_and_publish_snapshot_from_shards(
            leg="futures",
            fetch_run_id="run-bad",
            shard_paths=[bad],
            primary_token_ids={"tok-b"},
            register=False,
        )
    assert active_snapshot_id(root) == first.snapshot_id
    assert (
        validate_minute_odds_snapshot(first.directory).snapshot_id == first.snapshot_id
    )
    staging = root / "staging"
    assert not any(staging.iterdir()) if staging.exists() else True


def test_tokens_reusable_by_window_requires_matching_bounds():
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    end = datetime(2026, 7, 2, tzinfo=timezone.utc)

    class Plan:
        def __init__(self, token_id: str):
            self.token_id = token_id
            self.started_at = start
            self.finished_at = end

    class Snap:
        token_ids = ("tok-a", "tok-b")

    published = {
        "tok-a": PublishedTokenWindow(
            token_id="tok-a",
            market_id="m1",
            window_start_at=start,
            window_end_at=end,
            history_sha256="a" * 64,
            row_count=10,
        ),
        "tok-b": PublishedTokenWindow(
            token_id="tok-b",
            market_id="m2",
            window_start_at=start,
            window_end_at=end.replace(day=3),
            history_sha256="b" * 64,
            row_count=10,
        ),
    }
    assert tokens_reusable_by_window(
        [Plan("tok-a"), Plan("tok-b")],
        previous=Snap(),  # type: ignore[arg-type]
        published_windows=published,
    ) == {"tok-a"}
