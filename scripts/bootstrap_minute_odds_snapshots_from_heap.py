#!/usr/bin/env python3
"""One-shot: promote existing minute heap tables into Parquet snapshots + views.

Use when a warehouse already has match/futures minute history as BASE TABLEs
(pre parquet-first) and needs ``CURRENT`` snapshots before the unified minute
dbt pass-through can run. Does not call Gamma/CLOB.

Streams through DuckDB (no full Polars load) so ~377M-row futures heaps fit.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import polars as pl
import pyarrow.parquet as pq

from oddsfox_pipeline.contracts.schema import schema_fingerprint
from oddsfox_pipeline.storage.duckdb.dlt_batch import _resolve_primary_token_ids
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    bootstrap_all_polymarket_tables,
)
from oddsfox_pipeline.storage.minute_odds_snapshots import (
    DEFAULT_TOKEN_BUCKET_COUNT,
    SnapshotPartitionFile,
    active_snapshot_dir,
    compute_primary_minute_ohlc,
    minute_odds_snapshot_root,
    publish_snapshot,
    reconcile_snapshot_publication,
    retain_snapshots,
    stage_snapshot_dir,
    token_bucket,
    validate_minute_odds_snapshot,
    write_manifest,
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _raw_partition_file(path: Path, *, bucket: int) -> SnapshotPartitionFile:
    """Fingerprint DuckDB-written raw parts the same way validate does.

    ``pq.read_table`` on a path under ``bucket=N/`` infers a hive ``bucket``
    column; validate uses ``ParquetFile.schema_arrow`` (file schema only).
    """
    parquet = pq.ParquetFile(path)
    # Unique tokens only — sorting every row's id OOMs/hangs write_manifest.
    tokens = tuple(
        sorted(
            {
                str(v)
                for v in parquet.read(columns=["clobTokenId"]).column(0).to_pylist()
                if v is not None
            }
        )
    )
    return SnapshotPartitionFile(
        kind="raw",
        bucket=bucket,
        path=path,
        sha256=_sha256_file(path),
        schema_fingerprint=schema_fingerprint(parquet.schema_arrow),
        row_count=int(parquet.metadata.num_rows),
        byte_size=path.stat().st_size,
        token_ids=tokens,
    )


def _write_primary_partition(
    frame: pl.DataFrame, path: Path, *, bucket: int
) -> SnapshotPartitionFile:
    """Match production ``_write_partition_parquet`` fingerprinting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    arrow = frame.to_arrow()
    pq.write_table(arrow, path, compression="snappy", write_statistics=False)
    tokens = tuple(sorted(str(v) for v in frame["clob_token_id"].unique().to_list()))
    return SnapshotPartitionFile(
        kind="primary_ohlc",
        bucket=bucket,
        path=path,
        sha256=_sha256_file(path),
        schema_fingerprint=schema_fingerprint(arrow.schema),
        row_count=frame.height,
        byte_size=path.stat().st_size,
        token_ids=tokens,
    )


def _bootstrap_leg(
    conn: duckdb.DuckDBPyConnection,
    *,
    leg: str,
    relation: str,
    work_root: Path,
    bucket_count: int = DEFAULT_TOKEN_BUCKET_COUNT,
) -> None:
    root = minute_odds_snapshot_root(leg=leg)
    if (root / "CURRENT").exists():
        print(f"{leg}: CURRENT already present at {root}; re-registering", flush=True)
        snap = validate_minute_odds_snapshot(active_snapshot_dir(root))
        summary = reconcile_snapshot_publication(conn, snap)
        print(f"{leg}: reconciled {summary}", flush=True)
        return

    total = int(
        conn.execute(
            f'select count(*) from polymarket_wc2026_raw."{relation}"'
        ).fetchone()[0]
    )
    if total < 1:
        raise SystemExit(f"{relation} is empty; nothing to bootstrap")

    token_market_rows = [
        (str(market_id), str(token))
        for market_id, token in conn.execute(
            f"""
            select distinct market_id, "clobTokenId"
            from polymarket_wc2026_raw."{relation}"
            order by 1, 2
            """
        ).fetchall()
    ]
    tokens = sorted({token for _, token in token_market_rows})
    print(f"{leg}: {total} rows / {len(tokens)} tokens", flush=True)

    # Resolve primary tokens from heap pairs (no full parquet export).
    primary = _resolve_primary_token_ids(
        conn, [], extra_token_market_rows=token_market_rows
    )
    print(f"{leg}: primary tokens={len(primary)}", flush=True)

    conn.execute("DROP TABLE IF EXISTS _bootstrap_token_buckets")
    conn.execute(
        """
        CREATE TEMP TABLE _bootstrap_token_buckets (
            token VARCHAR PRIMARY KEY,
            bucket INTEGER NOT NULL
        )
        """
    )
    conn.executemany(
        "INSERT INTO _bootstrap_token_buckets VALUES (?, ?)",
        [(token, token_bucket(token, bucket_count=bucket_count)) for token in tokens],
    )

    snapshot_id = (
        f"bootstrap-{leg}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    )
    staged = stage_snapshot_dir(root, snapshot_id)
    raw_files: list[SnapshotPartitionFile] = []
    primary_files: list[SnapshotPartitionFile] = []
    hive_raw = work_root / leg / "hive_raw"
    if hive_raw.exists():
        shutil.rmtree(hive_raw)
    hive_raw.mkdir(parents=True)

    print(
        f"{leg}: PARTITION_BY copy from heap -> {hive_raw} (single scan)",
        flush=True,
    )
    conn.execute(
        f"""
        COPY (
            SELECT
                h.market_id,
                h."clobTokenId",
                h.timestamp,
                h.price,
                h.fidelity_minutes,
                h.window_start_at,
                h.window_end_at,
                h.ingested_at,
                b.bucket
            FROM polymarket_wc2026_raw."{relation}" AS h
            INNER JOIN _bootstrap_token_buckets AS b
                ON h."clobTokenId" = b.token
        ) TO '{hive_raw.as_posix()}' (
            FORMAT PARQUET,
            COMPRESSION SNAPPY,
            PARTITION_BY (bucket),
            OVERWRITE_OR_IGNORE
        )
        """
    )
    print(f"{leg}: hive copy done; normalizing buckets", flush=True)

    for bucket in range(bucket_count):
        src_dir = hive_raw / f"bucket={bucket}"
        if not src_dir.is_dir():
            continue
        parts = sorted(src_dir.glob("*.parquet"))
        if not parts:
            continue
        raw_out = staged / "raw" / f"bucket={bucket}" / "part-00000.parquet"
        raw_out.parent.mkdir(parents=True, exist_ok=True)
        if len(parts) == 1:
            shutil.move(str(parts[0]), str(raw_out))
        else:
            # Rare multi-file hive bucket: concat via DuckDB.
            paths = ", ".join(f"'{p.as_posix()}'" for p in parts)
            conn.execute(
                f"""
                COPY (
                    SELECT
                        market_id,
                        "clobTokenId",
                        timestamp,
                        price,
                        fidelity_minutes,
                        window_start_at,
                        window_end_at,
                        ingested_at
                    FROM read_parquet([{paths}], hive_partitioning=false)
                ) TO '{raw_out.as_posix()}' (FORMAT PARQUET, COMPRESSION SNAPPY)
                """
            )
        if (
            not raw_out.exists()
            or raw_out.stat().st_size == 0
            or pq.ParquetFile(raw_out).metadata.num_rows == 0
        ):
            raw_out.unlink(missing_ok=True)
            continue
        raw_files.append(_raw_partition_file(raw_out, bucket=bucket))
        nrows = pq.ParquetFile(raw_out).metadata.num_rows
        print(
            f"{leg}: bucket {bucket}/{bucket_count - 1} raw rows={nrows} "
            f"bytes={raw_out.stat().st_size}",
            flush=True,
        )

        ohlc = compute_primary_minute_ohlc(
            pl.read_parquet(raw_out, hive_partitioning=False),
            primary_token_ids=primary,
        )
        if ohlc.is_empty():
            continue
        primary_out = (
            staged / "primary_ohlc" / f"bucket={bucket}" / "part-00000.parquet"
        )
        primary_files.append(_write_primary_partition(ohlc, primary_out, bucket=bucket))

    if not raw_files:
        raise SystemExit(f"{leg}: no raw partitions written")

    write_manifest(
        staged,
        leg=leg,
        snapshot_id=snapshot_id,
        fetch_run_id=f"bootstrap-{leg}",
        collected_at=datetime.now(timezone.utc),
        previous_snapshot_id=None,
        primary_token_ids=sorted(primary),
        raw_files=raw_files,
        primary_files=primary_files,
        window_hashes=None,
    )
    snapshot = publish_snapshot(root, staged)
    try:
        summary = reconcile_snapshot_publication(conn, snapshot)
    except Exception:
        from oddsfox_pipeline.storage.minute_odds_snapshots import (
            rollback_snapshot_pointer,
        )

        rollback_snapshot_pointer(root, previous_snapshot_id=None)
        raise
    retain_snapshots(root, keep=2)
    print(
        f"{leg}: published {snapshot.snapshot_id} "
        f"raw={snapshot.raw_row_count} primary={snapshot.primary_row_count} "
        f"audit={summary['fetch_run_id']}",
        flush=True,
    )
    shutil.rmtree(work_root / leg, ignore_errors=True)


def _finish_staged(
    conn: duckdb.DuckDBPyConnection,
    *,
    leg: str,
    staged: Path,
) -> None:
    """Publish an already-written raw/primary tree (resume after interrupt)."""
    root = minute_odds_snapshot_root(leg=leg)
    snapshot_id = staged.name
    raw_files: list[SnapshotPartitionFile] = []
    primary_files: list[SnapshotPartitionFile] = []
    for bucket_dir in sorted((staged / "raw").glob("bucket=*")):
        bucket = int(bucket_dir.name.split("=", 1)[1])
        part = bucket_dir / "part-00000.parquet"
        if part.is_file():
            print(f"{leg}: resume hash raw bucket={bucket}", flush=True)
            raw_files.append(_raw_partition_file(part, bucket=bucket))
    for bucket_dir in sorted((staged / "primary_ohlc").glob("bucket=*")):
        bucket = int(bucket_dir.name.split("=", 1)[1])
        part = bucket_dir / "part-00000.parquet"
        if not part.is_file():
            continue
        print(f"{leg}: resume hash primary bucket={bucket}", flush=True)
        frame = pl.read_parquet(part, hive_partitioning=False)
        primary_files.append(
            SnapshotPartitionFile(
                kind="primary_ohlc",
                bucket=bucket,
                path=part,
                sha256=_sha256_file(part),
                schema_fingerprint=schema_fingerprint(frame.to_arrow().schema),
                row_count=frame.height,
                byte_size=part.stat().st_size,
                token_ids=tuple(
                    sorted(str(v) for v in frame["clob_token_id"].unique().to_list())
                ),
            )
        )
    if not raw_files:
        raise SystemExit(f"{leg}: staged {staged} has no raw partitions")
    primary = {token for part in primary_files for token in part.token_ids}
    write_manifest(
        staged,
        leg=leg,
        snapshot_id=snapshot_id,
        fetch_run_id=f"bootstrap-{leg}",
        collected_at=datetime.now(timezone.utc),
        previous_snapshot_id=None,
        primary_token_ids=sorted(primary),
        raw_files=raw_files,
        primary_files=primary_files,
        window_hashes=None,
    )
    snapshot = publish_snapshot(root, staged)
    try:
        summary = reconcile_snapshot_publication(conn, snapshot)
    except Exception:
        from oddsfox_pipeline.storage.minute_odds_snapshots import (
            rollback_snapshot_pointer,
        )

        rollback_snapshot_pointer(root, previous_snapshot_id=None)
        raise
    retain_snapshots(root, keep=2)
    print(
        f"{leg}: published {snapshot.snapshot_id} "
        f"raw={snapshot.raw_row_count} primary={snapshot.primary_row_count} "
        f"audit={summary['fetch_run_id']}",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duckdb-path",
        default=os.environ.get("DUCKDB_PATH") or os.environ.get("DUCKDB_NAME"),
    )
    parser.add_argument("--legs", default="match,futures")
    parser.add_argument(
        "--finish-staged",
        help="Resume publish from an existing staging snapshot directory",
    )
    args = parser.parse_args()
    duckdb_path = Path(str(args.duckdb_path).strip().strip('"')).expanduser()
    if not duckdb_path.is_file():
        raise SystemExit(f"warehouse not found: {duckdb_path}")
    runtime_root = Path(
        os.environ.get("ODDSFOX_RUNTIME_ROOT", ".cache/runtime")
    ).expanduser()
    runtime_root.mkdir(parents=True, exist_ok=True)
    print(f"warehouse={duckdb_path}", flush=True)
    with duckdb.connect(str(duckdb_path)) as conn:
        bootstrap_all_polymarket_tables(conn)
        if args.finish_staged:
            staged = Path(args.finish_staged).expanduser().resolve()
            # Infer leg from path .../minute-odds-snapshots/<leg>/staging/<id>
            leg = staged.parent.parent.name
            if leg not in {"match", "futures"}:
                raise SystemExit(f"cannot infer leg from {staged}")
            _finish_staged(conn, leg=leg, staged=staged)
            print("bootstrap complete", flush=True)
            return
        legs = [part.strip() for part in str(args.legs).split(",") if part.strip()]
        relation_by_leg = {
            "match": "match_minute_odds_history",
            "futures": "futures_minute_odds_history",
        }
        work_root = Path(
            tempfile.mkdtemp(prefix="minute-odds-bootstrap-", dir=str(runtime_root))
        )
        print(f"work_root={work_root}", flush=True)
        for leg in legs:
            if leg not in relation_by_leg:
                raise SystemExit(f"unknown leg {leg!r}")
            _bootstrap_leg(
                conn,
                leg=leg,
                relation=relation_by_leg[leg],
                work_root=work_root,
            )
    print("bootstrap complete", flush=True)


if __name__ == "__main__":
    main()
