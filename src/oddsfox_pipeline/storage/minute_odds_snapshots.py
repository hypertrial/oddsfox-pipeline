"""Immutable partitioned Parquet snapshots for WC2026 minute-odds raw + primary OHLC.

Canonical layout under ``${ODDSFOX_RUNTIME_ROOT}/minute-odds-snapshots/<leg>/``:

```
snapshots/<snapshot_id>/
  manifest.json
  raw/bucket=<N>/part-*.parquet          # every CLOB token
  primary_ohlc/bucket=<N>/part-*.parquet # primary-token minute OHLC only
CURRENT -> snapshots/<snapshot_id>
```

DuckDB registers views over the active snapshot so dbt sources keep stable names
without rewriting a 377M-row heap table on every publish.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import duckdb
import polars as pl
import pyarrow.parquet as pq

from oddsfox_pipeline.contracts.raw_snapshots import schema_fingerprint
from oddsfox_pipeline.naming import SCOPE_WC2026
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)

MINUTE_ODDS_SNAPSHOT_CONTRACT = "oddsfox.minute_odds.v1"
DEFAULT_TOKEN_BUCKET_COUNT = 64
_SNAPSHOT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_LEG = re.compile(r"^(match|futures)$")

_RAW_COLUMNS = (
    "market_id",
    "clobTokenId",
    "timestamp",
    "price",
    "fidelity_minutes",
    "window_start_at",
    "window_end_at",
    "ingested_at",
)
_PRIMARY_OHLC_COLUMNS = (
    "market_id",
    "clob_token_id",
    "odds_minute_epoch",
    "odds_minute_utc",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "avg_price",
    "observed_points",
    "first_observed_at",
    "last_observed_at",
)


class MinuteOddsSnapshotError(ValueError):
    """Raised when a minute-odds snapshot is incomplete or unsafe."""


@dataclass(frozen=True, slots=True)
class SnapshotPartitionFile:
    kind: str  # raw | primary_ohlc
    bucket: int
    path: Path
    sha256: str
    schema_fingerprint: str
    row_count: int
    byte_size: int
    token_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MinuteOddsSnapshot:
    leg: str
    snapshot_id: str
    directory: Path
    fetch_run_id: str
    collected_at: datetime
    previous_snapshot_id: str | None
    raw_row_count: int
    primary_row_count: int
    token_ids: tuple[str, ...]
    primary_token_ids: tuple[str, ...]
    primary_mapping_sha256: str
    files: tuple[SnapshotPartitionFile, ...]
    manifest: dict[str, Any]


def minute_odds_snapshot_root(
    *,
    leg: str,
    runtime_root: Path | None = None,
) -> Path:
    if not _LEG.fullmatch(leg):
        raise MinuteOddsSnapshotError(f"unknown minute-odds leg: {leg!r}")
    root = (
        Path(
            runtime_root
            or os.getenv(
                "ODDSFOX_RUNTIME_ROOT",
                Path(os.getenv("ODDSFOX_PIPELINE_ROOT", ".")).resolve()
                / ".cache"
                / "runtime",
            )
        )
        .expanduser()
        .resolve()
    )
    target = (root / "minute-odds-snapshots" / leg).resolve()
    if not target.is_relative_to(root):
        raise MinuteOddsSnapshotError("minute-odds snapshot path escaped runtime root")
    return target


def token_bucket(
    token_id: str, *, bucket_count: int = DEFAULT_TOKEN_BUCKET_COUNT
) -> int:
    digest = hashlib.sha256(str(token_id).encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % max(1, int(bucket_count))


def primary_mapping_sha256(primary_token_ids: Iterable[str]) -> str:
    payload = "\n".join(sorted({str(token) for token in primary_token_ids}))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(tmp, path)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MinuteOddsSnapshotError(f"invalid JSON at {path}") from exc
    if not isinstance(payload, dict):
        raise MinuteOddsSnapshotError(f"JSON object required at {path}")
    return payload


def active_snapshot_id(root: Path) -> str | None:
    current = root / "CURRENT"
    if not current.exists():
        return None
    if current.is_symlink():
        target = current.resolve()
        return target.name
    text = current.read_text(encoding="utf-8").strip()
    return text or None


def active_snapshot_dir(root: Path) -> Path | None:
    snapshot_id = active_snapshot_id(root)
    if snapshot_id is None:
        return None
    path = (root / "snapshots" / snapshot_id).resolve()
    if not path.is_dir():
        return None
    return path


def stage_snapshot_dir(root: Path, snapshot_id: str) -> Path:
    if not _SNAPSHOT_ID.fullmatch(snapshot_id):
        raise MinuteOddsSnapshotError(f"invalid snapshot_id: {snapshot_id!r}")
    staged = root / "staging" / snapshot_id
    if staged.exists():
        shutil.rmtree(staged)
    (staged / "raw").mkdir(parents=True, exist_ok=True)
    (staged / "primary_ohlc").mkdir(parents=True, exist_ok=True)
    return staged


def compute_primary_minute_ohlc(
    rows: Sequence[Mapping[str, Any]] | pl.DataFrame,
    *,
    primary_token_ids: set[str],
) -> pl.DataFrame:
    """Deterministic per-minute OHLC matching dbt arg_min/arg_max on epoch."""
    if isinstance(rows, pl.DataFrame):
        frame = rows
    else:
        if not rows:
            return pl.DataFrame(schema={c: pl.Null for c in _PRIMARY_OHLC_COLUMNS})
        frame = pl.DataFrame(rows)
    if frame.is_empty():
        return pl.DataFrame(schema={c: pl.Null for c in _PRIMARY_OHLC_COLUMNS})

    renamed = frame
    if "clobTokenId" in renamed.columns and "clob_token_id" not in renamed.columns:
        renamed = renamed.rename({"clobTokenId": "clob_token_id"})
    if "timestamp" in renamed.columns and "odds_timestamp_epoch" not in renamed.columns:
        renamed = renamed.rename({"timestamp": "odds_timestamp_epoch"})

    filtered = renamed.filter(pl.col("clob_token_id").is_in(sorted(primary_token_ids)))
    if filtered.is_empty():
        return pl.DataFrame(schema={c: pl.Null for c in _PRIMARY_OHLC_COLUMNS})

    # Keep only in-window points using epoch bounds from window_* columns.
    if "window_start_at" in filtered.columns and "window_end_at" in filtered.columns:
        start_col = filtered["window_start_at"]
        end_col = filtered["window_end_at"]
        if start_col.dtype == pl.Datetime(time_zone="UTC") or str(
            start_col.dtype
        ).startswith("Datetime"):
            start_epoch = start_col.dt.epoch("s")
            end_epoch = end_col.dt.epoch("s")
        else:
            start_epoch = start_col.cast(pl.Datetime(time_unit="us")).dt.epoch("s")
            end_epoch = end_col.cast(pl.Datetime(time_unit="us")).dt.epoch("s")
        filtered = filtered.with_columns(
            start_epoch.alias("_window_start_epoch"),
            end_epoch.alias("_window_end_epoch"),
        ).filter(
            (pl.col("odds_timestamp_epoch") >= pl.col("_window_start_epoch"))
            & (pl.col("odds_timestamp_epoch") <= pl.col("_window_end_epoch"))
        )

    prepared = filtered.with_columns(
        ((pl.col("odds_timestamp_epoch") // 60) * 60).alias("odds_minute_epoch")
    ).sort(["clob_token_id", "odds_minute_epoch", "odds_timestamp_epoch"])

    aggregated = prepared.group_by(
        ["market_id", "clob_token_id", "odds_minute_epoch"]
    ).agg(
        pl.col("price").first().alias("open_price"),
        pl.col("price").max().alias("high_price"),
        pl.col("price").min().alias("low_price"),
        pl.col("price").last().alias("close_price"),
        pl.col("price").mean().round(8).alias("avg_price"),
        pl.len().alias("observed_points"),
        pl.col("odds_timestamp_epoch").min().alias("_first_epoch"),
        pl.col("odds_timestamp_epoch").max().alias("_last_epoch"),
    )
    return (
        aggregated.with_columns(
            pl.from_epoch(pl.col("odds_minute_epoch"), time_unit="s")
            .dt.replace_time_zone("UTC")
            .dt.replace_time_zone(None)
            .alias("odds_minute_utc"),
            pl.from_epoch(pl.col("_first_epoch"), time_unit="s")
            .dt.replace_time_zone("UTC")
            .dt.replace_time_zone(None)
            .alias("first_observed_at"),
            pl.from_epoch(pl.col("_last_epoch"), time_unit="s")
            .dt.replace_time_zone("UTC")
            .dt.replace_time_zone(None)
            .alias("last_observed_at"),
        )
        .select(list(_PRIMARY_OHLC_COLUMNS))
        .sort(["market_id", "clob_token_id", "odds_minute_epoch"])
    )


def _write_partition_parquet(
    frame: pl.DataFrame,
    path: Path,
    *,
    kind: str,
    bucket: int,
) -> SnapshotPartitionFile:
    path.parent.mkdir(parents=True, exist_ok=True)
    if frame.is_empty():
        # Keep schema-only partitions out of the manifest.
        raise MinuteOddsSnapshotError("refusing to write empty partition")
    arrow = frame.to_arrow()
    pq.write_table(arrow, path, compression="snappy", write_statistics=False)
    token_col = "clobTokenId" if "clobTokenId" in frame.columns else "clob_token_id"
    token_ids = tuple(sorted(str(v) for v in frame[token_col].unique().to_list()))
    return SnapshotPartitionFile(
        kind=kind,
        bucket=bucket,
        path=path,
        sha256=_sha256_file(path),
        schema_fingerprint=schema_fingerprint(arrow.schema),
        row_count=frame.height,
        byte_size=path.stat().st_size,
        token_ids=token_ids,
    )


def write_snapshot_partitions_from_raw_parquet(
    staged_dir: Path,
    raw_paths: Sequence[Path],
    *,
    primary_token_ids: set[str],
    bucket_count: int = DEFAULT_TOKEN_BUCKET_COUNT,
) -> tuple[list[SnapshotPartitionFile], list[SnapshotPartitionFile]]:
    """Repartition spilled publish shards into durable raw + primary OHLC trees."""
    if not raw_paths:
        raise MinuteOddsSnapshotError("raw_paths must not be empty")
    raw_frames: dict[int, list[pl.DataFrame]] = {}
    primary_frames: dict[int, list[pl.DataFrame]] = {}
    for path in raw_paths:
        frame = pl.read_parquet(path)
        if "clobTokenId" not in frame.columns and "clob_token_id" in frame.columns:
            frame = frame.rename({"clob_token_id": "clobTokenId"})
        if "clobTokenId" not in frame.columns:
            raise MinuteOddsSnapshotError(f"raw parquet missing clobTokenId: {path}")
        if (frame["fidelity_minutes"] != 1).any():
            raise MinuteOddsSnapshotError(f"fidelity_minutes must be 1 in {path}")
        with_bucket = frame.with_columns(
            pl.col("clobTokenId")
            .map_elements(
                lambda token: token_bucket(str(token), bucket_count=bucket_count),
                return_dtype=pl.Int64,
            )
            .alias("_bucket")
        )
        for bucket_key, part in with_bucket.group_by("_bucket"):
            bucket = int(bucket_key[0] if isinstance(bucket_key, tuple) else bucket_key)
            cleaned = part.drop("_bucket")
            raw_frames.setdefault(bucket, []).append(cleaned)
            ohlc = compute_primary_minute_ohlc(
                cleaned, primary_token_ids=primary_token_ids
            )
            if not ohlc.is_empty():
                primary_frames.setdefault(bucket, []).append(ohlc)

    raw_files: list[SnapshotPartitionFile] = []
    for bucket, parts in sorted(raw_frames.items()):
        merged = pl.concat(parts, how="vertical_relaxed").unique(
            subset=["clobTokenId", "timestamp"], keep="last"
        )
        out = staged_dir / "raw" / f"bucket={bucket}" / "part-00000.parquet"
        raw_files.append(
            _write_partition_parquet(merged, out, kind="raw", bucket=bucket)
        )

    primary_files: list[SnapshotPartitionFile] = []
    for bucket, parts in sorted(primary_frames.items()):
        merged = (
            pl.concat(parts, how="vertical_relaxed")
            .unique(
                subset=["market_id", "clob_token_id", "odds_minute_epoch"],
                keep="last",
            )
            .sort(["market_id", "clob_token_id", "odds_minute_epoch"])
        )
        out = staged_dir / "primary_ohlc" / f"bucket={bucket}" / "part-00000.parquet"
        primary_files.append(
            _write_partition_parquet(merged, out, kind="primary_ohlc", bucket=bucket)
        )
    return raw_files, primary_files


def write_manifest(
    staged_dir: Path,
    *,
    leg: str,
    snapshot_id: str,
    fetch_run_id: str,
    collected_at: datetime,
    previous_snapshot_id: str | None,
    primary_token_ids: Sequence[str],
    raw_files: Sequence[SnapshotPartitionFile],
    primary_files: Sequence[SnapshotPartitionFile],
    window_hashes: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    token_ids = sorted({token for part in raw_files for token in part.token_ids})
    primary_ids = sorted({str(token) for token in primary_token_ids})
    mapping_hash = primary_mapping_sha256(primary_ids)
    files_payload = []
    for part in [*raw_files, *primary_files]:
        rel = part.path.relative_to(staged_dir).as_posix()
        files_payload.append(
            {
                "kind": part.kind,
                "bucket": part.bucket,
                "path": rel,
                "sha256": part.sha256,
                "schema_fingerprint": part.schema_fingerprint,
                "row_count": part.row_count,
                "byte_size": part.byte_size,
                "token_ids": list(part.token_ids),
            }
        )
    hashes = {
        str(token): str(digest)
        for token, digest in (window_hashes or {}).items()
        if str(digest)
    }
    manifest = {
        "contract_version": MINUTE_ODDS_SNAPSHOT_CONTRACT,
        "leg": leg,
        "snapshot_id": snapshot_id,
        "fetch_run_id": fetch_run_id,
        "collected_at": collected_at.astimezone(timezone.utc).isoformat(),
        "previous_snapshot_id": previous_snapshot_id,
        "status": "complete",
        "completeness": "complete",
        "bucket_count": DEFAULT_TOKEN_BUCKET_COUNT,
        "token_count": len(token_ids),
        "token_ids": token_ids,
        "primary_token_count": len(primary_ids),
        "primary_token_ids": primary_ids,
        "primary_mapping_sha256": mapping_hash,
        "window_hashes": hashes,
        "raw_row_count": int(sum(part.row_count for part in raw_files)),
        "primary_row_count": int(sum(part.row_count for part in primary_files)),
        "files": files_payload,
    }
    _atomic_write_json(staged_dir / "manifest.json", manifest)
    return manifest


def validate_minute_odds_snapshot(snapshot_dir: Path) -> MinuteOddsSnapshot:
    directory = snapshot_dir.resolve()
    manifest_path = directory / "manifest.json"
    if not manifest_path.is_file():
        raise MinuteOddsSnapshotError(f"snapshot is partial: missing {manifest_path}")
    manifest = _read_json(manifest_path)
    if manifest.get("contract_version") != MINUTE_ODDS_SNAPSHOT_CONTRACT:
        raise MinuteOddsSnapshotError("unknown minute-odds snapshot contract version")
    if (
        manifest.get("status") != "complete"
        or manifest.get("completeness") != "complete"
    ):
        raise MinuteOddsSnapshotError("snapshot status/completeness must be complete")
    leg = str(manifest.get("leg", ""))
    if not _LEG.fullmatch(leg):
        raise MinuteOddsSnapshotError(f"invalid leg in manifest: {leg!r}")
    snapshot_id = str(manifest.get("snapshot_id", ""))
    if not _SNAPSHOT_ID.fullmatch(snapshot_id) or directory.name != snapshot_id:
        raise MinuteOddsSnapshotError("snapshot_id must equal directory name")
    files_raw = manifest.get("files")
    if not isinstance(files_raw, list) or not files_raw:
        raise MinuteOddsSnapshotError("files must be a non-empty array")
    files: list[SnapshotPartitionFile] = []
    paths: set[Path] = set()
    kind_fingerprints: dict[str, str] = {}
    for entry in files_raw:
        if not isinstance(entry, dict):
            raise MinuteOddsSnapshotError("each files entry must be an object")
        relative = Path(str(entry["path"]))
        if relative.is_absolute() or ".." in relative.parts:
            raise MinuteOddsSnapshotError(f"unsafe parquet path: {relative}")
        path = (directory / relative).resolve()
        if directory not in path.parents or not path.is_file():
            raise MinuteOddsSnapshotError(f"declared payload missing: {relative}")
        if path in paths:
            raise MinuteOddsSnapshotError(f"duplicate payload path: {relative}")
        paths.add(path)
        kind = str(entry["kind"])
        if kind not in {"raw", "primary_ohlc"}:
            raise MinuteOddsSnapshotError(f"invalid payload kind: {kind!r}")
        if relative.parts[0] != kind:
            raise MinuteOddsSnapshotError(
                f"payload path does not match kind {kind!r}: {relative}"
            )
        actual_hash = _sha256_file(path)
        if actual_hash != str(entry["sha256"]):
            raise MinuteOddsSnapshotError(f"SHA-256 mismatch for {relative}")
        if path.stat().st_size != int(entry["byte_size"]):
            raise MinuteOddsSnapshotError(f"byte size mismatch for {relative}")
        parquet = pq.ParquetFile(path)
        if parquet.metadata.num_rows != int(entry["row_count"]):
            raise MinuteOddsSnapshotError(f"row count mismatch for {relative}")
        fingerprint = schema_fingerprint(parquet.schema_arrow)
        if fingerprint != str(entry["schema_fingerprint"]):
            raise MinuteOddsSnapshotError(f"schema fingerprint mismatch for {relative}")
        expected_fingerprint = kind_fingerprints.setdefault(kind, fingerprint)
        if fingerprint != expected_fingerprint:
            raise MinuteOddsSnapshotError(f"inconsistent {kind} schema: {relative}")
        token_ids = tuple(str(token) for token in entry.get("token_ids", []))
        files.append(
            SnapshotPartitionFile(
                kind=kind,
                bucket=int(entry["bucket"]),
                path=path,
                sha256=actual_hash,
                schema_fingerprint=fingerprint,
                row_count=int(entry["row_count"]),
                byte_size=int(entry["byte_size"]),
                token_ids=token_ids,
            )
        )
    raw_files = [part for part in files if part.kind == "raw"]
    primary_files = [part for part in files if part.kind == "primary_ohlc"]
    if not raw_files:
        raise MinuteOddsSnapshotError("snapshot must contain raw payloads")
    raw_tokens = sorted({token for part in raw_files for token in part.token_ids})
    primary_tokens = sorted(
        {token for part in primary_files for token in part.token_ids}
    )
    declared_tokens = [str(token) for token in manifest.get("token_ids", [])]
    declared_primary = [str(token) for token in manifest.get("primary_token_ids", [])]
    if declared_tokens != sorted(set(declared_tokens)) or declared_tokens != raw_tokens:
        raise MinuteOddsSnapshotError(
            "manifest token inventory does not match raw files"
        )
    if declared_primary != sorted(set(declared_primary)):
        raise MinuteOddsSnapshotError("manifest primary token inventory is not unique")
    if not set(primary_tokens) <= set(declared_primary) or not set(
        declared_primary
    ) <= set(raw_tokens):
        raise MinuteOddsSnapshotError(
            "manifest primary token inventory does not match payloads"
        )
    if int(manifest.get("token_count", -1)) != len(raw_tokens):
        raise MinuteOddsSnapshotError("manifest token_count does not match inventory")
    if int(manifest.get("primary_token_count", -1)) != len(declared_primary):
        raise MinuteOddsSnapshotError(
            "manifest primary_token_count does not match inventory"
        )
    if int(manifest.get("raw_row_count", -1)) != sum(
        part.row_count for part in raw_files
    ):
        raise MinuteOddsSnapshotError("manifest raw_row_count does not match files")
    if int(manifest.get("primary_row_count", -1)) != sum(
        part.row_count for part in primary_files
    ):
        raise MinuteOddsSnapshotError("manifest primary_row_count does not match files")
    if str(manifest.get("primary_mapping_sha256", "")) != primary_mapping_sha256(
        declared_primary
    ):
        raise MinuteOddsSnapshotError("manifest primary mapping hash is invalid")
    collected_at = datetime.fromisoformat(
        str(manifest["collected_at"]).replace("Z", "+00:00")
    )
    previous = manifest.get("previous_snapshot_id")
    return MinuteOddsSnapshot(
        leg=leg,
        snapshot_id=snapshot_id,
        directory=directory,
        fetch_run_id=str(manifest["fetch_run_id"]),
        collected_at=collected_at,
        previous_snapshot_id=str(previous) if previous else None,
        raw_row_count=int(manifest["raw_row_count"]),
        primary_row_count=int(manifest["primary_row_count"]),
        token_ids=tuple(str(t) for t in manifest.get("token_ids", [])),
        primary_token_ids=tuple(str(t) for t in manifest.get("primary_token_ids", [])),
        primary_mapping_sha256=str(manifest["primary_mapping_sha256"]),
        files=tuple(files),
        manifest=manifest,
    )


def publish_snapshot(root: Path, staged_dir: Path) -> MinuteOddsSnapshot:
    """Validate staged snapshot, move into snapshots/, and atomically advance CURRENT."""
    snapshot = validate_minute_odds_snapshot(staged_dir)
    final_dir = root / "snapshots" / snapshot.snapshot_id
    final_dir.parent.mkdir(parents=True, exist_ok=True)
    if final_dir.exists():
        raise MinuteOddsSnapshotError(
            f"snapshot already exists: {snapshot.leg}/{snapshot.snapshot_id}"
        )
    os.replace(staged_dir, final_dir)
    snapshot = validate_minute_odds_snapshot(final_dir)
    _write_current_pointer(root, snapshot.snapshot_id)
    return snapshot


def _write_current_pointer(root: Path, snapshot_id: str) -> None:
    if not _SNAPSHOT_ID.fullmatch(snapshot_id):
        raise MinuteOddsSnapshotError(f"invalid snapshot_id: {snapshot_id!r}")
    current = root / "CURRENT"
    tmp = root / "CURRENT.tmp"
    if tmp.exists() or tmp.is_symlink():
        tmp.unlink()
    try:
        tmp.symlink_to(Path("snapshots") / snapshot_id)
        os.replace(tmp, current)
    except OSError:
        if tmp.exists() or tmp.is_symlink():
            tmp.unlink()
        tmp.write_text(snapshot_id + "\n", encoding="utf-8")
        os.replace(tmp, current)


def rollback_snapshot_pointer(
    root: Path,
    *,
    previous_snapshot_id: str | None,
) -> None:
    """Restore CURRENT after a failed DuckDB registration; keep failed snapshot files.

    The failed snapshot directory is left in place for operator forensics and is
    eligible for later ``retain_snapshots`` cleanup once a successful publish
    advances CURRENT again.
    """
    if previous_snapshot_id:
        previous_dir = root / "snapshots" / previous_snapshot_id
        if not previous_dir.is_dir():
            raise MinuteOddsSnapshotError(
                f"cannot rollback: predecessor missing ({previous_snapshot_id})"
            )
        _write_current_pointer(root, previous_snapshot_id)
    else:
        current = root / "CURRENT"
        if current.exists() or current.is_symlink():
            current.unlink()


def retain_snapshots(root: Path, *, keep: int = 2) -> list[str]:
    """Delete old snapshots except active + predecessor (default keep=2)."""
    if keep < 1:
        raise MinuteOddsSnapshotError("keep must be >= 1")
    snapshots_dir = root / "snapshots"
    if not snapshots_dir.is_dir():
        return []
    active = active_snapshot_id(root)
    ids = sorted(
        (path.name for path in snapshots_dir.iterdir() if path.is_dir()),
        reverse=True,
    )
    protected: set[str] = set()
    if active:
        protected.add(active)
        active_dir = snapshots_dir / active
        if active_dir.is_dir():
            try:
                previous = validate_minute_odds_snapshot(
                    active_dir
                ).previous_snapshot_id
            except MinuteOddsSnapshotError:
                previous = None
            if previous:
                protected.add(previous)
    kept = list(protected)
    for snapshot_id in ids:
        if snapshot_id in protected:
            continue
        if len(kept) >= keep:
            shutil.rmtree(snapshots_dir / snapshot_id, ignore_errors=True)
        else:
            kept.append(snapshot_id)
    staging = root / "staging"
    if staging.is_dir():
        shutil.rmtree(staging, ignore_errors=True)
        staging.mkdir(parents=True, exist_ok=True)
    return kept


def _parquet_glob(snapshot_dir: Path, kind: str) -> str:
    pattern = (snapshot_dir / kind / "bucket=*" / "*.parquet").as_posix()
    return pattern


def _view_parquet_root(snapshot: MinuteOddsSnapshot) -> Path:
    """Prefer the CURRENT symlink so views track pointer advances at query time.

    Text-pointer CURRENT fallbacks (rare) keep the concrete snapshot directory.
    """
    root = snapshot.directory.parent.parent
    current = root / "CURRENT"
    if current.is_symlink():
        return current
    return snapshot.directory


def _drop_relation(conn: duckdb.DuckDBPyConnection, relation: str) -> None:
    schema_name, _, table_name = relation.partition(".")
    schema_name = schema_name.strip('"')
    table_name = table_name.strip('"')
    kind = conn.execute(
        """
        SELECT table_type
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema_name, table_name],
    ).fetchone()
    if kind is None:
        return
    if str(kind[0]).upper() == "VIEW":
        conn.execute(f"DROP VIEW IF EXISTS {relation}")
    else:
        conn.execute(f"DROP TABLE IF EXISTS {relation}")


def register_snapshot_views(
    conn: duckdb.DuckDBPyConnection,
    snapshot: MinuteOddsSnapshot,
    *,
    scope_name: str = SCOPE_WC2026,
) -> dict[str, str]:
    """Point stable DuckDB relations at the active snapshot Parquet trees."""
    raw_relation = polymarket_raw_tbl(
        scope_name,
        "match_minute_odds_history"
        if snapshot.leg == "match"
        else "futures_minute_odds_history",
    )
    primary_relation = polymarket_raw_tbl(
        scope_name,
        "match_primary_minute_ohlc"
        if snapshot.leg == "match"
        else "futures_primary_minute_ohlc",
    )
    raw_glob = _parquet_glob(_view_parquet_root(snapshot), "raw")
    primary_glob = _parquet_glob(_view_parquet_root(snapshot), "primary_ohlc")
    for relation in (raw_relation, primary_relation):
        _drop_relation(conn, relation)
    conn.execute(
        f"""
        CREATE OR REPLACE VIEW {raw_relation} AS
        SELECT
            market_id,
            "clobTokenId",
            timestamp,
            price,
            fidelity_minutes,
            window_start_at,
            window_end_at,
            ingested_at
        FROM read_parquet('{raw_glob}', hive_partitioning=true)
        """
    )
    # Primary OHLC may be empty for pathological fixtures; create empty view shape.
    primary_files = [part for part in snapshot.files if part.kind == "primary_ohlc"]
    if primary_files:
        conn.execute(
            f"""
            CREATE OR REPLACE VIEW {primary_relation} AS
            SELECT
                market_id,
                clob_token_id,
                odds_minute_epoch,
                odds_minute_utc,
                open_price,
                high_price,
                low_price,
                close_price,
                avg_price,
                observed_points,
                first_observed_at,
                last_observed_at
            FROM read_parquet('{primary_glob}', hive_partitioning=true)
            """
        )
    else:
        conn.execute(
            f"""
            CREATE OR REPLACE VIEW {primary_relation} AS
            SELECT
                CAST(NULL AS VARCHAR) AS market_id,
                CAST(NULL AS VARCHAR) AS clob_token_id,
                CAST(NULL AS BIGINT) AS odds_minute_epoch,
                CAST(NULL AS TIMESTAMP) AS odds_minute_utc,
                CAST(NULL AS DOUBLE) AS open_price,
                CAST(NULL AS DOUBLE) AS high_price,
                CAST(NULL AS DOUBLE) AS low_price,
                CAST(NULL AS DOUBLE) AS close_price,
                CAST(NULL AS DOUBLE) AS avg_price,
                CAST(NULL AS BIGINT) AS observed_points,
                CAST(NULL AS TIMESTAMP) AS first_observed_at,
                CAST(NULL AS TIMESTAMP) AS last_observed_at
            WHERE FALSE
            """
        )
    return {"raw": raw_relation, "primary_ohlc": primary_relation}


def reconcile_snapshot_publication(
    conn: duckdb.DuckDBPyConnection,
    snapshot: MinuteOddsSnapshot,
    *,
    scope_name: str = SCOPE_WC2026,
) -> dict[str, Any]:
    """Register a complete snapshot and publish its exact successful audit run.

    The snapshot is scanned once to prove token, market, row-count, and window
    equality before transactional DDL replaces a pre-snapshot heap relation.
    """
    audit_name = (
        "match_minute_odds_fetch_audit"
        if snapshot.leg == "match"
        else "futures_minute_odds_fetch_audit"
    )
    audit = polymarket_ops_tbl(scope_name, audit_name)
    row_count_column = (
        "in_game_row_count" if snapshot.leg == "match" else "window_row_count"
    )
    hash_column = (
        "in_game_history_sha256" if snapshot.leg == "match" else "window_history_sha256"
    )
    candidates = conn.execute(
        f"""
        SELECT
            fetch_run_id,
            count(*) FILTER (WHERE fetch_status = 'success') AS success_tokens,
            count(*) FILTER (WHERE fetch_status = 'empty') AS empty_tokens,
            sum({row_count_column}) FILTER (
                WHERE fetch_status = 'success'
            ) AS success_rows
        FROM {audit}
        GROUP BY fetch_run_id
        HAVING
            count(*) FILTER (WHERE fetch_status = 'success') = ?
            AND sum({row_count_column}) FILTER (
                WHERE fetch_status = 'success'
            ) = ?
            AND count(*) FILTER (
                WHERE fetch_status IN ('error', 'cancelled')
            ) = 0
            AND count(*) FILTER (
                WHERE fetch_status = 'success' AND {hash_column} IS NULL
            ) = 0
            AND count(*) FILTER (WHERE fidelity_minutes != 1) = 0
        ORDER BY max(fetch_finished_at) DESC, fetch_run_id DESC
        """,
        [len(snapshot.token_ids), snapshot.raw_row_count],
    ).fetchall()
    if not candidates:
        raise MinuteOddsSnapshotError(
            f"no complete {snapshot.leg} fetch audit matches snapshot "
            f"{snapshot.snapshot_id}"
        )
    raw_glob = _parquet_glob(_view_parquet_root(snapshot), "raw")
    snapshot_rows = conn.execute(
        """
        SELECT
            "clobTokenId",
            market_id,
            min(window_start_at),
            max(window_start_at),
            min(window_end_at),
            max(window_end_at),
            min(timestamp),
            max(timestamp),
            count(*),
            count(*) FILTER (
                WHERE fidelity_minutes != 1
                    OR price < 0 OR price > 1
            )
        FROM read_parquet(?, hive_partitioning=true)
        GROUP BY "clobTokenId", market_id
        """,
        [raw_glob],
    ).fetchall()
    measured: dict[str, tuple[Any, ...]] = {}
    for row in snapshot_rows:
        token = str(row[0])
        if token in measured:
            raise MinuteOddsSnapshotError(
                f"snapshot token maps to multiple markets: {token}"
            )
        if row[2] != row[3] or row[4] != row[5] or row[2] > row[4]:
            raise MinuteOddsSnapshotError(
                f"snapshot token has inconsistent windows: {token}"
            )
        if int(row[9]) != 0:
            raise MinuteOddsSnapshotError(
                f"snapshot token has invalid observations: {token}"
            )
        measured[token] = (
            str(row[1]),
            row[2],
            row[4],
            int(row[6]),
            int(row[7]),
            int(row[8]),
        )
    if set(measured) != set(snapshot.token_ids):
        raise MinuteOddsSnapshotError(
            "snapshot payload token inventory does not match its manifest"
        )

    candidate = None
    for proposed in candidates:
        proposed_run_id = str(proposed[0])
        audit_rows = conn.execute(
            f"""
            SELECT
                "clobTokenId",
                market_id,
                exact_window_start_at,
                exact_window_end_at,
                request_start_epoch,
                request_end_epoch,
                {row_count_column}
            FROM {audit}
            WHERE fetch_run_id = ? AND fetch_status = 'success'
            ORDER BY "clobTokenId"
            """,
            [proposed_run_id],
        ).fetchall()
        expected = {
            str(row[0]): (
                str(row[1]),
                row[2],
                row[3],
                int(row[4]),
                int(row[5]),
                int(row[6]),
            )
            for row in audit_rows
        }
        mismatch = None
        for token in sorted(set(measured) & set(expected)):
            market_id, raw_start, raw_end, first_epoch, last_epoch, rows = measured[
                token
            ]
            (
                expected_market,
                exact_start,
                exact_end,
                request_start,
                request_end,
                expected_rows,
            ) = expected[token]
            if (
                market_id != expected_market
                or raw_start != exact_start
                or raw_end != exact_end
                or rows != expected_rows
                or first_epoch < request_start
                or last_epoch > request_end
            ):
                mismatch = token
                break
        if set(measured) == set(expected) and mismatch is None:
            candidate = proposed
            break
    if candidate is None:
        raise MinuteOddsSnapshotError(
            f"snapshot payload does not match any complete {snapshot.leg} fetch audit"
        )
    fetch_run_id = str(candidate[0])

    conn.execute("BEGIN TRANSACTION")
    try:
        relations = register_snapshot_views(conn, snapshot, scope_name=scope_name)
        conn.execute(f"UPDATE {audit} SET raw_published = FALSE")
        conn.execute(
            f"""
            UPDATE {audit}
            SET raw_published = TRUE
            WHERE fetch_run_id = ? AND fetch_status = 'success'
            """,
            [fetch_run_id],
        )
        published = conn.execute(
            f"""
            SELECT
                count(*) FILTER (WHERE fetch_run_id = ? AND fetch_status = 'success'
                    AND raw_published),
                count(*) FILTER (WHERE raw_published AND NOT (
                    fetch_run_id = ? AND fetch_status = 'success'
                ))
            FROM {audit}
            """,
            [fetch_run_id, fetch_run_id],
        ).fetchone()
        if published != (len(snapshot.token_ids), 0):
            raise MinuteOddsSnapshotError(
                f"invalid published audit state for {fetch_run_id}: {published}"
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {
        "leg": snapshot.leg,
        "snapshot_id": snapshot.snapshot_id,
        "fetch_run_id": fetch_run_id,
        "raw_rows": snapshot.raw_row_count,
        "success_tokens": int(candidate[1]),
        "empty_tokens": int(candidate[2]),
        "relations": relations,
    }


@dataclass(frozen=True, slots=True)
class PublishedTokenWindow:
    token_id: str
    market_id: str
    window_start_at: datetime
    window_end_at: datetime
    history_sha256: str
    row_count: int


def load_latest_published_token_windows(
    conn: duckdb.DuckDBPyConnection,
    *,
    leg: str,
    scope_name: str = SCOPE_WC2026,
) -> dict[str, PublishedTokenWindow]:
    """Latest successful published audit window per token for reuse decisions."""
    if not _LEG.fullmatch(leg):
        raise MinuteOddsSnapshotError(f"unknown minute-odds leg: {leg!r}")
    from oddsfox_pipeline.storage.duckdb.schemas.constants import polymarket_ops_tbl

    audit = polymarket_ops_tbl(
        scope_name,
        "match_minute_odds_fetch_audit"
        if leg == "match"
        else "futures_minute_odds_fetch_audit",
    )
    hash_column = (
        "in_game_history_sha256" if leg == "match" else "window_history_sha256"
    )
    rows = conn.execute(
        f"""
        SELECT
            "clobTokenId",
            market_id,
            exact_window_start_at,
            exact_window_end_at,
            {hash_column},
            {"in_game_row_count" if leg == "match" else "window_row_count"}
        FROM {audit}
        WHERE fetch_status = 'success'
          AND raw_published
          AND {hash_column} IS NOT NULL
        QUALIFY row_number() OVER (
            PARTITION BY "clobTokenId"
            ORDER BY fetch_finished_at DESC, fetch_run_id DESC
        ) = 1
        """
    ).fetchall()
    out: dict[str, PublishedTokenWindow] = {}
    for row in rows:
        start = row[2]
        end = row[3]
        if not isinstance(start, datetime) or not isinstance(end, datetime):
            continue
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        out[str(row[0])] = PublishedTokenWindow(
            token_id=str(row[0]),
            market_id=str(row[1]),
            window_start_at=start.astimezone(timezone.utc),
            window_end_at=end.astimezone(timezone.utc),
            history_sha256=str(row[4]),
            row_count=int(row[5] or 0),
        )
    return out


def tokens_reusable_by_window(
    plans: Sequence[Any],
    *,
    previous: MinuteOddsSnapshot | None,
    published_windows: Mapping[str, PublishedTokenWindow],
) -> set[str]:
    """Skip CLOB when prior published window bounds still match the plan."""
    if previous is None:
        return set()
    previous_tokens = set(previous.token_ids)
    reusable: set[str] = set()
    for plan in plans:
        token_id = str(plan.token_id)
        if token_id not in previous_tokens:
            continue
        prior = published_windows.get(token_id)
        if prior is None or prior.row_count <= 0:
            continue
        start = plan.started_at
        end = plan.finished_at
        if not isinstance(start, datetime) or not isinstance(end, datetime):
            continue
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        if start.astimezone(timezone.utc) != prior.window_start_at:
            continue
        if end.astimezone(timezone.utc) != prior.window_end_at:
            continue
        reusable.add(token_id)
    return reusable


def dirty_token_buckets(
    token_ids: Iterable[str],
    *,
    bucket_count: int = DEFAULT_TOKEN_BUCKET_COUNT,
) -> set[int]:
    return {token_bucket(token, bucket_count=bucket_count) for token in token_ids}


def write_snapshot_partitions_incremental(
    staged_dir: Path,
    new_shard_paths: Sequence[Path],
    *,
    previous: MinuteOddsSnapshot | None,
    primary_token_ids: set[str],
    bucket_count: int = DEFAULT_TOKEN_BUCKET_COUNT,
) -> tuple[list[SnapshotPartitionFile], list[SnapshotPartitionFile], set[int]]:
    """Rebuild only dirty token buckets; copy unchanged raw/primary partitions.

    Non-changed prior tokens are always preserved from ``previous``; a partial
    shard publish must not shrink snapshot inventory.
    """
    changed_tokens = set()
    for path in new_shard_paths:
        frame = pl.read_parquet(path, columns=["clobTokenId"])
        if "clobTokenId" not in frame.columns:
            frame = pl.read_parquet(path)
            if "clob_token_id" in frame.columns:
                frame = frame.rename({"clob_token_id": "clobTokenId"})
        changed_tokens.update(str(token) for token in frame["clobTokenId"].unique())
    dirty_buckets = dirty_token_buckets(changed_tokens, bucket_count=bucket_count)
    if previous is None:
        if not new_shard_paths:
            raise MinuteOddsSnapshotError("raw_paths must not be empty")
        raw_files, primary_files = write_snapshot_partitions_from_raw_parquet(
            staged_dir,
            new_shard_paths,
            primary_token_ids=primary_token_ids,
            bucket_count=bucket_count,
        )
        return raw_files, primary_files, dirty_buckets

    previous_mapping = previous.primary_mapping_sha256
    mapping_changed = previous_mapping != primary_mapping_sha256(primary_token_ids)
    primary_dirty = set(range(bucket_count)) if mapping_changed else set(dirty_buckets)

    raw_files: list[SnapshotPartitionFile] = []
    primary_files: list[SnapshotPartitionFile] = []
    previous_raw = {part.bucket: part for part in previous.files if part.kind == "raw"}
    previous_primary = {
        part.bucket: part for part in previous.files if part.kind == "primary_ohlc"
    }

    # Copy unchanged raw buckets by hardlink/copy. Keep every prior token in
    # those buckets; only dirty buckets replace rows for changed tokens.
    for bucket, part in sorted(previous_raw.items()):
        if bucket in dirty_buckets:
            continue
        dest = staged_dir / "raw" / f"bucket={bucket}" / "part-00000.parquet"
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(part.path, dest)
        except OSError:
            shutil.copy2(part.path, dest)
        raw_files.append(
            SnapshotPartitionFile(
                kind="raw",
                bucket=bucket,
                path=dest,
                sha256=part.sha256,
                schema_fingerprint=part.schema_fingerprint,
                row_count=part.row_count,
                byte_size=dest.stat().st_size,
                token_ids=part.token_ids,
            )
        )

    # Rebuild dirty raw buckets from previous non-changed rows + new shards.
    shard_frames: list[pl.DataFrame] = []
    for path in new_shard_paths:
        frame = pl.read_parquet(path)
        if "clobTokenId" not in frame.columns and "clob_token_id" in frame.columns:
            frame = frame.rename({"clob_token_id": "clobTokenId"})
        if (frame["fidelity_minutes"] != 1).any():
            raise MinuteOddsSnapshotError(f"fidelity_minutes must be 1 in {path}")
        shard_frames.append(frame)
    new_frame = (
        pl.concat(shard_frames, how="vertical_relaxed") if shard_frames else None
    )
    for bucket in sorted(dirty_buckets):
        parts: list[pl.DataFrame] = []
        prior = previous_raw.get(bucket)
        if prior is not None:
            prior_frame = pl.read_parquet(prior.path)
            if (
                "clobTokenId" not in prior_frame.columns
                and "clob_token_id" in prior_frame.columns
            ):
                prior_frame = prior_frame.rename({"clob_token_id": "clobTokenId"})
            kept = prior_frame.filter(
                ~pl.col("clobTokenId").is_in(sorted(changed_tokens))
            )
            if not kept.is_empty():
                parts.append(kept)
        if new_frame is not None and not new_frame.is_empty():
            with_bucket = new_frame.with_columns(
                pl.col("clobTokenId")
                .map_elements(
                    lambda token: token_bucket(str(token), bucket_count=bucket_count),
                    return_dtype=pl.Int64,
                )
                .alias("_bucket")
            )
            bucket_rows = with_bucket.filter(pl.col("_bucket") == bucket).drop(
                "_bucket"
            )
            if not bucket_rows.is_empty():
                parts.append(bucket_rows)
        if not parts:
            continue
        merged = pl.concat(parts, how="vertical_relaxed").unique(
            subset=["clobTokenId", "timestamp"], keep="last"
        )
        out = staged_dir / "raw" / f"bucket={bucket}" / "part-00000.parquet"
        raw_files.append(
            _write_partition_parquet(merged, out, kind="raw", bucket=bucket)
        )

    # Unchanged rerun: no dirty buckets and no copied raw yet — hardlink the
    # full prior inventory (partial plans must not shrink the snapshot).
    if not dirty_buckets and not raw_files:
        for bucket, part in sorted(previous_raw.items()):
            dest = staged_dir / "raw" / f"bucket={bucket}" / "part-00000.parquet"
            dest.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.link(part.path, dest)
            except OSError:
                shutil.copy2(part.path, dest)
            raw_files.append(
                SnapshotPartitionFile(
                    kind="raw",
                    bucket=bucket,
                    path=dest,
                    sha256=part.sha256,
                    schema_fingerprint=part.schema_fingerprint,
                    row_count=part.row_count,
                    byte_size=dest.stat().st_size,
                    token_ids=part.token_ids,
                )
            )

    # Primary OHLC: rebuild dirty buckets (or all when mapping changed).
    for bucket, part in sorted(previous_primary.items()):
        if bucket in primary_dirty:
            continue
        dest = staged_dir / "primary_ohlc" / f"bucket={bucket}" / "part-00000.parquet"
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(part.path, dest)
        except OSError:
            shutil.copy2(part.path, dest)
        primary_files.append(
            SnapshotPartitionFile(
                kind="primary_ohlc",
                bucket=bucket,
                path=dest,
                sha256=part.sha256,
                schema_fingerprint=part.schema_fingerprint,
                row_count=part.row_count,
                byte_size=dest.stat().st_size,
                token_ids=part.token_ids,
            )
        )
    raw_by_bucket = {part.bucket: part for part in raw_files}
    for bucket in sorted(primary_dirty):
        raw_part = raw_by_bucket.get(bucket)
        if raw_part is None:
            continue
        ohlc = compute_primary_minute_ohlc(
            pl.read_parquet(raw_part.path),
            primary_token_ids=primary_token_ids,
        )
        if ohlc.is_empty():
            continue
        out = staged_dir / "primary_ohlc" / f"bucket={bucket}" / "part-00000.parquet"
        primary_files.append(
            _write_partition_parquet(ohlc, out, kind="primary_ohlc", bucket=bucket)
        )
    return raw_files, primary_files, dirty_buckets


def build_and_publish_snapshot_from_shards(
    *,
    leg: str,
    fetch_run_id: str,
    shard_paths: Sequence[Path],
    primary_token_ids: set[str],
    collected_at: datetime | None = None,
    runtime_root: Path | None = None,
    conn: duckdb.DuckDBPyConnection | None = None,
    register: bool = True,
    reuse_token_ids: set[str] | None = None,
    window_hashes: Mapping[str, str] | None = None,
    retain: bool = True,
) -> MinuteOddsSnapshot:
    """Promote ephemeral publish shards into an immutable snapshot and register views.

    When ``retain=False``, CURRENT still advances but old snapshots are not GC'd.
    Callers that register DuckDB views after publish should retain only after the
    warehouse commit succeeds, and call ``rollback_snapshot_pointer`` on failure.
    """
    if not shard_paths and not reuse_token_ids:
        raise MinuteOddsSnapshotError("shard_paths or reuse_token_ids required")
    root = minute_odds_snapshot_root(leg=leg, runtime_root=runtime_root)
    root.mkdir(parents=True, exist_ok=True)
    previous_id = active_snapshot_id(root)
    previous: MinuteOddsSnapshot | None = None
    if previous_id:
        try:
            previous = validate_minute_odds_snapshot(root / "snapshots" / previous_id)
        except MinuteOddsSnapshotError:
            previous = None
    snapshot_id = (
        f"{fetch_run_id}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    )
    staged = stage_snapshot_dir(root, snapshot_id)
    try:
        # Always merge into the prior snapshot when one exists. Fresh shards
        # overwrite only their tokens; out-of-plan prior tokens stay put so a
        # sampled/partial publish cannot shrink a full inventory.
        effective_primary = set(primary_token_ids)
        if previous is not None:
            effective_primary |= set(previous.primary_token_ids)
            raw_files, primary_files, _dirty = write_snapshot_partitions_incremental(
                staged,
                shard_paths,
                previous=previous,
                primary_token_ids=effective_primary,
            )
        else:
            raw_files, primary_files = write_snapshot_partitions_from_raw_parquet(
                staged,
                shard_paths,
                primary_token_ids=effective_primary,
            )
        merged_hashes: dict[str, str] = {}
        if previous is not None:
            merged_hashes.update(
                {
                    str(token): str(digest)
                    for token, digest in dict(
                        previous.manifest.get("window_hashes", {})
                    ).items()
                }
            )
        if window_hashes:
            merged_hashes.update(
                {str(token): str(digest) for token, digest in window_hashes.items()}
            )
        present = {token for part in raw_files for token in part.token_ids}
        merged_hashes = {
            token: digest for token, digest in merged_hashes.items() if token in present
        }
        manifest_primary = sorted(
            token for token in effective_primary if token in present
        )
        write_manifest(
            staged,
            leg=leg,
            snapshot_id=snapshot_id,
            fetch_run_id=fetch_run_id,
            collected_at=collected_at or datetime.now(timezone.utc),
            previous_snapshot_id=previous_id,
            primary_token_ids=manifest_primary,
            raw_files=raw_files,
            primary_files=primary_files,
            window_hashes=merged_hashes,
        )
        snapshot = publish_snapshot(root, staged)
    except Exception:
        shutil.rmtree(staged, ignore_errors=True)
        raise
    if retain:
        retain_snapshots(root, keep=2)
    if register and conn is not None:
        register_snapshot_views(conn, snapshot)
    return snapshot


def backfill_primary_ohlc_table(
    conn: duckdb.DuckDBPyConnection,
    *,
    leg: str,
    primary_token_ids: set[str] | None = None,
    scope_name: str = SCOPE_WC2026,
) -> int:
    """Populate the heap primary OHLC table from the current raw history relation.

    Used by synthetic seeds/CI that insert raw history without going through
    Parquet snapshot publish.
    """
    if not _LEG.fullmatch(leg):
        raise MinuteOddsSnapshotError(f"unknown minute-odds leg: {leg!r}")
    history = polymarket_raw_tbl(
        scope_name,
        "match_minute_odds_history"
        if leg == "match"
        else "futures_minute_odds_history",
    )
    target = polymarket_raw_tbl(
        scope_name,
        "match_primary_minute_ohlc"
        if leg == "match"
        else "futures_primary_minute_ohlc",
    )
    frame = conn.execute(
        f"""
        SELECT
            market_id,
            "clobTokenId" AS clob_token_id,
            timestamp AS odds_timestamp_epoch,
            price,
            window_start_at,
            window_end_at
        FROM {history}
        """
    ).pl()
    if primary_token_ids is None:
        primary_token_ids = set()
        if not frame.is_empty():
            for market_id in frame["market_id"].unique().to_list():
                tokens = [
                    str(token)
                    for token in frame.filter(pl.col("market_id") == market_id)[
                        "clob_token_id"
                    ]
                    .unique()
                    .to_list()
                ]
                yes = [
                    token
                    for token in tokens
                    if token.casefold().endswith("-yes") or "yes" in token.casefold()
                ]
                primary_token_ids.add(sorted(yes)[0] if yes else sorted(tokens)[0])
    ohlc = compute_primary_minute_ohlc(frame, primary_token_ids=set(primary_token_ids))
    # Relation may be a snapshot view; replace with a heap table for seeds/CI.
    _drop_relation(conn, target)
    conn.execute(
        f"""
        CREATE TABLE {target} (
            market_id VARCHAR,
            clob_token_id VARCHAR,
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
    if ohlc.is_empty():
        return 0
    conn.register("_primary_ohlc_backfill", ohlc.to_arrow())
    try:
        conn.execute(
            f"""
            INSERT INTO {target}
            SELECT * FROM _primary_ohlc_backfill
            """
        )
    finally:
        conn.unregister("_primary_ohlc_backfill")
    return int(ohlc.height)


__all__ = [
    "DEFAULT_TOKEN_BUCKET_COUNT",
    "MINUTE_ODDS_SNAPSHOT_CONTRACT",
    "MinuteOddsSnapshot",
    "MinuteOddsSnapshotError",
    "PublishedTokenWindow",
    "SnapshotPartitionFile",
    "active_snapshot_dir",
    "active_snapshot_id",
    "backfill_primary_ohlc_table",
    "build_and_publish_snapshot_from_shards",
    "compute_primary_minute_ohlc",
    "load_latest_published_token_windows",
    "minute_odds_snapshot_root",
    "primary_mapping_sha256",
    "publish_snapshot",
    "register_snapshot_views",
    "retain_snapshots",
    "stage_snapshot_dir",
    "token_bucket",
    "tokens_reusable_by_window",
    "validate_minute_odds_snapshot",
    "write_manifest",
    "write_snapshot_partitions_from_raw_parquet",
    "write_snapshot_partitions_incremental",
]
