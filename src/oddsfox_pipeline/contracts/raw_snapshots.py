"""Validation and transactional loading for canonical raw snapshots."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Mapping

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

RAW_CONTRACT_VERSION = "oddsfox.raw.v1"
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
_SENSITIVE_PROVENANCE_KEY = re.compile(
    r"(authorization|cookie|credential|password|private[_-]?key|secret|token)",
    re.IGNORECASE,
)


class RawSnapshotError(ValueError):
    """Raised when a snapshot is incomplete, unsafe, or contract-incompatible."""


@dataclass(frozen=True, slots=True)
class SnapshotFile:
    table: str
    path: Path
    sha256: str
    schema_fingerprint: str
    row_count: int
    byte_size: int


@dataclass(frozen=True, slots=True)
class RawSnapshot:
    directory: Path
    source: str
    snapshot_id: str
    collected_at: datetime
    collector_git_sha: str
    collector_container_digest: str
    previous_snapshot_id: str | None
    files: tuple[SnapshotFile, ...]
    manifest: dict[str, object]


def _require_text(manifest: Mapping[str, object], name: str) -> str:
    value = manifest.get(name)
    if not isinstance(value, str) or not value.strip():
        raise RawSnapshotError(f"manifest field {name!r} must be a non-empty string")
    return value


def _parse_timestamp(value: str) -> datetime:
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RawSnapshotError("collected_at must be an ISO-8601 timestamp") from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise RawSnapshotError("collected_at must include a timezone")
    return timestamp


def _safe_identifier(kind: str, value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise RawSnapshotError(f"{kind} must match {_IDENTIFIER.pattern!r}: {value!r}")
    return value


def _reject_sensitive_provenance(value: object, *, path: str = "upstream") -> None:
    """Reject credential-bearing fields before provenance enters a public warehouse."""
    if isinstance(value, dict):
        for key, nested in value.items():
            key_text = str(key)
            if _SENSITIVE_PROVENANCE_KEY.search(key_text):
                raise RawSnapshotError(
                    f"upstream provenance contains sensitive field: {path}.{key_text}"
                )
            _reject_sensitive_provenance(nested, path=f"{path}.{key_text}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _reject_sensitive_provenance(nested, path=f"{path}[{index}]")


def schema_fingerprint(schema: pa.Schema) -> str:
    """Hash the stable logical shape of an Arrow schema."""
    fields = [
        {
            "name": field.name,
            "type": str(field.type),
            "nullable": field.nullable,
        }
        for field in schema
    ]
    canonical = json.dumps(fields, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(canonical).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_snapshot(
    snapshot_dir: Path,
    *,
    expected_schemas: Mapping[str, str] | None = None,
    previous_collected_at: datetime | None = None,
    previous_snapshot_id: str | None = None,
) -> RawSnapshot:
    """Validate the published manifest and every declared Parquet payload."""
    directory = snapshot_dir.resolve()
    manifest_path = directory / "manifest.json"
    if not manifest_path.is_file():
        raise RawSnapshotError(f"snapshot is partial: missing {manifest_path}")
    try:
        raw_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RawSnapshotError("manifest.json is not valid UTF-8 JSON") from exc
    if not isinstance(raw_manifest, dict):
        raise RawSnapshotError("manifest.json must contain an object")
    manifest: dict[str, object] = raw_manifest

    contract_version = _require_text(manifest, "contract_version")
    if contract_version != RAW_CONTRACT_VERSION:
        raise RawSnapshotError(f"unknown raw contract version: {contract_version}")
    source = _safe_identifier("source", _require_text(manifest, "source"))
    snapshot_id = _require_text(manifest, "snapshot_id")
    if directory.name != snapshot_id:
        raise RawSnapshotError("snapshot_id must equal the snapshot directory name")
    if directory.parent.name != source:
        raise RawSnapshotError("source must equal the parent directory name")
    if (
        manifest.get("status") != "complete"
        or manifest.get("completeness") != "complete"
    ):
        raise RawSnapshotError("snapshot status and completeness must both be complete")

    collected_at = _parse_timestamp(_require_text(manifest, "collected_at"))
    if previous_collected_at is not None and collected_at <= previous_collected_at:
        raise RawSnapshotError("snapshot collection timestamp regressed")
    declared_previous = manifest.get("previous_snapshot_id")
    if declared_previous is not None and not isinstance(declared_previous, str):
        raise RawSnapshotError("previous_snapshot_id must be a string or null")
    if previous_snapshot_id is not None and declared_previous != previous_snapshot_id:
        raise RawSnapshotError(
            "previous_snapshot_id does not match the loaded predecessor"
        )

    collector_git_sha = _require_text(manifest, "collector_git_sha")
    collector_container_digest = _require_text(manifest, "collector_container_digest")
    upstream = manifest.get("upstream")
    if not isinstance(upstream, dict):
        raise RawSnapshotError("upstream provenance must be an object")
    _reject_sensitive_provenance(upstream)

    raw_files = manifest.get("files")
    if not isinstance(raw_files, list) or not raw_files:
        raise RawSnapshotError("files must be a non-empty array")
    files: list[SnapshotFile] = []
    seen_tables: set[str] = set()
    seen_paths: set[Path] = set()
    for entry in raw_files:
        if not isinstance(entry, dict):
            raise RawSnapshotError("each files entry must be an object")
        table = _safe_identifier("table", _require_text(entry, "table"))
        if table in seen_tables:
            raise RawSnapshotError(f"duplicate table in manifest: {table}")
        relative = Path(_require_text(entry, "path"))
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or relative.suffix.lower() != ".parquet"
        ):
            raise RawSnapshotError(f"unsafe Parquet path: {relative}")
        path = (directory / relative).resolve()
        if directory not in path.parents:
            raise RawSnapshotError(f"payload escapes snapshot directory: {relative}")
        if path in seen_paths:
            raise RawSnapshotError(f"duplicate file path in manifest: {relative}")
        if not path.is_file():
            raise RawSnapshotError(f"declared payload is missing: {relative}")

        expected_hash = _require_text(entry, "sha256")
        expected_fingerprint = _require_text(entry, "schema_fingerprint")
        try:
            expected_rows = int(entry["row_count"])
            expected_bytes = int(entry["byte_size"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RawSnapshotError(
                "row_count and byte_size must be non-negative integers"
            ) from exc
        if expected_rows < 0 or expected_bytes < 0:
            raise RawSnapshotError("row_count and byte_size must be non-negative")
        actual_bytes = path.stat().st_size
        if actual_bytes != expected_bytes:
            raise RawSnapshotError(f"byte size mismatch for {relative}")
        actual_hash = _sha256(path)
        if actual_hash != expected_hash:
            raise RawSnapshotError(f"SHA-256 mismatch for {relative}")
        parquet = pq.ParquetFile(path)
        actual_rows = parquet.metadata.num_rows
        actual_fingerprint = schema_fingerprint(parquet.schema_arrow)
        if actual_rows != expected_rows:
            raise RawSnapshotError(f"row count mismatch for {relative}")
        if actual_fingerprint != expected_fingerprint:
            raise RawSnapshotError(f"schema fingerprint mismatch for {relative}")
        if expected_schemas is not None:
            expected_schema = expected_schemas.get(table)
            if expected_schema is None:
                raise RawSnapshotError(f"unknown table for source {source}: {table}")
            if actual_fingerprint != expected_schema:
                raise RawSnapshotError(f"invalid canonical schema for table {table}")

        seen_tables.add(table)
        seen_paths.add(path)
        files.append(
            SnapshotFile(
                table=table,
                path=path,
                sha256=actual_hash,
                schema_fingerprint=actual_fingerprint,
                row_count=actual_rows,
                byte_size=actual_bytes,
            )
        )

    return RawSnapshot(
        directory=directory,
        source=source,
        snapshot_id=snapshot_id,
        collected_at=collected_at,
        collector_git_sha=collector_git_sha,
        collector_container_digest=collector_container_digest,
        previous_snapshot_id=declared_previous,
        files=tuple(files),
        manifest=manifest,
    )


def _ensure_ledger(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("create schema if not exists wc2026_ops")
    conn.execute("create schema if not exists wc2026_raw")
    conn.execute(
        """
        create table if not exists wc2026_ops.raw_snapshot_ledger (
            source varchar not null,
            snapshot_id varchar not null,
            collected_at timestamptz not null,
            collector_git_sha varchar not null,
            collector_container_digest varchar not null,
            manifest_sha256 varchar not null,
            loaded_at timestamptz not null default current_timestamp,
            primary key (source, snapshot_id)
        )
        """
    )


def load_snapshot(
    snapshot_dir: Path,
    warehouse_path: Path,
    *,
    expected_schemas: Mapping[str, str] | None = None,
) -> RawSnapshot:
    """Validate and append a snapshot exactly once inside one DuckDB transaction."""
    if expected_schemas is None:
        raise RawSnapshotError(
            "expected_schemas is required when loading a canonical snapshot"
        )
    directory = snapshot_dir.resolve()
    manifest_path = directory / "manifest.json"
    if not manifest_path.is_file():
        raise RawSnapshotError(f"snapshot is partial: missing {manifest_path}")
    preliminary = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(preliminary, dict):
        raise RawSnapshotError("manifest.json must contain an object")
    source = _safe_identifier("source", _require_text(preliminary, "source"))
    snapshot_id = _require_text(preliminary, "snapshot_id")

    warehouse = warehouse_path.resolve()
    warehouse.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(warehouse))
    try:
        _ensure_ledger(conn)
        predecessor = conn.execute(
            """
            select snapshot_id, collected_at
            from wc2026_ops.raw_snapshot_ledger
            where source = ?
            order by collected_at desc
            limit 1
            """,
            [source],
        ).fetchone()
        duplicate = conn.execute(
            """
            select 1
            from wc2026_ops.raw_snapshot_ledger
            where source = ? and snapshot_id = ?
            """,
            [source, snapshot_id],
        ).fetchone()
        if duplicate is not None:
            raise RawSnapshotError(f"duplicate snapshot ID: {source}/{snapshot_id}")
        snapshot = validate_snapshot(
            directory,
            expected_schemas=expected_schemas,
            previous_snapshot_id=str(predecessor[0]) if predecessor else None,
            previous_collected_at=predecessor[1] if predecessor else None,
        )
        conn.execute("begin transaction")
        for payload in snapshot.files:
            relation = f'wc2026_raw."{source}__{payload.table}"'
            parquet_path = str(payload.path)
            conn.execute(
                f"""
                create table if not exists {relation} as
                select
                    *,
                    cast(? as varchar) as _source,
                    cast(? as varchar) as _snapshot_id,
                    cast(? as timestamptz) as _collected_at
                from read_parquet(?)
                limit 0
                """,
                [source, snapshot.snapshot_id, snapshot.collected_at, parquet_path],
            )
            try:
                conn.execute(
                    f"""
                    insert into {relation} by name
                    select
                        *,
                        cast(? as varchar) as _source,
                        cast(? as varchar) as _snapshot_id,
                        cast(? as timestamptz) as _collected_at
                    from read_parquet(?)
                    """,
                    [source, snapshot.snapshot_id, snapshot.collected_at, parquet_path],
                )
            except duckdb.Error as exc:
                raise RawSnapshotError(
                    f"warehouse schema mismatch for {source}/{payload.table}"
                ) from exc
        conn.execute(
            """
            insert into wc2026_ops.raw_snapshot_ledger (
                source,
                snapshot_id,
                collected_at,
                collector_git_sha,
                collector_container_digest,
                manifest_sha256
            ) values (?, ?, ?, ?, ?, ?)
            """,
            [
                snapshot.source,
                snapshot.snapshot_id,
                snapshot.collected_at,
                snapshot.collector_git_sha,
                snapshot.collector_container_digest,
                _sha256(manifest_path),
            ],
        )
        conn.execute("commit")
        return snapshot
    except BaseException:
        try:
            conn.execute("rollback")
        except duckdb.TransactionException:
            pass
        raise
    finally:
        conn.close()
