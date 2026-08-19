from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from oddsfox_pipeline.contracts.reference_bundle import (
    REFERENCE_TABLE_PRIMARY_KEYS,
    ReferenceBundleError,
    load_reference_bundle,
)
from oddsfox_pipeline.contracts.schema import schema_fingerprint


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _bundle(root: Path, bundle_id: str, values: list[str]) -> Path:
    directory = root / bundle_id
    directory.mkdir()
    entries = []
    for name, keys in REFERENCE_TABLE_PRIMARY_KEYS.items():
        columns = {
            key: values if name == "wc2026_team_ratings_current" else [f"{name}-{key}"]
            for key in keys
        }
        table = pa.table(columns)
        payload = directory / f"{name}.parquet"
        pq.write_table(table, payload)
        entries.append(
            {
                "table": name,
                "path": payload.name,
                "primary_key": list(keys),
                "row_count": table.num_rows,
                "sha256": _sha(payload),
                "schema_fingerprint": schema_fingerprint(table.schema),
            }
        )
    manifest = {
        "contract_version": "oddsfox.reference.v1",
        "schema_version": "1.0.0",
        "bundle_id": bundle_id,
        "status": "complete",
        "created_at": "2026-01-01T00:00:00Z",
        "scraper_revision": "a" * 40,
        "scraper_image_digest": "sha256:" + "b" * 64,
        "sources": [
            {
                "source": "fixture",
                "snapshot_id": "one",
                "revision": "a" * 40,
                "checksum": "c" * 64,
                "acquired_at": "2026-01-01T00:00:00Z",
                "license": "CC0-1.0",
            }
        ],
        "tables": entries,
    }
    manifest_path = directory / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    checksum_paths = [manifest_path, *(directory / entry["path"] for entry in entries)]
    (directory / "checksums.sha256").write_text(
        "".join(f"{_sha(path)}  {path.name}\n" for path in checksum_paths),
        encoding="utf-8",
    )
    return directory


def test_reference_bundle_load_is_transactional_and_idempotent(tmp_path) -> None:
    bundle = _bundle(tmp_path, "one", ["arg", "fra"])
    warehouse = tmp_path / "warehouse.duckdb"
    load_reference_bundle(bundle, warehouse)
    load_reference_bundle(bundle, warehouse)
    connection = duckdb.connect(str(warehouse), read_only=True)
    try:
        assert connection.execute(
            "select team_code from oddsfox_reference.wc2026_team_ratings_current order by team_code"
        ).fetchall() == [("arg",), ("fra",)]
    finally:
        connection.close()


def test_reference_bundle_rejects_duplicate_primary_key_without_replacing(
    tmp_path,
) -> None:
    warehouse = tmp_path / "warehouse.duckdb"
    load_reference_bundle(_bundle(tmp_path, "one", ["arg"]), warehouse)
    with pytest.raises(ReferenceBundleError, match="duplicate primary key"):
        load_reference_bundle(_bundle(tmp_path, "two", ["fra", "fra"]), warehouse)
    connection = duckdb.connect(str(warehouse), read_only=True)
    try:
        assert connection.execute(
            "select team_code from oddsfox_reference.wc2026_team_ratings_current"
        ).fetchall() == [("arg",)]
    finally:
        connection.close()
