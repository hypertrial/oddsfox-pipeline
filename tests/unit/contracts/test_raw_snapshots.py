from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from oddsfox_pipeline.contracts.raw_snapshots import (
    RAW_CONTRACT_VERSION,
    RawSnapshotError,
    load_snapshot,
    schema_fingerprint,
    validate_snapshot,
)


def _snapshot(
    root: Path,
    *,
    source: str = "eloratings",
    snapshot_id: str = "snapshot-1",
    collected_at: datetime | None = None,
    previous_snapshot_id: str | None = None,
    rows: list[dict[str, object]] | None = None,
) -> Path:
    directory = root / source / snapshot_id
    directory.mkdir(parents=True)
    table = pa.Table.from_pylist(
        rows
        or [
            {
                "team": "United States",
                "rating": 1820.0,
                "rating_date": "2026-07-18",
            }
        ]
    )
    parquet_path = directory / "team_ratings.parquet"
    pq.write_table(table, parquet_path)
    manifest = {
        "contract_version": RAW_CONTRACT_VERSION,
        "source": source,
        "snapshot_id": snapshot_id,
        "collected_at": (
            collected_at or datetime(2026, 7, 18, 17, tzinfo=timezone.utc)
        ).isoformat(),
        "collector_git_sha": "a" * 40,
        "collector_container_digest": "sha256:" + "b" * 64,
        "upstream": {"revision": "fixture-revision"},
        "status": "complete",
        "completeness": "complete",
        "previous_snapshot_id": previous_snapshot_id,
        "files": [
            {
                "table": "team_ratings",
                "path": parquet_path.name,
                "sha256": hashlib.sha256(parquet_path.read_bytes()).hexdigest(),
                "schema_fingerprint": schema_fingerprint(table.schema),
                "row_count": table.num_rows,
                "byte_size": parquet_path.stat().st_size,
            }
        ],
    }
    (directory / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True),
        encoding="utf-8",
    )
    return directory


def test_validates_and_loads_snapshot_with_provenance(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path / "raw")
    warehouse = tmp_path / "warehouse.duckdb"
    fingerprint = validate_snapshot(snapshot).files[0].schema_fingerprint
    validated = load_snapshot(
        snapshot,
        warehouse,
        expected_schemas={"team_ratings": fingerprint},
    )
    assert validated.source == "eloratings"
    with duckdb.connect(str(warehouse), read_only=True) as conn:
        row = conn.execute(
            """
            select team, rating, _source, _snapshot_id
            from wc2026_raw.eloratings__team_ratings
            """
        ).fetchone()
        assert row == ("United States", 1820.0, "eloratings", "snapshot-1")


def test_rejects_partial_hash_and_unknown_contract(tmp_path: Path) -> None:
    partial = tmp_path / "raw" / "eloratings" / "partial"
    partial.mkdir(parents=True)
    with pytest.raises(RawSnapshotError, match="partial"):
        validate_snapshot(partial)

    snapshot = _snapshot(tmp_path / "hash")
    (snapshot / "team_ratings.parquet").write_bytes(b"tampered")
    with pytest.raises(RawSnapshotError, match="byte size|SHA-256"):
        validate_snapshot(snapshot)

    unknown = _snapshot(tmp_path / "version")
    manifest_path = unknown / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["contract_version"] = "oddsfox.raw.v999"
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(RawSnapshotError, match="unknown"):
        validate_snapshot(unknown)


def test_rejects_duplicate_id_and_timestamp_regression(tmp_path: Path) -> None:
    root = tmp_path / "raw"
    warehouse = tmp_path / "warehouse.duckdb"
    first_time = datetime(2026, 7, 18, 17, tzinfo=timezone.utc)
    first = _snapshot(root, collected_at=first_time)
    fingerprint = validate_snapshot(first).files[0].schema_fingerprint
    schemas = {"team_ratings": fingerprint}
    load_snapshot(first, warehouse, expected_schemas=schemas)
    with pytest.raises(RawSnapshotError, match="duplicate"):
        load_snapshot(first, warehouse, expected_schemas=schemas)

    regressed = _snapshot(
        root,
        snapshot_id="snapshot-2",
        collected_at=first_time - timedelta(minutes=1),
        previous_snapshot_id="snapshot-1",
    )
    with pytest.raises(RawSnapshotError, match="regressed"):
        load_snapshot(regressed, warehouse, expected_schemas=schemas)


def test_rejects_predecessor_and_schema_mismatch(tmp_path: Path) -> None:
    root = tmp_path / "raw"
    warehouse = tmp_path / "warehouse.duckdb"
    first = _snapshot(root)
    fingerprint = validate_snapshot(first).files[0].schema_fingerprint
    load_snapshot(first, warehouse, expected_schemas={"team_ratings": fingerprint})

    wrong_predecessor = _snapshot(
        root,
        snapshot_id="snapshot-2",
        collected_at=datetime(2026, 7, 18, 18, tzinfo=timezone.utc),
        previous_snapshot_id="some-other-snapshot",
    )
    with pytest.raises(RawSnapshotError, match="predecessor"):
        load_snapshot(
            wrong_predecessor,
            warehouse,
            expected_schemas={"team_ratings": fingerprint},
        )

    other = _snapshot(
        tmp_path / "other",
        rows=[{"team": "United States", "rating": "not-a-number"}],
    )
    with pytest.raises(RawSnapshotError, match="canonical schema"):
        validate_snapshot(other, expected_schemas={"team_ratings": fingerprint})


def test_load_requires_schema_registry_and_rejects_sensitive_provenance(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot(tmp_path / "raw")
    with pytest.raises(RawSnapshotError, match="expected_schemas"):
        load_snapshot(snapshot, tmp_path / "warehouse.duckdb")

    manifest_path = snapshot / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["upstream"] = {
        "request": {"revision": "fixture-revision", "authorization": "secret"}
    }
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(RawSnapshotError, match="sensitive"):
        validate_snapshot(snapshot)
