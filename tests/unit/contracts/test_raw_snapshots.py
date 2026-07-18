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


def test_rejects_non_utc_unsafe_ids_and_empty_predecessors(tmp_path: Path) -> None:
    non_utc = _snapshot(
        tmp_path / "non-utc",
        collected_at=datetime(2026, 7, 18, 17, tzinfo=timezone(timedelta(hours=2))),
    )
    with pytest.raises(RawSnapshotError, match="must be UTC"):
        validate_snapshot(non_utc)

    unsafe = _snapshot(tmp_path / "unsafe")
    unsafe_manifest = unsafe / "manifest.json"
    manifest = json.loads(unsafe_manifest.read_text())
    manifest["snapshot_id"] = "../snapshot-1"
    unsafe_manifest.write_text(json.dumps(manifest))
    with pytest.raises(RawSnapshotError, match="snapshot_id must match"):
        validate_snapshot(unsafe)

    empty_predecessor = _snapshot(tmp_path / "predecessor")
    predecessor_manifest = empty_predecessor / "manifest.json"
    manifest = json.loads(predecessor_manifest.read_text())
    manifest["previous_snapshot_id"] = ""
    predecessor_manifest.write_text(json.dumps(manifest))
    with pytest.raises(RawSnapshotError, match="non-empty"):
        validate_snapshot(empty_predecessor)


@pytest.mark.parametrize("field,value", [("row_count", 1.5), ("byte_size", True)])
def test_rejects_non_integer_manifest_counts(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    snapshot = _snapshot(tmp_path / field)
    manifest_path = snapshot / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["files"][0][field] = value
    manifest_path.write_text(json.dumps(manifest))

    with pytest.raises(RawSnapshotError, match="non-negative integers"):
        validate_snapshot(snapshot)


def test_rejects_corrupt_parquet_and_missing_canonical_tables(tmp_path: Path) -> None:
    corrupt = _snapshot(tmp_path / "corrupt")
    parquet_path = corrupt / "team_ratings.parquet"
    parquet_path.write_bytes(b"not parquet")
    manifest_path = corrupt / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["files"][0]["sha256"] = hashlib.sha256(
        parquet_path.read_bytes()
    ).hexdigest()
    manifest["files"][0]["byte_size"] = parquet_path.stat().st_size
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(RawSnapshotError, match="invalid Parquet"):
        validate_snapshot(corrupt)

    snapshot = _snapshot(tmp_path / "missing")
    fingerprint = validate_snapshot(snapshot).files[0].schema_fingerprint
    with pytest.raises(RawSnapshotError, match="missing canonical tables"):
        validate_snapshot(
            snapshot,
            expected_schemas={
                "team_ratings": fingerprint,
                "required_companion": fingerprint,
            },
        )


@pytest.mark.parametrize("payload", ["{", "[]"])
def test_load_wraps_invalid_manifest_json(
    tmp_path: Path,
    payload: str,
) -> None:
    snapshot = tmp_path / "raw" / "eloratings" / "snapshot-1"
    snapshot.mkdir(parents=True)
    (snapshot / "manifest.json").write_text(payload)
    with pytest.raises(RawSnapshotError, match="UTF-8 JSON|contain an object"):
        load_snapshot(
            snapshot,
            tmp_path / "warehouse.duckdb",
            expected_schemas={"team_ratings": "fingerprint"},
        )


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("blank_required", "non-empty string"),
        ("invalid_timestamp", "ISO-8601"),
        ("missing_timezone", "include a timezone"),
        ("invalid_source", "must match"),
        ("directory_mismatch", "directory name"),
        ("parent_mismatch", "parent directory"),
        ("incomplete", "must both be complete"),
        ("invalid_predecessor", "string or null"),
        ("invalid_upstream", "must be an object"),
        ("empty_files", "non-empty array"),
        ("invalid_file_entry", "must be an object"),
        ("duplicate_table", "duplicate table"),
        ("unsafe_path", "unsafe Parquet path"),
        ("missing_payload", "payload is missing"),
        ("hash_mismatch", "SHA-256 mismatch"),
        ("row_mismatch", "row count mismatch"),
        ("fingerprint_mismatch", "schema fingerprint mismatch"),
        ("unknown_table", "unknown table"),
    ],
)
def test_rejects_invalid_manifest_shapes(
    tmp_path: Path,
    case: str,
    message: str,
) -> None:
    snapshot = _snapshot(tmp_path / case)
    manifest_path = snapshot / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    entry = manifest["files"][0]

    if case == "blank_required":
        manifest["collector_git_sha"] = ""
    elif case == "invalid_timestamp":
        manifest["collected_at"] = "not-a-timestamp"
    elif case == "missing_timezone":
        manifest["collected_at"] = "2026-07-18T17:00:00"
    elif case == "invalid_source":
        manifest["source"] = "Invalid-Source"
    elif case == "directory_mismatch":
        manifest["snapshot_id"] = "snapshot-2"
    elif case == "parent_mismatch":
        manifest["source"] = "clubelo"
    elif case == "incomplete":
        manifest["status"] = "partial"
    elif case == "invalid_predecessor":
        manifest["previous_snapshot_id"] = 123
    elif case == "invalid_upstream":
        manifest["upstream"] = []
    elif case == "empty_files":
        manifest["files"] = []
    elif case == "invalid_file_entry":
        manifest["files"] = ["not-an-object"]
    elif case == "duplicate_table":
        manifest["files"].append(dict(entry))
    elif case == "unsafe_path":
        entry["path"] = "team_ratings.csv"
    elif case == "missing_payload":
        entry["path"] = "missing.parquet"
    elif case == "hash_mismatch":
        entry["sha256"] = "0" * 64
    elif case == "row_mismatch":
        entry["row_count"] += 1
    elif case == "fingerprint_mismatch":
        entry["schema_fingerprint"] = "0" * 64

    manifest_path.write_text(json.dumps(manifest))
    expected_schemas = {} if case == "unknown_table" else None
    with pytest.raises(RawSnapshotError, match=message):
        validate_snapshot(snapshot, expected_schemas=expected_schemas)


def test_rejects_duplicate_and_escaping_payload_paths(tmp_path: Path) -> None:
    duplicate = _snapshot(tmp_path / "duplicate")
    manifest_path = duplicate / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    second_entry = dict(manifest["files"][0])
    second_entry["table"] = "other_ratings"
    manifest["files"].append(second_entry)
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(RawSnapshotError, match="duplicate file path"):
        validate_snapshot(duplicate)

    escaping = _snapshot(tmp_path / "escaping")
    outside = tmp_path / "outside.parquet"
    outside.write_bytes((escaping / "team_ratings.parquet").read_bytes())
    link = escaping / "linked.parquet"
    link.symlink_to(outside)
    manifest_path = escaping / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["files"][0]["path"] = link.name
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(RawSnapshotError, match="escapes snapshot directory"):
        validate_snapshot(escaping)


def test_accepts_nested_list_provenance(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path / "nested-upstream")
    manifest_path = snapshot / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["upstream"] = {"requests": [{"revision": "fixture-revision"}]}
    manifest_path.write_text(json.dumps(manifest))

    assert validate_snapshot(snapshot).source == "eloratings"


def test_load_rejects_missing_manifest_and_rolls_back_schema_mismatch(
    tmp_path: Path,
) -> None:
    partial = tmp_path / "partial" / "eloratings" / "snapshot-1"
    partial.mkdir(parents=True)
    with pytest.raises(RawSnapshotError, match="partial"):
        load_snapshot(
            partial,
            tmp_path / "partial.duckdb",
            expected_schemas={"team_ratings": "fingerprint"},
        )

    root = tmp_path / "raw"
    warehouse = tmp_path / "warehouse.duckdb"
    first = _snapshot(root)
    first_fingerprint = validate_snapshot(first).files[0].schema_fingerprint
    load_snapshot(
        first,
        warehouse,
        expected_schemas={"team_ratings": first_fingerprint},
    )
    incompatible = _snapshot(
        root,
        snapshot_id="snapshot-2",
        collected_at=datetime(2026, 7, 18, 18, tzinfo=timezone.utc),
        previous_snapshot_id="snapshot-1",
        rows=[
            {
                "team": "United States",
                "rating": "not-a-number",
                "rating_date": "2026-07-18",
            }
        ],
    )
    incompatible_fingerprint = (
        validate_snapshot(incompatible).files[0].schema_fingerprint
    )

    with pytest.raises(RawSnapshotError, match="warehouse schema mismatch"):
        load_snapshot(
            incompatible,
            warehouse,
            expected_schemas={"team_ratings": incompatible_fingerprint},
        )

    with duckdb.connect(str(warehouse), read_only=True) as conn:
        assert conn.execute(
            "select count(*) from wc2026_raw.eloratings__team_ratings"
        ).fetchone() == (1,)
        assert conn.execute(
            "select count(*) from wc2026_ops.raw_snapshot_ledger"
        ).fetchone() == (1,)
