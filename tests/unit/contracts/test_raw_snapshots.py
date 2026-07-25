from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, call

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from oddsfox_pipeline.contracts import raw_snapshots as raw_snapshots_mod
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
        (
            "blank_required",
            "manifest field 'collector_git_sha' must be a non-empty string",
        ),
        ("invalid_timestamp", "collected_at must be an ISO-8601 timestamp"),
        ("missing_timezone", "collected_at must include a timezone"),
        (
            "invalid_source",
            "source must match '^[a-z][a-z0-9_]*$': 'Invalid-Source'",
        ),
        (
            "directory_mismatch",
            "snapshot_id must equal the snapshot directory name",
        ),
        ("parent_mismatch", "source must equal the parent directory name"),
        (
            "incomplete",
            "snapshot status and completeness must both be complete",
        ),
        (
            "invalid_predecessor",
            "previous_snapshot_id must be a string or null",
        ),
        ("invalid_upstream", "upstream provenance must be an object"),
        ("empty_files", "files must be a non-empty array"),
        ("invalid_file_entry", "each files entry must be an object"),
        ("duplicate_table", "duplicate table in manifest: team_ratings"),
        ("unsafe_path", "unsafe Parquet path: team_ratings.csv"),
        ("missing_payload", "declared payload is missing: missing.parquet"),
        ("hash_mismatch", "SHA-256 mismatch for team_ratings.parquet"),
        ("row_mismatch", "row count mismatch for team_ratings.parquet"),
        (
            "fingerprint_mismatch",
            "schema fingerprint mismatch for team_ratings.parquet",
        ),
        (
            "unknown_table",
            "unknown table for source eloratings: team_ratings",
        ),
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
    with pytest.raises(RawSnapshotError) as raised:
        validate_snapshot(snapshot, expected_schemas=expected_schemas)
    assert str(raised.value) == message


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


def test_snapshot_value_contract_is_complete(tmp_path: Path) -> None:
    directory = _snapshot(tmp_path / "raw")
    manifest = json.loads((directory / "manifest.json").read_text())
    validated = validate_snapshot(directory)
    payload = validated.files[0]

    assert validated.directory == directory.resolve()
    assert validated.source == "eloratings"
    assert validated.snapshot_id == "snapshot-1"
    assert validated.collected_at == datetime(2026, 7, 18, 17, tzinfo=timezone.utc)
    assert validated.collector_git_sha == "a" * 40
    assert validated.collector_container_digest == "sha256:" + "b" * 64
    assert validated.previous_snapshot_id is None
    assert validated.manifest == manifest
    assert len(validated.files) == 1
    assert payload.table == "team_ratings"
    assert payload.path == (directory / "team_ratings.parquet").resolve()
    assert payload.sha256 == manifest["files"][0]["sha256"]
    assert payload.schema_fingerprint == manifest["files"][0]["schema_fingerprint"]
    assert payload.row_count == 1
    assert payload.byte_size == (directory / "team_ratings.parquet").stat().st_size

    with_previous = _snapshot(
        tmp_path / "previous",
        previous_snapshot_id="snapshot-0",
    )
    assert validate_snapshot(with_previous).previous_snapshot_id == "snapshot-0"


def test_schema_fingerprint_has_a_stable_canonical_encoding() -> None:
    schema = pa.schema(
        [
            pa.field("name", pa.string(), nullable=False),
            pa.field("score", pa.int64(), nullable=True),
        ]
    )

    assert schema_fingerprint(schema) == (
        "63fb1515be791b1239ce2ca2eab368814282ae66568359d652af023b8789ea0b"
    )


def test_timestamp_parser_preserves_utc_semantics_and_exact_errors() -> None:
    expected = datetime(2026, 7, 18, 17, 1, 2, tzinfo=timezone.utc)
    assert raw_snapshots_mod._parse_timestamp("2026-07-18T17:01:02Z") == expected
    assert raw_snapshots_mod._parse_timestamp("2026-07-18T17:01:02+00:00") == expected
    assert raw_snapshots_mod._parse_timestamp(
        "2026-07-18T17:01:02.123456Z"
    ) == expected.replace(microsecond=123456)
    assert raw_snapshots_mod._parse_timestamp("2026-07-18 17:01:02+00:00") == expected

    for value, message in [
        ("bad", "collected_at must be an ISO-8601 timestamp"),
        ("2026-07-18T17:01:02z", "collected_at must be an ISO-8601 timestamp"),
        ("2026-07-18T17:01:02", "collected_at must include a timezone"),
        ("2026-07-18T18:01:02+01:00", "collected_at must be UTC"),
    ]:
        with pytest.raises(RawSnapshotError) as raised:
            raw_snapshots_mod._parse_timestamp(value)
        assert str(raised.value) == message


def test_timestamp_parser_normalizes_z_before_python_310_parser(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parsed_values: list[str] = []

    class RecordingDateTime:
        @staticmethod
        def fromisoformat(value: str) -> datetime:
            parsed_values.append(value)
            return datetime.fromisoformat(value)

    monkeypatch.setattr(raw_snapshots_mod, "datetime", RecordingDateTime)

    assert raw_snapshots_mod._parse_timestamp("2026-07-18T17:01:02Z") == datetime(
        2026, 7, 18, 17, 1, 2, tzinfo=timezone.utc
    )
    assert parsed_values == ["2026-07-18T17:01:02+00:00"]


def test_sensitive_provenance_reports_the_exact_nested_path() -> None:
    with pytest.raises(RawSnapshotError) as raised:
        raw_snapshots_mod._reject_sensitive_provenance(
            {"requests": [{"metadata": {"private-key": "value"}}]}
        )

    assert str(raised.value) == (
        "upstream provenance contains sensitive field: "
        "upstream.requests[0].metadata.private-key"
    )


def test_manifest_helpers_accept_zero_and_reject_bad_values_exactly(
    tmp_path: Path,
) -> None:
    assert raw_snapshots_mod._nonnegative_integer({"row_count": 0}, "row_count") == 0
    for value in (-1, True, 1.5):
        with pytest.raises(RawSnapshotError) as raised:
            raw_snapshots_mod._nonnegative_integer({"row_count": value}, "row_count")
        assert str(raised.value) == (
            "row_count and byte_size must be non-negative integers"
        )

    invalid = tmp_path / "manifest.json"
    invalid.write_text("{")
    with pytest.raises(RawSnapshotError) as raised:
        raw_snapshots_mod._read_manifest(invalid)
    assert str(raised.value) == "manifest.json is not valid UTF-8 JSON"

    invalid.write_text("[]")
    with pytest.raises(RawSnapshotError) as raised:
        raw_snapshots_mod._read_manifest(invalid)
    assert str(raised.value) == "manifest.json must contain an object"


def test_manifest_and_ledger_use_exact_storage_contracts() -> None:
    manifest_path = MagicMock(spec=Path)
    manifest_path.read_text.return_value = "{}"
    assert raw_snapshots_mod._read_manifest(manifest_path) == {}
    manifest_path.read_text.assert_called_once_with(encoding="utf-8")

    conn = MagicMock()
    raw_snapshots_mod._ensure_ledger(conn)
    assert conn.execute.call_args_list[:2] == [
        call("create schema if not exists wc2026_ops"),
        call("create schema if not exists wc2026_raw"),
    ]


def test_load_uses_exact_transaction_commands(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_connect = duckdb.connect
    commands: list[str] = []
    fail_ledger_insert = False

    class RecordingConnection:
        def __init__(self, path: str) -> None:
            self.inner = real_connect(path)

        def execute(self, query: str, parameters=None):
            commands.append(query)
            if (
                fail_ledger_insert
                and "insert into wc2026_ops.raw_snapshot_ledger" in query
            ):
                raise RuntimeError("forced ledger failure")
            if parameters is None:
                return self.inner.execute(query)
            return self.inner.execute(query, parameters)

        def close(self) -> None:
            self.inner.close()

    monkeypatch.setattr(
        raw_snapshots_mod.duckdb,
        "connect",
        lambda path: RecordingConnection(path),
    )
    snapshot = _snapshot(tmp_path / "success")
    fingerprint = validate_snapshot(snapshot).files[0].schema_fingerprint
    load_snapshot(
        snapshot,
        tmp_path / "success.duckdb",
        expected_schemas={"team_ratings": fingerprint},
    )
    assert "begin transaction" in commands
    assert commands[-1] == "commit"

    commands.clear()
    fail_ledger_insert = True
    failing = _snapshot(tmp_path / "failure")
    fingerprint = validate_snapshot(failing).files[0].schema_fingerprint
    with pytest.raises(RuntimeError, match="forced ledger failure"):
        load_snapshot(
            failing,
            tmp_path / "failure.duckdb",
            expected_schemas={"team_ratings": fingerprint},
        )
    assert "begin transaction" in commands
    assert commands[-1] == "rollback"


@pytest.mark.parametrize("unsafe_path", ["/tmp/payload.parquet", "../payload.parquet"])
def test_rejects_each_unsafe_parquet_path_condition(
    tmp_path: Path,
    unsafe_path: str,
) -> None:
    snapshot = _snapshot(tmp_path / unsafe_path.replace("/", "_"))
    manifest_path = snapshot / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["files"][0]["path"] = unsafe_path
    manifest_path.write_text(json.dumps(manifest))

    with pytest.raises(RawSnapshotError) as raised:
        validate_snapshot(snapshot)
    assert str(raised.value) == f"unsafe Parquet path: {unsafe_path}"


def test_rejects_equal_predecessor_timestamp(tmp_path: Path) -> None:
    collected_at = datetime(2026, 7, 18, 17, tzinfo=timezone.utc)
    snapshot = _snapshot(tmp_path / "raw", collected_at=collected_at)

    with pytest.raises(RawSnapshotError) as raised:
        validate_snapshot(snapshot, previous_collected_at=collected_at)

    assert str(raised.value) == "snapshot collection timestamp regressed"


def test_load_creates_nested_warehouse_parents(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path / "raw")
    fingerprint = validate_snapshot(snapshot).files[0].schema_fingerprint
    warehouse = tmp_path / "deep" / "nested" / "warehouse.duckdb"

    loaded = load_snapshot(
        snapshot,
        warehouse,
        expected_schemas={"team_ratings": fingerprint},
    )

    assert warehouse.is_file()
    assert loaded.snapshot_id == "snapshot-1"


def test_snapshot_and_load_errors_preserve_exact_contract_context(
    tmp_path: Path,
) -> None:
    partial = tmp_path / "raw" / "eloratings" / "snapshot-1"
    partial.mkdir(parents=True)
    expected_missing = f"snapshot is partial: missing {partial / 'manifest.json'}"
    with pytest.raises(RawSnapshotError) as raised:
        validate_snapshot(partial)
    assert str(raised.value) == expected_missing
    with pytest.raises(RawSnapshotError) as raised:
        load_snapshot(
            partial,
            tmp_path / "warehouse.duckdb",
            expected_schemas={"team_ratings": "fingerprint"},
        )
    assert str(raised.value) == expected_missing

    snapshot = _snapshot(tmp_path / "invalid-table")
    manifest_path = snapshot / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["files"][0]["table"] = "Invalid-Table"
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(RawSnapshotError) as raised:
        validate_snapshot(snapshot)
    assert str(raised.value) == (
        "table must match '^[a-z][a-z0-9_]*$': 'Invalid-Table'"
    )

    invalid_source = _snapshot(tmp_path / "invalid-source")
    manifest_path = invalid_source / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["source"] = "Invalid-Source"
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(RawSnapshotError) as raised:
        load_snapshot(
            invalid_source,
            tmp_path / "invalid-source.duckdb",
            expected_schemas={"team_ratings": "fingerprint"},
        )
    assert str(raised.value) == (
        "source must match '^[a-z][a-z0-9_]*$': 'Invalid-Source'"
    )


def test_predecessor_errors_and_load_schema_requirement_are_exact(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot(tmp_path / "raw")
    manifest_path = snapshot / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["previous_snapshot_id"] = ""
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(RawSnapshotError) as raised:
        validate_snapshot(snapshot)
    assert str(raised.value) == "previous_snapshot_id must be non-empty or null"

    manifest["previous_snapshot_id"] = "wrong"
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(RawSnapshotError) as raised:
        validate_snapshot(snapshot, previous_snapshot_id="expected")
    assert str(raised.value) == (
        "previous_snapshot_id does not match the loaded predecessor"
    )

    with pytest.raises(RawSnapshotError) as raised:
        load_snapshot(snapshot, tmp_path / "warehouse.duckdb")
    assert str(raised.value) == (
        "expected_schemas is required when loading a canonical snapshot"
    )


def test_load_enforces_expected_schema_registry_on_fresh_warehouse(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot(tmp_path / "raw")

    with pytest.raises(RawSnapshotError) as raised:
        load_snapshot(
            snapshot,
            tmp_path / "fresh.duckdb",
            expected_schemas={},
        )

    assert str(raised.value) == "unknown table for source eloratings: team_ratings"
