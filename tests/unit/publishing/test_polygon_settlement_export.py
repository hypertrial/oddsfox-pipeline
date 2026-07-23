"""Tests for the publisher-neutral Polygon settlement export boundary."""

from __future__ import annotations

import csv
import hashlib
import json
import shutil
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pytest

from oddsfox_pipeline.ingestion.polymarket.polygon_resolution import (
    load_polygon_resolution_attestation,
)
from oddsfox_pipeline.publishing import polygon_settlement_export as export


def _contract(monkeypatch) -> None:
    monkeypatch.setattr(export, "EXPECTED_MART_ROWS", 6)
    monkeypatch.setattr(export, "EXPECTED_MARKETS", 3)
    monkeypatch.setattr(export, "EXPECTED_MATCHES", 1)
    monkeypatch.setattr(export, "GROUP_MATCHES", 1)
    monkeypatch.setattr(export, "GROUP_WINDOW_MINUTES", 2)
    monkeypatch.setattr(export, "KNOCKOUT_WINDOW_MINUTES", 2)
    monkeypatch.setattr(
        export,
        "EXPECTED_PROPOSITION_INVENTORY",
        Counter({"home_win": 1, "draw": 1, "away_win": 1}),
    )
    monkeypatch.setattr(export, "_current_clean_commit", lambda _root: "b" * 40)


def _row(proposition_type: str, proposition_id: str, minute: int) -> dict[str, str]:
    observed = minute == 0
    timestamp = f"2026-06-11T12:0{minute}:00Z"
    row = {
        "dataset_version": "1.2.3",
        "fifa_match_id": "1",
        "stage": "group_stage",
        "group_name": "A",
        "home_team": "Alpha",
        "away_team": "Beta",
        "proposition_id": proposition_id,
        "proposition_type": proposition_type,
        "yes_represents": f"Yes {proposition_type}",
        "no_represents": f"No {proposition_type}",
        "scheduled_kickoff_at_utc": "2026-06-11T12:00:00Z",
        "analysis_window_start_at_utc": "2026-06-11T12:00:00Z",
        "analysis_window_end_at_utc": "2026-06-11T12:02:00Z",
        "settlement_minute_utc": timestamp,
        "elapsed_window_minute": str(minute),
        "yes_open": "0.400000000000000000" if observed else "",
        "yes_high": "0.400000000000000000" if observed else "",
        "yes_low": "0.400000000000000000" if observed else "",
        "yes_close": "0.400000000000000000" if observed else "",
        "yes_vwap": "0.400000000000000000" if observed else "",
        "yes_normalized_fill_count": "1" if observed else "0",
        "yes_derived_fill_count": "0",
        "yes_share_volume": "10.000000" if observed else "0.000000",
        "yes_gross_collateral_volume": "4.000000" if observed else "0.000000",
        "yes_first_settlement_at_utc": timestamp if observed else "",
        "yes_last_settlement_at_utc": timestamp if observed else "",
        "yes_observed": "true" if observed else "false",
        "no_open": "0.550000000000000000" if observed else "",
        "no_high": "0.550000000000000000" if observed else "",
        "no_low": "0.550000000000000000" if observed else "",
        "no_close": "0.550000000000000000" if observed else "",
        "no_vwap": "0.550000000000000000" if observed else "",
        "no_normalized_fill_count": "1" if observed else "0",
        "no_derived_fill_count": "0",
        "no_share_volume": "10.000000" if observed else "0.000000",
        "no_gross_collateral_volume": "5.500000" if observed else "0.000000",
        "no_first_settlement_at_utc": timestamp if observed else "",
        "no_last_settlement_at_utc": timestamp if observed else "",
        "no_observed": "true" if observed else "false",
        "minute_complete": "true" if observed else "false",
        "minute_status": "both_observed" if observed else "no_fills",
    }
    assert tuple(row) == export.PUBLIC_COLUMNS
    return row


def _rows() -> list[dict[str, str]]:
    rows = []
    for proposition_type in ("home_win", "draw", "away_win"):
        proposition_id = f"wc2026-m001-{proposition_type.replace('_', '-')}"
        rows.extend(
            _row(proposition_type, proposition_id, minute) for minute in range(2)
        )
    return sorted(
        rows,
        key=lambda row: (
            int(row["fifa_match_id"]),
            row["proposition_id"],
            row["settlement_minute_utc"],
        ),
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=export.PUBLIC_COLUMNS,
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


def _write_audit(
    root: Path,
    rows: list[dict[str, str]] | None = None,
) -> tuple[Path, bytes]:
    release = root / "1.2.3"
    release.mkdir(parents=True)
    csv_path = release / export.MAIN_CSV_NAME
    _write_csv(csv_path, rows or _rows())
    original = csv_path.read_bytes()
    resolution = load_polygon_resolution_attestation()
    provenance = {
        "dataset_version": "1.2.3",
        "generator_commit": "a" * 40,
        "seed_version": resolution.manifest_version,
        "seed_sha256": resolution.manifest_sha256,
        "chain_id": 137,
        "normalizer_version": "polygon-v2-settlement-v4",
        "verification_status": "not_requested",
        "finalized_head_block_number": 123,
        "finalized_head_block_hash": "0x" + "c" * 64,
        "resolution_attestation": resolution.as_mapping(),
        "source_revisions": {
            "fifa_match_number_schedule": {
                "revision": "schedule-v1",
                "sha256": "d" * 64,
            },
            "openfootball_worldcup": ["e" * 40],
            "conditional_tokens": "f" * 40,
            "uma_ctf_adapter": "1" * 40,
            "neg_risk_ctf_adapter": "2" * 40,
            "ctf_exchange_v2": "3" * 40,
        },
        "output_sha256": {export.MAIN_CSV_NAME: _sha256(csv_path)},
    }
    (release / "PROVENANCE.json").write_text(
        json.dumps(provenance),
        encoding="utf-8",
    )
    (release / "QUALITY_REPORT.json").write_text(
        json.dumps(
            {
                "warehouse_gate": {
                    "publication_ready": True,
                    "error_issue_count": 0,
                    "blocking_issue_keys": "",
                }
            }
        ),
        encoding="utf-8",
    )
    for name in export.AUDIT_FILES - {
        export.MAIN_CSV_NAME,
        "PROVENANCE.json",
        "QUALITY_REPORT.json",
        "CHECKSUMS.sha256",
    }:
        (release / name).write_text(f"{name}\n", encoding="utf-8")
    checksum_lines = [
        f"{_sha256(release / name)}  {name}"
        for name in sorted(export.AUDIT_FILES - {"CHECKSUMS.sha256"})
    ]
    (release / "CHECKSUMS.sha256").write_text(
        "\n".join(checksum_lines) + "\n",
        encoding="utf-8",
    )
    return release, original


def test_export_is_byte_identical_redacted_and_aggregate_only(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _contract(monkeypatch)
    audit, original = _write_audit(tmp_path / "audit")

    result = export.export_polygon_settlement_minute_odds(
        audit,
        tmp_path / "exports",
        repo_root=tmp_path,
    )

    release = Path(result["release_dir"])
    assert {path.name for path in release.iterdir()} == set(export.EXPORT_FILES)
    assert (release / export.MAIN_CSV_NAME).read_bytes() == original
    assert result["csv_sha256"] == hashlib.sha256(original).hexdigest()

    quality = json.loads((release / "QUALITY_SUMMARY.json").read_text())
    assert quality["inventory"] == {
        "columns": 41,
        "empty_minutes": 3,
        "matches": 1,
        "minute_states": {
            "both_observed": 3,
            "no_fills": 3,
            "no_only": 0,
            "yes_only": 0,
        },
        "observed_minutes": 3,
        "propositions": 3,
        "rows": 6,
    }
    assert quality["disclosures"]["pair_close_deviations_over_0_05"] == 0
    assert quality["disclosures"]["maximum_pair_close_deviation"] == (
        "0.050000000000000000"
    )
    assert quality["disclosures"]["secondary_verification_state"] == "not_requested"
    assert quality["disclosures"]["secondary_verification_matched"] is False
    assert "issues" not in quality
    quality_text = json.dumps(quality)
    assert "proposition_id" not in quality_text
    assert "settlement_minute_utc" not in quality_text

    resolution = load_polygon_resolution_attestation()
    manifest = json.loads((release / "MANIFEST.json").read_text())
    assert manifest["resolution_attestation"] == resolution.public_summary()
    assert "finalized_head_block_number" not in json.dumps(manifest)
    assert "rpc_provider" not in json.dumps(manifest)
    schema = json.loads((release / "schema.json").read_text())
    assert [column["name"] for column in schema["columns"]] == list(
        export.PUBLIC_COLUMNS
    )
    schema_by_name = {column["name"]: column for column in schema["columns"]}
    assert schema_by_name["yes_close"]["type"] == "DECIMAL(38,18)"
    assert schema_by_name["yes_share_volume"]["type"] == "DECIMAL(38,6)"
    assert schema_by_name["group_name"]["enum"] == list("ABCDEFGHIJKL")

    names = "\n".join(path.name for path in release.iterdir()).lower()
    assert "license" not in names
    assert "notice" not in names
    assert "provenance" not in names
    assert "markets.csv" not in names
    checksums = (release / "CHECKSUMS.sha256").read_text().splitlines()
    assert len(checksums) == len(export.EXPORT_FILES) - 1
    assert [line.split("  ", maxsplit=1)[1] for line in checksums] == sorted(
        set(export.EXPORT_FILES) - {"CHECKSUMS.sha256"}
    )


def test_public_contract_constants_are_fixed() -> None:
    assert len(export.PUBLIC_COLUMNS) == 41
    assert export.EXPECTED_MART_ROWS == 39_120
    assert export.EXPECTED_MARKETS == 248
    assert export.EXPECTED_MATCHES == 104


def test_exact_decimal_pair_boundary_is_not_a_warning(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _contract(monkeypatch)
    rows = _rows()
    rows[0]["no_normalized_fill_count"] = "2"
    rows[0]["no_low"] = "0.500000000000000000"
    rows[0]["no_high"] = "0.600000000000000000"
    rows[0]["no_close"] = "0.549999999999999999"
    path = tmp_path / "values.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=export.PUBLIC_COLUMNS,
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)

    summary = export.analyze_polygon_settlement_csv(path)

    assert summary["disclosures"]["pair_close_deviations_over_0_05"] == 1
    assert summary["disclosures"]["maximum_pair_close_deviation"] == (
        "0.050000000000000001"
    )


@pytest.mark.parametrize(
    ("column", "value", "message"),
    [
        ("elapsed_window_minute", "01", "invalid integer"),
        ("yes_normalized_fill_count", "1.2", "invalid integer"),
        ("yes_derived_fill_count", "1e0", "invalid integer"),
        (
            "yes_normalized_fill_count",
            str(export._MAX_BIGINT + 1),
            "invalid integer",
        ),
        ("yes_open", "1e-1", "invalid decimal"),
        ("yes_high", "1.0000000000000000004", "DECIMAL\\(38,18\\)"),
        ("yes_share_volume", "10.0000004", "DECIMAL\\(38,6\\)"),
        ("yes_share_volume", "1" * 33, "DECIMAL\\(38,6\\)"),
        ("yes_share_volume", "01.000000", "invalid decimal"),
    ],
)
def test_csv_analyzer_rejects_noncanonical_or_lossy_numeric_text(
    monkeypatch,
    tmp_path: Path,
    column: str,
    value: str,
    message: str,
) -> None:
    _contract(monkeypatch)
    rows = _rows()
    rows[0][column] = value
    path = tmp_path / f"{column}.csv"
    _write_csv(path, rows)

    with pytest.raises(ValueError, match=message):
        export.analyze_polygon_settlement_csv(path)


def test_csv_analyzer_accepts_valid_decimals_without_fractional_text(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _contract(monkeypatch)
    rows = _rows()
    for row in rows:
        row["yes_share_volume"] = "10" if row["yes_observed"] == "true" else "0"
    path = tmp_path / "integer-decimals.csv"
    _write_csv(path, rows)

    assert export.analyze_polygon_settlement_csv(path)["hard_failure_count"] == 0


@pytest.mark.parametrize(
    ("column", "value", "message"),
    [
        ("yes_vwap", "0.450000000000000000", "VWAP inconsistent with volumes"),
        ("yes_high", "0.500000000000000000", "incoherent single-fill"),
    ],
)
def test_csv_analyzer_rejects_vwap_and_single_fill_inconsistency(
    monkeypatch,
    tmp_path: Path,
    column: str,
    value: str,
    message: str,
) -> None:
    _contract(monkeypatch)
    rows = _rows()
    rows[0][column] = value
    path = tmp_path / f"{column}-relationship.csv"
    _write_csv(path, rows)

    with pytest.raises(ValueError, match=message):
        export.analyze_polygon_settlement_csv(path)


def test_vwap_uses_high_precision_half_even_rounding(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _contract(monkeypatch)
    rows = _rows()
    rows[0]["yes_share_volume"] = "999999999999.999999"
    rows[0]["yes_gross_collateral_volume"] = "500000000000.000000"
    rows[0]["yes_open"] = "0.500000000000000001"
    rows[0]["yes_high"] = "0.500000000000000001"
    rows[0]["yes_low"] = "0.500000000000000001"
    rows[0]["yes_close"] = "0.500000000000000001"
    rows[0]["yes_vwap"] = "0.500000000000000001"
    path = tmp_path / "half-even.csv"
    _write_csv(path, rows)

    assert export.analyze_polygon_settlement_csv(path)["hard_failure_count"] == 0


@pytest.mark.parametrize(
    ("column", "value", "message"),
    [
        ("yes_represents", "0x" + "a" * 40, "forbidden identifier"),
        ("yes_represents", "0x" + "a" * 64, "forbidden identifier"),
        ("yes_represents", "9" * 77, "forbidden identifier"),
        ("yes_represents", "condition_id c1", "forbidden identifier"),
        ("yes_represents", "token_id t1", "forbidden identifier"),
        ("yes_represents", "transaction_hash t1", "forbidden identifier"),
        ("yes_represents", "log_index 1", "forbidden identifier"),
        ("yes_represents", "block_number 1", "forbidden identifier"),
        ("yes_represents", "exchange_address e1", "forbidden identifier"),
        ("yes_represents", "wallet_address w1", "forbidden identifier"),
        ("yes_represents", "RPC provider label primary", "forbidden identifier"),
        ("yes_represents", "https://rpc.example/secret", "forbidden identifier"),
        ("yes_represents", "person@example.com", "forbidden identifier"),
        ("yes_represents", "=HYPERLINK(A1)", "formula-like"),
        ("yes_represents", "+SUM(A1)", "formula-like"),
        ("yes_represents", "-SUM(A1)", "formula-like"),
        ("yes_represents", "@SUM(A1)", "formula-like"),
        ("yes_represents", "unsafe\u0085text", "control character"),
        ("yes_open", "1.100000000000000000", "OHLC"),
        ("minute_status", "yes_only", "minute state"),
    ],
)
def test_export_rejects_unsafe_or_invalid_rows(
    monkeypatch,
    tmp_path: Path,
    column: str,
    value: str,
    message: str,
) -> None:
    _contract(monkeypatch)
    rows = _rows()
    rows[0][column] = value
    audit, _ = _write_audit(tmp_path / "audit", rows)

    with pytest.raises(ValueError, match=message):
        export.export_polygon_settlement_minute_odds(
            audit,
            tmp_path / "exports",
            repo_root=tmp_path,
        )
    assert not (tmp_path / "exports" / "releases" / "1.2.3").exists()


@pytest.mark.parametrize(
    ("column", "value", "scope", "message"),
    [
        ("home_team", "", "first", "required values"),
        ("stage", "unknown", "all", "stage or group label"),
        (
            "analysis_window_end_at_utc",
            "2026-06-11T12:03:00Z",
            "all",
            "analysis window",
        ),
        ("yes_normalized_fill_count", "0", "first", "observed/empty"),
        ("yes_derived_fill_count", "2", "first", "observed/empty"),
        ("yes_share_volume", "0.000000", "first", "observed/empty"),
        ("yes_high", "0.200000000000000000", "first", "OHLC"),
        (
            "yes_first_settlement_at_utc",
            "2026-06-11T11:59:59Z",
            "first",
            "settlement timestamps",
        ),
        (
            "yes_last_settlement_at_utc",
            "2026-06-11T12:00:30Z",
            "first",
            "settlement timestamps",
        ),
        ("yes_open", "0.400000000000000000", "empty", "observed/empty"),
        ("group_name", "", "all", "stage or group label"),
        ("group_name", "   ", "all", "stage or group label"),
    ],
)
def test_csv_analyzer_rejects_structural_temporal_and_value_failures(
    monkeypatch,
    tmp_path: Path,
    column: str,
    value: str,
    scope: str,
    message: str,
) -> None:
    _contract(monkeypatch)
    rows = _rows()
    targets = rows if scope == "all" else [rows[0] if scope == "first" else rows[1]]
    for row in targets:
        row[column] = value
    path = tmp_path / f"{column}.csv"
    _write_csv(path, rows)

    with pytest.raises(ValueError, match=message):
        export.analyze_polygon_settlement_csv(path)


def test_csv_analyzer_rejects_cross_proposition_match_identity_drift(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _contract(monkeypatch)
    rows = _rows()
    first_proposition = rows[0]["proposition_id"]
    for row in rows:
        if row["proposition_id"] == first_proposition:
            row["home_team"] = "Gamma"
    path = tmp_path / "match-identity.csv"
    _write_csv(path, rows)

    with pytest.raises(ValueError, match="match identity changes"):
        export.analyze_polygon_settlement_csv(path)


def test_csv_analyzer_rejects_non_minute_aligned_axis(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _contract(monkeypatch)
    rows = _rows()
    for row in rows:
        row["scheduled_kickoff_at_utc"] = "2026-06-11T12:00:30Z"
        row["analysis_window_start_at_utc"] = "2026-06-11T12:00:30Z"
        row["analysis_window_end_at_utc"] = "2026-06-11T12:02:30Z"
        minute = int(row["elapsed_window_minute"])
        row["settlement_minute_utc"] = f"2026-06-11T12:0{minute}:30Z"
        if row["yes_observed"] == "true":
            row["yes_first_settlement_at_utc"] = row["settlement_minute_utc"]
            row["yes_last_settlement_at_utc"] = row["settlement_minute_utc"]
            row["no_first_settlement_at_utc"] = row["settlement_minute_utc"]
            row["no_last_settlement_at_utc"] = row["settlement_minute_utc"]
    path = tmp_path / "unaligned-axis.csv"
    _write_csv(path, rows)

    with pytest.raises(ValueError, match="analysis window or elapsed-minute axis"):
        export.analyze_polygon_settlement_csv(path)


def test_csv_analyzer_rejects_duplicate_grain(monkeypatch, tmp_path: Path) -> None:
    _contract(monkeypatch)
    rows = _rows()
    rows[1] = dict(rows[0])
    path = tmp_path / "duplicate.csv"
    _write_csv(path, rows)

    with pytest.raises(ValueError, match="duplicate proposition-minute grain"):
        export.analyze_polygon_settlement_csv(path)


def test_export_rejects_checksum_tampering_and_collisions(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _contract(monkeypatch)
    audit, _ = _write_audit(tmp_path / "audit")
    (audit / "README.md").write_text("tampered\n", encoding="utf-8")

    with pytest.raises(ValueError, match="checksum mismatch"):
        export.export_polygon_settlement_minute_odds(
            audit,
            tmp_path / "exports",
            repo_root=tmp_path,
        )

    audit, _ = _write_audit(tmp_path / "second-audit")
    export.export_polygon_settlement_minute_odds(
        audit,
        tmp_path / "exports",
        repo_root=tmp_path,
    )
    with pytest.raises(FileExistsError, match="already exists"):
        export.export_polygon_settlement_minute_odds(
            audit,
            tmp_path / "exports",
            repo_root=tmp_path,
        )


def test_export_cleans_temporary_directory_after_failure(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _contract(monkeypatch)
    audit, _ = _write_audit(tmp_path / "audit")
    monkeypatch.setattr(
        export,
        "_write_sources",
        lambda *_args: (_ for _ in ()).throw(ValueError("source failure")),
    )

    with pytest.raises(ValueError, match="source failure"):
        export.export_polygon_settlement_minute_odds(
            audit,
            tmp_path / "exports",
            repo_root=tmp_path,
        )

    release_root = tmp_path / "exports" / "releases"
    assert list(release_root.iterdir()) == []


def test_export_is_deterministic_across_output_roots(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _contract(monkeypatch)
    first_audit, _ = _write_audit(tmp_path / "audit-a")
    second_audit, _ = _write_audit(tmp_path / "audit-b")

    first = Path(
        export.export_polygon_settlement_minute_odds(
            first_audit,
            tmp_path / "exports-a",
            repo_root=tmp_path,
        )["release_dir"]
    )
    second = Path(
        export.export_polygon_settlement_minute_odds(
            second_audit,
            tmp_path / "exports-b",
            repo_root=tmp_path,
        )["release_dir"]
    )

    assert {name: (first / name).read_bytes() for name in export.EXPORT_FILES} == {
        name: (second / name).read_bytes() for name in export.EXPORT_FILES
    }


def test_export_consumes_private_snapshot_when_input_changes(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _contract(monkeypatch)
    audit, original = _write_audit(tmp_path / "audit")
    validate_checksums = export._validate_checksum_manifest

    def validate_then_mutate(directory, expected_files):
        checksums = validate_checksums(directory, expected_files)
        (audit / export.MAIN_CSV_NAME).write_bytes(b"changed after snapshot\n")
        return checksums

    monkeypatch.setattr(
        export,
        "_validate_checksum_manifest",
        validate_then_mutate,
    )
    result = export.export_polygon_settlement_minute_odds(
        audit,
        tmp_path / "exports",
        repo_root=tmp_path,
    )

    release = Path(result["release_dir"])
    assert (release / export.MAIN_CSV_NAME).read_bytes() == original


def test_export_rejects_unexpected_header_and_malformed_checksum(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _contract(monkeypatch)
    audit, _ = _write_audit(tmp_path / "audit")
    checksum_path = audit / "CHECKSUMS.sha256"
    checksum_path.write_text(
        checksum_path.read_text().replace("  README.md", " README.md"),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="malformed"):
        export.export_polygon_settlement_minute_odds(
            audit,
            tmp_path / "exports",
            repo_root=tmp_path,
        )

    csv_path = tmp_path / "bad-header.csv"
    rows = _rows()
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(*export.PUBLIC_COLUMNS, "condition_id"),
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows({**row, "condition_id": ""} for row in rows)
    with pytest.raises(ValueError, match="literal allowlist"):
        export.analyze_polygon_settlement_csv(csv_path)


def test_export_rejects_wrong_audit_version_and_copy_corruption(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _contract(monkeypatch)
    audit, _ = _write_audit(tmp_path / "wrong-version")
    renamed = audit.rename(audit.with_name("not-the-version"))
    with pytest.raises(ValueError, match="directory name"):
        export.export_polygon_settlement_minute_odds(
            renamed,
            tmp_path / "exports",
            repo_root=tmp_path,
        )

    audit, _ = _write_audit(tmp_path / "copy-corruption")
    real_copy = shutil.copyfile

    def corrupt_copy(source, destination):
        result = real_copy(source, destination)
        Path(destination).write_bytes(Path(destination).read_bytes() + b"x")
        return result

    monkeypatch.setattr(export.shutil, "copyfile", corrupt_copy)
    with pytest.raises(RuntimeError, match="differs"):
        export.export_polygon_settlement_minute_odds(
            audit,
            tmp_path / "corrupt-exports",
            repo_root=tmp_path,
        )
    assert list((tmp_path / "corrupt-exports" / "releases").iterdir()) == []


def test_audit_path_rejects_missing_nested_and_child_symlink(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="does not exist"):
        export._validate_audit_release_path(tmp_path / "missing")

    audit, _ = _write_audit(tmp_path / "nested")
    (audit / "nested").mkdir()
    with pytest.raises(ValueError, match="files differ"):
        export._validate_audit_release_path(audit)

    audit, _ = _write_audit(tmp_path / "child-link")
    readme = audit / "README.md"
    readme.unlink()
    readme.symlink_to(audit / "CHANGELOG.md")
    with pytest.raises(ValueError, match="contain symlinks"):
        export._validate_audit_release_path(audit)


def test_audit_snapshot_rejects_changed_inventory_and_nonregular_files(
    tmp_path: Path,
) -> None:
    changed = tmp_path / "changed"
    changed.mkdir()
    (changed / "unexpected").write_text("x", encoding="utf-8")
    with pytest.raises(ValueError, match="changed while"):
        export._snapshot_audit_release(changed, tmp_path / "changed-copy")

    nonregular = tmp_path / "nonregular"
    nonregular.mkdir()
    for name in export.AUDIT_FILES:
        path = nonregular / name
        if name == "README.md":
            path.mkdir()
        else:
            path.write_text(name, encoding="utf-8")
    with pytest.raises(ValueError, match="regular files"):
        export._snapshot_audit_release(nonregular, tmp_path / "nonregular-copy")


def test_export_rejects_symlinked_releases_directory(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _contract(monkeypatch)
    audit, _ = _write_audit(tmp_path / "audit")
    output_root = tmp_path / "exports"
    external = tmp_path / "external"
    output_root.mkdir()
    external.mkdir()
    (output_root / "releases").symlink_to(external, target_is_directory=True)

    with pytest.raises(ValueError, match="Symlink"):
        export.export_polygon_settlement_minute_odds(
            audit,
            output_root,
            repo_root=tmp_path,
        )
    assert list(external.iterdir()) == []

    dangling_root = tmp_path / "dangling-exports"
    dangling_root.mkdir()
    dangling_releases = dangling_root / "releases"
    dangling_releases.symlink_to(
        tmp_path / "missing-releases",
        target_is_directory=True,
    )
    with pytest.raises(ValueError, match="Symlink"):
        export.export_polygon_settlement_minute_odds(
            audit,
            dangling_root,
            repo_root=tmp_path,
        )
    assert dangling_releases.is_symlink()


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("empty", "nonempty"),
        ("invalid_digest", "invalid entry"),
        ("blank_name", "invalid entry"),
        ("traversal", "invalid entry"),
        ("duplicate", "invalid entry"),
        ("unsorted", "sorted by filename"),
        ("incomplete", "exact bundle"),
    ],
)
def test_checksum_manifest_rejects_every_malformed_shape(
    tmp_path: Path,
    case: str,
    message: str,
) -> None:
    audit, _ = _write_audit(tmp_path / case)
    checksum_path = audit / "CHECKSUMS.sha256"
    lines = checksum_path.read_text().splitlines()
    if case == "empty":
        lines = []
    elif case == "invalid_digest":
        lines[0] = f"{'g' * 64}  {lines[0].split('  ', 1)[1]}"
    elif case == "blank_name":
        lines[0] = f"{'a' * 64}  "
    elif case == "traversal":
        lines[0] = f"{'a' * 64}  ../outside"
    elif case == "duplicate":
        lines.insert(1, lines[0])
    elif case == "unsorted":
        lines.reverse()
    else:
        lines.pop()
    checksum_path.write_text(
        "\n".join(lines) + ("\n" if lines else ""), encoding="utf-8"
    )

    with pytest.raises(ValueError, match=message):
        export._validate_checksum_manifest(audit, export.AUDIT_FILES)


def test_audit_quality_rejects_missing_and_failed_gates() -> None:
    with pytest.raises(ValueError, match="no warehouse gate"):
        export._validate_audit_quality({})
    for gate in (
        {
            "publication_ready": False,
            "error_issue_count": 0,
            "blocking_issue_keys": "",
        },
        {
            "publication_ready": True,
            "error_issue_count": "0",
            "blocking_issue_keys": "",
        },
        {
            "publication_ready": True,
            "error_issue_count": 0,
            "blocking_issue_keys": "failure",
        },
    ):
        with pytest.raises(ValueError, match="not ready"):
            export._validate_audit_quality({"warehouse_gate": gate})


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ({"dataset_version": "2.0.0"}, "dataset_version"),
        ({"chain_id": 1}, "chain ID"),
        ({"generator_commit": "invalid"}, "generator_commit"),
        ({"seed_sha256": "invalid"}, "seed_sha256"),
        ({"seed_version": "invalid"}, "seed_version"),
        ({"normalizer_version": "https://rpc.example"}, "normalizer_version"),
        ({"verification_status": "unknown"}, "verification_status"),
        ({"output_sha256": None}, "CSV hash"),
        ({"output_sha256": {export.MAIN_CSV_NAME: "0" * 64}}, "CSV hash"),
        ({"finalized_head_block_number": True}, "finalized scan"),
        ({"finalized_head_block_hash": "not-a-block-hash"}, "finalized scan"),
        ({"resolution_attestation": {}}, "reviewed release"),
        ({"seed_sha256": "f" * 64}, "does not match the audit seed"),
        ({"seed_version": "2.0.0"}, "does not match the audit seed"),
        ({"source_revisions": None}, "source revisions"),
    ],
)
def test_audit_provenance_rejects_each_contract_break(
    tmp_path: Path,
    mutation: dict,
    message: str,
) -> None:
    audit, _ = _write_audit(tmp_path / message.replace(" ", "-"))
    provenance = json.loads((audit / "PROVENANCE.json").read_text())
    provenance.update(mutation)
    reviewed = load_polygon_resolution_attestation().as_mapping()

    with pytest.raises(ValueError, match=message):
        export._validate_audit_provenance(
            provenance,
            dataset_version="1.2.3",
            csv_sha256=_sha256(audit / export.MAIN_CSV_NAME),
            reviewed_resolution=reviewed,
        )


def test_csv_scanner_rejects_empty_short_mixed_and_unsorted_rows(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _contract(monkeypatch)
    empty = tmp_path / "empty.csv"
    empty.write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="empty"):
        export._scan_csv(empty)
    with pytest.raises(ValueError, match="regular file"):
        export._scan_csv(tmp_path / "missing.csv")

    header_only = tmp_path / "header-only.csv"
    _write_csv(header_only, [])
    with pytest.raises(ValueError, match="no data rows"):
        export._scan_csv(header_only)

    short = tmp_path / "short.csv"
    with short.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(export.PUBLIC_COLUMNS)
        writer.writerow(list(_rows()[0].values())[:-1])
    with pytest.raises(ValueError, match="wrong column count"):
        export._scan_csv(short)

    cases = [
        ("mixed.csv", "dataset_version", "2.0.0", "more than one"),
        ("boolean.csv", "yes_observed", "yes", "invalid yes_observed"),
        (
            "timestamp.csv",
            "scheduled_kickoff_at_utc",
            "2026-06-11 12:00:00",
            "non-RFC3339",
        ),
        ("match.csv", "fifa_match_id", "not-an-int", "invalid integer fifa_match_id"),
    ]
    for name, column, value, message in cases:
        rows = _rows()
        rows[1][column] = value
        path = tmp_path / name
        _write_csv(path, rows)
        with pytest.raises(ValueError, match=message):
            export._scan_csv(path)

    unsorted = tmp_path / "unsorted.csv"
    _write_csv(unsorted, list(reversed(_rows())))
    with pytest.raises(ValueError, match="canonical grain order"):
        export._scan_csv(unsorted)


def test_csv_analysis_rejects_malformed_csv_syntax(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _contract(monkeypatch)
    path = tmp_path / "malformed.csv"
    path.write_text(
        ",".join(export.PUBLIC_COLUMNS) + '\n"unterminated', encoding="utf-8"
    )

    with pytest.raises(ValueError, match="syntax is invalid"):
        export.analyze_polygon_settlement_csv(path)


def test_csv_analysis_rejects_row_count_duckdb_and_inventory_errors(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _contract(monkeypatch)
    path = tmp_path / "valid.csv"
    _write_csv(path, _rows())

    monkeypatch.setattr(export, "EXPECTED_MART_ROWS", 7)
    with pytest.raises(ValueError, match="CSV rows=6"):
        export.analyze_polygon_settlement_csv(path)
    monkeypatch.setattr(export, "EXPECTED_MART_ROWS", 6)

    monkeypatch.setattr(
        export,
        "_load_csv",
        lambda *_args: (_ for _ in ()).throw(export.duckdb.Error("typed failure")),
    )
    with pytest.raises(ValueError, match="typed contract"):
        export.analyze_polygon_settlement_csv(path)
    monkeypatch.undo()
    _contract(monkeypatch)

    monkeypatch.setattr(export, "EXPECTED_MATCHES", 2)
    with pytest.raises(ValueError, match="inventory differs"):
        export.analyze_polygon_settlement_csv(path)
    monkeypatch.setattr(export, "EXPECTED_MATCHES", 1)
    monkeypatch.setattr(
        export,
        "EXPECTED_PROPOSITION_INVENTORY",
        Counter({"home_win": 3}),
    )
    with pytest.raises(ValueError, match="Proposition inventory"):
        export.analyze_polygon_settlement_csv(path)


@pytest.mark.parametrize(
    "case",
    [
        "fifa",
        "fifa_revision",
        "fifa_hash",
        "openfootball",
        "openfootball_commit",
        "revision",
    ],
)
def test_sources_reject_incomplete_revisions(tmp_path: Path, case: str) -> None:
    audit, _ = _write_audit(tmp_path / case)
    provenance = json.loads((audit / "PROVENANCE.json").read_text())
    if case == "fifa":
        provenance["source_revisions"]["fifa_match_number_schedule"] = "bad"
    elif case == "fifa_revision":
        provenance["source_revisions"]["fifa_match_number_schedule"]["revision"] = (
            "=unsafe"
        )
    elif case == "fifa_hash":
        provenance["source_revisions"]["fifa_match_number_schedule"]["sha256"] = "bad"
    elif case == "openfootball":
        provenance["source_revisions"]["openfootball_worldcup"] = "bad"
    elif case == "openfootball_commit":
        provenance["source_revisions"]["openfootball_worldcup"] = ["bad"]
    else:
        provenance["source_revisions"]["conditional_tokens"] = ""
    with pytest.raises(ValueError, match="incomplete or invalid"):
        export._write_sources(tmp_path / "sources.csv", provenance)


def test_git_revision_validation_covers_success_and_failures(
    monkeypatch,
    tmp_path: Path,
) -> None:
    responses = iter(
        [SimpleNamespace(stdout=""), SimpleNamespace(stdout="A" * 40 + "\n")]
    )
    monkeypatch.setattr(
        export.subprocess, "run", lambda *_args, **_kwargs: next(responses)
    )
    assert export._current_clean_commit(tmp_path) == "a" * 40

    responses = iter(
        [SimpleNamespace(stdout="dirty"), SimpleNamespace(stdout="a" * 40)]
    )
    monkeypatch.setattr(
        export.subprocess, "run", lambda *_args, **_kwargs: next(responses)
    )
    with pytest.raises(RuntimeError, match="clean Git"):
        export._current_clean_commit(tmp_path)

    responses = iter([SimpleNamespace(stdout=""), SimpleNamespace(stdout="bad")])
    monkeypatch.setattr(
        export.subprocess, "run", lambda *_args, **_kwargs: next(responses)
    )
    with pytest.raises(RuntimeError, match="invalid export generator"):
        export._current_clean_commit(tmp_path)

    def unavailable(*_args, **_kwargs):
        raise OSError("git unavailable")

    monkeypatch.setattr(export.subprocess, "run", unavailable)
    with pytest.raises(RuntimeError, match="Could not resolve"):
        export._current_clean_commit(tmp_path)


def test_small_validation_helpers_cover_invalid_inputs(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="SemVer"):
        export._validate_dataset_version("01.0.0")

    invalid_json = tmp_path / "invalid.json"
    invalid_json.write_text("{", encoding="utf-8")
    with pytest.raises(ValueError, match="Could not read"):
        export._read_json_object(invalid_json)
    array_json = tmp_path / "array.json"
    array_json.write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="must contain an object"):
        export._read_json_object(array_json)

    output = tmp_path / "output"
    output.mkdir()
    with pytest.raises(RuntimeError, match="Export files differ"):
        export._validate_export_files(output)
    linked_file = tmp_path / "linked-file"
    linked_file.write_text("linked", encoding="utf-8")
    (output / "linked-entry").symlink_to(linked_file)
    with pytest.raises(RuntimeError, match="Export files differ"):
        export._validate_export_files(output)


def test_export_rejects_symlinked_audit_input(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _contract(monkeypatch)
    audit, _ = _write_audit(tmp_path / "audit")
    link = tmp_path / "audit-link"
    link.symlink_to(audit, target_is_directory=True)

    with pytest.raises(ValueError, match="Symlink"):
        export.export_polygon_settlement_minute_odds(
            link,
            tmp_path / "exports",
            repo_root=tmp_path,
        )
