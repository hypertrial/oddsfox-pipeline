"""Tests for immutable WC2026 Polygon settlement audit bundles."""

from __future__ import annotations

import csv
import hashlib
import json
import subprocess
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import duckdb
import pytest
from tests.unit.ingestion.test_polygon_seed import complete_seed_rows

from oddsfox_pipeline.ingestion.polymarket.polygon_seed import SEED_COLUMNS
from oddsfox_pipeline.publishing import polygon_settlement as publishing
from oddsfox_pipeline.publishing.polygon_settlement import (
    AUDIT_BUNDLE_FILES,
    MAIN_CSV_NAME,
    MARKETS_CSV_NAME,
    PolygonSettlementAuditSpec,
    build_polygon_settlement_audit_release,
    current_generator_commit,
    validate_dataset_version,
)


@pytest.fixture
def release_connection(monkeypatch) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute("create schema polymarket_wc2026_staging")
    conn.execute("create schema polymarket_wc2026_marts")
    conn.execute("create schema polymarket_wc2026_observability")
    conn.execute(
        """
        create table polymarket_wc2026_staging.stg_polymarket_wc2026_polygon_settlement_markets as
        with markets as (
            select
                i,
                case when i <= 216 then cast(((i - 1) // 3) + 1 as integer)
                     else cast(i - 144 as integer) end as fifa_match_id
            from range(1, 249) as source(i)
        )
        select
            'prop_' || lpad(cast(i as varchar), 3, '0') as proposition_id,
            fifa_match_id,
            case
                when fifa_match_id <= 72 then 'group_stage'
                when fifa_match_id <= 88 then 'round_of_32'
                when fifa_match_id <= 96 then 'round_of_16'
                when fifa_match_id <= 100 then 'quarterfinal'
                when fifa_match_id <= 102 then 'semifinal'
                when fifa_match_id = 103 then 'third_place'
                else 'final'
            end as stage,
            case when fifa_match_id <= 72 then 'A' else null end as group_name,
            'Home ' || fifa_match_id as home_team,
            'Away ' || fifa_match_id as away_team,
            timestamp '2026-06-11 12:00:00' + fifa_match_id * interval '1 day'
                as scheduled_kickoff_at_utc,
            timestamp '2026-06-11 12:00:00' + fifa_match_id * interval '1 day'
                as analysis_window_start_at_utc,
            timestamp '2026-06-11 12:00:00' + fifa_match_id * interval '1 day'
                + case when fifa_match_id <= 72 then interval '150 minutes'
                       else interval '210 minutes' end as analysis_window_end_at_utc,
            case when fifa_match_id <= 72 then
                case (i - 1) % 3 when 0 then 'home_win' when 1 then 'draw'
                    else 'away_win' end
            when fifa_match_id <= 102 then 'home_advances'
            when fifa_match_id = 103 then 'home_win_third_place'
            else 'home_wins_final' end as proposition_type,
            'Yes meaning ' || i as yes_represents,
            'No meaning ' || i as no_represents,
            '0x' || lpad(to_hex(i), 64, '0') as condition_id,
            cast(i * 2 as varchar) as yes_token_id,
            cast(i * 2 + 1 as varchar) as no_token_id,
            case when i % 2 = 0 then 'standard' else 'neg_risk' end
                as market_structure,
            case when i % 2 = 0
                then '0xE111180000d2663C0091e4f400237545B87B996B'
                else '0xe2222d279d744050d28e00520010520000310F59'
            end as exchange_address,
            repeat('a', 40) as openfootball_revision,
            case when fifa_match_id <= 72 then '2026--usa/cup.txt'
                 else '2026--usa/cup_finals.txt' end as openfootball_path,
            '1-2' as openfootball_source_lines,
            repeat('b', 64) as openfootball_line_hash,
            '0x' || repeat('1', 64) as condition_init_tx_hash,
            i as condition_init_log_index,
            '0x' || repeat('2', 64) as question_init_tx_hash,
            i + 1 as question_init_log_index,
            repeat('c', 64) as ancillary_data_sha256,
            100000 + i as token_verification_block_number,
            '0x' || repeat('3', 64) as token_verification_block_hash,
            repeat('a', 64) as manifest_sha256,
            '1.0.0' as manifest_version,
            timestamp '2026-07-22 00:00:00' as reviewed_at_utc
        from markets
        """
    )
    conn.execute(
        """
        create table polymarket_wc2026_marts.polymarket_wc2026_polygon_settlement_minute_odds as
        with markets as (
            select *
            from polymarket_wc2026_staging.stg_polymarket_wc2026_polygon_settlement_markets
        )
        select
            fifa_match_id,
            stage,
            group_name,
            home_team,
            away_team,
            proposition_id,
            proposition_type,
            yes_represents,
            no_represents,
            scheduled_kickoff_at_utc,
            analysis_window_start_at_utc,
            analysis_window_end_at_utc,
            analysis_window_start_at_utc + minute_index * interval '1 minute'
                as settlement_minute_utc,
            cast(minute_index as integer) as elapsed_window_minute,
            cast(0.4 as decimal(38,18)) as yes_open,
            cast(0.5 as decimal(38,18)) as yes_high,
            cast(0.3 as decimal(38,18)) as yes_low,
            cast(0.45 as decimal(38,18)) as yes_close,
            cast(0.44 as decimal(38,18)) as yes_vwap,
            1::bigint as yes_normalized_fill_count,
            0::bigint as yes_derived_fill_count,
            cast(10 as decimal(38,6)) as yes_share_volume,
            cast(4.4 as decimal(38,6)) as yes_gross_collateral_volume,
            analysis_window_start_at_utc + minute_index * interval '1 minute'
                as yes_first_settlement_at_utc,
            analysis_window_start_at_utc + minute_index * interval '1 minute'
                as yes_last_settlement_at_utc,
            true as yes_observed,
            cast(0.6 as decimal(38,18)) as no_open,
            cast(0.7 as decimal(38,18)) as no_high,
            cast(0.5 as decimal(38,18)) as no_low,
            cast(0.55 as decimal(38,18)) as no_close,
            cast(0.56 as decimal(38,18)) as no_vwap,
            1::bigint as no_normalized_fill_count,
            0::bigint as no_derived_fill_count,
            cast(10 as decimal(38,6)) as no_share_volume,
            cast(5.6 as decimal(38,6)) as no_gross_collateral_volume,
            analysis_window_start_at_utc + minute_index * interval '1 minute'
                as no_first_settlement_at_utc,
            analysis_window_start_at_utc + minute_index * interval '1 minute'
                as no_last_settlement_at_utc,
            true as no_observed,
            true as minute_complete,
            'both_observed' as minute_status
        from markets
        cross join lateral range(
            0,
            case when fifa_match_id <= 72 then 150 else 210 end
        ) as minutes(minute_index)
        """
    )
    conn.execute(
        """
        create table polymarket_wc2026_observability.polymarket_wc2026_polygon_settlement_data_quality as
        select
            'scan-1' as scan_id,
            'published' as scan_status,
            true as publication_ready,
            '' as blocking_issue_keys,
            0::bigint as warning_issue_count,
            0::bigint as error_issue_count
        """
    )
    conn.execute(
        """
        create table polymarket_wc2026_observability.polymarket_wc2026_polygon_settlement_quality_issues (
            issue_key varchar,
            severity varchar,
            issue_type varchar,
            proposition_id varchar,
            fifa_match_id integer,
            token_id varchar,
            settlement_minute_utc timestamp,
            measured_value double,
            threshold_value double,
            issue_detail varchar,
            observed_at timestamp
        )
        """
    )
    seed_rows = publishing._read_market_rows(conn)
    monkeypatch.setattr(
        publishing,
        "load_polygon_market_seed",
        MagicMock(
            return_value=SimpleNamespace(
                markets=tuple(SimpleNamespace(**row) for row in seed_rows),
                sha256="a" * 64,
                version="1.0.0",
            )
        ),
    )
    monkeypatch.setattr(
        publishing,
        "load_polygon_resolution_attestation",
        MagicMock(
            return_value=SimpleNamespace(
                as_mapping=lambda: {
                    "schema_version": 1,
                    "manifest_version": "1.0.0",
                    "manifest_sha256": "a" * 64,
                    "resolved_condition_count": 248,
                    "verified_at_utc": "2026-07-22T11:02:27Z",
                    "authoring_evidence_sha256": "b" * 64,
                    "finalized_head_block_number": 123456,
                    "finalized_head_block_hash": "0x" + "c" * 64,
                }
            )
        ),
    )
    yield conn
    conn.close()


@pytest.fixture
def provenance() -> dict:
    return {
        "scan_id": "scan-1",
        "seed_sha256": "a" * 64,
        "seed_version": "1.0.0",
        "chain_id": 137,
        "exchange_addresses": [
            "0xE111180000d2663C0091e4f400237545B87B996B",
            "0xe2222d279d744050d28e00520010520000310F59",
        ],
        "finalized_head_block_number": 123456,
        "finalized_head_block_hash": "0x" + "b" * 64,
        "block_ranges": [
            {
                "exchange_address": "0xE111180000d2663C0091e4f400237545B87B996B",
                "from_block": 100,
                "to_block": 200,
                "from_block_hash": "0x" + "c" * 64,
                "to_block_hash": "0x" + "d" * 64,
                "chunk_sha256": "e" * 64,
            },
            {
                "exchange_address": "0xe2222d279d744050d28e00520010520000310F59",
                "from_block": 100,
                "to_block": 200,
                "from_block_hash": "0x" + "c" * 64,
                "to_block_hash": "0x" + "d" * 64,
                "chunk_sha256": "f" * 64,
            },
        ],
        "normalizer_version": "polygon-v2-settlement-v4",
        "scan_published_at_utc": "2026-07-22T00:00:00Z",
        "rpc_provider_label": "test-provider",
        "rpc_provider_origin": "https://rpc.example",
        "verification_status": "not_requested",
        "verification_rpc_provider_label": None,
        "verification_rpc_provider_origin": None,
    }


def _write_seed(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SEED_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def test_release_preflight_reloads_seed_and_rejects_file_or_sidecar_mutation(
    monkeypatch,
    tmp_path: Path,
) -> None:
    rows = complete_seed_rows()
    seed_path = tmp_path / "seed.csv"
    _write_seed(seed_path, rows)
    manifest = publishing.load_polygon_market_seed(seed_path)
    warehouse_rows = publishing._seed_rows_from_manifest(manifest)
    provenance = {
        "seed_sha256": manifest.sha256,
        "seed_version": manifest.version,
    }
    monkeypatch.setattr(publishing, "DEFAULT_POLYGON_MARKET_SEED_PATH", seed_path)
    attestation = MagicMock()
    attestation.as_mapping.return_value = {"resolved_condition_count": 248}
    load_attestation = MagicMock(return_value=attestation)
    monkeypatch.setattr(
        publishing,
        "load_polygon_resolution_attestation",
        load_attestation,
    )

    assert publishing._validate_committed_seed(warehouse_rows, provenance) == {
        "resolved_condition_count": 248
    }
    assert load_attestation.call_args.kwargs["manifest"].sha256 == manifest.sha256

    changed_file_rows = [dict(row) for row in rows]
    changed_file_rows[0]["yes_represents"] = "mutated without a refreshed hash"
    _write_seed(seed_path, changed_file_rows)
    with pytest.raises(ValueError, match="canonical logical seed content"):
        publishing._validate_committed_seed(warehouse_rows, provenance)

    _write_seed(seed_path, rows)
    changed_warehouse_rows = [dict(row) for row in warehouse_rows]
    changed_warehouse_rows[0]["yes_represents"] = "mutated warehouse sidecar"
    with pytest.raises(ValueError, match="differs from warehouse sidecar"):
        publishing._validate_committed_seed(changed_warehouse_rows, provenance)
    with pytest.raises(ValueError, match="row counts differ"):
        publishing._validate_committed_seed(warehouse_rows[:-1], provenance)

    with pytest.raises(ValueError, match="version/hash"):
        publishing._validate_committed_seed(
            warehouse_rows,
            {**provenance, "seed_version": "2.0.0"},
        )


def test_builds_complete_immutable_internal_audit_bundle(
    release_connection: duckdb.DuckDBPyConnection,
    provenance: dict,
    tmp_path: Path,
) -> None:
    provenance["rpc_url"] = "https://rpc.example/secret"
    provenance["generator_commit"] = "0" * 40
    provenance["block_ranges"][0]["provider_response"] = "not public"
    spec = PolygonSettlementAuditSpec(dataset_version="1.0.0")
    summary = build_polygon_settlement_audit_release(
        release_connection,
        tmp_path,
        spec,
        provenance=provenance,
        generator_commit="f" * 40,
    )

    release = tmp_path / "releases" / "1.0.0"
    assert summary["rows"] == 39_120
    assert summary["markets"] == 248
    assert {path.name for path in release.iterdir()} == set(AUDIT_BUNDLE_FILES)
    assert not (release / "dataset-metadata.json").exists()

    with (release / MAIN_CSV_NAME).open(newline="", encoding="utf-8") as handle:
        main_header = next(csv.reader(handle))
    with (release / MARKETS_CSV_NAME).open(newline="", encoding="utf-8") as handle:
        market_header = next(csv.reader(handle))
    assert main_header[0] == "dataset_version"
    assert "condition_id" not in main_header
    assert "condition_id" in market_header
    assert "condition_init_tx_hash" in market_header
    assert "condition_init_log_index" in market_header
    assert "token_verification_block_number" in market_header
    assert "token_verification_block_hash" in market_header
    assert "transaction_hash" not in main_header
    assert "rpc_provider_label" not in main_header

    readme = (release / "README.md").read_text(encoding="utf-8")
    assert "settlement times, not order-match times" in readme
    assert "initialization transaction/log locators" in readme
    assert "token-verification block locators" in readme
    assert "internal audit bundle" in readme
    assert not (release / "LICENSE.txt").exists()
    assert not (release / "NOTICE.md").exists()
    do_not_publish = (release / "DO_NOT_PUBLISH.md").read_text(encoding="utf-8")
    assert "Do not publish this directory" in do_not_publish
    assert "standalone Polygon settlement exporter" in do_not_publish

    provenance_json = json.loads(
        (release / "PROVENANCE.json").read_text(encoding="utf-8")
    )
    assert provenance_json["scan_id"] == "scan-1"
    assert provenance_json["seed_version"] == "1.0.0"
    assert provenance_json["generator_commit"] == "f" * 40
    assert provenance_json["source_revisions"]["fifa_match_number_schedule"] == {
        "revision": publishing.FIFA_SCHEDULE_REVISION,
        "sha256": publishing.FIFA_SCHEDULE_SHA256,
    }
    assert provenance_json["source_revisions"]["openfootball_worldcup"] == ["a" * 40]
    assert provenance_json["source_revisions"]["openfootball_license"] == {
        "path": "LICENSE.md",
        "revision": publishing.OPENFOOTBALL_REVISION,
        "sha256": publishing.OPENFOOTBALL_LICENSE_SHA256,
        "uri": publishing.OPENFOOTBALL_LICENSE_URI,
    }
    assert provenance_json["resolution_attestation"] == {
        "schema_version": 1,
        "manifest_version": "1.0.0",
        "manifest_sha256": "a" * 64,
        "resolved_condition_count": 248,
        "verified_at_utc": "2026-07-22T11:02:27Z",
        "authoring_evidence_sha256": "b" * 64,
        "finalized_head_block_number": 123456,
        "finalized_head_block_hash": "0x" + "c" * 64,
    }
    assert "publisher_name" not in provenance_json
    assert "attribution_url" not in provenance_json
    assert "rights_review_status" not in provenance_json
    assert "rpc_provider_terms" not in provenance_json
    assert "rpc_url" not in provenance_json
    assert "provider_response" not in provenance_json["block_ranges"][0]
    assert set(provenance_json["output_sha256"]) == {
        MAIN_CSV_NAME,
        MARKETS_CSV_NAME,
    }
    quality_json = json.loads(
        (release / "QUALITY_REPORT.json").read_text(encoding="utf-8")
    )
    assert quality_json["verification_status"] == "not_requested"
    assert quality_json["warehouse_gate"]["warning_issue_count"] == 1
    assert [issue["issue_type"] for issue in quality_json["issues"]] == ["verification"]

    sources = (release / "SOURCES.csv").read_text(encoding="utf-8")
    assert publishing.FIFA_SCHEDULE_SHA256 in sources
    assert "https://rpc.example" in sources
    assert "license_or_terms" not in sources.splitlines()[0]
    assert "provider terms" not in sources.lower()

    checksum_lines = (
        (release / "CHECKSUMS.sha256").read_text(encoding="utf-8").splitlines()
    )
    assert len(checksum_lines) == len(AUDIT_BUNDLE_FILES) - 1
    for line in checksum_lines:
        expected, filename = line.split("  ", maxsplit=1)
        assert hashlib.sha256((release / filename).read_bytes()).hexdigest() == expected

    copy_root = tmp_path / "copy"
    build_polygon_settlement_audit_release(
        release_connection,
        copy_root,
        spec,
        provenance=provenance,
        generator_commit="f" * 40,
    )
    copy_release = copy_root / "releases" / "1.0.0"
    assert {name: (release / name).read_bytes() for name in AUDIT_BUNDLE_FILES} == {
        name: (copy_release / name).read_bytes() for name in AUDIT_BUNDLE_FILES
    }


@pytest.mark.parametrize(
    "verification_status",
    ["not_requested", "matched", "mismatched", "error"],
)
def test_release_reconciles_current_verification_status_and_warning(
    release_connection: duckdb.DuckDBPyConnection,
    provenance: dict,
    tmp_path: Path,
    verification_status: str,
) -> None:
    release_connection.execute(
        """
        insert into polymarket_wc2026_observability.polymarket_wc2026_polygon_settlement_quality_issues
        values (
            'secondary_verification:scan-1', 'warn', 'verification',
            null, null, null, null, null, null,
            'stale verification warning (not_requested).',
            timestamp '2026-07-22 00:00:00'
        )
        """
    )
    provenance["verification_status"] = verification_status
    if verification_status != "not_requested":
        provenance["verification_rpc_provider_label"] = "secondary"
        provenance["verification_rpc_provider_origin"] = (
            "https://rpc.example"
            if verification_status == "error"
            else "https://verify.example"
        )

    build_polygon_settlement_audit_release(
        release_connection,
        tmp_path,
        PolygonSettlementAuditSpec("1.0.0"),
        provenance=provenance,
        generator_commit="f" * 40,
    )
    release = tmp_path / "releases" / "1.0.0"
    provenance_json = json.loads(
        (release / "PROVENANCE.json").read_text(encoding="utf-8")
    )
    quality_json = json.loads(
        (release / "QUALITY_REPORT.json").read_text(encoding="utf-8")
    )
    verification_issues = [
        issue
        for issue in quality_json["issues"]
        if issue["issue_type"] == "verification"
    ]

    assert provenance_json["verification_status"] == verification_status
    assert "rpc_provider_terms" not in provenance_json
    assert quality_json["verification_status"] == verification_status
    if verification_status == "matched":
        assert verification_issues == []
        assert quality_json["warehouse_gate"]["warning_issue_count"] == 0
    else:
        assert len(verification_issues) == 1
        assert verification_status in verification_issues[0]["issue_detail"]
        assert quality_json["warehouse_gate"]["warning_issue_count"] == 1
    if verification_status == "error":
        assert "non-independent" in verification_issues[0]["issue_detail"]
    if verification_status != "not_requested":
        assert provenance_json["verification_rpc_provider_label"] == "secondary"
        assert provenance_json["verification_rpc_provider_origin"].startswith(
            "https://"
        )


def test_verification_reconciliation_downgrades_same_source_match(
    provenance: dict,
) -> None:
    provenance.update(
        verification_status="matched",
        verification_rpc_provider_label="test-provider",
        verification_rpc_provider_origin="https://verify.example",
    )
    effective = publishing._effective_release_provenance(provenance)
    assert effective["verification_status"] == "error"

    quality, issues = publishing._reconcile_verification_quality(
        [],
        [],
        effective,
    )
    assert quality == []
    assert len(issues) == 1
    assert "non-independent" in issues[0]["issue_detail"]

    quality, issues = publishing._reconcile_verification_quality(
        [{"warning_issue_count": 0, "error_issue_count": 0}],
        [
            {
                "issue_key": "blocking",
                "severity": "error",
                "issue_type": "price",
            }
        ],
        {**provenance, "verification_status": "matched"},
    )
    assert quality == [{"warning_issue_count": 0, "error_issue_count": 1}]
    assert [issue["issue_key"] for issue in issues] == ["blocking"]


def test_release_refuses_overwrite_and_preserves_existing_bundle(
    release_connection: duckdb.DuckDBPyConnection,
    provenance: dict,
    tmp_path: Path,
) -> None:
    spec = PolygonSettlementAuditSpec("1.0.0")
    kwargs = {
        "provenance": provenance,
        "generator_commit": "f" * 40,
    }
    build_polygon_settlement_audit_release(release_connection, tmp_path, spec, **kwargs)
    original = (tmp_path / "releases" / "1.0.0" / "CHECKSUMS.sha256").read_bytes()

    with pytest.raises(FileExistsError, match="release already exists"):
        build_polygon_settlement_audit_release(
            release_connection, tmp_path, spec, **kwargs
        )

    assert (
        tmp_path / "releases" / "1.0.0" / "CHECKSUMS.sha256"
    ).read_bytes() == original


def test_release_refuses_dangling_version_symlink(
    release_connection: duckdb.DuckDBPyConnection,
    provenance: dict,
    tmp_path: Path,
) -> None:
    releases = tmp_path / "releases"
    releases.mkdir()
    version_link = releases / "1.0.0"
    version_link.symlink_to(tmp_path / "missing-release", target_is_directory=True)

    with pytest.raises(FileExistsError, match="release already exists"):
        build_polygon_settlement_audit_release(
            release_connection,
            tmp_path,
            PolygonSettlementAuditSpec("1.0.0"),
            provenance=provenance,
            generator_commit="f" * 40,
        )

    assert version_link.is_symlink()


def test_failed_quality_gate_leaves_no_partial_release(
    release_connection: duckdb.DuckDBPyConnection,
    provenance: dict,
    tmp_path: Path,
) -> None:
    release_connection.execute(
        """
        update polymarket_wc2026_observability.polymarket_wc2026_polygon_settlement_data_quality
        set publication_ready = false
        """
    )
    with pytest.raises(ValueError, match="not publication-ready"):
        build_polygon_settlement_audit_release(
            release_connection,
            tmp_path,
            PolygonSettlementAuditSpec("1.0.1"),
            provenance=provenance,
            generator_commit="f" * 40,
        )
    assert list((tmp_path / "releases").iterdir()) == []


@pytest.mark.parametrize(
    "version",
    ["1", "v1.0.0", "01.0.0", "1.0.0/escape", "1.0.0-01"],
)
def test_rejects_non_semver_versions(version: str) -> None:
    with pytest.raises(ValueError, match="SemVer"):
        validate_dataset_version(version)


def test_validates_audit_spec_and_provenance(
    release_connection: duckdb.DuckDBPyConnection,
    provenance: dict,
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="SemVer"):
        PolygonSettlementAuditSpec("latest")

    provenance["rpc_provider_origin"] = "https://rpc.example/secret/key"
    with pytest.raises(ValueError, match="sanitized origin"):
        build_polygon_settlement_audit_release(
            release_connection,
            tmp_path,
            PolygonSettlementAuditSpec("1.0.0"),
            provenance=provenance,
            generator_commit="f" * 40,
        )


def test_generator_commit_requires_a_clean_repo(monkeypatch, tmp_path: Path) -> None:
    responses = iter(
        [
            SimpleNamespace(stdout=""),
            SimpleNamespace(stdout="F" * 40 + "\n"),
        ]
    )
    monkeypatch.setattr(publishing.subprocess, "run", lambda *_a, **_k: next(responses))
    assert current_generator_commit(tmp_path) == "f" * 40

    responses = iter(
        [
            SimpleNamespace(stdout=" M source.py\n"),
            SimpleNamespace(stdout="f" * 40 + "\n"),
        ]
    )
    monkeypatch.setattr(publishing.subprocess, "run", lambda *_a, **_k: next(responses))
    with pytest.raises(RuntimeError, match="clean Git working tree"):
        current_generator_commit(tmp_path)

    monkeypatch.setattr(
        publishing.subprocess,
        "run",
        MagicMock(side_effect=subprocess.CalledProcessError(1, "git")),
    )
    with pytest.raises(RuntimeError, match="resolve the generator Git commit"):
        current_generator_commit(tmp_path)

    responses = iter(
        [SimpleNamespace(stdout=""), SimpleNamespace(stdout="not-a-commit\n")]
    )
    monkeypatch.setattr(publishing.subprocess, "run", lambda *_a, **_k: next(responses))
    with pytest.raises(RuntimeError, match="invalid generator commit"):
        current_generator_commit(tmp_path)


def test_release_input_reader_fails_closed(
    release_connection: duckdb.DuckDBPyConnection,
) -> None:
    with pytest.raises(LookupError, match="Missing required relation"):
        publishing._read_relation(
            release_connection,
            "polymarket_wc2026_staging",
            "missing",
            ("value",),
            order_by=("value",),
        )
    release_connection.execute(
        "create table polymarket_wc2026_staging.incomplete (other integer)"
    )
    with pytest.raises(ValueError, match="missing release columns"):
        publishing._read_relation(
            release_connection,
            "polymarket_wc2026_staging",
            "incomplete",
            ("value",),
            order_by=("value",),
        )


def _warehouse_release_rows(conn: duckdb.DuckDBPyConnection):
    mart = publishing._read_relation(
        conn,
        "polymarket_wc2026_marts",
        publishing.MART_NAME,
        publishing.MAIN_COLUMNS,
        order_by=("fifa_match_id", "proposition_id", "settlement_minute_utc"),
    )
    markets = publishing._read_market_rows(conn)
    quality = publishing._read_relation(
        conn,
        "polymarket_wc2026_observability",
        publishing.QUALITY_NAME,
        publishing.QUALITY_COLUMNS,
        order_by=("scan_id",),
    )
    return mart, markets, quality


def test_release_row_validation_rejects_every_public_contract_break(
    release_connection: duckdb.DuckDBPyConnection,
    provenance: dict,
) -> None:
    mart, markets, quality = _warehouse_release_rows(release_connection)

    def rejects(
        *,
        mart_rows=mart,
        market_rows=markets,
        quality_rows=quality,
    ) -> None:
        with pytest.raises(ValueError, match="Invalid Polygon settlement release"):
            publishing._validate_rows(mart_rows, market_rows, quality_rows, provenance)

    rejects(mart_rows=mart[:-1])
    rejects(market_rows=markets[:-1])

    for column, value in (
        ("fifa_match_id", 0),
        ("proposition_id", markets[1]["proposition_id"]),
        ("yes_token_id", markets[1]["yes_token_id"]),
        ("manifest_sha256", "f" * 64),
        ("proposition_type", "unexpected"),
    ):
        changed = [dict(row) for row in markets]
        changed[0][column] = value
        rejects(market_rows=changed)

    changed_mart = list(mart)
    changed_mart[0] = {**mart[0], "proposition_id": "unknown"}
    rejects(mart_rows=changed_mart)
    changed_mart[0] = {**mart[0], "home_team": "Different"}
    rejects(mart_rows=changed_mart)
    changed_mart[0] = {
        **mart[0],
        "settlement_minute_utc": mart[1]["settlement_minute_utc"],
    }
    rejects(mart_rows=changed_mart)
    changed_mart[0] = {**mart[0], "minute_status": "invalid"}
    rejects(mart_rows=changed_mart)
    changed_mart[0] = {**mart[0], "minute_complete": False}
    rejects(mart_rows=changed_mart)
    shifted_timestamp = mart[0]["settlement_minute_utc"] + publishing.timedelta(
        seconds=30
    )
    changed_mart[0] = {
        **mart[0],
        "settlement_minute_utc": shifted_timestamp,
        "yes_first_settlement_at_utc": shifted_timestamp,
        "yes_last_settlement_at_utc": shifted_timestamp,
        "no_first_settlement_at_utc": shifted_timestamp,
        "no_last_settlement_at_utc": shifted_timestamp,
    }
    rejects(mart_rows=changed_mart)

    changed_mart = list(mart)
    changed_mart[1] = {
        **mart[1],
        "settlement_minute_utc": mart[0]["settlement_minute_utc"],
    }
    rejects(mart_rows=changed_mart)

    rejects(quality_rows=[])
    for column, value in (
        ("scan_id", "different"),
        ("scan_status", "failed"),
        ("error_issue_count", 1),
        ("blocking_issue_keys", "blocking"),
    ):
        changed_quality = [{**quality[0], column: value}]
        rejects(quality_rows=changed_quality)


@pytest.mark.parametrize(
    "changes",
    (
        {"yes_open": Decimal("1.1")},
        {"yes_open": Decimal("0.2")},
        {"yes_normalized_fill_count": 0},
        {"yes_derived_fill_count": 2},
        {"yes_gross_collateral_volume": Decimal("11")},
        {"yes_observed": "true"},
        {"yes_normalized_fill_count": Decimal("1")},
        {"yes_derived_fill_count": Decimal("0")},
        {"yes_share_volume": "invalid"},
        {"yes_gross_collateral_volume": "invalid"},
        {"yes_share_volume": Decimal("NaN")},
        {"yes_gross_collateral_volume": Decimal("NaN")},
        {"yes_open": "invalid"},
        {"yes_first_settlement_at_utc": "invalid"},
        {
            "yes_observed": False,
            "minute_complete": False,
            "minute_status": "no_only",
        },
    ),
    ids=(
        "price-outside-unit-interval",
        "open-outside-ohlc-range",
        "zero-observed-count",
        "derived-count-exceeds-total",
        "collateral-exceeds-shares",
        "wrong-observed-type",
        "wrong-normalized-count-type",
        "wrong-derived-count-type",
        "invalid-share-volume",
        "invalid-collateral-volume",
        "nan-share-volume",
        "nan-collateral-volume",
        "invalid-price",
        "invalid-settlement-timestamp",
        "unobserved-side-retains-values",
    ),
)
def test_release_row_validation_rejects_corrupt_public_mart_values(
    release_connection: duckdb.DuckDBPyConnection,
    provenance: dict,
    changes: dict,
) -> None:
    mart, markets, quality = _warehouse_release_rows(release_connection)
    changed = list(mart)
    changed[0] = {**mart[0], **changes}

    with pytest.raises(ValueError, match="invalid audit mart values"):
        publishing._validate_rows(changed, markets, quality, provenance)


def test_release_row_validation_accepts_all_four_minute_states(
    release_connection: duckdb.DuckDBPyConnection,
    provenance: dict,
) -> None:
    mart, markets, quality = _warehouse_release_rows(release_connection)
    changed = list(mart)
    states = (
        (True, True, True, "both_observed"),
        (True, False, False, "yes_only"),
        (False, True, False, "no_only"),
        (False, False, False, "no_fills"),
    )
    for index, (yes, no, complete, status) in enumerate(states):
        row = {
            **mart[index],
            "yes_observed": yes,
            "no_observed": no,
            "minute_complete": complete,
            "minute_status": status,
        }
        for side, observed in (("yes", yes), ("no", no)):
            if observed:
                continue
            row.update(
                {
                    f"{side}_{field}": None
                    for field in (
                        "open",
                        "high",
                        "low",
                        "close",
                        "vwap",
                        "first_settlement_at_utc",
                        "last_settlement_at_utc",
                    )
                }
            )
            row.update(
                {
                    f"{side}_normalized_fill_count": 0,
                    f"{side}_derived_fill_count": 0,
                    f"{side}_share_volume": Decimal("0"),
                    f"{side}_gross_collateral_volume": Decimal("0"),
                }
            )
        changed[index] = row
    summary = publishing._validate_rows(changed, markets, quality, provenance)
    assert summary["empty_minutes"] == 1


def test_provenance_validation_rejects_incomplete_or_unsafe_values(
    provenance: dict,
) -> None:
    cases = []
    for key in publishing._PROVENANCE_KEYS:
        changed = dict(provenance)
        changed.pop(key)
        cases.append(changed)
        break
    cases.extend(
        [
            {**provenance, "chain_id": 1},
            {**provenance, "seed_sha256": "bad"},
            {**provenance, "seed_version": "latest"},
            {**provenance, "finalized_head_block_hash": "bad"},
            {**provenance, "exchange_addresses": []},
            {**provenance, "block_ranges": []},
            {**provenance, "block_ranges": ["bad"]},
            {
                **provenance,
                "block_ranges": [{**provenance["block_ranges"][0], "to_block": 50}],
            },
            {
                **provenance,
                "block_ranges": [
                    {**provenance["block_ranges"][0], "from_block_hash": "bad"}
                ],
            },
            {
                **provenance,
                "block_ranges": [
                    {**provenance["block_ranges"][0], "chunk_sha256": "bad"}
                ],
            },
            {
                **provenance,
                "block_ranges": [
                    {
                        **provenance["block_ranges"][0],
                        "exchange_address": "0x0000000000000000000000000000000000000000",
                    },
                    provenance["block_ranges"][1],
                ],
            },
            {**provenance, "block_ranges": [provenance["block_ranges"][0]]},
            {**provenance, "rpc_provider_label": ""},
            {**provenance, "rpc_provider_origin": "ftp://rpc.example"},
            {**provenance, "rpc_provider_origin": "http://rpc.example"},
            {**provenance, "rpc_provider_origin": "https://:secret@rpc.example"},
            {**provenance, "rpc_provider_origin": "https://rpc.example:bad"},
            {**provenance, "rpc_provider_origin": "https://rpc.example/"},
            {**provenance, "verification_status": "unknown"},
            {
                **provenance,
                "verification_rpc_provider_label": "secondary",
                "verification_rpc_provider_origin": "https://verify.example/secret",
            },
            {
                **provenance,
                "verification_rpc_provider_label": "secondary",
                "verification_rpc_provider_origin": ("https://:secret@verify.example"),
            },
        ]
    )
    for changed in cases:
        with pytest.raises(ValueError):
            publishing._validate_provenance(changed)


def test_release_format_helpers_cover_public_types_and_formula_safety(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="control characters"):
        publishing._validate_plain_label("provider\nlabel", "provider", maximum=100)
    publishing._validate_provider_origin(
        "https://[2001:4860:4860::8888]", "rpc_provider_origin"
    )
    with pytest.raises(ValueError, match="sanitized origin"):
        publishing._validate_provider_origin(
            "https://RPC.EXAMPLE", "rpc_provider_origin"
        )
    assert publishing._csv_value("=SUM(1,1)") == "'=SUM(1,1)"
    assert publishing._format_value(date(2026, 7, 22)) == "2026-07-22"
    assert publishing._format_value(None) == ""
    assert publishing._format_value(False) == "false"
    assert publishing._format_value(Decimal("1.2300")) == "1.2300"
    assert publishing._as_bool("yes") is True
    assert publishing._as_bool("no") is False
    assert publishing._utc_datetime("2026-07-22T00:00:00Z") == datetime(
        2026, 7, 22, tzinfo=timezone.utc
    )
    assert publishing._jsonable((Decimal("1.2"), date(2026, 7, 22))) == [
        "1.2",
        "2026-07-22",
    ]
    assert publishing._quote_identifier('a"b') == '"a""b"'

    assert publishing._column_schema("settlement_minute_utc")["type"].startswith(
        "RFC3339"
    )
    assert publishing._column_schema("fifa_match_id")["type"] == "integer"
    assert publishing._column_schema("yes_open")["type"].startswith("decimal")
    assert publishing._column_schema("yes_share_volume")["type"].startswith("decimal")
    assert publishing._column_schema("yes_observed")["type"] == "boolean"
    assert publishing._column_schema("home_team")["type"] == "string"

    with pytest.raises(RuntimeError, match="audit release files differ"):
        publishing._validate_audit_bundle_files(tmp_path)
    linked_file = tmp_path / "linked-file"
    linked_file.write_text("linked", encoding="utf-8")
    (tmp_path / "linked-entry").symlink_to(linked_file)
    with pytest.raises(RuntimeError, match="audit release files differ"):
        publishing._validate_audit_bundle_files(tmp_path)


def test_build_rejects_invalid_generator_sha_before_writing(
    release_connection: duckdb.DuckDBPyConnection,
    provenance: dict,
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="generator_commit"):
        build_polygon_settlement_audit_release(
            release_connection,
            tmp_path,
            PolygonSettlementAuditSpec("1.0.0"),
            provenance=provenance,
            generator_commit="bad",
        )
    assert not (tmp_path / "releases").exists()
