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
from tests.support.polygon_settlement_release_fixtures import (
    _build_full_release_db,
    minimal_market_row,
    minimal_mart_row,
    minimal_quality_row,
)
from tests.unit.ingestion.test_polygon_seed import complete_seed_rows

from oddsfox_pipeline.ingestion.polymarket.polygon_seed import SEED_COLUMNS
from oddsfox_pipeline.publishing import _bundle_io as bundle_io
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


@pytest.fixture(scope="module")
def full_release_template(tmp_path_factory):
    path = tmp_path_factory.mktemp("polygon-release") / "template.duckdb"
    _build_full_release_db(path)
    return path


@pytest.fixture
def release_connection(full_release_template, monkeypatch, tmp_path):
    import shutil
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    db_path = tmp_path / "release.duckdb"
    shutil.copy2(full_release_template, db_path)
    conn = duckdb.connect(str(db_path))
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
    provenance["verification_rpc_provider_label"] = "secondary"
    provenance["verification_rpc_provider_origin"] = None
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
    assert provenance_json["verification_rpc_provider_label"] == "secondary"
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
    ("verification_status", "secondary_origin"),
    [
        ("not_requested", None),
        ("matched", "https://verify.example"),
        ("mismatched", "https://verify.example"),
        ("error", "https://rpc.example"),
    ],
)
def test_release_reconciles_current_verification_status_and_warning(
    provenance: dict,
    verification_status: str,
    secondary_origin: str | None,
) -> None:
    provenance["verification_status"] = verification_status
    if secondary_origin is not None:
        provenance["verification_rpc_provider_label"] = "secondary"
        provenance["verification_rpc_provider_origin"] = secondary_origin

    effective = publishing._effective_release_provenance(provenance)
    assert effective["verification_status"] == verification_status

    quality, issues = publishing._reconcile_verification_quality(
        [{"warning_issue_count": 0, "error_issue_count": 0}],
        [
            {
                "issue_key": "secondary_verification:scan-1",
                "severity": "warn",
                "issue_type": "verification",
                "issue_detail": "stale verification warning (not_requested).",
            }
        ],
        effective,
    )
    verification_issues = [
        issue for issue in issues if issue["issue_type"] == "verification"
    ]
    if verification_status == "matched":
        assert verification_issues == []
        assert quality[0]["warning_issue_count"] == 0
    else:
        assert len(verification_issues) == 1
        assert verification_status in verification_issues[0]["issue_detail"]
        assert quality[0]["warning_issue_count"] == 1
    if verification_status == "error":
        assert "non-independent" in verification_issues[0]["issue_detail"]


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
    release_dir = tmp_path / "releases" / "1.0.0"
    release_dir.mkdir(parents=True)
    sentinel = release_dir / "CHECKSUMS.sha256"
    sentinel.write_bytes(b"keep-me")

    with pytest.raises(FileExistsError, match="release already exists"):
        build_polygon_settlement_audit_release(
            release_connection,
            tmp_path,
            PolygonSettlementAuditSpec("1.0.0"),
            provenance=provenance,
            generator_commit="f" * 40,
        )

    assert sentinel.read_bytes() == b"keep-me"


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
    monkeypatch.setattr(bundle_io.subprocess, "run", lambda *_a, **_k: next(responses))
    assert current_generator_commit(tmp_path) == "f" * 40

    responses = iter(
        [
            SimpleNamespace(stdout=" M source.py\n"),
            SimpleNamespace(stdout="f" * 40 + "\n"),
        ]
    )
    monkeypatch.setattr(bundle_io.subprocess, "run", lambda *_a, **_k: next(responses))
    with pytest.raises(RuntimeError, match="clean Git working tree"):
        current_generator_commit(tmp_path)

    monkeypatch.setattr(
        bundle_io.subprocess,
        "run",
        MagicMock(side_effect=subprocess.CalledProcessError(1, "git")),
    )
    with pytest.raises(RuntimeError, match="resolve the generator Git commit"):
        current_generator_commit(tmp_path)

    responses = iter(
        [SimpleNamespace(stdout=""), SimpleNamespace(stdout="not-a-commit\n")]
    )
    monkeypatch.setattr(bundle_io.subprocess, "run", lambda *_a, **_k: next(responses))
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


def _complete_market_sidecar() -> list[dict]:
    """248 synthetic sidecar markets covering the production inventory."""
    markets: list[dict] = []
    for fifa_match_id in range(1, 105):
        if fifa_match_id <= 72:
            types = ("home_win", "draw", "away_win")
            stage = "group_stage"
        elif fifa_match_id <= 102:
            types = ("home_advances",)
            stage = "round_of_32"
        elif fifa_match_id == 103:
            types = ("home_win_third_place",)
            stage = "third_place"
        else:
            types = ("home_wins_final",)
            stage = "final"
        for proposition_type in types:
            markets.append(
                minimal_market_row(
                    proposition_id=f"prop_{len(markets) + 1:03d}",
                    fifa_match_id=fifa_match_id,
                    proposition_type=proposition_type,
                    stage=stage,
                )
            )
    assert len(markets) == 248
    return markets


def _assert_failures(failures: list[str]) -> None:
    assert failures, "expected validation failures"


def test_release_row_validation_rejects_every_public_contract_break(
    provenance: dict,
) -> None:
    markets = _complete_market_sidecar()
    quality = [minimal_quality_row()]

    failures: list[str] = []
    publishing._validate_market_sidecar_inventory(markets[:-1], provenance, failures)
    _assert_failures(failures)

    for column, value in (
        ("fifa_match_id", 0),
        ("proposition_id", markets[1]["proposition_id"]),
        ("yes_token_id", markets[1]["yes_token_id"]),
        ("manifest_sha256", "f" * 64),
        ("proposition_type", "unexpected"),
    ):
        changed = [dict(row) for row in markets]
        changed[0][column] = value
        failures = []
        publishing._validate_market_sidecar_inventory(changed, provenance, failures)
        _assert_failures(failures)

    market = markets[0]
    mart_row = minimal_mart_row(market)
    grain: set[tuple[str, str]] = set()
    axes = {market["proposition_id"]: set()}

    failures = []
    publishing._validate_mart_row_contract(
        {**mart_row, "proposition_id": "unknown"},
        market,
        grain=grain,
        axes=axes,
        failures=failures,
    )
    _assert_failures(failures)

    grain = set()
    axes = {market["proposition_id"]: set()}
    failures = []
    publishing._validate_mart_row_contract(
        {**mart_row, "home_team": "Different"},
        market,
        grain=grain,
        axes=axes,
        failures=failures,
    )
    _assert_failures(failures)

    grain = {
        (
            market["proposition_id"],
            publishing._format_value(mart_row["settlement_minute_utc"]),
        )
    }
    axes = {market["proposition_id"]: set()}
    failures = []
    publishing._validate_mart_row_contract(
        mart_row,
        market,
        grain=grain,
        axes=axes,
        failures=failures,
    )
    _assert_failures(failures)

    grain = set()
    axes = {market["proposition_id"]: set()}
    failures = []
    publishing._validate_mart_row_contract(
        {**mart_row, "minute_status": "invalid"},
        market,
        grain=grain,
        axes=axes,
        failures=failures,
    )
    _assert_failures(failures)

    grain = set()
    axes = {market["proposition_id"]: set()}
    failures = []
    publishing._validate_mart_row_contract(
        {**mart_row, "minute_complete": False},
        market,
        grain=grain,
        axes=axes,
        failures=failures,
    )
    _assert_failures(failures)

    shifted = mart_row["settlement_minute_utc"] + publishing.timedelta(seconds=30)
    grain = set()
    axes = {market["proposition_id"]: set()}
    failures = []
    publishing._validate_mart_row_contract(
        {
            **mart_row,
            "settlement_minute_utc": shifted,
            "yes_first_settlement_at_utc": shifted,
            "yes_last_settlement_at_utc": shifted,
            "no_first_settlement_at_utc": shifted,
            "no_last_settlement_at_utc": shifted,
        },
        market,
        grain=grain,
        axes=axes,
        failures=failures,
    )
    _assert_failures(failures)

    # Axis coverage failure without materializing 39,120 rows.
    failures = []
    publishing._validate_mart_global_inventory([mart_row], [market], failures)
    _assert_failures(failures)

    failures = []
    publishing._validate_mart_global_inventory(
        [{**mart_row, "proposition_id": "unknown"}],
        [market],
        failures,
    )
    assert any("unknown mart proposition" in failure for failure in failures)

    failures = []
    publishing._validate_mart_global_inventory(
        [{**mart_row, "home_team": "Different"}],
        [market],
        failures,
    )
    _assert_failures(failures)

    failures = []
    publishing._validate_quality_summary_row([], provenance, failures)
    _assert_failures(failures)
    for column, value in (
        ("scan_id", "different"),
        ("scan_status", "failed"),
        ("error_issue_count", 1),
        ("blocking_issue_keys", "blocking"),
    ):
        failures = []
        publishing._validate_quality_summary_row(
            [{**quality[0], column: value}], provenance, failures
        )
        _assert_failures(failures)


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
    changes: dict,
) -> None:
    market = minimal_market_row()
    row = {**minimal_mart_row(market), **changes}
    failures: list[str] = []
    publishing._validate_mart_row_contract(
        row,
        market,
        grain=set(),
        axes={market["proposition_id"]: set()},
        failures=failures,
    )
    assert any("invalid audit mart values" in item for item in failures)


def test_release_row_validation_accepts_all_four_minute_states() -> None:
    market = minimal_market_row()
    states = (
        (True, True, True, "both_observed"),
        (True, False, False, "yes_only"),
        (False, True, False, "no_only"),
        (False, False, False, "no_fills"),
    )
    empty_minutes = 0
    for index, (yes, no, complete, status) in enumerate(states):
        row = minimal_mart_row(
            market,
            elapsed_window_minute=index,
            minute_status=status,
            yes_observed=yes,
            no_observed=no,
        )
        assert row["minute_complete"] is complete
        failures: list[str] = []
        publishing._validate_mart_row_contract(
            row,
            market,
            grain=set(),
            axes={market["proposition_id"]: set()},
            failures=failures,
        )
        assert failures == []
        if not (yes or no):
            empty_minutes += 1
    assert empty_minutes == 1


def test_release_row_validation_accepts_complete_warehouse(
    release_connection: duckdb.DuckDBPyConnection,
    provenance: dict,
) -> None:
    mart, markets, quality = _warehouse_release_rows(release_connection)
    summary = publishing._validate_rows(mart, markets, quality, provenance)
    assert summary["rows"] == publishing.EXPECTED_MART_ROWS
    assert summary["markets"] == publishing.EXPECTED_MARKETS
    assert summary["matches"] == publishing.EXPECTED_MATCHES


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
