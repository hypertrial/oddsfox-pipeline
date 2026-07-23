"""Build a publisher-neutral sanitized export from a Polygon audit release."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
from collections import Counter
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from pathlib import Path
from typing import Any, Final, Mapping, Sequence

import duckdb

from oddsfox_pipeline.config.settings_warehouse import BASE_DIR
from oddsfox_pipeline.ingestion.polymarket.polygon_resolution import (
    load_polygon_resolution_attestation,
)

DATASET_TITLE: Final = "WC2026 Polygon Settlement Minute Aggregates"
MAIN_CSV_NAME: Final = "wc2026_polygon_settlement_minute_odds.csv"
DEFAULT_POLYGON_SETTLEMENT_EXPORT_ROOT: Final = (
    BASE_DIR / "artifacts" / "polygon_settlement" / "exports"
)

# This literal allowlist is intentionally independent of the dbt mart and audit
# builder. Adding a mart column must never add it to an export implicitly.
PUBLIC_COLUMNS: Final[tuple[str, ...]] = (
    "dataset_version",
    "fifa_match_id",
    "stage",
    "group_name",
    "home_team",
    "away_team",
    "proposition_id",
    "proposition_type",
    "yes_represents",
    "no_represents",
    "scheduled_kickoff_at_utc",
    "analysis_window_start_at_utc",
    "analysis_window_end_at_utc",
    "settlement_minute_utc",
    "elapsed_window_minute",
    "yes_open",
    "yes_high",
    "yes_low",
    "yes_close",
    "yes_vwap",
    "yes_normalized_fill_count",
    "yes_derived_fill_count",
    "yes_share_volume",
    "yes_gross_collateral_volume",
    "yes_first_settlement_at_utc",
    "yes_last_settlement_at_utc",
    "yes_observed",
    "no_open",
    "no_high",
    "no_low",
    "no_close",
    "no_vwap",
    "no_normalized_fill_count",
    "no_derived_fill_count",
    "no_share_volume",
    "no_gross_collateral_volume",
    "no_first_settlement_at_utc",
    "no_last_settlement_at_utc",
    "no_observed",
    "minute_complete",
    "minute_status",
)

EXPORT_FILES: Final[tuple[str, ...]] = (
    MAIN_CSV_NAME,
    "schema.json",
    "README.md",
    "SOURCES.csv",
    "MANIFEST.json",
    "QUALITY_SUMMARY.json",
    "QUALITY_SUMMARY.md",
    "CHANGELOG.md",
    "CHECKSUMS.sha256",
)
AUDIT_FILES: Final[frozenset[str]] = frozenset(
    {
        MAIN_CSV_NAME,
        "wc2026_polygon_settlement_markets.csv",
        "schema.json",
        "README.md",
        "SOURCES.csv",
        "PROVENANCE.json",
        "QUALITY_REPORT.json",
        "DO_NOT_PUBLISH.md",
        "CHANGELOG.md",
        "CHECKSUMS.sha256",
    }
)

EXPECTED_MART_ROWS: Final = 39_120
EXPECTED_MARKETS: Final = 248
EXPECTED_MATCHES: Final = 104
GROUP_MATCHES: Final = 72
GROUP_WINDOW_MINUTES: Final = 150
KNOCKOUT_WINDOW_MINUTES: Final = 210
EXPECTED_PROPOSITION_INVENTORY: Final = Counter(
    {
        "home_win": 72,
        "draw": 72,
        "away_win": 72,
        "home_advances": 30,
        "home_win_third_place": 1,
        "home_wins_final": 1,
    }
)
_SEMVER_RE: Final = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*))?"
    r"(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
)
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_COMMIT_RE: Final = re.compile(r"^[0-9a-f]{40}$")
_BLOCK_HASH_RE: Final = re.compile(r"^0x[0-9a-fA-F]{64}$")
_NORMALIZER_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")
_SOURCE_LABEL_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._/-]{0,199}$")
_RFC3339_UTC_RE: Final = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$"
)
_ADDRESS_OR_HASH_RE: Final = re.compile(
    r"(?i)(?<![0-9a-f])0x(?:[0-9a-f]{40}|[0-9a-f]{64})(?![0-9a-f])"
)
_LONG_INTEGER_RE: Final = re.compile(r"(?<![\d.])\d{40,}(?![\d.])")
_URL_RE: Final = re.compile(r"(?i)\b(?:https?|wss?)://")
_EMAIL_RE: Final = re.compile(r"(?i)(?<![\w.+-])[\w.+-]+@[\w-]+(?:\.[\w-]+)+(?![\w.-])")
_LOCATOR_RE: Final = re.compile(
    r"(?i)\b(?:condition|token|transaction|tx|log|block|exchange|wallet)"
    r"[\s_-]*(?:id|hash|index|number|address)\b"
)
_RPC_DETAIL_RE: Final = re.compile(
    r"(?i)\b(?:json[\s_-]*rpc|rpc[\s_-]*(?:url|provider|endpoint|origin|label))\b"
)
_UNSIGNED_INTEGER_RE: Final = re.compile(r"^(?:0|[1-9]\d*)$")
_UNSIGNED_DECIMAL_RE: Final = re.compile(r"^(0|[1-9]\d*)(?:\.(\d+))?$")
_MAX_BIGINT: Final = 9_223_372_036_854_775_807
_PROBABILITY_QUANTUM: Final = Decimal("0.000000000000000001")

_TIMESTAMP_COLUMNS: Final = (
    "scheduled_kickoff_at_utc",
    "analysis_window_start_at_utc",
    "analysis_window_end_at_utc",
    "settlement_minute_utc",
    "yes_first_settlement_at_utc",
    "yes_last_settlement_at_utc",
    "no_first_settlement_at_utc",
    "no_last_settlement_at_utc",
)
_NULLABLE_TIMESTAMP_COLUMNS: Final = frozenset(
    {
        "yes_first_settlement_at_utc",
        "yes_last_settlement_at_utc",
        "no_first_settlement_at_utc",
        "no_last_settlement_at_utc",
    }
)
_BOOLEAN_COLUMNS: Final = ("yes_observed", "no_observed", "minute_complete")
_TEXT_COLUMNS: Final = (
    "dataset_version",
    "stage",
    "home_team",
    "away_team",
    "proposition_id",
    "proposition_type",
    "yes_represents",
    "no_represents",
    "minute_status",
)
_PROBABILITY_COLUMNS: Final = tuple(
    f"{side}_{field}"
    for side in ("yes", "no")
    for field in ("open", "high", "low", "close", "vwap")
)
_COUNT_COLUMNS: Final = tuple(
    f"{side}_{field}_fill_count"
    for side in ("yes", "no")
    for field in ("normalized", "derived")
)
_VOLUME_COLUMNS: Final = tuple(
    f"{side}_{field}_volume"
    for side in ("yes", "no")
    for field in ("share", "gross_collateral")
)
_INTEGER_COLUMNS: Final = (
    "fifa_match_id",
    "elapsed_window_minute",
    *_COUNT_COLUMNS,
)
_DECIMAL_SCALES: Final = {
    **{column: 18 for column in _PROBABILITY_COLUMNS},
    **{column: 6 for column in _VOLUME_COLUMNS},
}
_DUCKDB_TYPES: Final[dict[str, str]] = {
    **{column: "VARCHAR" for column in PUBLIC_COLUMNS},
    "fifa_match_id": "INTEGER",
    "elapsed_window_minute": "INTEGER",
    **{column: "TIMESTAMPTZ" for column in _TIMESTAMP_COLUMNS},
    **{column: "DECIMAL(38,18)" for column in _PROBABILITY_COLUMNS},
    **{column: "BIGINT" for column in _COUNT_COLUMNS},
    **{column: "DECIMAL(38,6)" for column in _VOLUME_COLUMNS},
    **{column: "BOOLEAN" for column in _BOOLEAN_COLUMNS},
}


def export_polygon_settlement_minute_odds(
    audit_release: Path,
    output_root: Path = DEFAULT_POLYGON_SETTLEMENT_EXPORT_ROOT,
    *,
    repo_root: Path = BASE_DIR,
) -> dict[str, Any]:
    """Validate an audit release and atomically build its sanitized export."""
    audit_dir = _validate_audit_release_path(audit_release)
    audit_release_name = audit_dir.name
    with tempfile.TemporaryDirectory(prefix="oddsfox-polygon-audit-") as snapshot_root:
        snapshot_dir = Path(snapshot_root) / "audit"
        _snapshot_audit_release(audit_dir, snapshot_dir)
        audit_checksums = _validate_checksum_manifest(snapshot_dir, AUDIT_FILES)
        provenance = _read_json_object(snapshot_dir / "PROVENANCE.json")
        quality = _read_json_object(snapshot_dir / "QUALITY_REPORT.json")
        _validate_audit_quality(quality)
        export_commit = _current_clean_commit(repo_root)
        reviewed_resolution = load_polygon_resolution_attestation()

        csv_path = snapshot_dir / MAIN_CSV_NAME
        analysis = analyze_polygon_settlement_csv(csv_path)
        dataset_version = str(analysis["dataset_version"])
        _validate_audit_provenance(
            provenance,
            dataset_version=dataset_version,
            csv_sha256=audit_checksums[MAIN_CSV_NAME],
            reviewed_resolution=reviewed_resolution.as_mapping(),
        )
        if audit_release_name != dataset_version:
            raise ValueError(
                "Audit release directory name must equal its CSV dataset_version"
            )

        output_root = Path(output_root).absolute()
        _reject_existing_path_symlinks(output_root)
        releases_root = output_root / "releases"
        _reject_existing_path_symlinks(releases_root)
        release_dir = releases_root / dataset_version
        if release_dir.exists() or release_dir.is_symlink():
            raise FileExistsError(f"sanitized export already exists: {release_dir}")
        releases_root.mkdir(parents=True, exist_ok=True)
        _reject_existing_path_symlinks(releases_root)
        temporary_dir = Path(
            tempfile.mkdtemp(prefix=f".{dataset_version}.", dir=releases_root)
        )
        try:
            copied_csv = temporary_dir / MAIN_CSV_NAME
            shutil.copyfile(csv_path, copied_csv)
            if _sha256(copied_csv) != audit_checksums[MAIN_CSV_NAME]:
                raise RuntimeError("Sanitized CSV differs from the audit CSV")

            verification_state = str(provenance["verification_status"])
            analysis["disclosures"]["secondary_verification_state"] = verification_state
            analysis["disclosures"]["secondary_verification_matched"] = (
                verification_state == "matched"
            )
            resolution = reviewed_resolution.public_summary()
            manifest = _manifest_document(
                dataset_version=dataset_version,
                provenance=provenance,
                analysis=analysis,
                resolution=resolution,
                audit_checksums_sha256=_sha256(snapshot_dir / "CHECKSUMS.sha256"),
                csv_sha256=audit_checksums[MAIN_CSV_NAME],
                export_commit=export_commit,
            )
            _write_json(temporary_dir / "schema.json", _schema_document())
            _write_sources(temporary_dir / "SOURCES.csv", provenance)
            _write_json(temporary_dir / "MANIFEST.json", manifest)
            _write_json(temporary_dir / "QUALITY_SUMMARY.json", analysis)
            _write_text(
                temporary_dir / "QUALITY_SUMMARY.md",
                _quality_markdown(analysis),
            )
            _write_text(
                temporary_dir / "README.md",
                _readme(dataset_version, analysis),
            )
            _write_text(
                temporary_dir / "CHANGELOG.md",
                _changelog(dataset_version),
            )
            _write_checksums(temporary_dir)
            _validate_export_files(temporary_dir)
            temporary_dir.rename(release_dir)
        except BaseException:
            shutil.rmtree(temporary_dir, ignore_errors=True)
            raise

        return {
            "dataset_version": dataset_version,
            "rows": analysis["inventory"]["rows"],
            "release_dir": str(release_dir),
            "csv_sha256": audit_checksums[MAIN_CSV_NAME],
        }


def analyze_polygon_settlement_csv(path: Path) -> dict[str, Any]:
    """Fail closed on the public contract and return aggregate-only metrics."""
    path = Path(path)
    try:
        dataset_version, scanned_rows = _scan_csv(path)
    except csv.Error as exc:
        raise ValueError("Sanitized CSV syntax is invalid") from exc
    if scanned_rows != EXPECTED_MART_ROWS:
        raise ValueError(f"CSV rows={scanned_rows}, expected {EXPECTED_MART_ROWS}")

    conn = duckdb.connect(":memory:")
    try:
        _load_csv(conn, path)
        _validate_relational_contract(conn, dataset_version)
        return _quality_summary(conn, dataset_version)
    except duckdb.Error as exc:
        raise ValueError(f"CSV does not satisfy its typed contract: {exc}") from exc
    finally:
        conn.close()


def _validate_audit_release_path(path: Path) -> Path:
    candidate = Path(path).absolute()
    _reject_existing_path_symlinks(candidate)
    if not candidate.is_dir():
        raise ValueError(f"Audit release directory does not exist: {candidate}")
    entries = list(candidate.iterdir())
    if any(entry.is_symlink() for entry in entries):
        raise ValueError("Audit release must not contain symlinks")
    names = {entry.name for entry in entries if entry.is_file()}
    if any(not entry.is_file() for entry in entries) or names != AUDIT_FILES:
        raise ValueError(
            "Audit release files differ: "
            f"missing={sorted(AUDIT_FILES - names)}, "
            f"unexpected={sorted(names - AUDIT_FILES)}"
        )
    return candidate.resolve()


def _snapshot_audit_release(source: Path, destination: Path) -> None:
    """Copy one directory snapshot through no-follow file descriptors."""
    destination.mkdir()
    read_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    directory_fd = os.open(
        source,
        read_flags | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        names = set(os.listdir(directory_fd))
        if names != AUDIT_FILES:
            raise ValueError("Audit release changed while it was being snapshotted")
        for name in sorted(AUDIT_FILES):
            source_fd = os.open(name, read_flags, dir_fd=directory_fd)
            if not stat.S_ISREG(os.fstat(source_fd).st_mode):
                os.close(source_fd)
                raise ValueError("Audit release must contain only regular files")
            with os.fdopen(source_fd, "rb") as input_handle:
                with (destination / name).open("xb") as output_handle:
                    shutil.copyfileobj(input_handle, output_handle)
    finally:
        os.close(directory_fd)


def _reject_existing_path_symlinks(path: Path) -> None:
    current = path
    while True:
        if current.is_symlink():
            raise ValueError(f"Symlink paths are not accepted: {current}")
        if current.parent == current:
            return
        current = current.parent


def _validate_checksum_manifest(
    directory: Path,
    expected_files: frozenset[str],
) -> dict[str, str]:
    manifest = directory / "CHECKSUMS.sha256"
    lines = manifest.read_text(encoding="utf-8").splitlines()
    expected_names = expected_files - {"CHECKSUMS.sha256"}
    if not lines:
        raise ValueError("Audit checksum manifest must be nonempty")

    checksums: dict[str, str] = {}
    for line in lines:
        if line.count("  ") != 1:
            raise ValueError("Audit checksum manifest has a malformed line")
        digest, name = line.split("  ", maxsplit=1)
        if (
            not _SHA256_RE.fullmatch(digest)
            or not name
            or Path(name).name != name
            or name in checksums
        ):
            raise ValueError("Audit checksum manifest has an invalid entry")
        checksums[name] = digest
    if list(checksums) != sorted(checksums):
        raise ValueError("Audit checksum manifest must be sorted by filename")
    if set(checksums) != expected_names:
        raise ValueError("Audit checksum manifest does not cover the exact bundle")
    for name, digest in checksums.items():
        if _sha256(directory / name) != digest:
            raise ValueError(f"Audit checksum mismatch for {name}")
    return checksums


def _validate_audit_quality(quality: Mapping[str, Any]) -> None:
    gate = quality.get("warehouse_gate")
    if not isinstance(gate, Mapping):
        raise ValueError("Audit quality report has no warehouse gate")
    if (
        gate.get("publication_ready") is not True
        or type(gate.get("error_issue_count")) is not int
        or gate["error_issue_count"] != 0
        or str(gate.get("blocking_issue_keys") or "").strip()
    ):
        raise ValueError("Audit warehouse quality gate is not ready")


def _validate_audit_provenance(
    provenance: Mapping[str, Any],
    *,
    dataset_version: str,
    csv_sha256: str,
    reviewed_resolution: Mapping[str, Any],
) -> None:
    if provenance.get("dataset_version") != dataset_version:
        raise ValueError("Audit provenance dataset_version differs from the CSV")
    if provenance.get("chain_id") != 137:
        raise ValueError("Audit provenance must identify Polygon chain ID 137")
    if not _COMMIT_RE.fullmatch(str(provenance.get("generator_commit", ""))):
        raise ValueError("Audit provenance generator_commit is invalid")
    if not _SHA256_RE.fullmatch(str(provenance.get("seed_sha256", ""))):
        raise ValueError("Audit provenance seed_sha256 is invalid")
    if not _SEMVER_RE.fullmatch(str(provenance.get("seed_version", ""))):
        raise ValueError("Audit provenance seed_version is invalid")
    if not _NORMALIZER_RE.fullmatch(str(provenance.get("normalizer_version", ""))):
        raise ValueError("Audit provenance normalizer_version is invalid")
    if provenance.get("verification_status") not in {
        "not_requested",
        "matched",
        "mismatched",
        "error",
    }:
        raise ValueError("Audit provenance verification_status is invalid")
    output_hashes = provenance.get("output_sha256")
    if (
        not isinstance(output_hashes, Mapping)
        or output_hashes.get(MAIN_CSV_NAME) != csv_sha256
    ):
        raise ValueError("Audit provenance CSV hash does not match the bundle")
    finalized_number = provenance.get("finalized_head_block_number")
    finalized_hash = str(provenance.get("finalized_head_block_hash", ""))
    if (
        type(finalized_number) is not int
        or finalized_number <= 0
        or not _BLOCK_HASH_RE.fullmatch(finalized_hash)
    ):
        raise ValueError("Audit provenance does not establish a finalized scan")
    resolution = provenance.get("resolution_attestation")
    if resolution != reviewed_resolution:
        raise ValueError("Audit resolution attestation is not the reviewed release")
    if (
        resolution["manifest_sha256"] != provenance["seed_sha256"]
        or resolution["manifest_version"] != provenance["seed_version"]
    ):
        raise ValueError("Resolution attestation does not match the audit seed")
    if not isinstance(provenance.get("source_revisions"), Mapping):
        raise ValueError("Audit provenance source revisions are missing")


def _scan_csv(path: Path) -> tuple[str, int]:
    if path.is_symlink() or not path.is_file():
        raise ValueError("Sanitized CSV must be a regular file")
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, strict=True)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise ValueError("Sanitized CSV is empty") from exc
        if tuple(header) != PUBLIC_COLUMNS:
            raise ValueError("Sanitized CSV header differs from the literal allowlist")
        _scan_cells(header, row_number=1)

        version: str | None = None
        previous_sort_key: tuple[int, str, str] | None = None
        row_count = 0
        indexes = {name: index for index, name in enumerate(PUBLIC_COLUMNS)}
        for row_number, row in enumerate(reader, start=2):
            if len(row) != len(PUBLIC_COLUMNS):
                raise ValueError(f"CSV row {row_number} has the wrong column count")
            _scan_cells(row, row_number=row_number)
            row_version = row[indexes["dataset_version"]]
            if version is None:
                version = _validate_dataset_version(row_version)
            elif row_version != version:
                raise ValueError("CSV contains more than one dataset_version")
            for column in _BOOLEAN_COLUMNS:
                if row[indexes[column]] not in {"true", "false"}:
                    raise ValueError(
                        f"CSV row {row_number} has an invalid {column} value"
                    )
            for column in _TIMESTAMP_COLUMNS:
                value = row[indexes[column]]
                if not value and column in _NULLABLE_TIMESTAMP_COLUMNS:
                    continue
                if not _RFC3339_UTC_RE.fullmatch(value):
                    raise ValueError(f"CSV row {row_number} has a non-RFC3339 {column}")
            _validate_numeric_cells(row, indexes=indexes, row_number=row_number)
            _validate_row_math(row, indexes=indexes, row_number=row_number)
            sort_key = (
                int(row[indexes["fifa_match_id"]]),
                row[indexes["proposition_id"]],
                row[indexes["settlement_minute_utc"]],
            )
            if previous_sort_key is not None and sort_key < previous_sort_key:
                raise ValueError("Sanitized CSV is not in canonical grain order")
            previous_sort_key = sort_key
            row_count += 1
    if version is None:
        raise ValueError("Sanitized CSV contains no data rows")
    return version, row_count


def _validate_numeric_cells(
    row: Sequence[str],
    *,
    indexes: Mapping[str, int],
    row_number: int,
) -> None:
    for column in _INTEGER_COLUMNS:
        value = row[indexes[column]]
        if not _UNSIGNED_INTEGER_RE.fullmatch(value) or int(value) > _MAX_BIGINT:
            raise ValueError(f"CSV row {row_number} has an invalid integer {column}")
    for column, scale in _DECIMAL_SCALES.items():
        value = row[indexes[column]]
        if not value and column in _PROBABILITY_COLUMNS:
            continue
        match = _UNSIGNED_DECIMAL_RE.fullmatch(value)
        if match is None:
            raise ValueError(f"CSV row {row_number} has an invalid decimal {column}")
        integer_digits, fractional_digits = match.groups()
        if len(integer_digits) > 38 - scale or len(fractional_digits or "") > scale:
            raise ValueError(
                f"CSV row {row_number} exceeds DECIMAL(38,{scale}) for {column}"
            )


def _validate_row_math(
    row: Sequence[str],
    *,
    indexes: Mapping[str, int],
    row_number: int,
) -> None:
    for side in ("yes", "no"):
        if row[indexes[f"{side}_observed"]] != "true":
            continue
        share_volume = Decimal(row[indexes[f"{side}_share_volume"]])
        collateral_volume = Decimal(row[indexes[f"{side}_gross_collateral_volume"]])
        if share_volume > 0:
            with localcontext() as context:
                context.prec = 100
                expected_vwap = (collateral_volume / share_volume).quantize(
                    _PROBABILITY_QUANTUM,
                    rounding=ROUND_HALF_EVEN,
                )
            actual_vwap = Decimal(row[indexes[f"{side}_vwap"]])
            if actual_vwap != expected_vwap:
                raise ValueError(
                    f"CSV row {row_number} has {side} VWAP inconsistent with volumes"
                )
        if row[indexes[f"{side}_normalized_fill_count"]] == "1":
            values = {
                Decimal(row[indexes[f"{side}_{field}"]])
                for field in ("open", "high", "low", "close", "vwap")
            }
            if len(values) != 1:
                raise ValueError(
                    f"CSV row {row_number} has incoherent single-fill {side} OHLC/VWAP"
                )


def _scan_cells(values: Sequence[str], *, row_number: int) -> None:
    for value in values:
        if any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in value):
            raise ValueError(f"CSV row {row_number} contains a control character")
        if value.lstrip().startswith(("=", "+", "-", "@")):
            raise ValueError(f"CSV row {row_number} contains formula-like text")
        if (
            _ADDRESS_OR_HASH_RE.search(value)
            or _LONG_INTEGER_RE.search(value)
            or _URL_RE.search(value)
            or _EMAIL_RE.search(value)
            or _LOCATOR_RE.search(value)
            or _RPC_DETAIL_RE.search(value)
        ):
            raise ValueError(
                f"CSV row {row_number} contains a forbidden identifier or locator"
            )


def _load_csv(conn: duckdb.DuckDBPyConnection, path: Path) -> None:
    columns = ", ".join(
        f"{_sql_string(column)}: {_sql_string(_DUCKDB_TYPES[column])}"
        for column in PUBLIC_COLUMNS
    )
    conn.execute(
        f"""
        create table exported as
        select *
        from read_csv(
            ?,
            header = true,
            auto_detect = false,
            columns = {{{columns}}},
            nullstr = '',
            strict_mode = true
        )
        """,
        [str(path)],
    )


def _validate_relational_contract(
    conn: duckdb.DuckDBPyConnection,
    dataset_version: str,
) -> None:
    required = (
        *_TEXT_COLUMNS,
        "fifa_match_id",
        "elapsed_window_minute",
        "scheduled_kickoff_at_utc",
        "analysis_window_start_at_utc",
        "analysis_window_end_at_utc",
        "settlement_minute_utc",
        *_COUNT_COLUMNS,
        *_VOLUME_COLUMNS,
        *_BOOLEAN_COLUMNS,
    )
    text_required = " or ".join(
        f"{_ident(column)} is null or trim({_ident(column)}) = ''"
        for column in _TEXT_COLUMNS
    )
    other_required = " or ".join(f"{_ident(column)} is null" for column in required)
    _require_zero(
        conn,
        "required values are null or blank",
        f"({text_required}) or ({other_required})",
    )
    _require_zero(
        conn,
        "dataset version mismatch",
        f"dataset_version <> {_sql_string(dataset_version)}",
    )
    _require_zero(
        conn,
        "duplicate proposition-minute grain",
        """
        (proposition_id, settlement_minute_utc) in (
            select (proposition_id, settlement_minute_utc)
            from exported
            group by proposition_id, settlement_minute_utc
            having count(*) > 1
        )
        """,
    )

    matches, markets = conn.execute(
        """
        select count(distinct fifa_match_id), count(distinct proposition_id)
        from exported
        """
    ).fetchone()
    if (matches, markets) != (EXPECTED_MATCHES, EXPECTED_MARKETS):
        raise ValueError(
            "CSV inventory differs: "
            f"matches={matches}, propositions={markets}, "
            f"expected {EXPECTED_MATCHES}/{EXPECTED_MARKETS}"
        )
    _require_zero(
        conn,
        "FIFA match IDs are outside the fixed inventory",
        f"fifa_match_id < 1 or fifa_match_id > {EXPECTED_MATCHES}",
    )
    inventory = Counter(
        dict(
            conn.execute(
                """
                select proposition_type, count(distinct proposition_id)
                from exported
                group by proposition_type
                """
            ).fetchall()
        )
    )
    if inventory != EXPECTED_PROPOSITION_INVENTORY:
        raise ValueError(f"Proposition inventory is invalid: {dict(inventory)}")

    _validate_identity_and_axis(conn)
    _validate_minute_values(conn)


def _validate_identity_and_axis(conn: duckdb.DuckDBPyConnection) -> None:
    _require_zero(
        conn,
        "proposition identity changes within its minute window",
        """
        proposition_id in (
            select proposition_id
            from exported
            group by proposition_id
            having count(distinct row(
                fifa_match_id, stage, group_name, home_team, away_team,
                proposition_type, yes_represents, no_represents,
                scheduled_kickoff_at_utc, analysis_window_start_at_utc,
                analysis_window_end_at_utc
            )) <> 1
        )
        """,
    )
    _require_zero(
        conn,
        "match identity changes across propositions",
        """
        fifa_match_id in (
            select fifa_match_id
            from exported
            group by fifa_match_id
            having count(distinct row(
                stage, group_name, home_team, away_team,
                scheduled_kickoff_at_utc, analysis_window_start_at_utc,
                analysis_window_end_at_utc
            )) <> 1
        )
        """,
    )
    _require_zero(
        conn,
        "match proposition cardinality is invalid",
        f"""
        fifa_match_id in (
            select fifa_match_id
            from exported
            group by fifa_match_id
            having count(distinct proposition_id)
                <> case when fifa_match_id <= {GROUP_MATCHES} then 3 else 1 end
        )
        """,
    )
    _require_zero(
        conn,
        "group proposition set is invalid",
        f"""
        fifa_match_id in (
            select fifa_match_id
            from exported
            where fifa_match_id <= {GROUP_MATCHES}
            group by fifa_match_id
            having
                count(distinct proposition_id)
                    filter (where proposition_type = 'home_win') <> 1
                or count(distinct proposition_id)
                    filter (where proposition_type = 'draw') <> 1
                or count(distinct proposition_id)
                    filter (where proposition_type = 'away_win') <> 1
        )
        """,
    )
    expected_stage = f"""
        case
            when fifa_match_id <= {GROUP_MATCHES} then 'group_stage'
            when fifa_match_id <= 88 then 'round_of_32'
            when fifa_match_id <= 96 then 'round_of_16'
            when fifa_match_id <= 100 then 'quarterfinal'
            when fifa_match_id <= 102 then 'semifinal'
            when fifa_match_id = 103 then 'third_place'
            else 'final'
        end
    """
    _require_zero(
        conn,
        "stage or group label is inconsistent with match identity",
        f"""
        stage <> {expected_stage}
        or (
            fifa_match_id <= {GROUP_MATCHES}
            and (
                group_name is null
                or trim(group_name) = ''
                or group_name not in (
                    'A', 'B', 'C', 'D', 'E', 'F',
                    'G', 'H', 'I', 'J', 'K', 'L'
                )
            )
        )
        or (fifa_match_id > {GROUP_MATCHES} and group_name is not null)
        """,
    )
    _require_zero(
        conn,
        "knockout proposition type is invalid",
        f"""
        (fifa_match_id between {GROUP_MATCHES + 1} and 102
            and proposition_type <> 'home_advances')
        or (fifa_match_id = 103 and proposition_type <> 'home_win_third_place')
        or (fifa_match_id = 104 and proposition_type <> 'home_wins_final')
        """,
    )
    expected_minutes = (
        f"case when fifa_match_id <= {GROUP_MATCHES} "
        f"then {GROUP_WINDOW_MINUTES} else {KNOCKOUT_WINDOW_MINUTES} end"
    )
    _require_zero(
        conn,
        "analysis window or elapsed-minute axis is invalid",
        f"""
        proposition_id in (
            select proposition_id
            from exported
            group by proposition_id, fifa_match_id
            having
                count(*) <> {expected_minutes}
                or count(distinct elapsed_window_minute) <> {expected_minutes}
                or min(elapsed_window_minute) <> 0
                or max(elapsed_window_minute) <> {expected_minutes} - 1
        )
        or analysis_window_start_at_utc <> scheduled_kickoff_at_utc
        or analysis_window_end_at_utc
            <> analysis_window_start_at_utc + {expected_minutes} * interval '1 minute'
        or settlement_minute_utc
            <> analysis_window_start_at_utc
                + elapsed_window_minute * interval '1 minute'
        or settlement_minute_utc >= analysis_window_end_at_utc
        or scheduled_kickoff_at_utc
            <> date_trunc('minute', scheduled_kickoff_at_utc)
        or analysis_window_start_at_utc
            <> date_trunc('minute', analysis_window_start_at_utc)
        or analysis_window_end_at_utc
            <> date_trunc('minute', analysis_window_end_at_utc)
        or settlement_minute_utc
            <> date_trunc('minute', settlement_minute_utc)
        """,
    )


def _validate_minute_values(conn: duckdb.DuckDBPyConnection) -> None:
    _require_zero(
        conn,
        "minute state is inconsistent",
        """
        minute_complete <> (yes_observed and no_observed)
        or minute_status <> case
            when yes_observed and no_observed then 'both_observed'
            when yes_observed then 'yes_only'
            when no_observed then 'no_only'
            else 'no_fills'
        end
        """,
    )
    for side in ("yes", "no"):
        prices = [
            f"{side}_{field}" for field in ("open", "high", "low", "close", "vwap")
        ]
        normalized = f"{side}_normalized_fill_count"
        derived = f"{side}_derived_fill_count"
        share = f"{side}_share_volume"
        collateral = f"{side}_gross_collateral_volume"
        first_at = f"{side}_first_settlement_at_utc"
        last_at = f"{side}_last_settlement_at_utc"
        observed = f"{side}_observed"
        empty_values = " or ".join(f"{column} is not null" for column in prices)
        observed_nulls = " or ".join(f"{column} is null" for column in prices)
        price_bounds = " or ".join(f"{column} < 0 or {column} > 1" for column in prices)
        _require_zero(
            conn,
            f"{side} observed/empty values are inconsistent",
            f"""
            (not {observed} and (
                {normalized} <> 0 or {derived} <> 0
                or {share} <> 0 or {collateral} <> 0
                or {empty_values}
                or {first_at} is not null or {last_at} is not null
            ))
            or ({observed} and (
                {normalized} <= 0
                or {derived} < 0 or {derived} > {normalized}
                or {share} <= 0 or {collateral} <= 0 or {collateral} > {share}
                or {observed_nulls}
                or {first_at} is null or {last_at} is null
            ))
            """,
        )
        _require_zero(
            conn,
            f"{side} probability or OHLC values are invalid",
            f"""
            {observed} and (
                {price_bounds}
                or {side}_low > {side}_high
                or {side}_open not between {side}_low and {side}_high
                or {side}_close not between {side}_low and {side}_high
                or {side}_vwap not between {side}_low and {side}_high
            )
            """,
        )
        _require_zero(
            conn,
            f"{side} settlement timestamps are invalid",
            f"""
            {observed} and (
                {first_at} > {last_at}
                or ({normalized} = 1 and {first_at} <> {last_at})
                or {first_at} < settlement_minute_utc
                or {last_at} >= settlement_minute_utc + interval '1 minute'
            )
            """,
        )


def _quality_summary(
    conn: duckdb.DuckDBPyConnection,
    dataset_version: str,
) -> dict[str, Any]:
    state_counts = {
        str(status): int(count)
        for status, count in conn.execute(
            "select minute_status, count(*) from exported group by minute_status"
        ).fetchall()
    }
    for status in ("both_observed", "yes_only", "no_only", "no_fills"):
        state_counts.setdefault(status, 0)

    coverage = conn.execute(
        """
        with side_coverage as (
            select
                proposition_id,
                count(*) as expected_minutes,
                count(*) filter (where yes_observed) as observed_minutes
            from exported
            group by proposition_id
            union all
            select
                proposition_id,
                count(*) as expected_minutes,
                count(*) filter (where no_observed) as observed_minutes
            from exported
            group by proposition_id
        )
        select
            count(*) filter (where observed_minutes < expected_minutes),
            count(*) filter (where observed_minutes = expected_minutes),
            min(round(
                observed_minutes::decimal(38,6) * 100 / expected_minutes, 6
            )),
            median(round(
                observed_minutes::decimal(38,6) * 100 / expected_minutes, 6
            )),
            max(round(
                observed_minutes::decimal(38,6) * 100 / expected_minutes, 6
            ))
        from side_coverage
        """
    ).fetchone()
    normalized_legs, derived_legs = conn.execute(
        """
        select
            sum(yes_normalized_fill_count + no_normalized_fill_count),
            sum(yes_derived_fill_count + no_derived_fill_count)
        from exported
        """
    ).fetchone()
    pair_count, max_pair_deviation = conn.execute(
        """
        select
            count(*) filter (
                where
                    minute_complete
                    and abs(
                        yes_close + no_close - cast(1 as decimal(38,18))
                    ) > cast('0.05' as decimal(38,18))
            ),
            max(
                abs(yes_close + no_close - cast(1 as decimal(38,18)))
            ) filter (where minute_complete)
        from exported
        """
    ).fetchone()
    one_leg_minutes, one_leg_side_minutes = conn.execute(
        """
        select
            count(*) filter (
                where yes_normalized_fill_count + no_normalized_fill_count = 1
            ),
            count(*) filter (where yes_normalized_fill_count = 1)
                + count(*) filter (where no_normalized_fill_count = 1)
        from exported
        """
    ).fetchone()
    rows, markets, matches = conn.execute(
        """
        select
            count(*),
            count(distinct proposition_id),
            count(distinct fifa_match_id)
        from exported
        """
    ).fetchone()
    observed_minutes = rows - state_counts["no_fills"]
    derived_ratio = (
        Decimal(derived_legs) * Decimal(100) / Decimal(normalized_legs)
        if normalized_legs
        else Decimal(0)
    )
    disclosures = {
        "sparse_proposition_sides": int(coverage[0]),
        "complete_proposition_sides": int(coverage[1]),
        "side_coverage_percent": {
            "minimum": _decimal_text(coverage[2], places=6),
            "median": _decimal_text(coverage[3], places=6),
            "maximum": _decimal_text(coverage[4], places=6),
        },
        "normalized_legs": int(normalized_legs),
        "derived_legs": int(derived_legs),
        "derived_leg_percent": _decimal_text(derived_ratio, places=6),
        "pair_close_deviations_over_0_05": int(pair_count),
        "maximum_pair_close_deviation": _decimal_text(
            max_pair_deviation or Decimal(0),
            places=18,
        ),
        "single_leg_minutes": int(one_leg_minutes),
        "single_leg_side_minutes": int(one_leg_side_minutes),
    }
    return {
        "title": DATASET_TITLE,
        "dataset_version": dataset_version,
        "status": "passed_with_disclosures",
        "hard_failure_count": 0,
        "inventory": {
            "rows": int(rows),
            "columns": len(PUBLIC_COLUMNS),
            "propositions": int(markets),
            "matches": int(matches),
            "observed_minutes": int(observed_minutes),
            "empty_minutes": int(state_counts["no_fills"]),
            "minute_states": dict(sorted(state_counts.items())),
        },
        "disclosures": disclosures,
    }


def _schema_document() -> dict[str, Any]:
    return {
        "title": DATASET_TITLE,
        "file": MAIN_CSV_NAME,
        "grain": ["proposition_id", "settlement_minute_utc"],
        "columns": [_column_schema(column) for column in PUBLIC_COLUMNS],
    }


def _column_schema(column: str) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "name": column,
        "type": _DUCKDB_TYPES[column],
        "nullable": column == "group_name"
        or column in _NULLABLE_TIMESTAMP_COLUMNS
        or column in _PROBABILITY_COLUMNS,
    }
    if column in _TIMESTAMP_COLUMNS:
        schema["format"] = "RFC3339 UTC (Z)"
    if column in _PROBABILITY_COLUMNS:
        schema["unit"] = "probability"
    elif column.endswith("_share_volume"):
        schema["unit"] = "shares"
    elif column.endswith("_gross_collateral_volume"):
        schema["unit"] = "collateral asset units"
    elif column.endswith("_fill_count"):
        schema["unit"] = "normalized settlement legs"
    elif column == "elapsed_window_minute":
        schema["unit"] = "minutes from analysis-window start"
    enums = {
        "group_name": list("ABCDEFGHIJKL"),
        "stage": [
            "group_stage",
            "round_of_32",
            "round_of_16",
            "quarterfinal",
            "semifinal",
            "third_place",
            "final",
        ],
        "proposition_type": [
            "home_win",
            "draw",
            "away_win",
            "home_advances",
            "home_win_third_place",
            "home_wins_final",
        ],
        "minute_status": [
            "both_observed",
            "yes_only",
            "no_only",
            "no_fills",
        ],
    }
    if column in enums:
        schema["enum"] = enums[column]
    return schema


def _manifest_document(
    *,
    dataset_version: str,
    provenance: Mapping[str, Any],
    analysis: Mapping[str, Any],
    resolution: Mapping[str, Any],
    audit_checksums_sha256: str,
    csv_sha256: str,
    export_commit: str,
) -> dict[str, Any]:
    return {
        "title": DATASET_TITLE,
        "dataset_version": dataset_version,
        "files": list(EXPORT_FILES),
        "csv_sha256": csv_sha256,
        "audit_checksum_manifest_sha256": audit_checksums_sha256,
        "generator_revisions": {
            "audit": provenance["generator_commit"],
            "export": export_commit,
        },
        "inventory": analysis["inventory"],
        "seed": {
            "version": provenance["seed_version"],
            "sha256": provenance["seed_sha256"],
        },
        "chain_id": 137,
        "normalizer_version": provenance["normalizer_version"],
        "finalized": True,
        "resolution_attestation": dict(resolution),
        "secondary_verification_state": provenance["verification_status"],
    }


def _write_sources(path: Path, provenance: Mapping[str, Any]) -> None:
    revisions = provenance["source_revisions"]
    fifa = revisions.get("fifa_match_number_schedule")
    openfootball = revisions.get("openfootball_worldcup")
    contract_revisions = tuple(
        revisions.get(key)
        for key in (
            "conditional_tokens",
            "uma_ctf_adapter",
            "neg_risk_ctf_adapter",
            "ctf_exchange_v2",
        )
    )
    if (
        not isinstance(fifa, Mapping)
        or not isinstance(openfootball, list)
        or not openfootball
        or not _SOURCE_LABEL_RE.fullmatch(str(fifa.get("revision", "")))
        or not _SHA256_RE.fullmatch(str(fifa.get("sha256", "")))
        or any(not _COMMIT_RE.fullmatch(str(value)) for value in openfootball)
        or any(not _COMMIT_RE.fullmatch(str(value)) for value in contract_revisions)
    ):
        raise ValueError("Audit source revisions are incomplete or invalid")
    rows = [
        {
            "source_name": "Polygon",
            "role": "finalized settlement-event records",
            "uri": "https://polygon.technology/",
            "revision": "chain_id=137",
            "content_sha256": "",
        },
        {
            "source_name": "FIFA World Cup 26 Match Schedule",
            "role": "numeric match identifiers",
            "uri": (
                "https://digitalhub.fifa.com/asset/"
                "4b5d4417-3343-4732-9cdf-14b6662af407/"
                "FWC26-Match-Schedule_English.pdf"
            ),
            "revision": str(fifa.get("revision", "")),
            "content_sha256": str(fifa.get("sha256", "")),
        },
        {
            "source_name": "OpenFootball World Cup",
            "role": "fixture identity and scheduled kickoff",
            "uri": "https://github.com/openfootball/worldcup",
            "revision": ";".join(sorted(str(value) for value in openfootball)),
            "content_sha256": "",
        },
        {
            "source_name": "Committed Polygon market mapping",
            "role": "reviewed proposition and token orientation input",
            "uri": ("dbt/seeds/polymarket_wc2026_polygon_settlement_markets.csv"),
            "revision": str(provenance["seed_version"]),
            "content_sha256": str(provenance["seed_sha256"]),
        },
        {
            "source_name": "Gnosis ConditionalTokens",
            "role": "event-interface reference",
            "uri": "https://github.com/gnosis/conditional-tokens-contracts",
            "revision": str(revisions["conditional_tokens"]),
            "content_sha256": "",
        },
        {
            "source_name": "UMA CTF Adapter",
            "role": "event-interface reference",
            "uri": "https://github.com/Polymarket/uma-ctf-adapter",
            "revision": str(revisions["uma_ctf_adapter"]),
            "content_sha256": "",
        },
        {
            "source_name": "NegRisk CTF Adapter",
            "role": "event-interface reference",
            "uri": "https://github.com/Polymarket/neg-risk-ctf-adapter",
            "revision": str(revisions["neg_risk_ctf_adapter"]),
            "content_sha256": "",
        },
        {
            "source_name": "CTF Exchange V2",
            "role": "settlement-event interface reference",
            "uri": "https://github.com/Polymarket/ctf-exchange-v2",
            "revision": str(revisions["ctf_exchange_v2"]),
            "content_sha256": "",
        },
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "source_name",
                "role",
                "uri",
                "revision",
                "content_sha256",
            ),
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


def _readme(dataset_version: str, analysis: Mapping[str, Any]) -> str:
    inventory = analysis["inventory"]
    return f"""# {DATASET_TITLE}

Version `{dataset_version}` contains {inventory["rows"]:,} rows for
{inventory["propositions"]} propositions across {inventory["matches"]} FIFA
World Cup 2026 matches.

Each row is one proposition and one scheduled minute in a fixed, half-open
analysis window. Group-stage windows contain 150 minutes and knockout windows
contain 210 minutes. Timestamps record Polygon settlement time, not order-match
time, and the values are not quotes or order-book snapshots.

- Fill counts are normalized economic legs, not necessarily unique trades.
- Derived MINT/MERGE counterparts are counted separately.
- Empty minutes remain empty; no forward fill, interpolation, complement
  inference, or pair normalization is applied.
- The dataset is de-identified, not anonymous. Sparse aggregates can remain
  linkable to public-chain activity.
- Collection uses finalized Polygon JSON-RPC records and does not use a
  Polymarket account, UI, Gamma API, or CLOB API.

This exporter creates technical metadata only. It does not create a dataset
licence, publisher metadata, upload configuration, or an upload operation.
See `schema.json`, `MANIFEST.json`, and `QUALITY_SUMMARY.md` for the exact
contract and aggregate validation results.
"""


def _quality_markdown(analysis: Mapping[str, Any]) -> str:
    inventory = analysis["inventory"]
    disclosures = analysis["disclosures"]
    states = inventory["minute_states"]
    coverage = disclosures["side_coverage_percent"]
    return f"""# Aggregate data-quality summary

All hard structural, schema, timestamp, state, OHLC, unsafe-text, forbidden
identifier, and checksum checks passed.

## Inventory

- Rows: {inventory["rows"]:,}
- Columns: {inventory["columns"]}
- Propositions: {inventory["propositions"]}
- Matches: {inventory["matches"]}
- Observed minutes: {inventory["observed_minutes"]:,}
- Empty minutes: {inventory["empty_minutes"]:,}
- Both observed: {states["both_observed"]:,}
- Yes only: {states["yes_only"]:,}
- No only: {states["no_only"]:,}

## Nonblocking disclosures

- Sparse proposition-sides: {disclosures["sparse_proposition_sides"]}
- Complete proposition-sides: {disclosures["complete_proposition_sides"]}
- Side coverage: {coverage["minimum"]}% minimum,
  {coverage["median"]}% median, {coverage["maximum"]}% maximum
- Normalized legs: {disclosures["normalized_legs"]:,}
- Derived legs: {disclosures["derived_legs"]:,}
  ({disclosures["derived_leg_percent"]}%)
- Pair-close deviations over 0.05:
  {disclosures["pair_close_deviations_over_0_05"]}
- Maximum complete-minute pair-close deviation:
  {disclosures["maximum_pair_close_deviation"]}
- Single-leg minutes: {disclosures["single_leg_minutes"]:,}
- Single-leg side-minutes: {disclosures["single_leg_side_minutes"]:,}
- Secondary verification state: {disclosures["secondary_verification_state"]}

These are aggregate disclosures. This report contains no proposition, token, or
minute-level exception locations.
"""


def _changelog(dataset_version: str) -> str:
    return f"""# Dataset changelog

## {dataset_version}

- Created a byte-identical sanitized CSV from the checksum-valid audit release.
- Recomputed the public structural and aggregate data-quality contract.
- Omitted audit-only identifiers, locators, provenance, and issue-level rows.
"""


def _require_zero(
    conn: duckdb.DuckDBPyConnection,
    label: str,
    predicate: str,
) -> None:
    count = conn.execute(f"select count(*) from exported where {predicate}").fetchone()[
        0
    ]
    if count:
        raise ValueError(f"CSV contract failure: {label} ({count} rows)")


def _current_clean_commit(repo_root: Path) -> str:
    try:
        status = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=normal"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
        commit = (
            subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=repo_root,
                check=True,
                capture_output=True,
                text=True,
            )
            .stdout.strip()
            .lower()
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError("Could not resolve the export generator revision") from exc
    if status.stdout.strip():
        raise RuntimeError("Sanitized exports require a clean Git working tree")
    if not _COMMIT_RE.fullmatch(commit):
        raise RuntimeError("Git returned an invalid export generator revision")
    return commit


def _validate_dataset_version(value: str) -> str:
    if not _SEMVER_RE.fullmatch(value):
        raise ValueError(f"dataset_version must be SemVer 2.0, got {value!r}")
    return value


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Could not read audit JSON: {path.name}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"Audit JSON must contain an object: {path.name}")
    return value


def _write_checksums(directory: Path) -> None:
    lines = [
        f"{_sha256(directory / name)}  {name}"
        for name in sorted(set(EXPORT_FILES) - {"CHECKSUMS.sha256"})
    ]
    _write_text(directory / "CHECKSUMS.sha256", "\n".join(lines))


def _validate_export_files(directory: Path) -> None:
    entries = list(directory.iterdir())
    names = {
        entry.name for entry in entries if entry.is_file() and not entry.is_symlink()
    }
    expected = set(EXPORT_FILES)
    if (
        any(entry.is_symlink() or not entry.is_file() for entry in entries)
        or names != expected
    ):
        raise RuntimeError(
            f"Export files differ: missing={sorted(expected - names)}, "
            f"unexpected={sorted(names - expected)}"
        )
    forbidden = {
        "dataset-metadata.json",
        "LICENSE.txt",
        "NOTICE.md",
        "PROVENANCE.json",
        "QUALITY_REPORT.json",
        "wc2026_polygon_settlement_markets.csv",
    }
    if names & forbidden:  # pragma: no cover - exact allowlist already protects this
        raise RuntimeError("Export contains an audit-only or publisher-controlled file")


def _write_json(path: Path, value: Any) -> None:
    _write_text(
        path,
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False),
    )


def _write_text(path: Path, value: str) -> None:
    path.write_text(value.rstrip() + "\n", encoding="utf-8", newline="\n")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _decimal_text(value: Any, *, places: int) -> str:
    decimal = Decimal(str(value))
    return format(decimal.quantize(Decimal(1).scaleb(-places)), "f")


def _ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


__all__ = [
    "DEFAULT_POLYGON_SETTLEMENT_EXPORT_ROOT",
    "EXPORT_FILES",
    "PUBLIC_COLUMNS",
    "analyze_polygon_settlement_csv",
    "export_polygon_settlement_minute_odds",
]
