"""Read-only validation and transactional loading for Scraper reference bundles."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

import duckdb
import pyarrow.parquet as pq

from oddsfox_pipeline.contracts.schema import schema_fingerprint

REFERENCE_CONTRACT_VERSION: Final = "oddsfox.reference.v1"
REFERENCE_SCHEMA_VERSION: Final = "1.0.0"
REFERENCE_SCHEMA: Final = "oddsfox_reference"
REFERENCE_TABLE_PRIMARY_KEYS: Final[dict[str, tuple[str, ...]]] = {
    "international_results_wc2026_matches": ("match_id",),
    "international_results_wc2026_team_aliases": ("market_team_name",),
    "international_results_wc2026_team_status": ("team_name",),
    "openfootball_wc2026_schedule_fixtures": ("fifa_match_id",),
    "wc2026_base_camp_venues": ("venue",),
    "wc2026_club_strength_current": ("club_key",),
    "wc2026_club_strength_history": ("club_key", "valid_from"),
    "wc2026_club_strength_snapshot": ("snapshot_date", "club_key"),
    "wc2026_event_state_timing": ("snapshot_id", "match_id", "event_id"),
    "wc2026_fixtures": ("match_id",),
    "wc2026_international_matches": ("match_id",),
    "wc2026_player_features": ("game_slug", "competition_key", "player_id"),
    "wc2026_results": ("match_id",),
    "wc2026_source_availability": ("source",),
    "wc2026_source_provenance": ("source", "snapshot_id"),
    "wc2026_squad_player_features": ("source_player_key",),
    "wc2026_team_canonical_aliases": ("variant_match_key",),
    "wc2026_team_identities": ("team_name",),
    "wc2026_team_ratings_current": ("team_code",),
    "wc2026_team_ratings_history": ("snapshot_year", "snapshot_scope", "team_code"),
    "wc2026_team_ratings_pre_match": (
        "match_date",
        "team_code",
        "opponent_code",
        "competition",
    ),
    "wc2026_third_place_lookup": ("option_id",),
    "wc2026_third_place_slot_assignments": ("option_id", "round_of_32_slot"),
    "wc2026_travel_features": ("match_id", "team_name_model"),
}
_SAFE_ID: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SAFE_TABLE: Final = re.compile(r"^[a-z][a-z0-9_]*$")
_SHA256: Final = re.compile(r"^[0-9a-f]{64}$")


class ReferenceBundleError(ValueError):
    """Raised when a reference bundle is unsafe, corrupt, or incompatible."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _text(value: Mapping[str, Any], key: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result.strip():
        raise ReferenceBundleError(f"manifest field {key!r} must be non-empty text")
    return result


def validate_reference_bundle(
    directory: Path,
    *,
    expected_schemas: Mapping[str, str] | None = None,
    expected_bundle_id: str | None = None,
) -> dict[str, Any]:
    """Fail closed on the manifest, inventory, Parquet schemas, and checksums."""
    root = directory.resolve()
    manifest_path = root / "manifest.json"
    checksum_path = root / "checksums.sha256"
    if not manifest_path.is_file() or not checksum_path.is_file():
        raise ReferenceBundleError("reference bundle is incomplete")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ReferenceBundleError("manifest.json is not valid UTF-8 JSON") from exc
    if not isinstance(manifest, dict):
        raise ReferenceBundleError("manifest.json must contain an object")
    if _text(manifest, "contract_version") != REFERENCE_CONTRACT_VERSION:
        raise ReferenceBundleError("unsupported reference contract version")
    if _text(manifest, "schema_version") != REFERENCE_SCHEMA_VERSION:
        raise ReferenceBundleError("unsupported reference schema version")
    revision = _text(manifest, "scraper_revision")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ReferenceBundleError("scraper_revision must be a full Git SHA")
    image_digest = _text(manifest, "scraper_image_digest")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", image_digest):
        raise ReferenceBundleError("scraper_image_digest must be a SHA-256 digest")
    _text(manifest, "created_at")
    sources = manifest.get("sources")
    if not isinstance(sources, list) or not sources:
        raise ReferenceBundleError("sources must be a non-empty array")
    for source in sources:
        if not isinstance(source, dict):
            raise ReferenceBundleError("each source must be an object")
        for key in (
            "source",
            "snapshot_id",
            "revision",
            "checksum",
            "acquired_at",
            "license",
        ):
            _text(source, key)
        if not _SHA256.fullmatch(str(source["checksum"])):
            raise ReferenceBundleError("source checksum must be SHA-256")
    bundle_id = _text(manifest, "bundle_id")
    expected_name = expected_bundle_id or root.name
    if not _SAFE_ID.fullmatch(bundle_id) or expected_name != bundle_id:
        raise ReferenceBundleError(
            "bundle_id is unsafe or does not match its directory"
        )
    if manifest.get("status") != "complete":
        raise ReferenceBundleError("reference bundle is not complete")
    tables = manifest.get("tables")
    if not isinstance(tables, list) or len(tables) != len(REFERENCE_TABLE_PRIMARY_KEYS):
        raise ReferenceBundleError("reference table inventory is incomplete")

    expected_files = {"manifest.json"}
    seen: set[str] = set()
    for entry in tables:
        if not isinstance(entry, dict):
            raise ReferenceBundleError("each table entry must be an object")
        table = _text(entry, "table")
        if not _SAFE_TABLE.fullmatch(table) or table in seen:
            raise ReferenceBundleError(f"invalid or duplicate table: {table!r}")
        path = root / f"{table}.parquet"
        if entry.get("path") != path.name or not path.is_file():
            raise ReferenceBundleError(f"missing or unsafe payload for {table}")
        expected_sha = _text(entry, "sha256")
        if not _SHA256.fullmatch(expected_sha) or _sha256(path) != expected_sha:
            raise ReferenceBundleError(f"checksum mismatch for {table}")
        parquet = pq.ParquetFile(path)
        fingerprint = schema_fingerprint(parquet.schema_arrow)
        if parquet.metadata.num_rows != entry.get("row_count"):
            raise ReferenceBundleError(f"row count mismatch for {table}")
        if fingerprint != entry.get("schema_fingerprint"):
            raise ReferenceBundleError(f"schema mismatch for {table}")
        keys = entry.get("primary_key")
        if (
            not isinstance(keys, list)
            or not keys
            or any(
                not isinstance(key, str) or key not in parquet.schema_arrow.names
                for key in keys
            )
        ):
            raise ReferenceBundleError(f"invalid primary key for {table}")
        if tuple(keys) != REFERENCE_TABLE_PRIMARY_KEYS.get(table):
            raise ReferenceBundleError(f"unsupported primary key for {table}")
        if expected_schemas is not None and expected_schemas.get(table) != fingerprint:
            raise ReferenceBundleError(f"unsupported schema for {table}")
        expected_files.add(path.name)
        seen.add(table)
    if seen != set(REFERENCE_TABLE_PRIMARY_KEYS):
        raise ReferenceBundleError("reference table inventory mismatch")
    if expected_schemas is not None and seen != set(expected_schemas):
        raise ReferenceBundleError("reference table inventory mismatch")

    checksums: dict[str, str] = {}
    for line in checksum_path.read_text(encoding="utf-8").splitlines():
        digest, separator, name = line.partition("  ")
        if not separator or not _SHA256.fullmatch(digest) or name in checksums:
            raise ReferenceBundleError("checksums.sha256 is malformed")
        checksums[name] = digest
    if set(checksums) != expected_files:
        raise ReferenceBundleError("checksums.sha256 inventory mismatch")
    if any(_sha256(root / name) != digest for name, digest in checksums.items()):
        raise ReferenceBundleError("checksums.sha256 verification failed")
    return manifest


def load_reference_bundle(
    directory: Path,
    warehouse_path: Path,
    *,
    expected_schemas: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Replace the active reference schema atomically; replay is idempotent."""
    manifest = validate_reference_bundle(directory, expected_schemas=expected_schemas)
    bundle_id = str(manifest["bundle_id"])
    warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(warehouse_path))
    try:
        connection.execute("create schema if not exists oddsfox_reference_ops")
        connection.execute(
            """
            create table if not exists oddsfox_reference_ops.bundle_ledger (
                bundle_id varchar primary key,
                manifest_sha256 varchar not null,
                loaded_at timestamptz not null default current_timestamp
            )
            """
        )
        prior = connection.execute(
            "select manifest_sha256 from oddsfox_reference_ops.bundle_ledger where bundle_id = ?",
            [bundle_id],
        ).fetchone()
        manifest_sha = _sha256(directory / "manifest.json")
        if prior is not None:
            if prior[0] != manifest_sha:
                raise ReferenceBundleError("immutable bundle ID has changed")
            return manifest

        connection.execute("begin transaction")
        connection.execute(f"drop schema if exists {REFERENCE_SCHEMA} cascade")
        connection.execute(f"create schema {REFERENCE_SCHEMA}")
        for entry in manifest["tables"]:
            table = str(entry["table"])
            path = str((directory / str(entry["path"])).resolve())
            connection.execute(
                f'create table {REFERENCE_SCHEMA}."{table}" as select * from read_parquet(?)',
                [path],
            )
            keys = [str(key) for key in entry["primary_key"]]
            quoted = ", ".join(f'"{key}"' for key in keys)
            duplicate = connection.execute(
                f'select 1 from {REFERENCE_SCHEMA}."{table}" group by {quoted} having count(*) > 1 limit 1'
            ).fetchone()
            if duplicate is not None:
                raise ReferenceBundleError(f"duplicate primary key in {table}")
        connection.execute(
            "insert into oddsfox_reference_ops.bundle_ledger (bundle_id, manifest_sha256) values (?, ?)",
            [bundle_id, manifest_sha],
        )
        connection.execute("commit")
        return manifest
    except BaseException:
        try:
            connection.execute("rollback")
        except duckdb.TransactionException:
            pass
        raise
    finally:
        connection.close()


__all__ = [
    "REFERENCE_CONTRACT_VERSION",
    "REFERENCE_SCHEMA_VERSION",
    "REFERENCE_SCHEMA",
    "REFERENCE_TABLE_PRIMARY_KEYS",
    "ReferenceBundleError",
    "load_reference_bundle",
    "validate_reference_bundle",
]
