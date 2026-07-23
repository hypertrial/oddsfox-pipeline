"""Build an immutable internal WC2026 Polygon settlement audit bundle."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import shutil
import subprocess
import tempfile
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, Iterable, Mapping, Sequence
from urllib.parse import urlsplit

import duckdb

from oddsfox_pipeline.config.settings_warehouse import BASE_DIR
from oddsfox_pipeline.ingestion.polymarket.polygon_resolution import (
    load_polygon_resolution_attestation,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (
    DEFAULT_POLYGON_MARKET_SEED_PATH,
    PolygonMarketManifest,
    load_polygon_market_seed,
)
from oddsfox_pipeline.storage.duckdb.schemas.dbt_schemas import (
    POLYMARKET_WC2026_MARTS_SCHEMA,
    POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
    POLYMARKET_WC2026_STAGING_SCHEMA,
)

DATASET_TITLE: Final = "WC2026 Polygon Settlement Audit Bundle"
MART_NAME: Final = "polymarket_wc2026_polygon_settlement_minute_odds"
MARKETS_NAME: Final = "stg_polymarket_wc2026_polygon_settlement_markets"
QUALITY_NAME: Final = "polymarket_wc2026_polygon_settlement_data_quality"
QUALITY_ISSUES_NAME: Final = "polymarket_wc2026_polygon_settlement_quality_issues"
MAIN_CSV_NAME: Final = "wc2026_polygon_settlement_minute_odds.csv"
MARKETS_CSV_NAME: Final = "wc2026_polygon_settlement_markets.csv"
EXPECTED_MART_ROWS: Final = 39_120
EXPECTED_MARKETS: Final = 248
EXPECTED_MATCHES: Final = 104
DEFAULT_POLYGON_SETTLEMENT_AUDIT_ROOT: Final = (
    BASE_DIR / "artifacts" / "polygon_settlement" / "audit"
)

STANDARD_EXCHANGE: Final = "0xE111180000d2663C0091e4f400237545B87B996B"
NEG_RISK_EXCHANGE: Final = "0xe2222d279d744050d28e00520010520000310F59"
OPENFOOTBALL_REVISION: Final = "bd46a148289f9930da66c140d4d7d2325e95d387"
OPENFOOTBALL_LICENSE_SHA256: Final = (
    "36ffd9dc085d529a7e60e1276d73ae5a030b020313e6c5408593a6ae2af39673"
)
OPENFOOTBALL_LICENSE_URI: Final = (
    f"https://github.com/openfootball/worldcup/blob/{OPENFOOTBALL_REVISION}/LICENSE.md"
)
FIFA_SCHEDULE_URI: Final = (
    "https://digitalhub.fifa.com/asset/"
    "4b5d4417-3343-4732-9cdf-14b6662af407/"
    "FWC26-Match-Schedule_English.pdf"
)
FIFA_SCHEDULE_REVISION: Final = "FWC26 Match Schedule_v31_16072026_EN"
FIFA_SCHEDULE_SHA256: Final = (
    "165fb909253b746e6173a4443bdc3e5d786530f0684af6e85c1fd21fff252811"
)

_SEMVER_RE: Final = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*))?"
    r"(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
)
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_BLOCK_HASH_RE: Final = re.compile(r"^0x[0-9a-fA-F]{64}$")
_COMMIT_RE: Final = re.compile(r"^[0-9a-f]{40}$")
_VERIFICATION_STATUSES: Final = {
    "not_requested",
    "matched",
    "mismatched",
    "error",
}
_PROVENANCE_KEYS: Final[tuple[str, ...]] = (
    "scan_id",
    "seed_sha256",
    "seed_version",
    "chain_id",
    "exchange_addresses",
    "finalized_head_block_number",
    "finalized_head_block_hash",
    "block_ranges",
    "normalizer_version",
    "scan_published_at_utc",
    "rpc_provider_label",
    "rpc_provider_origin",
    "verification_status",
)
_BLOCK_RANGE_KEYS: Final[tuple[str, ...]] = (
    "exchange_address",
    "from_block",
    "to_block",
    "from_block_hash",
    "to_block_hash",
    "chunk_sha256",
)

MAIN_COLUMNS: Final[tuple[str, ...]] = (
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

MARKET_COLUMNS: Final[tuple[str, ...]] = (
    "proposition_id",
    "fifa_match_id",
    "stage",
    "group_label",
    "home_team",
    "away_team",
    "kickoff_at_utc",
    "window_start_at_utc",
    "window_end_at_utc",
    "proposition_type",
    "yes_represents",
    "no_represents",
    "condition_id",
    "yes_token_id",
    "no_token_id",
    "market_structure",
    "exchange_address",
    "openfootball_revision",
    "openfootball_path",
    "openfootball_source_lines",
    "openfootball_line_hash",
    "condition_init_tx_hash",
    "condition_init_log_index",
    "question_init_tx_hash",
    "question_init_log_index",
    "ancillary_data_sha256",
    "token_verification_block_number",
    "token_verification_block_hash",
    "manifest_sha256",
    "manifest_version",
    "reviewed_at_utc",
)

# The sidecar keeps the reviewed seed vocabulary, while dbt staging uses the
# mart-facing identity and timestamp names below.
_MARKET_SOURCE_BY_OUTPUT: Final[dict[str, str]] = {
    **{column: column for column in MARKET_COLUMNS},
    "group_label": "group_name",
    "kickoff_at_utc": "scheduled_kickoff_at_utc",
    "window_start_at_utc": "analysis_window_start_at_utc",
    "window_end_at_utc": "analysis_window_end_at_utc",
}

QUALITY_COLUMNS: Final[tuple[str, ...]] = (
    "scan_id",
    "scan_status",
    "publication_ready",
    "blocking_issue_keys",
    "warning_issue_count",
    "error_issue_count",
)

QUALITY_ISSUE_COLUMNS: Final[tuple[str, ...]] = (
    "issue_key",
    "severity",
    "issue_type",
    "proposition_id",
    "fifa_match_id",
    "token_id",
    "settlement_minute_utc",
    "measured_value",
    "threshold_value",
    "issue_detail",
    "observed_at",
)

AUDIT_BUNDLE_FILES: Final[tuple[str, ...]] = (
    MAIN_CSV_NAME,
    MARKETS_CSV_NAME,
    "schema.json",
    "README.md",
    "SOURCES.csv",
    "PROVENANCE.json",
    "QUALITY_REPORT.json",
    "CHANGELOG.md",
    "DO_NOT_PUBLISH.md",
    "CHECKSUMS.sha256",
)


@dataclass(frozen=True)
class PolygonSettlementAuditSpec:
    """Inputs for one immutable internal audit release."""

    dataset_version: str

    def __post_init__(self) -> None:
        validate_dataset_version(self.dataset_version)


def validate_dataset_version(value: str) -> str:
    """Return a valid SemVer 2.0 version or raise a release-safe error."""
    if not _SEMVER_RE.fullmatch(value):
        raise ValueError(f"dataset_version must be SemVer 2.0, got {value!r}")
    return value


def current_generator_commit(repo_root: Path = BASE_DIR) -> str:
    """Return the exact tracked generator revision used for a release."""
    try:
        status = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=normal"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError("Could not resolve the generator Git commit") from exc
    if status.stdout.strip():
        raise RuntimeError("Dataset releases require a clean Git working tree")
    commit = completed.stdout.strip().lower()
    if not _COMMIT_RE.fullmatch(commit):
        raise RuntimeError("Git returned an invalid generator commit")
    return commit


def build_polygon_settlement_audit_release(
    conn: duckdb.DuckDBPyConnection,
    output_root: Path,
    spec: PolygonSettlementAuditSpec,
    *,
    provenance: Mapping[str, Any],
    generator_commit: str,
) -> dict[str, Any]:
    """Validate warehouse inputs and atomically build an internal audit bundle."""
    release_provenance = _effective_release_provenance(provenance)
    _validate_provenance(release_provenance)
    if not _COMMIT_RE.fullmatch(generator_commit):
        raise ValueError("generator_commit must be a lowercase 40-character Git SHA")

    market_rows = _read_market_rows(conn)
    resolution_attestation = _validate_committed_seed(market_rows, release_provenance)

    release_root = output_root.resolve() / "releases"
    release_dir = release_root / spec.dataset_version
    if release_dir.exists() or release_dir.is_symlink():
        raise FileExistsError(f"release already exists: {release_dir}")
    release_root.mkdir(parents=True, exist_ok=True)
    temporary_dir = Path(
        tempfile.mkdtemp(prefix=f".{spec.dataset_version}.", dir=release_root)
    )
    try:
        mart_rows = _read_relation(
            conn,
            POLYMARKET_WC2026_MARTS_SCHEMA,
            MART_NAME,
            MAIN_COLUMNS,
            order_by=(
                "fifa_match_id",
                "proposition_id",
                "settlement_minute_utc",
            ),
        )
        quality_rows = _read_relation(
            conn,
            POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
            QUALITY_NAME,
            QUALITY_COLUMNS,
            order_by=("scan_id",),
        )
        issue_rows = _read_relation(
            conn,
            POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
            QUALITY_ISSUES_NAME,
            QUALITY_ISSUE_COLUMNS,
            order_by=("severity", "issue_key"),
        )
        quality_rows, issue_rows = _reconcile_verification_quality(
            quality_rows,
            issue_rows,
            release_provenance,
        )
        summary = _validate_rows(
            mart_rows,
            market_rows,
            quality_rows,
            release_provenance,
        )

        _write_csv(
            temporary_dir / MAIN_CSV_NAME,
            ("dataset_version", *MAIN_COLUMNS),
            ({"dataset_version": spec.dataset_version, **row} for row in mart_rows),
        )
        _write_csv(
            temporary_dir / MARKETS_CSV_NAME,
            ("dataset_version", *MARKET_COLUMNS),
            ({"dataset_version": spec.dataset_version, **row} for row in market_rows),
        )
        _write_audit_metadata(
            temporary_dir,
            spec=spec,
            provenance=release_provenance,
            resolution_attestation=resolution_attestation,
            generator_commit=generator_commit,
            summary=summary,
            market_rows=market_rows,
            quality_rows=quality_rows,
            issue_rows=issue_rows,
        )
        _write_checksums(temporary_dir)
        _validate_audit_bundle_files(temporary_dir)
        temporary_dir.rename(release_dir)
    except BaseException:
        shutil.rmtree(temporary_dir, ignore_errors=True)
        raise

    return {
        **summary,
        "dataset_version": spec.dataset_version,
        "release_dir": str(release_dir),
        "files": list(AUDIT_BUNDLE_FILES),
    }


def _read_relation(
    conn: duckdb.DuckDBPyConnection,
    schema: str,
    relation: str,
    columns: Sequence[str],
    *,
    order_by: Sequence[str],
) -> list[dict[str, Any]]:
    available = {
        row[0]
        for row in conn.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = ? and table_name = ?
            """,
            [schema, relation],
        ).fetchall()
    }
    if not available:
        raise LookupError(f"Missing required relation {schema}.{relation}")
    missing = set(columns) - available
    if missing:
        raise ValueError(
            f"{schema}.{relation} is missing release columns: {sorted(missing)}"
        )
    selected = ", ".join(_quote_identifier(column) for column in columns)
    ordering = ", ".join(_quote_identifier(column) for column in order_by)
    cursor = conn.execute(
        f"select {selected} from {_quote_identifier(schema)}."
        f"{_quote_identifier(relation)} order by {ordering}"
    )
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _read_market_rows(
    conn: duckdb.DuckDBPyConnection,
) -> list[dict[str, Any]]:
    source_rows = _read_relation(
        conn,
        POLYMARKET_WC2026_STAGING_SCHEMA,
        MARKETS_NAME,
        tuple(dict.fromkeys(_MARKET_SOURCE_BY_OUTPUT.values())),
        order_by=("fifa_match_id", "proposition_id"),
    )
    return [
        {
            output_column: row[source_column]
            for output_column, source_column in _MARKET_SOURCE_BY_OUTPUT.items()
        }
        for row in source_rows
    ]


_LOWERCASE_SEED_COLUMNS: Final = {
    "condition_id",
    "exchange_address",
    "openfootball_revision",
    "openfootball_line_hash",
    "condition_init_tx_hash",
    "question_init_tx_hash",
    "ancillary_data_sha256",
    "token_verification_block_hash",
    "manifest_sha256",
}


def _seed_rows_from_manifest(
    manifest: PolygonMarketManifest,
) -> list[dict[str, Any]]:
    return [
        {column: getattr(market, column) for column in MARKET_COLUMNS}
        for market in manifest.markets
    ]


def _canonical_seed_value(column: str, value: Any) -> str:
    rendered = _format_value(value)
    return rendered.lower() if column in _LOWERCASE_SEED_COLUMNS else rendered


def _validate_committed_seed(
    market_rows: Sequence[Mapping[str, Any]],
    provenance: Mapping[str, Any],
) -> dict[str, Any]:
    manifest = load_polygon_market_seed(DEFAULT_POLYGON_MARKET_SEED_PATH)
    if manifest.sha256 != str(provenance["seed_sha256"]) or manifest.version != str(
        provenance["seed_version"]
    ):
        raise ValueError(
            "Committed Polygon seed version/hash does not match scan provenance"
        )

    committed = sorted(
        _seed_rows_from_manifest(manifest),
        key=lambda row: (int(row["fifa_match_id"]), str(row["proposition_id"])),
    )
    warehouse = sorted(
        market_rows,
        key=lambda row: (int(row["fifa_match_id"]), str(row["proposition_id"])),
    )
    if len(committed) != len(warehouse):
        raise ValueError(
            "Committed Polygon seed and warehouse sidecar row counts differ"
        )
    for expected, actual in zip(committed, warehouse, strict=True):
        for column in MARKET_COLUMNS:
            if _canonical_seed_value(column, expected[column]) != _canonical_seed_value(
                column, actual[column]
            ):
                raise ValueError(
                    "Committed Polygon seed differs from warehouse sidecar at "
                    f"proposition_id={expected['proposition_id']!r}, column={column!r}"
                )
    return load_polygon_resolution_attestation(manifest=manifest).as_mapping()


def _validate_rows(
    mart_rows: Sequence[Mapping[str, Any]],
    market_rows: Sequence[Mapping[str, Any]],
    quality_rows: Sequence[Mapping[str, Any]],
    provenance: Mapping[str, Any],
) -> dict[str, Any]:
    failures: list[str] = []
    if len(mart_rows) != EXPECTED_MART_ROWS:
        failures.append(f"mart rows={len(mart_rows)}, expected {EXPECTED_MART_ROWS}")
    if len(market_rows) != EXPECTED_MARKETS:
        failures.append(f"markets={len(market_rows)}, expected {EXPECTED_MARKETS}")

    match_ids = {int(row["fifa_match_id"]) for row in market_rows}
    if match_ids != set(range(1, EXPECTED_MATCHES + 1)):
        failures.append("market sidecar must contain FIFA match IDs 1..104")
    proposition_ids = [str(row["proposition_id"]) for row in market_rows]
    if len(set(proposition_ids)) != EXPECTED_MARKETS:
        failures.append("market sidecar proposition_id values are not unique")
    token_ids = {
        str(row[column])
        for row in market_rows
        for column in ("yes_token_id", "no_token_id")
    }
    if len(token_ids) != EXPECTED_MARKETS * 2:
        failures.append("market sidecar must contain 496 unique token IDs")
    manifest_hashes = {str(row["manifest_sha256"]) for row in market_rows}
    if manifest_hashes != {str(provenance["seed_sha256"])}:
        failures.append("market sidecar manifest hash does not match provenance")
    proposition_inventory = Counter(str(row["proposition_type"]) for row in market_rows)
    if proposition_inventory != Counter(
        {
            "home_win": 72,
            "draw": 72,
            "away_win": 72,
            "home_advances": 30,
            "home_win_third_place": 1,
            "home_wins_final": 1,
        }
    ):
        failures.append("market sidecar proposition inventory is invalid")

    expected_minutes = {
        str(row["proposition_id"]): 150 if int(row["fifa_match_id"]) <= 72 else 210
        for row in market_rows
    }
    axes: dict[str, set[int]] = {key: set() for key in expected_minutes}
    markets_by_proposition = {str(row["proposition_id"]): row for row in market_rows}
    grain: set[tuple[str, str]] = set()
    statuses = {"both_observed", "yes_only", "no_only", "no_fills"}
    for row in mart_rows:
        proposition_id = str(row["proposition_id"])
        minute = int(row["elapsed_window_minute"])
        if proposition_id not in axes:
            failures.append(f"unknown mart proposition_id={proposition_id!r}")
            break
        market = markets_by_proposition[proposition_id]
        identity_pairs = (
            (row["fifa_match_id"], market["fifa_match_id"]),
            (row["stage"], market["stage"]),
            (row["group_name"], market["group_label"]),
            (row["home_team"], market["home_team"]),
            (row["away_team"], market["away_team"]),
            (row["proposition_type"], market["proposition_type"]),
            (row["yes_represents"], market["yes_represents"]),
            (row["no_represents"], market["no_represents"]),
            (row["scheduled_kickoff_at_utc"], market["kickoff_at_utc"]),
            (row["analysis_window_start_at_utc"], market["window_start_at_utc"]),
            (row["analysis_window_end_at_utc"], market["window_end_at_utc"]),
        )
        if any(
            _format_value(left) != _format_value(right)
            for left, right in identity_pairs
        ):
            failures.append(
                f"mart identity differs from sidecar for proposition_id={proposition_id!r}"
            )
            break
        axes[proposition_id].add(minute)
        grain_key = (proposition_id, _format_value(row["settlement_minute_utc"]))
        if grain_key in grain:
            failures.append(f"duplicate mart grain={grain_key!r}")
            break
        grain.add(grain_key)
        if row["minute_status"] not in statuses:
            failures.append(f"invalid minute_status={row['minute_status']!r}")
            break
        if not all(_audit_side_values_are_valid(row, side) for side in ("yes", "no")):
            failures.append(f"invalid audit mart values for grain={grain_key!r}")
            break
        yes_observed = _as_bool(row["yes_observed"])
        no_observed = _as_bool(row["no_observed"])
        expected_status = (
            "both_observed"
            if yes_observed and no_observed
            else "yes_only"
            if yes_observed
            else "no_only"
            if no_observed
            else "no_fills"
        )
        if (
            type(row["minute_complete"]) is not bool
            or row["minute_status"] != expected_status
            or row["minute_complete"] != (yes_observed and no_observed)
        ):
            failures.append(f"invalid minute state for grain={grain_key!r}")
            break
        expected_timestamp = _utc_datetime(market["window_start_at_utc"]) + timedelta(
            minutes=minute
        )
        if _utc_datetime(row["settlement_minute_utc"]) != expected_timestamp:
            failures.append(f"invalid settlement timestamp for grain={grain_key!r}")
            break
    for proposition_id, minute_count in expected_minutes.items():
        if axes[proposition_id] != set(range(minute_count)):
            failures.append(
                f"invalid elapsed-minute axis for proposition_id={proposition_id!r}"
            )
            break

    if len(quality_rows) != 1:
        failures.append(f"quality summary rows={len(quality_rows)}, expected 1")
    elif not _as_bool(quality_rows[0]["publication_ready"]):
        failures.append("quality summary is not publication-ready")
    elif str(quality_rows[0]["scan_id"]) != str(provenance["scan_id"]):
        failures.append("quality summary scan_id does not match provenance")
    elif str(quality_rows[0]["scan_status"]) != "published":
        failures.append("quality summary scan_status is not published")
    elif (
        int(quality_rows[0]["error_issue_count"]) != 0
        or str(quality_rows[0]["blocking_issue_keys"] or "").strip()
    ):
        failures.append("quality summary contains blocking issues")

    if failures:
        raise ValueError("Invalid Polygon settlement release: " + "; ".join(failures))
    observed = sum(
        1
        for row in mart_rows
        if _as_bool(row["yes_observed"]) or _as_bool(row["no_observed"])
    )
    return {
        "rows": len(mart_rows),
        "markets": len(market_rows),
        "matches": len(match_ids),
        "tokens": len(token_ids),
        "observed_minutes": observed,
        "empty_minutes": len(mart_rows) - observed,
    }


def _audit_side_values_are_valid(row: Mapping[str, Any], side: str) -> bool:
    observed = row[f"{side}_observed"]
    normalized_count = row[f"{side}_normalized_fill_count"]
    derived_count = row[f"{side}_derived_fill_count"]
    price_values = tuple(
        row[f"{side}_{field}"] for field in ("open", "high", "low", "close", "vwap")
    )
    timestamps = (
        row[f"{side}_first_settlement_at_utc"],
        row[f"{side}_last_settlement_at_utc"],
    )
    if (
        type(observed) is not bool
        or type(normalized_count) is not int
        or type(derived_count) is not int
    ):
        return False
    try:
        share_volume = Decimal(str(row[f"{side}_share_volume"]))
        collateral_volume = Decimal(str(row[f"{side}_gross_collateral_volume"]))
    except (ArithmeticError, TypeError, ValueError):
        return False
    if not share_volume.is_finite() or not collateral_volume.is_finite():
        return False
    if not observed:
        return (
            normalized_count == 0
            and derived_count == 0
            and share_volume == 0
            and collateral_volume == 0
            and all(value is None for value in (*price_values, *timestamps))
        )
    if (
        normalized_count <= 0
        or not 0 <= derived_count <= normalized_count
        or share_volume <= 0
        or collateral_volume <= 0
        or collateral_volume > share_volume
        or any(value is None for value in (*price_values, *timestamps))
    ):
        return False
    try:
        open_price, high_price, low_price, close_price, vwap = (
            Decimal(str(value)) for value in price_values
        )
        first_at, last_at = (_utc_datetime(value) for value in timestamps)
        minute_at = _utc_datetime(row["settlement_minute_utc"])
    except (ArithmeticError, TypeError, ValueError):
        return False
    prices = (open_price, high_price, low_price, close_price, vwap)
    return (
        all(value.is_finite() and 0 <= value <= 1 for value in prices)
        and low_price <= open_price <= high_price
        and low_price <= close_price <= high_price
        and low_price <= vwap <= high_price
        and minute_at <= first_at <= last_at < minute_at + timedelta(minutes=1)
    )


def _validate_provenance(provenance: Mapping[str, Any]) -> None:
    required = set(_PROVENANCE_KEYS)
    missing = required - set(provenance)
    if missing:
        raise ValueError(f"provenance is missing required fields: {sorted(missing)}")
    if int(provenance["chain_id"]) != 137:
        raise ValueError("provenance chain_id must be 137")
    if not _SEMVER_RE.fullmatch(str(provenance["seed_version"])):
        raise ValueError("provenance seed_version must be SemVer 2.0")
    if str(provenance["verification_status"]) not in _VERIFICATION_STATUSES:
        raise ValueError("provenance verification_status is invalid")
    if not _SHA256_RE.fullmatch(str(provenance["seed_sha256"])):
        raise ValueError("provenance seed_sha256 must be lowercase SHA-256")
    if not _BLOCK_HASH_RE.fullmatch(str(provenance["finalized_head_block_hash"])):
        raise ValueError("provenance finalized_head_block_hash is invalid")
    exchanges = {str(value).lower() for value in provenance["exchange_addresses"]}
    if exchanges != {STANDARD_EXCHANGE.lower(), NEG_RISK_EXCHANGE.lower()}:
        raise ValueError("provenance must name both fixed V2 exchange addresses")
    block_ranges = provenance["block_ranges"]
    if not isinstance(block_ranges, list) or not block_ranges:
        raise ValueError("provenance block_ranges must not be empty")
    for block_range in block_ranges:
        if not isinstance(block_range, Mapping) or set(_BLOCK_RANGE_KEYS) - set(
            block_range
        ):
            raise ValueError("provenance block range is missing required fields")
        if str(block_range["exchange_address"]).lower() not in exchanges:
            raise ValueError("provenance block range exchange_address is invalid")
        if int(block_range["to_block"]) < int(block_range["from_block"]):
            raise ValueError("provenance block range is reversed")
        for key in ("from_block_hash", "to_block_hash"):
            if not _BLOCK_HASH_RE.fullmatch(str(block_range[key])):
                raise ValueError(f"provenance block range {key} is invalid")
        if not _SHA256_RE.fullmatch(str(block_range["chunk_sha256"])):
            raise ValueError("provenance block range chunk_sha256 is invalid")
    if {str(item["exchange_address"]).lower() for item in block_ranges} != exchanges:
        raise ValueError("provenance block ranges must cover both V2 exchanges")
    _validate_plain_label(
        str(provenance["rpc_provider_label"]),
        "provenance rpc_provider_label",
        maximum=100,
    )
    _validate_provider_origin(
        str(provenance["rpc_provider_origin"]),
        "provenance rpc_provider_origin",
    )
    secondary_label = provenance.get("verification_rpc_provider_label")
    if secondary_label is not None:
        _validate_plain_label(
            str(secondary_label),
            "provenance verification_rpc_provider_label",
            maximum=100,
        )
    secondary_origin_value = provenance.get("verification_rpc_provider_origin")
    if secondary_origin_value is not None:
        _validate_provider_origin(
            str(secondary_origin_value),
            "provenance verification_rpc_provider_origin",
        )


def _verification_sources_overlap(provenance: Mapping[str, Any]) -> bool:
    secondary_origin = provenance.get("verification_rpc_provider_origin")
    secondary_label = provenance.get("verification_rpc_provider_label")
    return bool(
        secondary_origin
        and str(secondary_origin).casefold()
        == str(provenance["rpc_provider_origin"]).casefold()
    ) or bool(
        secondary_label
        and str(secondary_label).strip().casefold()
        == str(provenance["rpc_provider_label"]).strip().casefold()
    )


def _effective_release_provenance(
    provenance: Mapping[str, Any],
) -> dict[str, Any]:
    effective = dict(provenance)
    if str(
        effective.get("verification_status")
    ) == "matched" and _verification_sources_overlap(effective):
        effective["verification_status"] = "error"
    return effective


def _reconcile_verification_quality(
    quality_rows: Sequence[Mapping[str, Any]],
    issue_rows: Sequence[Mapping[str, Any]],
    provenance: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    status = str(provenance["verification_status"])
    reconciled_issues = [
        dict(row) for row in issue_rows if str(row["issue_type"]) != "verification"
    ]
    if status != "matched":
        if _verification_sources_overlap(provenance):
            detail = (
                "Secondary RPC verification is advisory and non-independent "
                "because its provider origin or label matches the primary RPC "
                f"source ({status})."
            )
        else:
            detail = (
                "Secondary RPC verification is advisory and is not in the "
                f"matched state ({status})."
            )
        reconciled_issues.append(
            {
                "issue_key": f"secondary_verification:{provenance['scan_id']}",
                "severity": "warn",
                "issue_type": "verification",
                "proposition_id": None,
                "fifa_match_id": None,
                "token_id": None,
                "settlement_minute_utc": None,
                "measured_value": None,
                "threshold_value": None,
                "issue_detail": detail,
                "observed_at": provenance["scan_published_at_utc"],
            }
        )
    reconciled_issues.sort(
        key=lambda row: (str(row["severity"]), str(row["issue_key"]))
    )

    reconciled_quality = [dict(row) for row in quality_rows]
    if len(reconciled_quality) == 1:
        reconciled_quality[0]["warning_issue_count"] = sum(
            str(row["severity"]) == "warn" for row in reconciled_issues
        )
        reconciled_quality[0]["error_issue_count"] = sum(
            str(row["severity"]) == "error" for row in reconciled_issues
        )
    return reconciled_quality, reconciled_issues


def _write_audit_metadata(
    directory: Path,
    *,
    spec: PolygonSettlementAuditSpec,
    provenance: Mapping[str, Any],
    resolution_attestation: Mapping[str, Any],
    generator_commit: str,
    summary: Mapping[str, Any],
    market_rows: Sequence[Mapping[str, Any]],
    quality_rows: Sequence[Mapping[str, Any]],
    issue_rows: Sequence[Mapping[str, Any]],
) -> None:
    data_hashes = {
        name: _sha256(directory / name) for name in (MAIN_CSV_NAME, MARKETS_CSV_NAME)
    }
    audit_provenance = {
        key: provenance[key] for key in _PROVENANCE_KEYS if key != "block_ranges"
    }
    for key in (
        "verification_rpc_provider_label",
        "verification_rpc_provider_origin",
    ):
        if provenance.get(key) is not None:
            audit_provenance[key] = provenance[key]
    audit_provenance["block_ranges"] = [
        {key: block_range[key] for key in _BLOCK_RANGE_KEYS}
        for block_range in provenance["block_ranges"]
    ]
    generated_provenance = {
        **_jsonable(audit_provenance),
        "dataset_title": DATASET_TITLE,
        "dataset_version": spec.dataset_version,
        "generator_commit": generator_commit,
        "resolution_attestation": _jsonable(resolution_attestation),
        "source_revisions": {
            "fifa_match_number_schedule": {
                "revision": FIFA_SCHEDULE_REVISION,
                "sha256": FIFA_SCHEDULE_SHA256,
            },
            "openfootball_worldcup": sorted(
                {str(row["openfootball_revision"]) for row in market_rows}
            ),
            "openfootball_license": {
                "revision": OPENFOOTBALL_REVISION,
                "path": "LICENSE.md",
                "sha256": OPENFOOTBALL_LICENSE_SHA256,
                "uri": OPENFOOTBALL_LICENSE_URI,
            },
            "conditional_tokens": "eeefca66eb46c800a9aaab88db2064a99026fde5",
            "uma_ctf_adapter": "8b76cc9e0d46c6f7450a0adb0ddc0f5b0568c9cc",
            "neg_risk_ctf_adapter": "f78b35b0863b4308a431ca307d06f49b2ea65e78",
            "ctf_exchange_v2": "ccc0596074f4dfd62c944fbca4de252893b82b4b",
        },
        "output_sha256": data_hashes,
    }
    _write_json(directory / "PROVENANCE.json", generated_provenance)
    _write_json(
        directory / "QUALITY_REPORT.json",
        {
            "dataset_version": spec.dataset_version,
            "verification_status": provenance["verification_status"],
            "summary": dict(summary),
            "warehouse_gate": _jsonable(dict(quality_rows[0])),
            "issues": _jsonable(list(issue_rows)),
        },
    )
    _write_json(directory / "schema.json", _schema_document())
    _write_sources(directory / "SOURCES.csv", market_rows, provenance)
    _write_text(directory / "README.md", _readme(spec, summary))
    _write_text(directory / "CHANGELOG.md", _changelog(spec))
    _write_text(directory / "DO_NOT_PUBLISH.md", _do_not_publish())


def _write_sources(
    path: Path,
    market_rows: Sequence[Mapping[str, Any]],
    provenance: Mapping[str, Any],
) -> None:
    revisions = sorted({str(row["openfootball_revision"]) for row in market_rows})
    fixture_paths = sorted({str(row["openfootball_path"]) for row in market_rows})
    rows = [
        {
            "source_name": "Polygon PoS blockchain",
            "role": "finalized settlement events and block timestamps",
            "uri": "https://polygon.technology/",
            "revision": f"finalized block {provenance['finalized_head_block_number']}",
            "notes": "Finalized block and settlement observations.",
        },
        {
            "source_name": f"RPC provider: {provenance['rpc_provider_label']}",
            "role": "transport used to acquire finalized Polygon JSON-RPC data",
            "uri": str(provenance["rpc_provider_origin"]),
            "revision": "origin recorded in scan provenance",
            "notes": (
                "The bundle does not redistribute provider responses; "
                f"origin={provenance['rpc_provider_origin']}."
            ),
        },
        {
            "source_name": "FIFA World Cup 26 Match Schedule",
            "role": "official numeric match identifiers only",
            "uri": FIFA_SCHEDULE_URI,
            "revision": f"{FIFA_SCHEDULE_REVISION}; sha256={FIFA_SCHEDULE_SHA256}",
            "notes": "The PDF and its expressive layout are not redistributed.",
        },
        {
            "source_name": "OpenFootball World Cup",
            "role": "fixture identity and scheduled kickoff",
            "uri": "https://github.com/openfootball/worldcup",
            "revision": ",".join(revisions),
            "notes": ("Fixture files: " + ", ".join(fixture_paths)),
        },
        {
            "source_name": "Gnosis ConditionalTokens",
            "role": "minimal on-chain semantic interface reference",
            "uri": "https://github.com/gnosis/conditional-tokens-contracts/tree/eeefca66eb46c800a9aaab88db2064a99026fde5",
            "revision": "eeefca66eb46c800a9aaab88db2064a99026fde5",
            "notes": "No upstream source code is redistributed in this bundle.",
        },
        {
            "source_name": "UMA CTF Adapter",
            "role": "minimal on-chain semantic interface reference",
            "uri": "https://github.com/Polymarket/uma-ctf-adapter/tree/8b76cc9e0d46c6f7450a0adb0ddc0f5b0568c9cc",
            "revision": "8b76cc9e0d46c6f7450a0adb0ddc0f5b0568c9cc",
            "notes": "No upstream source code is redistributed in this bundle.",
        },
        {
            "source_name": "NegRisk CTF Adapter",
            "role": "minimal on-chain semantic interface reference",
            "uri": "https://github.com/Polymarket/neg-risk-ctf-adapter/tree/f78b35b0863b4308a431ca307d06f49b2ea65e78",
            "revision": "f78b35b0863b4308a431ca307d06f49b2ea65e78",
            "notes": "No upstream source code is redistributed in this bundle.",
        },
        {
            "source_name": "Polymarket CTF Exchange V2",
            "role": "minimal settlement-event interface reference",
            "uri": "https://github.com/Polymarket/ctf-exchange-v2/tree/ccc0596074f4dfd62c944fbca4de252893b82b4b",
            "revision": "ccc0596074f4dfd62c944fbca4de252893b82b4b",
            "notes": "No upstream source code is redistributed in this bundle.",
        },
    ]
    _write_csv(
        path,
        ("source_name", "role", "uri", "revision", "notes"),
        rows,
    )


def _schema_document() -> dict[str, Any]:
    return {
        "title": DATASET_TITLE,
        "files": {
            MAIN_CSV_NAME: {
                "grain": ["proposition_id", "settlement_minute_utc"],
                "columns": [
                    _column_schema(column)
                    for column in ("dataset_version", *MAIN_COLUMNS)
                ],
            },
            MARKETS_CSV_NAME: {
                "grain": ["proposition_id"],
                "columns": [
                    _column_schema(column)
                    for column in ("dataset_version", *MARKET_COLUMNS)
                ],
            },
        },
    }


def _column_schema(column: str) -> dict[str, str]:
    if column.endswith(("_at_utc", "_minute_utc")) or column in {
        "reviewed_at_utc",
        "yes_first_settlement_at_utc",
        "yes_last_settlement_at_utc",
        "no_first_settlement_at_utc",
        "no_last_settlement_at_utc",
    }:
        kind = "RFC3339 UTC timestamp or empty"
    elif column.endswith(("_count", "_minute", "_number", "_log_index")) or column in {
        "fifa_match_id",
    }:
        kind = "integer"
    elif column.endswith(("_open", "_high", "_low", "_close", "_vwap")):
        kind = "decimal probability or empty"
    elif column.endswith(("_volume",)):
        kind = "decimal with six asset units"
    elif column.endswith(("_observed",)) or column == "minute_complete":
        kind = "boolean"
    else:
        kind = "string"
    return {"name": column, "type": kind}


def _readme(spec: PolygonSettlementAuditSpec, summary: Mapping[str, Any]) -> str:
    return f"""# {DATASET_TITLE}

Version `{spec.dataset_version}`.

This internal audit bundle contains {summary["rows"]:,} dense proposition-minute
rows for
{summary["markets"]} independently identified FIFA World Cup 2026 propositions.
Odds are exact aggregates of finalized Polygon settlement events inside fixed,
half-open scheduled analysis windows: 150 minutes for group matches and 210
minutes for knockout matches.

## Interpretation

- Timestamps are Polygon settlement times, not order-match times.
- Values are not quotes, order-book snapshots, or Polymarket CLOB price history.
- Fill counts are normalized economic legs, not necessarily unique user trades.
- MINT/MERGE derived counterparts are included and counted separately.
- Unused active maker-asset refunds are validated but are not settlement legs and
  are excluded from counts and volumes.
- Empty minutes remain empty; no forward fill, interpolation, pair normalization,
  or inferred complement is applied.

The market sidecar intentionally retains condition/token identity, exchange
addresses, semantic
initialization transaction/log locators, and token-verification block locators
needed to audit the independently authored mapping. `PROVENANCE.json`,
`QUALITY_REPORT.json`, and `CHECKSUMS.sha256` describe and authenticate this
exact internal release. This directory is not a sanitized publication artifact;
see `DO_NOT_PUBLISH.md`.
"""


def _changelog(spec: PolygonSettlementAuditSpec) -> str:
    return f"""# Dataset changelog

## {spec.dataset_version}

- Immutable internal audit release generated from the finalized scan identified in
  `PROVENANCE.json`.
- Seed corrections and material methodology changes require a new semantic
  version; audit version directories are never overwritten.
"""


def _do_not_publish() -> str:
    return """# Do not publish this directory

This is an internal audit bundle, not a sanitized publication artifact. It
contains condition and token identifiers, exchange addresses, transaction and
log locators, block locators, detailed provenance, and issue-level quality rows.

Use the standalone Polygon settlement exporter to create the strictly allowlisted
technical export. Do not upload, mirror, or otherwise distribute this directory.
"""


def _write_csv(
    path: Path, columns: Sequence[str], rows: Iterable[Mapping[str, Any]]
) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
            extrasaction="raise",
            lineterminator="\n",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_value(row.get(column)) for column in columns})


def _write_json(path: Path, value: Any) -> None:
    _write_text(
        path,
        json.dumps(_jsonable(value), indent=2, sort_keys=True, ensure_ascii=False)
        + "\n",
    )


def _write_text(path: Path, text: str) -> None:
    path.write_text(text.rstrip() + "\n", encoding="utf-8", newline="\n")


def _write_checksums(directory: Path) -> None:
    lines = [
        f"{_sha256(directory / name)}  {name}"
        for name in sorted(set(AUDIT_BUNDLE_FILES) - {"CHECKSUMS.sha256"})
    ]
    _write_text(directory / "CHECKSUMS.sha256", "\n".join(lines))


def _validate_audit_bundle_files(directory: Path) -> None:
    entries = list(directory.iterdir())
    names = {path.name for path in entries if path.is_file() and not path.is_symlink()}
    expected = set(AUDIT_BUNDLE_FILES)
    if (
        any(path.is_symlink() or not path.is_file() for path in entries)
        or names != expected
    ):
        raise RuntimeError(
            f"audit release files differ: missing={sorted(expected - names)}, "
            f"unexpected={sorted(names - expected)}"
        )
    if (directory / "dataset-metadata.json").exists():  # pragma: no cover
        raise RuntimeError("dataset-metadata.json must not be in the audit bundle")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _validate_plain_label(value: str, field: str, *, maximum: int) -> None:
    if not value.strip():
        raise ValueError(f"{field} must not be blank")
    if len(value) > maximum or any(ord(char) < 32 for char in value):
        raise ValueError(f"{field} contains invalid control characters or is too long")


def _validate_provider_origin(value: str, field: str) -> None:
    parsed = urlsplit(value)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError(f"{field} must be a sanitized origin (HTTPS only)") from exc
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"{field} must be a sanitized origin (HTTPS only)")
    host = parsed.hostname.casefold()
    if ":" in host:
        host = f"[{host}]"
    canonical = f"https://{host}{'' if port in (None, 443) else f':{port}'}"
    if value != canonical:
        raise ValueError(f"{field} must be a sanitized origin (HTTPS only)")


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes"}


def _utc_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _csv_value(value: Any) -> str:
    rendered = _format_value(value)
    if isinstance(value, str) and rendered.startswith(("=", "+", "-", "@")):
        return "'" + rendered
    return rendered


def _format_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        normalized = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return normalized.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date, Decimal)):
        return _format_value(value)
    return value


__all__ = [
    "AUDIT_BUNDLE_FILES",
    "DEFAULT_POLYGON_SETTLEMENT_AUDIT_ROOT",
    "MAIN_COLUMNS",
    "MARKET_COLUMNS",
    "PolygonSettlementAuditSpec",
    "build_polygon_settlement_audit_release",
    "current_generator_commit",
    "validate_dataset_version",
]
