"""Build an immutable, sanitized WC2026 Polygon settlement dataset bundle."""

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

DATASET_TITLE: Final = "WC2026 Polygon Settlement Odds"
DATASET_SLUG: Final = "polymarket_wc2026_polygon_settlement_odds"
MART_NAME: Final = "polymarket_wc2026_polygon_settlement_minute_odds"
MARKETS_NAME: Final = "stg_polymarket_wc2026_polygon_settlement_markets"
QUALITY_NAME: Final = "polymarket_wc2026_polygon_settlement_data_quality"
QUALITY_ISSUES_NAME: Final = "polymarket_wc2026_polygon_settlement_quality_issues"
MAIN_CSV_NAME: Final = "wc2026_polygon_settlement_minute_odds.csv"
MARKETS_CSV_NAME: Final = "wc2026_polygon_settlement_markets.csv"
EXPECTED_MART_ROWS: Final = 39_120
EXPECTED_MARKETS: Final = 248
EXPECTED_MATCHES: Final = 104
DEFAULT_POLYGON_SETTLEMENT_RELEASE_ROOT: Final = (
    BASE_DIR / "artifacts" / "kaggle" / DATASET_SLUG
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
# Exact LICENSE.md contents at OPENFOOTBALL_REVISION. Keep this verbatim so an
# immutable release carries the notice even if its upstream link later changes.
OPENFOOTBALL_CC0_NOTICE: Final = """CC0 1.0 Universal

Statement of Purpose

The laws of most jurisdictions throughout the world automatically confer
exclusive Copyright and Related Rights (defined below) upon the creator and
subsequent owner(s) (each and all, an "owner") of an original work of
authorship and/or a database (each, a "Work").

Certain owners wish to permanently relinquish those rights to a Work for the
purpose of contributing to a commons of creative, cultural and scientific
works ("Commons") that the public can reliably and without fear of later
claims of infringement build upon, modify, incorporate in other works, reuse
and redistribute as freely as possible in any form whatsoever and for any
purposes, including without limitation commercial purposes. These owners may
contribute to the Commons to promote the ideal of a free culture and the
further production of creative, cultural and scientific works, or to gain
reputation or greater distribution for their Work in part through the use and
efforts of others.

For these and/or other purposes and motivations, and without any expectation
of additional consideration or compensation, the person associating CC0 with a
Work (the "Affirmer"), to the extent that he or she is an owner of Copyright
and Related Rights in the Work, voluntarily elects to apply CC0 to the Work
and publicly distribute the Work under its terms, with knowledge of his or her
Copyright and Related Rights in the Work and the meaning and intended legal
effect of CC0 on those rights.

1. Copyright and Related Rights. A Work made available under CC0 may be
protected by copyright and related or neighboring rights ("Copyright and
Related Rights"). Copyright and Related Rights include, but are not limited
to, the following:

  i. the right to reproduce, adapt, distribute, perform, display, communicate,
  and translate a Work;

  ii. moral rights retained by the original author(s) and/or performer(s);

  iii. publicity and privacy rights pertaining to a person's image or likeness
  depicted in a Work;

  iv. rights protecting against unfair competition in regards to a Work,
  subject to the limitations in paragraph 4(a), below;

  v. rights protecting the extraction, dissemination, use and reuse of data in
  a Work;

  vi. database rights (such as those arising under Directive 96/9/EC of the
  European Parliament and of the Council of 11 March 1996 on the legal
  protection of databases, and under any national implementation thereof,
  including any amended or successor version of such directive); and

  vii. other similar, equivalent or corresponding rights throughout the world
  based on applicable law or treaty, and any national implementations thereof.

2. Waiver. To the greatest extent permitted by, but not in contravention of,
applicable law, Affirmer hereby overtly, fully, permanently, irrevocably and
unconditionally waives, abandons, and surrenders all of Affirmer's Copyright
and Related Rights and associated claims and causes of action, whether now
known or unknown (including existing as well as future claims and causes of
action), in the Work (i) in all territories worldwide, (ii) for the maximum
duration provided by applicable law or treaty (including future time
extensions), (iii) in any current or future medium and for any number of
copies, and (iv) for any purpose whatsoever, including without limitation
commercial, advertising or promotional purposes (the "Waiver"). Affirmer makes
the Waiver for the benefit of each member of the public at large and to the
detriment of Affirmer's heirs and successors, fully intending that such Waiver
shall not be subject to revocation, rescission, cancellation, termination, or
any other legal or equitable action to disrupt the quiet enjoyment of the Work
by the public as contemplated by Affirmer's express Statement of Purpose.

3. Public License Fallback. Should any part of the Waiver for any reason be
judged legally invalid or ineffective under applicable law, then the Waiver
shall be preserved to the maximum extent permitted taking into account
Affirmer's express Statement of Purpose. In addition, to the extent the Waiver
is so judged Affirmer hereby grants to each affected person a royalty-free,
non transferable, non sublicensable, non exclusive, irrevocable and
unconditional license to exercise Affirmer's Copyright and Related Rights in
the Work (i) in all territories worldwide, (ii) for the maximum duration
provided by applicable law or treaty (including future time extensions), (iii)
in any current or future medium and for any number of copies, and (iv) for any
purpose whatsoever, including without limitation commercial, advertising or
promotional purposes (the "License"). The License shall be deemed effective as
of the date CC0 was applied by Affirmer to the Work. Should any part of the
License for any reason be judged legally invalid or ineffective under
applicable law, such partial invalidity or ineffectiveness shall not
invalidate the remainder of the License, and in such case Affirmer hereby
affirms that he or she will not (i) exercise any of his or her remaining
Copyright and Related Rights in the Work or (ii) assert any associated claims
and causes of action with respect to the Work, in either case contrary to
Affirmer's express Statement of Purpose.

4. Limitations and Disclaimers.

  a. No trademark or patent rights held by Affirmer are waived, abandoned,
  surrendered, licensed or otherwise affected by this document.

  b. Affirmer offers the Work as-is and makes no representations or warranties
  of any kind concerning the Work, express, implied, statutory or otherwise,
  including without limitation warranties of title, merchantability, fitness
  for a particular purpose, non infringement, or the absence of latent or
  other defects, accuracy, or the present or absence of errors, whether or not
  discoverable, all to the greatest extent permissible under applicable law.

  c. Affirmer disclaims responsibility for clearing rights of other persons
  that may apply to the Work or any use thereof, including without limitation
  any person's Copyright and Related Rights in the Work. Further, Affirmer
  disclaims responsibility for obtaining any necessary consents, permissions
  or other rights required for any use of the Work.

  d. Affirmer understands and acknowledges that Creative Commons is not a
  party to this document and has no duty or obligation with respect to this
  CC0 or use of the Work.

For more information, please see
<http://creativecommons.org/publicdomain/zero/1.0/>"""
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
_RIGHTS_REVIEW_STATUSES: Final = {"not_reviewed", "reviewed", "cleared"}
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

BUNDLE_FILES: Final[tuple[str, ...]] = (
    MAIN_CSV_NAME,
    MARKETS_CSV_NAME,
    "schema.json",
    "README.md",
    "SOURCES.csv",
    "PROVENANCE.json",
    "QUALITY_REPORT.json",
    "LICENSE.txt",
    "NOTICE.md",
    "CHANGELOG.md",
    "CHECKSUMS.sha256",
)


@dataclass(frozen=True)
class PolygonSettlementBundleSpec:
    """Publisher-controlled inputs for one immutable dataset release."""

    dataset_version: str
    publisher_name: str
    attribution_url: str | None = None
    rights_review_status: str = "not_reviewed"
    rpc_provider_terms_url: str | None = None
    rpc_provider_terms_snapshot_sha256: str | None = None
    rpc_provider_terms_snapshot_at_utc: str | None = None

    def __post_init__(self) -> None:
        validate_dataset_version(self.dataset_version)
        _validate_plain_label(self.publisher_name, "publisher_name", maximum=200)
        if self.rights_review_status not in _RIGHTS_REVIEW_STATUSES:
            allowed = ", ".join(sorted(_RIGHTS_REVIEW_STATUSES))
            raise ValueError(f"rights_review_status must be one of: {allowed}")
        if self.attribution_url is not None:
            parsed = urlsplit(self.attribution_url)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.netloc
                or parsed.username
                or parsed.password
            ):
                raise ValueError("attribution_url must be an absolute HTTP(S) URL")
        if self.rpc_provider_terms_url is not None:
            _validate_plain_label(
                self.rpc_provider_terms_url,
                "rpc_provider_terms_url",
                maximum=2_048,
            )
            parsed = urlsplit(self.rpc_provider_terms_url)
            try:
                parsed.port
            except ValueError as exc:
                raise ValueError("rpc_provider_terms_url has an invalid port") from exc
            if (
                parsed.scheme != "https"
                or not parsed.netloc
                or not parsed.hostname
                or parsed.username
                or parsed.password
                or parsed.query
                or parsed.fragment
                or any(char.isspace() for char in self.rpc_provider_terms_url)
            ):
                raise ValueError(
                    "rpc_provider_terms_url must be a sanitized absolute HTTPS URL "
                    "without credentials, query, or fragment"
                )
        snapshot_values = (
            self.rpc_provider_terms_snapshot_sha256,
            self.rpc_provider_terms_snapshot_at_utc,
        )
        if any(snapshot_values) and not all(snapshot_values):
            raise ValueError(
                "RPC provider terms snapshot SHA-256 and timestamp must be supplied "
                "together"
            )
        if any(snapshot_values) and self.rpc_provider_terms_url is None:
            raise ValueError("RPC provider terms snapshot requires a terms URL")
        if self.rpc_provider_terms_snapshot_sha256 is not None and (
            not isinstance(self.rpc_provider_terms_snapshot_sha256, str)
            or not _SHA256_RE.fullmatch(self.rpc_provider_terms_snapshot_sha256)
        ):
            raise ValueError("RPC provider terms snapshot SHA-256 is invalid")
        if self.rpc_provider_terms_snapshot_at_utc is not None:
            try:
                datetime.strptime(
                    self.rpc_provider_terms_snapshot_at_utc,
                    "%Y-%m-%dT%H:%M:%SZ",
                )
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "RPC provider terms snapshot timestamp must be UTC as "
                    "YYYY-MM-DDTHH:MM:SSZ"
                ) from exc


def _rpc_provider_terms_metadata(
    spec: PolygonSettlementBundleSpec,
) -> dict[str, str | None]:
    """Return deterministic evidence state for the primary RPC provider terms."""
    if spec.rpc_provider_terms_url is None:
        status = "unavailable"
    elif spec.rpc_provider_terms_snapshot_sha256 is None:
        status = "not_reviewed"
    else:
        status = "snapshotted"
    return {
        "status": status,
        "terms_url": spec.rpc_provider_terms_url,
        "snapshot_sha256": spec.rpc_provider_terms_snapshot_sha256,
        "snapshot_at_utc": spec.rpc_provider_terms_snapshot_at_utc,
    }


def _rpc_provider_terms_notice(spec: PolygonSettlementBundleSpec) -> str:
    metadata = _rpc_provider_terms_metadata(spec)
    if metadata["status"] == "unavailable":
        return (
            "No sanitized primary RPC provider terms URL or snapshot was supplied; "
            "the provider-terms evidence status is `unavailable`."
        )
    if metadata["status"] == "not_reviewed":
        return (
            f"The sanitized primary RPC provider terms URL is "
            f"{metadata['terms_url']}; no immutable snapshot was supplied, so the "
            "provider-terms evidence status is `not_reviewed`."
        )
    return (
        f"The sanitized primary RPC provider terms URL is {metadata['terms_url']}. "
        f"Its snapshot captured at {metadata['snapshot_at_utc']} has SHA-256 "
        f"{metadata['snapshot_sha256']}; the provider-terms evidence status is "
        "`snapshotted`. This records source evidence, not a legal conclusion."
    )


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


def build_polygon_settlement_release(
    conn: duckdb.DuckDBPyConnection,
    output_root: Path,
    spec: PolygonSettlementBundleSpec,
    *,
    provenance: Mapping[str, Any],
    generator_commit: str,
) -> dict[str, Any]:
    """Validate warehouse inputs and atomically publish an immutable bundle."""
    release_provenance = _effective_release_provenance(provenance)
    _validate_provenance(release_provenance)
    if not _COMMIT_RE.fullmatch(generator_commit):
        raise ValueError("generator_commit must be a lowercase 40-character Git SHA")

    market_rows = _read_market_rows(conn)
    _validate_committed_seed(market_rows, release_provenance)

    release_root = output_root.resolve() / "releases"
    release_dir = release_root / spec.dataset_version
    if release_dir.exists():
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
        _write_bundle_metadata(
            temporary_dir,
            spec=spec,
            provenance=release_provenance,
            generator_commit=generator_commit,
            summary=summary,
            market_rows=market_rows,
            quality_rows=quality_rows,
            issue_rows=issue_rows,
        )
        _write_checksums(temporary_dir)
        _validate_bundle_files(temporary_dir)
        temporary_dir.rename(release_dir)
    except BaseException:
        shutil.rmtree(temporary_dir, ignore_errors=True)
        raise

    return {
        **summary,
        "dataset_version": spec.dataset_version,
        "release_dir": str(release_dir),
        "files": list(BUNDLE_FILES),
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
) -> None:
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
        if not all(_public_side_values_are_valid(row, side) for side in ("yes", "no")):
            failures.append(f"invalid public mart values for grain={grain_key!r}")
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


def _public_side_values_are_valid(row: Mapping[str, Any], side: str) -> bool:
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


def _write_bundle_metadata(
    directory: Path,
    *,
    spec: PolygonSettlementBundleSpec,
    provenance: Mapping[str, Any],
    generator_commit: str,
    summary: Mapping[str, Any],
    market_rows: Sequence[Mapping[str, Any]],
    quality_rows: Sequence[Mapping[str, Any]],
    issue_rows: Sequence[Mapping[str, Any]],
) -> None:
    data_hashes = {
        name: _sha256(directory / name) for name in (MAIN_CSV_NAME, MARKETS_CSV_NAME)
    }
    public_provenance = {
        key: provenance[key] for key in _PROVENANCE_KEYS if key != "block_ranges"
    }
    for key in (
        "verification_rpc_provider_label",
        "verification_rpc_provider_origin",
    ):
        if provenance.get(key) is not None:
            public_provenance[key] = provenance[key]
    public_provenance["block_ranges"] = [
        {key: block_range[key] for key in _BLOCK_RANGE_KEYS}
        for block_range in provenance["block_ranges"]
    ]
    generated_provenance = {
        **_jsonable(public_provenance),
        "dataset_title": DATASET_TITLE,
        "dataset_version": spec.dataset_version,
        "publisher_name": spec.publisher_name,
        "attribution_url": spec.attribution_url,
        "rights_review_status": spec.rights_review_status,
        "rpc_provider_terms": _rpc_provider_terms_metadata(spec),
        "generator_commit": generator_commit,
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
            "rights_review_status": spec.rights_review_status,
            "rights_review_is_advisory": True,
        },
    )
    _write_json(directory / "schema.json", _schema_document())
    _write_sources(directory / "SOURCES.csv", market_rows, provenance, spec)
    _write_text(directory / "README.md", _readme(spec, summary))
    _write_text(directory / "LICENSE.txt", _license_text(spec))
    _write_text(directory / "NOTICE.md", _notice(spec, provenance))
    _write_text(directory / "CHANGELOG.md", _changelog(spec))


def _write_sources(
    path: Path,
    market_rows: Sequence[Mapping[str, Any]],
    provenance: Mapping[str, Any],
    spec: PolygonSettlementBundleSpec,
) -> None:
    revisions = sorted({str(row["openfootball_revision"]) for row in market_rows})
    fixture_paths = sorted({str(row["openfootball_path"]) for row in market_rows})
    provider_terms = _rpc_provider_terms_metadata(spec)
    if provider_terms["status"] == "snapshotted":
        provider_terms_revision = (
            f"snapshot at {provider_terms['snapshot_at_utc']}; "
            f"sha256={provider_terms['snapshot_sha256']}"
        )
    elif provider_terms["status"] == "not_reviewed":
        provider_terms_revision = "terms URL supplied; no immutable snapshot"
    else:
        provider_terms_revision = "terms URL and snapshot unavailable"
    rows = [
        {
            "source_name": "Polygon PoS blockchain",
            "role": "finalized settlement events and block timestamps",
            "uri": "https://polygon.technology/",
            "revision": f"finalized block {provenance['finalized_head_block_number']}",
            "license_or_terms": "public blockchain facts; provider terms may apply",
            "notes": "Public finalized block and settlement facts.",
        },
        {
            "source_name": f"RPC provider: {provenance['rpc_provider_label']}",
            "role": "transport used to acquire finalized Polygon JSON-RPC data",
            "uri": provider_terms["terms_url"]
            or str(provenance["rpc_provider_origin"]),
            "revision": provider_terms_revision,
            "license_or_terms": (
                f"provider terms evidence status: {provider_terms['status']}"
            ),
            "notes": (
                "The bundle does not redistribute provider responses; "
                f"origin={provenance['rpc_provider_origin']}; "
                f"snapshot_sha256={provider_terms['snapshot_sha256'] or 'unavailable'}; "
                f"snapshot_at_utc={provider_terms['snapshot_at_utc'] or 'unavailable'}."
            ),
        },
        {
            "source_name": "FIFA World Cup 26 Match Schedule",
            "role": "official numeric match identifiers only",
            "uri": FIFA_SCHEDULE_URI,
            "revision": f"{FIFA_SCHEDULE_REVISION}; sha256={FIFA_SCHEDULE_SHA256}",
            "license_or_terms": "official source; schedule facts only",
            "notes": "The PDF and its expressive layout are not redistributed.",
        },
        {
            "source_name": "OpenFootball World Cup",
            "role": "fixture identity and scheduled kickoff",
            "uri": "https://github.com/openfootball/worldcup",
            "revision": ",".join(revisions),
            "license_or_terms": (
                "CC0 1.0 Universal public-domain dedication; "
                "https://creativecommons.org/publicdomain/zero/1.0/"
            ),
            "notes": (
                f"Pinned license: {OPENFOOTBALL_LICENSE_URI}; "
                f"license sha256={OPENFOOTBALL_LICENSE_SHA256}; fixtures: "
                + ", ".join(fixture_paths)
            ),
        },
        {
            "source_name": "Gnosis ConditionalTokens",
            "role": "minimal on-chain semantic interface reference",
            "uri": "https://github.com/gnosis/conditional-tokens-contracts/tree/eeefca66eb46c800a9aaab88db2064a99026fde5",
            "revision": "eeefca66eb46c800a9aaab88db2064a99026fde5",
            "license_or_terms": "LGPL-3.0",
            "notes": "No upstream source code is redistributed in this bundle.",
        },
        {
            "source_name": "UMA CTF Adapter",
            "role": "minimal on-chain semantic interface reference",
            "uri": "https://github.com/Polymarket/uma-ctf-adapter/tree/8b76cc9e0d46c6f7450a0adb0ddc0f5b0568c9cc",
            "revision": "8b76cc9e0d46c6f7450a0adb0ddc0f5b0568c9cc",
            "license_or_terms": "repository license/terms",
            "notes": "No upstream source code is redistributed in this bundle.",
        },
        {
            "source_name": "NegRisk CTF Adapter",
            "role": "minimal on-chain semantic interface reference",
            "uri": "https://github.com/Polymarket/neg-risk-ctf-adapter/tree/f78b35b0863b4308a431ca307d06f49b2ea65e78",
            "revision": "f78b35b0863b4308a431ca307d06f49b2ea65e78",
            "license_or_terms": "repository license/terms",
            "notes": "No upstream source code is redistributed in this bundle.",
        },
        {
            "source_name": "Polymarket CTF Exchange V2",
            "role": "minimal settlement-event interface reference",
            "uri": "https://github.com/Polymarket/ctf-exchange-v2/tree/ccc0596074f4dfd62c944fbca4de252893b82b4b",
            "revision": "ccc0596074f4dfd62c944fbca4de252893b82b4b",
            "license_or_terms": "BUSL-1.1",
            "notes": "No upstream source code is redistributed in this bundle.",
        },
    ]
    _write_csv(
        path,
        ("source_name", "role", "uri", "revision", "license_or_terms", "notes"),
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


def _readme(spec: PolygonSettlementBundleSpec, summary: Mapping[str, Any]) -> str:
    attribution = (
        f" ({spec.attribution_url})" if spec.attribution_url is not None else ""
    )
    return f"""# {DATASET_TITLE}

Version `{spec.dataset_version}`, published by {spec.publisher_name}{attribution}.

This bundle contains {summary["rows"]:,} dense proposition-minute rows for
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
  are excluded from published counts and volumes.
- Empty minutes remain empty; no forward fill, interpolation, pair normalization,
  or inferred complement is applied.
- The data are de-identified, not anonymous. Sparse aggregates can be linked back
  to public-chain activity.

The primary CSV omits transaction, log, and block locators. The market sidecar
intentionally retains condition/token identity, exchange addresses, semantic
initialization transaction/log locators, and token-verification block locators
needed to audit the independently authored mapping. `PROVENANCE.json`,
`QUALITY_REPORT.json`, and `CHECKSUMS.sha256` describe and authenticate this
exact release.

Rights review status `{spec.rights_review_status}` is advisory and is not a
technical publication gate. See `LICENSE.txt` and `NOTICE.md`.
"""


def _license_text(spec: PolygonSettlementBundleSpec) -> str:
    return f"""{DATASET_TITLE}
Copyright (c) {spec.publisher_name}

To the extent copyright or database rights apply, the publisher's original
selection, arrangement, schema, annotations, transformations, and documentation
are licensed under the Creative Commons Attribution 4.0 International License
(CC BY 4.0): https://creativecommons.org/licenses/by/4.0/legalcode

Attribution: "{DATASET_TITLE}, version {spec.dataset_version}, {spec.publisher_name}."

This license does not assert ownership of underlying public blockchain facts,
OpenFootball CC0 material, third-party contract interfaces, names, trademarks,
or other third-party material. Those items remain subject to their own rights,
notices, or terms as described in NOTICE.md and SOURCES.csv.
"""


def _notice(spec: PolygonSettlementBundleSpec, provenance: Mapping[str, Any]) -> str:
    return f"""# Notices and limitations

This is an independent analytics dataset published by {spec.publisher_name}. It
is not affiliated with, endorsed by, or sponsored by Polymarket, Polygon Labs,
FIFA, or the RPC provider `{provenance["rpc_provider_label"]}`. Their names and
marks belong to their respective owners.

OpenFootball dedicates its fixture material to the public domain under CC0 1.0
Universal; the pinned notice and waiver are reproduced below. The FIFA
schedule was used only to review numeric match identifiers, and the PDF and its
expressive layout are not redistributed. The on-chain interfaces listed in
`SOURCES.csv` were used as technical references; their upstream code is not
redistributed. The named RPC provider's terms may apply to acquisition. Its
sanitized origin, publisher-supplied label, and the evidence state below are
retained; the credential-bearing endpoint is not.

The collection path does not call Polymarket Gamma or CLOB APIs. Polymarket's
Terms of Use supplied to the publisher (effective July 17, 2026) nevertheless
define covered Data to include on-chain information in raw, derived, aggregated,
or anonymized form. They restrict access by defined Capital Market Clients and
market-data distributors, and restrict commercial redistribution to those
groups, absent written agreement. The same terms state that the company does
not own or control the deployed contracts, Polygon network, or activity and data
on that network. The scope and enforceability of those provisions for this
independently collected dataset remain legal questions for the publisher.

{_rpc_provider_terms_notice(spec)}

The publisher must also assess applicable law, names, and marks independently.
The recorded rights-review status
`{spec.rights_review_status}` is informational and does not constitute legal
advice or a legal clearance.

Underlying Polygon records are public facts. CC BY 4.0 applies only to the
publisher's protectable contributions identified in LICENSE.txt.

## Pinned OpenFootball CC0 notice

The text below is the exact `LICENSE.md` at OpenFootball World Cup revision
`{OPENFOOTBALL_REVISION}` (SHA-256 `{OPENFOOTBALL_LICENSE_SHA256}`):

{OPENFOOTBALL_CC0_NOTICE}
"""


def _changelog(spec: PolygonSettlementBundleSpec) -> str:
    return f"""# Dataset changelog

## {spec.dataset_version}

- Immutable release generated from the finalized scan identified in
  `PROVENANCE.json`.
- Seed corrections and material methodology changes require a new semantic
  version; published version directories are never overwritten.
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
        for name in sorted(set(BUNDLE_FILES) - {"CHECKSUMS.sha256"})
    ]
    _write_text(directory / "CHECKSUMS.sha256", "\n".join(lines))


def _validate_bundle_files(directory: Path) -> None:
    names = {path.name for path in directory.iterdir() if path.is_file()}
    expected = set(BUNDLE_FILES)
    if names != expected:
        raise RuntimeError(
            f"release files differ: missing={sorted(expected - names)}, "
            f"unexpected={sorted(names - expected)}"
        )
    if (directory / "dataset-metadata.json").exists():  # pragma: no cover
        raise RuntimeError("dataset-metadata.json must not be published")


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
    "BUNDLE_FILES",
    "DEFAULT_POLYGON_SETTLEMENT_RELEASE_ROOT",
    "MAIN_COLUMNS",
    "MARKET_COLUMNS",
    "PolygonSettlementBundleSpec",
    "build_polygon_settlement_release",
    "current_generator_commit",
    "validate_dataset_version",
]
