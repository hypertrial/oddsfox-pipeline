"""Immutable event-grain release builder for pre-match soccer Elo."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from oddsfox_pipeline.contracts.raw_snapshots import schema_fingerprint
from oddsfox_pipeline.features.pre_match_elo.benchmarks import (
    BenchmarkIndex,
    BenchmarkRating,
    reconstruct_eloratings,
)
from oddsfox_pipeline.features.pre_match_elo.elo import (
    RATING_POOLS,
    HistoricalMatch,
    PreMatchRating,
    compute_pre_match_ratings,
    tune_parameters,
)
from oddsfox_pipeline.features.pre_match_elo.identity import (
    IdentityRegistry,
    Resolution,
    SourceConflict,
    canonicalize_and_deduplicate,
    rows_from_mappings,
)
from oddsfox_pipeline.features.pre_match_elo.sources import (
    ParseIssue,
    RawResult,
    SourceSnapshot,
    parse_snapshot,
    snapshot_manifest_rows,
)
from oddsfox_pipeline.publishing._bundle_io import (
    sha256_file,
    write_json,
)

CONTRACT_VERSION: Final = "oddsfox.soccer.pre-match-elo.v1"
DATASET_VERSION: Final = "1.0.0"
MODEL_VERSION: Final = "oddsfox.soccer.elo.v1"
TARGET_SNAPSHOT_SHA256: Final = (
    "7b3b3c375254bc33b2746147c0b447783188153504cb1b18d5b813492e0ebaf9"
)
TARGET_EVENT_COUNT: Final = 8_255
COVERAGE_STATUSES: Final = frozenset(
    {
        "rated_stable",
        "rated_provisional",
        "partial_team_history",
        "missing_team_mapping",
        "missing_result_history",
        "ambiguous_target_match",
        "source_conflict",
        "unsupported_pool",
    }
)
RELEASE_FILES: Final = frozenset(
    {
        "event_pre_match_elo.parquet",
        "team_identity_map.parquet",
        "coverage_by_competition.parquet",
        "manifest.json",
        "checksums.sha256",
    }
)
_REVISION: Final = re.compile(r"^[0-9a-f]{40}$")
_SHA256: Final = re.compile(r"^[0-9a-f]{64}$")

EVENT_SCHEMA: Final = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("event_slug", pa.string(), nullable=False),
        pa.field("kickoff_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("competition_slugs_json", pa.string(), nullable=False),
        pa.field("home_source_name", pa.string(), nullable=False),
        pa.field("away_source_name", pa.string(), nullable=False),
        pa.field("home_team_id", pa.string()),
        pa.field("away_team_id", pa.string()),
        pa.field("rating_pool", pa.string()),
        pa.field("model_version", pa.string(), nullable=False),
        pa.field("home_pre_match_elo", pa.float64()),
        pa.field("away_pre_match_elo", pa.float64()),
        pa.field("pre_match_elo_difference", pa.float64()),
        pa.field("home_quality", pa.string(), nullable=False),
        pa.field("away_quality", pa.string(), nullable=False),
        pa.field("home_prior_match_count", pa.int64(), nullable=False),
        pa.field("away_prior_match_count", pa.int64(), nullable=False),
        pa.field("home_last_result_date", pa.date32()),
        pa.field("away_last_result_date", pa.date32()),
        pa.field("home_rating_age_days", pa.int32()),
        pa.field("away_rating_age_days", pa.int32()),
        pa.field("home_connected_component_id", pa.string()),
        pa.field("away_connected_component_id", pa.string()),
        pa.field("home_mapping_status", pa.string(), nullable=False),
        pa.field("away_mapping_status", pa.string(), nullable=False),
        pa.field("target_match_status", pa.string(), nullable=False),
        pa.field("coverage_status", pa.string(), nullable=False),
        pa.field("coverage_reason", pa.string(), nullable=False),
        pa.field("home_benchmark_rating", pa.float64()),
        pa.field("away_benchmark_rating", pa.float64()),
        pa.field("home_benchmark_system", pa.string()),
        pa.field("away_benchmark_system", pa.string()),
        pa.field("home_benchmark_as_of_date", pa.date32()),
        pa.field("away_benchmark_as_of_date", pa.date32()),
        pa.field("home_benchmark_snapshot_id", pa.string()),
        pa.field("away_benchmark_snapshot_id", pa.string()),
        pa.field("home_benchmark_mapping_method", pa.string()),
        pa.field("away_benchmark_mapping_method", pa.string()),
    ]
)

IDENTITY_SCHEMA: Final = pa.schema(
    [
        pa.field("source_system", pa.string(), nullable=False),
        pa.field("source_name", pa.string(), nullable=False),
        pa.field("team_id", pa.string()),
        pa.field("canonical_display_name", pa.string()),
        pa.field("rating_pool", pa.string()),
        pa.field("country", pa.string()),
        pa.field("confederation", pa.string()),
        pa.field("mapping_status", pa.string(), nullable=False),
        pa.field("candidate_team_ids_json", pa.string(), nullable=False),
    ]
)

COVERAGE_SCHEMA: Final = pa.schema(
    [
        pa.field("dimension", pa.string(), nullable=False),
        pa.field("dimension_value", pa.string(), nullable=False),
        pa.field("coverage_status", pa.string(), nullable=False),
        pa.field("event_count", pa.int64(), nullable=False),
        pa.field("rated_event_count", pa.int64(), nullable=False),
        pa.field("benchmark_event_count", pa.int64(), nullable=False),
    ]
)


class EloReleaseError(ValueError):
    """Raised when a build would publish an incomplete or invalid release."""


@dataclass(frozen=True, slots=True)
class TargetEvent:
    event_id: str
    event_slug: str
    kickoff_at_utc: datetime
    competition_slugs_json: str
    home_source_name: str
    away_source_name: str

    @property
    def match_date(self) -> date:
        return self.kickoff_at_utc.date()


def _read_records(path: Path) -> list[dict[str, object]]:
    if path.suffix.casefold() == ".parquet":
        return pl.read_parquet(path).to_dicts()
    if path.suffix.casefold() == ".csv":
        return pl.read_csv(path, infer_schema_length=10_000).to_dicts()
    if path.suffix.casefold() in {".yml", ".yaml"}:
        try:
            value = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, yaml.YAMLError) as exc:
            raise EloReleaseError(f"invalid YAML mapping file: {path}") from exc
        rows = value.get("mappings") if isinstance(value, dict) else None
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise EloReleaseError("YAML mapping file must contain a mappings list")
        return rows
    raise EloReleaseError(f"expected CSV, YAML, or Parquet: {path}")


def load_target_events(path: Path) -> tuple[TargetEvent, ...]:
    columns = [
        "event_id",
        "event_slug",
        "match_started_at_utc",
        "series_slugs_json",
        "home_team",
        "away_team",
    ]
    try:
        unique = (
            pl.scan_parquet(path)
            .select(columns)
            .unique(maintain_order=False)
            .collect()
            .sort("event_id")
        )
    except (OSError, pl.exceptions.PolarsError) as exc:
        raise EloReleaseError(f"invalid target Parquet: {path}") from exc
    if unique.height != unique["event_id"].n_unique():
        raise EloReleaseError("target event fields vary within event_id")
    events: list[TargetEvent] = []
    for row in unique.to_dicts():
        kickoff = row["match_started_at_utc"]
        if not isinstance(kickoff, datetime):
            raise EloReleaseError("target kickoff must be a timestamp")
        if kickoff.tzinfo is None:
            kickoff = kickoff.replace(tzinfo=timezone.utc)
        else:
            kickoff = kickoff.astimezone(timezone.utc)
        values = [row[column] for column in columns if column != "match_started_at_utc"]
        if any(value is None or str(value) == "" for value in values):
            raise EloReleaseError("target event identity fields must be populated")
        events.append(
            TargetEvent(
                event_id=str(row["event_id"]),
                event_slug=str(row["event_slug"]),
                kickoff_at_utc=kickoff,
                competition_slugs_json=str(row["series_slugs_json"]),
                home_source_name=str(row["home_team"]),
                away_source_name=str(row["away_team"]),
            )
        )
    return tuple(events)


def normalize_sources(
    snapshots: Sequence[SourceSnapshot], raw_root: Path
) -> tuple[tuple[RawResult, ...], list[dict[str, object]], tuple[ParseIssue, ...]]:
    parsed = {}
    issues: list[ParseIssue] = []
    rows: list[RawResult] = []
    for snapshot in sorted(snapshots, key=lambda row: row.snapshot_id):
        path = raw_root / snapshot.snapshot_id / snapshot.filename
        try:
            payload = path.read_bytes()
        except OSError as exc:
            raise EloReleaseError(f"missing source snapshot: {path}") from exc
        if hashlib.sha256(payload).hexdigest() != snapshot.sha256:
            raise EloReleaseError(f"source checksum mismatch: {snapshot.snapshot_id}")
        result = parse_snapshot(payload, snapshot)
        parsed[snapshot.snapshot_id] = result
        rows.extend(row for row in result.rows if row.match_date >= date(2018, 1, 1))
        issues.extend(
            ParseIssue(
                f"{snapshot.snapshot_id}:{issue.source_locator}",
                issue.reason,
                issue.text,
            )
            for issue in result.issues
        )
    return tuple(rows), snapshot_manifest_rows(snapshots, parsed), tuple(issues)


def load_benchmarks(path: Path | None) -> tuple[BenchmarkRating, ...]:
    if path is None:
        return ()
    records = _read_records(path)
    eloratings_fields = {
        "match_date",
        "home_team_id",
        "away_team_id",
        "home_post_rating",
        "away_post_rating",
        "home_rating_change",
        "snapshot_id",
    }
    if records and eloratings_fields <= set(records[0]):
        return reconstruct_eloratings(records)
    output: list[BenchmarkRating] = []
    for row in records:
        as_of = row["as_of_date"]
        if not isinstance(as_of, date):
            as_of = date.fromisoformat(str(as_of)[:10])
        output.append(
            BenchmarkRating(
                system=str(row["system"]),
                team_id=str(row["team_id"]),
                rating=float(row["rating"]),
                as_of_date=as_of,
                snapshot_id=str(row["snapshot_id"]),
                mapping_method=str(row["mapping_method"]),
                is_pre_match=bool(row.get("is_pre_match", False)),
            )
        )
    return tuple(output)


def _missing_rating(
    team_id: str, pool: str | None, target_date: date
) -> PreMatchRating:
    return PreMatchRating(
        team_id=team_id,
        rating_pool=pool or "unsupported",
        target_date=target_date,
        rating=None,
        quality="missing",
        prior_match_count=0,
        last_result_date=None,
        rating_age_days=None,
        connected_component_id=None,
    )


def _pair_key(
    rating_pool: str, match_date: date, left: str, right: str
) -> tuple[str, date, str, str]:
    first, second = sorted((left, right))
    return rating_pool, match_date, first, second


def _target_match_status(
    rating_pool: str,
    event: TargetEvent,
    home_team_id: str,
    away_team_id: str,
    result_index: Mapping[tuple[str, date, str, str], int],
    conflict_index: set[tuple[str, date, str, str]],
) -> str:
    exact_key = _pair_key(rating_pool, event.match_date, home_team_id, away_team_id)
    if exact_key in conflict_index:
        return "source_conflict"
    exact_count = result_index.get(exact_key, 0)
    if exact_count > 1:
        return "ambiguous"
    if exact_count == 1:
        return "exact"
    for offset in (-1, 1):
        review_date = date.fromordinal(event.match_date.toordinal() + offset)
        if result_index.get(
            _pair_key(rating_pool, review_date, home_team_id, away_team_id), 0
        ):
            return "date_review_candidate"
    return "not_found"


def _coverage(
    home: Resolution,
    away: Resolution,
    home_rating: PreMatchRating,
    away_rating: PreMatchRating,
    target_match_status: str,
) -> tuple[str, str]:
    if home.status == "ambiguous" or away.status == "ambiguous":
        return "missing_team_mapping", "one or both target names map to multiple pools"
    if home.team_id is None or away.team_id is None:
        return "missing_team_mapping", "one or both target team names are not reviewed"
    if home.rating_pool not in RATING_POOLS or away.rating_pool not in RATING_POOLS:
        return (
            "unsupported_pool",
            "target teams do not belong to a supported rating pool",
        )
    if home.rating_pool != away.rating_pool:
        return "unsupported_pool", "target teams resolve to different rating pools"
    if target_match_status == "source_conflict":
        return "source_conflict", "historical sources disagree on the target score"
    if target_match_status == "ambiguous":
        return (
            "ambiguous_target_match",
            "multiple historical matches fit the target pair and date",
        )
    if home_rating.rating is None and away_rating.rating is None:
        return (
            "missing_result_history",
            "neither mapped team has a prior completed result",
        )
    if home_rating.rating is None or away_rating.rating is None:
        return (
            "partial_team_history",
            "only one mapped team has prior completed results",
        )
    if home_rating.quality == "stable" and away_rating.quality == "stable":
        return "rated_stable", "both teams have stable pre-match ratings"
    return "rated_provisional", "both ratings exist but at least one is provisional"


def _benchmark_values(
    index: BenchmarkIndex,
    rating_pool: str | None,
    team_id: str | None,
    match_date: date,
) -> BenchmarkRating | None:
    if not rating_pool or not team_id:
        return None
    system = "ClubElo" if rating_pool.startswith("club_") else "EloRatings"
    return index.latest_before(system, team_id, match_date)


def _event_rows(
    events: Sequence[TargetEvent],
    registry: IdentityRegistry,
    ratings: Mapping[tuple[str, date, str], PreMatchRating],
    canonical_results: Sequence[object],
    conflicts: Sequence[SourceConflict],
    benchmarks: Sequence[BenchmarkRating],
) -> tuple[list[dict[str, object]], list[Resolution]]:
    result_index: dict[tuple[str, date, str, str], int] = defaultdict(int)
    for result in canonical_results:
        result_index[
            _pair_key(
                result.rating_pool,
                result.match_date,
                result.home_team_id,
                result.away_team_id,
            )
        ] += 1
    conflict_index = {
        _pair_key(
            conflict.rating_pool,
            conflict.match_date,
            conflict.home_team_id,
            conflict.away_team_id,
        )
        for conflict in conflicts
    }
    benchmark_index = BenchmarkIndex(benchmarks)
    output: list[dict[str, object]] = []
    resolutions: list[Resolution] = []
    for event in events:
        home, away = registry.resolve_pair(
            "polymarket", event.home_source_name, event.away_source_name
        )
        resolutions.extend((home, away))
        rating_pool = (
            home.rating_pool
            if home.rating_pool and home.rating_pool == away.rating_pool
            else None
        )
        home_id = home.team_id
        away_id = away.team_id
        home_rating = (
            ratings.get((rating_pool, event.match_date, home_id))
            if rating_pool and home_id
            else None
        ) or _missing_rating(home_id or "unmapped", rating_pool, event.match_date)
        away_rating = (
            ratings.get((rating_pool, event.match_date, away_id))
            if rating_pool and away_id
            else None
        ) or _missing_rating(away_id or "unmapped", rating_pool, event.match_date)
        match_status = (
            _target_match_status(
                rating_pool,
                event,
                home_id,
                away_id,
                result_index,
                conflict_index,
            )
            if rating_pool and home_id and away_id
            else "unmapped"
        )
        coverage_status, reason = _coverage(
            home, away, home_rating, away_rating, match_status
        )
        home_benchmark = _benchmark_values(
            benchmark_index, rating_pool, home_id, event.match_date
        )
        away_benchmark = _benchmark_values(
            benchmark_index, rating_pool, away_id, event.match_date
        )
        difference = (
            home_rating.rating - away_rating.rating
            if rating_pool
            and home_rating.rating is not None
            and away_rating.rating is not None
            else None
        )
        row: dict[str, object] = {
            "event_id": event.event_id,
            "event_slug": event.event_slug,
            "kickoff_at_utc": event.kickoff_at_utc,
            "competition_slugs_json": event.competition_slugs_json,
            "home_source_name": event.home_source_name,
            "away_source_name": event.away_source_name,
            "home_team_id": home_id,
            "away_team_id": away_id,
            "rating_pool": rating_pool,
            "model_version": MODEL_VERSION,
            "home_pre_match_elo": home_rating.rating,
            "away_pre_match_elo": away_rating.rating,
            "pre_match_elo_difference": difference,
            "home_quality": home_rating.quality,
            "away_quality": away_rating.quality,
            "home_prior_match_count": home_rating.prior_match_count,
            "away_prior_match_count": away_rating.prior_match_count,
            "home_last_result_date": home_rating.last_result_date,
            "away_last_result_date": away_rating.last_result_date,
            "home_rating_age_days": home_rating.rating_age_days,
            "away_rating_age_days": away_rating.rating_age_days,
            "home_connected_component_id": home_rating.connected_component_id,
            "away_connected_component_id": away_rating.connected_component_id,
            "home_mapping_status": home.status,
            "away_mapping_status": away.status,
            "home_country": home.country,
            "away_country": away.country,
            "home_confederation": home.confederation,
            "away_confederation": away.confederation,
            "target_match_status": match_status,
            "coverage_status": coverage_status,
            "coverage_reason": reason,
        }
        for side, benchmark in (
            ("home", home_benchmark),
            ("away", away_benchmark),
        ):
            row[f"{side}_benchmark_rating"] = benchmark.rating if benchmark else None
            row[f"{side}_benchmark_system"] = benchmark.system if benchmark else None
            row[f"{side}_benchmark_as_of_date"] = (
                benchmark.as_of_date if benchmark else None
            )
            row[f"{side}_benchmark_snapshot_id"] = (
                benchmark.snapshot_id if benchmark else None
            )
            row[f"{side}_benchmark_mapping_method"] = (
                benchmark.mapping_method if benchmark else None
            )
        output.append(row)
    return output, resolutions


def _identity_rows(
    registry: IdentityRegistry, unresolved: Iterable[Resolution]
) -> list[dict[str, object]]:
    output = [
        {
            **asdict(row),
            "candidate_team_ids_json": "[]",
        }
        for row in registry.rows
    ]
    known = {
        (row["source_system"], row["source_name"], row["rating_pool"]) for row in output
    }
    for row in unresolved:
        key = (row.source_system, row.source_name, row.rating_pool)
        if key in known:
            continue
        known.add(key)
        output.append(
            {
                "source_system": row.source_system,
                "source_name": row.source_name,
                "team_id": row.team_id,
                "canonical_display_name": row.canonical_display_name,
                "rating_pool": row.rating_pool,
                "country": row.country,
                "confederation": row.confederation,
                "mapping_status": row.status,
                "candidate_team_ids_json": json.dumps(row.candidate_team_ids),
            }
        )
    return sorted(
        output,
        key=lambda row: (
            str(row["source_system"]),
            str(row["source_name"]),
            str(row["rating_pool"]),
        ),
    )


def _dimension_values(row: Mapping[str, object]) -> Iterable[tuple[str, str]]:
    yield "month", str(row["kickoff_at_utc"])[:7]
    rating_pool = row.get("rating_pool")
    yield "gender", str(rating_pool).rsplit("_", 1)[-1] if rating_pool else "unknown"
    yield "rating_pool", str(rating_pool or "unknown")
    yield "mapping_method", f"{row['home_mapping_status']}+{row['away_mapping_status']}"
    yield "quality", f"{row['home_quality']}+{row['away_quality']}"
    yield "missing_reason", str(row["coverage_reason"])
    for country in sorted(
        {
            str(value)
            for value in (row.get("home_country"), row.get("away_country"))
            if value
        }
    ):
        yield "country", country
    for confederation in sorted(
        {
            str(value)
            for value in (
                row.get("home_confederation"),
                row.get("away_confederation"),
            )
            if value
        }
    ):
        yield "confederation", confederation
    try:
        slugs = json.loads(str(row["competition_slugs_json"]))
    except json.JSONDecodeError:
        slugs = []
    for slug in slugs or ["untagged"]:
        yield "competition", str(slug)


def _coverage_rows(
    event_rows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    aggregate: dict[tuple[str, str, str], list[int]] = defaultdict(lambda: [0, 0, 0])
    for row in event_rows:
        rated = row["pre_match_elo_difference"] is not None
        benchmark = (
            row["home_benchmark_rating"] is not None
            and row["away_benchmark_rating"] is not None
        )
        for dimension, value in _dimension_values(row):
            counts = aggregate[(dimension, value, str(row["coverage_status"]))]
            counts[0] += 1
            counts[1] += int(rated)
            counts[2] += int(benchmark)
    return [
        {
            "dimension": key[0],
            "dimension_value": key[1],
            "coverage_status": key[2],
            "event_count": values[0],
            "rated_event_count": values[1],
            "benchmark_event_count": values[2],
        }
        for key, values in sorted(aggregate.items())
    ]


def _write_parquet(
    path: Path, rows: Sequence[Mapping[str, object]], schema: pa.Schema
) -> None:
    table = pa.Table.from_pylist(list(rows), schema=schema)
    pq.write_table(
        table,
        path,
        compression="zstd",
        use_dictionary=False,
        write_statistics=True,
        data_page_version="1.0",
    )


def _write_checksums(directory: Path) -> None:
    lines = [
        f"{sha256_file(directory / name)}  {name}"
        for name in sorted(RELEASE_FILES - {"checksums.sha256"})
    ]
    (directory / "checksums.sha256").write_text(
        "\n".join(lines) + "\n", encoding="utf-8", newline="\n"
    )


def _validate_event_rows(
    rows: Sequence[Mapping[str, object]], expected_event_count: int
) -> None:
    if len(rows) != expected_event_count:
        raise EloReleaseError(
            f"event accounting mismatch: expected {expected_event_count}, got {len(rows)}"
        )
    event_ids = [str(row["event_id"]) for row in rows]
    if len(event_ids) != len(set(event_ids)):
        raise EloReleaseError("event_id must be unique")
    for row in rows:
        if (
            row["coverage_status"] not in COVERAGE_STATUSES
            or not row["coverage_reason"]
        ):
            raise EloReleaseError(
                "every event requires a recognized coverage status and reason"
            )
        if row["pre_match_elo_difference"] is not None:
            if (
                row["home_pre_match_elo"] is None
                or row["away_pre_match_elo"] is None
                or row["rating_pool"] not in RATING_POOLS
            ):
                raise EloReleaseError("Elo differences require two same-pool ratings")


def build_release(
    *,
    target_parquet: Path,
    snapshots: Sequence[SourceSnapshot],
    raw_root: Path,
    identity_map: Path,
    identity_review_report: Path,
    source_catalog_sha256: str,
    output_directory: Path,
    build_revision: str,
    benchmark_path: Path | None = None,
    dataset_version: str = DATASET_VERSION,
    expected_target_sha256: str = TARGET_SNAPSHOT_SHA256,
    expected_event_count: int = TARGET_EVENT_COUNT,
) -> Path:
    """Build and atomically publish one immutable event-grain release."""
    if not _REVISION.fullmatch(build_revision):
        raise EloReleaseError("build_revision must be a full Git SHA")
    if output_directory.exists():
        raise EloReleaseError(f"immutable release already exists: {output_directory}")
    target_sha = sha256_file(target_parquet)
    if target_sha != expected_target_sha256:
        raise EloReleaseError(
            f"target snapshot SHA mismatch: expected {expected_target_sha256}, got {target_sha}"
        )
    events = load_target_events(target_parquet)
    if len(events) != expected_event_count:
        raise EloReleaseError(
            f"target event count mismatch: expected {expected_event_count}, got {len(events)}"
        )
    try:
        identity_review = json.loads(identity_review_report.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise EloReleaseError("invalid identity review report") from exc
    required_review_fields = {
        "authoring_version",
        "target_snapshot_sha256",
        "source_catalog_sha256",
        "review_ledger_sha256",
        "identity_map_sha256",
        "reviewer_labels",
        "reviewed_at_utc",
        "decision_counts",
        "target_label_count",
        "unresolved_target_labels",
        "source_identity_count",
        "compiled_identity_rows",
    }
    identity_records = _read_records(identity_map)
    target_label_count = len(
        {
            name
            for event in events
            for name in (event.home_source_name, event.away_source_name)
        }
    )
    decision_counts = identity_review.get("decision_counts")
    if (
        set(identity_review) != required_review_fields
        or identity_review["target_snapshot_sha256"] != target_sha
        or identity_review["source_catalog_sha256"] != source_catalog_sha256
        or identity_review["identity_map_sha256"] != sha256_file(identity_map)
        or identity_review["authoring_version"] != "oddsfox.soccer.identity-review.v1"
        or not _SHA256.fullmatch(str(identity_review["review_ledger_sha256"]))
        or not identity_review["reviewer_labels"]
        or not identity_review["reviewed_at_utc"]
        or identity_review["target_label_count"] != target_label_count
        or identity_review["compiled_identity_rows"] != len(identity_records)
        or not isinstance(decision_counts, dict)
        or any(
            type(value) is not int or value < 0 for value in decision_counts.values()
        )
        or sum(decision_counts.values()) != target_label_count
        or identity_review["unresolved_target_labels"]
        != decision_counts.get("ambiguous", 0)
    ):
        raise EloReleaseError("identity review provenance is incomplete")
    source_rows, source_manifest, parse_issues = normalize_sources(snapshots, raw_root)
    if parse_issues:
        details = "\n".join(
            f"{issue.source_locator}: {issue.reason}: {issue.text}"
            for issue in parse_issues
        )
        raise EloReleaseError(f"unparsed scored-match lines:\n{details}")

    registry = IdentityRegistry(rows_from_mappings(identity_records))
    canonical, conflicts, unresolved_results = canonicalize_and_deduplicate(
        source_rows, registry
    )
    historical = tuple(
        HistoricalMatch(
            match_id=row.source_match_id,
            match_date=row.match_date,
            home_team_id=row.home_team_id,
            away_team_id=row.away_team_id,
            home_score=row.home_score,
            away_score=row.away_score,
            rating_pool=row.rating_pool,
            neutral=row.neutral,
            friendly=row.friendly,
        )
        for row in canonical
    )
    parameters = tune_parameters(historical)
    target_resolutions = [
        registry.resolve_pair(
            "polymarket", event.home_source_name, event.away_source_name
        )
        for event in events
    ]
    targets = {
        (resolution.rating_pool, event.match_date, resolution.team_id)
        for event, pair in zip(events, target_resolutions, strict=True)
        for resolution in pair
        if resolution.rating_pool in RATING_POOLS and resolution.team_id
    }
    ratings = compute_pre_match_ratings(historical, parameters, targets)
    event_rows, event_resolutions = _event_rows(
        events,
        registry,
        ratings,
        canonical,
        conflicts,
        load_benchmarks(benchmark_path),
    )
    event_rows.sort(key=lambda row: str(row["event_id"]))
    _validate_event_rows(event_rows, expected_event_count)
    identity_rows = _identity_rows(registry, (*unresolved_results, *event_resolutions))
    coverage_rows = _coverage_rows(event_rows)

    output_directory.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(
            prefix=f".{output_directory.name}.", dir=str(output_directory.parent)
        )
    )
    try:
        _write_parquet(
            temporary / "event_pre_match_elo.parquet", event_rows, EVENT_SCHEMA
        )
        _write_parquet(
            temporary / "team_identity_map.parquet", identity_rows, IDENTITY_SCHEMA
        )
        _write_parquet(
            temporary / "coverage_by_competition.parquet",
            coverage_rows,
            COVERAGE_SCHEMA,
        )
        parquet_files = sorted(
            name for name in RELEASE_FILES if name.endswith(".parquet")
        )
        files = {}
        for name in parquet_files:
            path = temporary / name
            parquet = pq.ParquetFile(path)
            files[name] = {
                "sha256": sha256_file(path),
                "byte_size": path.stat().st_size,
                "row_count": parquet.metadata.num_rows,
                "schema_fingerprint": schema_fingerprint(parquet.schema_arrow),
            }
        coverage_counts: dict[str, int] = defaultdict(int)
        for row in event_rows:
            coverage_counts[str(row["coverage_status"])] += 1
        manifest = {
            "contract_version": CONTRACT_VERSION,
            "dataset_version": dataset_version,
            "build_revision": build_revision,
            "target_snapshot": {
                "sha256": target_sha,
                "event_count": len(events),
                "event_id_sha256": hashlib.sha256(
                    "\n".join(row.event_id for row in events).encode()
                ).hexdigest(),
            },
            "date_rules": {
                "target_date": "UTC calendar date of match_started_at_utc",
                "rating_capture": "strictly before target UTC calendar date",
                "same_date_updates": "batched from start-of-date ratings",
                "clubelo": "strictly before target date",
                "eloratings": "reconstructed pre-match rows may equal target date",
            },
            "model": {
                "version": MODEL_VERSION,
                "initial_rating": 1500,
                "friendlies_k_multiplier": 0.5,
                "parameters": {
                    pool: asdict(parameters[pool]) for pool in sorted(parameters)
                },
            },
            "identity_review": identity_review,
            "sources": source_manifest,
            "source_licenses": sorted({row["license"] for row in source_manifest}),
            "counts": {
                "target_events": len(events),
                "source_results_since_2018": len(source_rows),
                "canonical_results": len(canonical),
                "source_conflicts": len(conflicts),
                "unresolved_result_teams": len(unresolved_results),
                "coverage": dict(sorted(coverage_counts.items())),
            },
            "files": files,
        }
        write_json(temporary / "manifest.json", manifest)
        _write_checksums(temporary)
        os.replace(temporary, output_directory)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return output_directory


__all__ = [
    "CONTRACT_VERSION",
    "COVERAGE_STATUSES",
    "DATASET_VERSION",
    "EloReleaseError",
    "EVENT_SCHEMA",
    "MODEL_VERSION",
    "RELEASE_FILES",
    "TARGET_EVENT_COUNT",
    "TARGET_SNAPSHOT_SHA256",
    "TargetEvent",
    "build_release",
    "load_target_events",
    "normalize_sources",
]
