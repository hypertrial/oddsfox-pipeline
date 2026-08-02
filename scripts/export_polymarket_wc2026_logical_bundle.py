#!/usr/bin/env python3
"""Export the versioned WC2026 logical-v1 bundle as Parquet + manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Final
from uuid import uuid4

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path
from _export_common import qualified_mart_name

REPO_ROOT: Final[Path] = ensure_src_on_path()
EXPECTED_CONTRACT_NAME: Final[str] = "polymarket-wc2026-logical-v1"
EXPECTED_CONTRACT_VERSION: Final[str] = "1"
EXPECTED_TAXONOMY_VERSION: Final[str] = "wc2026-logical-taxonomy-v1"
EXPECTED_MEMBERSHIP_POLICY_VERSION: Final[str] = "wc2026-finals-sporting-v1"

from oddsfox_pipeline.ingestion.openfootball.schedule_fixtures import (  # noqa: E402
    FIFA_SCHEDULE_SHA256,
    FIFA_SCHEDULE_TITLE,
    FIFA_SCHEDULE_URL,
    OPENFOOTBALL_FILES,
    OPENFOOTBALL_REVISION,
)
from oddsfox_pipeline.publishing._bundle_io import (  # noqa: E402
    current_clean_commit,
    git_head_sha,
    sha256_file,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import (  # noqa: E402
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)
from oddsfox_pipeline.storage.duckdb.schemas.dbt_schemas import (  # noqa: E402
    OPENFOOTBALL_WC2026_STAGING_SCHEMA,
    POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
    POLYMARKET_WC2026_MARTS_SCHEMA,
    POLYMARKET_WC2026_STAGING_SCHEMA,
    WC2026_MARTS_SCHEMA,
)

BUNDLE_FILES: Final[dict[str, tuple[str, tuple[str, ...]]]] = {
    "events.parquet": (
        "polymarket_wc2026_logical_events",
        ("event_id",),
    ),
    "markets.parquet": (
        "polymarket_wc2026_logical_markets",
        ("market_id",),
    ),
    "market_events.parquet": (
        "polymarket_wc2026_logical_market_events",
        ("market_id", "source_ordinal", "event_id"),
    ),
    "propositions.parquet": (
        "polymarket_wc2026_logical_propositions",
        ("market_id", "outcome_index"),
    ),
    "entities.parquet": (
        "polymarket_wc2026_logical_entities",
        ("entity_type", "entity_id"),
    ),
    "proposition_entities.parquet": (
        "polymarket_wc2026_logical_proposition_entities",
        ("source_proposition_id", "entity_role", "entity_id"),
    ),
    "scopes.parquet": (
        "polymarket_wc2026_logical_scopes",
        ("stage_rank", "scope_id"),
    ),
}

EXPECTED_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    "events.parquet": (
        "event_id",
        "event_slug",
        "event_title",
        "event_description",
        "source_text",
        "resolution_source",
        "source_url",
        "tags_json",
        "series_slugs_json",
        "candidate_sources_json",
        "event_volume_usd_lifetime_reported",
        "event_volume_observed_at",
        "first_seen_at",
        "first_eligible_observed_at",
        "eligibility_effective_from",
        "ever_eligible",
        "currently_eligible",
        "volume_unknown",
        "event_created_at",
        "is_active",
        "is_closed",
        "is_archived",
        "neg_risk",
        "neg_risk_market_id",
        "show_all_outcomes",
        "start_at",
        "end_at",
        "finished_at",
        "game_id",
        "fifa_match_id",
        "fixture_group_label",
        "fixture_mapping_basis",
        "membership_status",
        "membership_class",
        "tournament_part",
        "scope_id",
        "membership_basis",
        "membership_reason",
        "membership_policy_version",
        "event_logical_eligible",
        "event_constraint_group_id",
        "event_constraint_kind",
        "event_constraint_complete",
    ),
    "markets.parquet": (
        "market_id",
        "condition_id",
        "market_slug",
        "question",
        "description",
        "resolution_text",
        "resolution_source",
        "outcome_format",
        "source_url",
        "tags_json",
        "market_volume_usd_lifetime_reported",
        "is_active",
        "is_closed",
        "is_resolved",
        "winning_outcome",
        "winning_clob_token_id",
        "market_family",
        "tournament_part",
        "scope_id",
        "resolution_scope",
        "resolution_period",
        "void_semantics",
        "sports_market_type",
        "group_item_title",
        "group_item_threshold",
        "line",
        "normalized_threshold",
        "threshold_source",
        "start_at",
        "end_at",
        "logical_usable",
        "outcomes_usable",
        "tokens_usable",
        "quarantine_reason",
        "market_neg_risk_market_id",
        "market_neg_risk_request_id",
        "market_neg_risk_other",
        "primary_event_id",
        "fifa_match_id",
        "subject_entity_ids",
        "participant_entity_ids",
        "player_national_team_entity_ids",
        "referenced_entity_ids",
    ),
    "market_events.parquet": (
        "event_id",
        "market_id",
        "source_ordinal",
        "is_enclosing_event",
        "event_logical_eligible",
        "event_membership_status",
        "event_ever_eligible",
        "event_volume_unknown",
        "fifa_match_id",
        "fixture_mapping_basis",
        "is_primary_qualifying_event",
    ),
    "propositions.parquet": (
        "source_proposition_id",
        "market_id",
        "condition_id",
        "outcome_index",
        "outcome_label",
        "clob_token_id",
        "statement",
        "market_family",
        "tournament_part",
        "scope_id",
        "resolution_scope",
        "resolution_period",
        "void_semantics",
        "predicate",
        "predicate_subject_entity_id",
        "predicate_object",
        "predicate_stage_rank",
        "target_stage_key",
        "polarity",
        "operator",
        "interval_lower",
        "interval_upper",
        "lower_inclusive",
        "upper_inclusive",
        "threshold_value",
        "threshold_source",
        "handicap_value",
        "score_home",
        "score_away",
        "market_constraint_group_id",
        "market_constraint_kind",
        "market_constraint_complete",
        "event_constraint_group_id",
        "event_constraint_kind",
        "event_constraint_complete",
        "logical_usable",
        "semantic_usable",
    ),
    "entities.parquet": (
        "entity_id",
        "entity_type",
        "canonical_name",
        "display_name",
        "tournament_part",
        "fifa_match_id",
        "group_label",
        "home_team_entity_id",
        "away_team_entity_id",
        "source",
    ),
    "proposition_entities.parquet": (
        "source_proposition_id",
        "entity_id",
        "entity_role",
    ),
    "scopes.parquet": (
        "scope_id",
        "parent_scope_id",
        "scope_type",
        "scope_key",
        "display_name",
        "stage_rank",
        "progression_path",
        "progression_branch",
    ),
}

TOPOLOGY_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    "events.parquet": (
        "event_id",
        "event_logical_eligible",
        "tournament_part",
        "scope_id",
    ),
    "markets.parquet": (
        "market_id",
        "primary_event_id",
        "market_family",
        "tournament_part",
        "scope_id",
        "resolution_scope",
        "logical_usable",
        "subject_entity_ids",
        "participant_entity_ids",
        "player_national_team_entity_ids",
        "referenced_entity_ids",
    ),
    "market_events.parquet": (
        "event_id",
        "market_id",
        "source_ordinal",
        "is_enclosing_event",
        "event_logical_eligible",
        "is_primary_qualifying_event",
    ),
    "propositions.parquet": (
        "source_proposition_id",
        "market_id",
        "predicate_subject_entity_id",
        "predicate_object",
        "market_constraint_group_id",
        "market_constraint_kind",
        "market_constraint_complete",
        "event_constraint_group_id",
        "event_constraint_kind",
        "event_constraint_complete",
        "logical_usable",
        "semantic_usable",
    ),
    "entities.parquet": (
        "entity_id",
        "entity_type",
        "home_team_entity_id",
        "away_team_entity_id",
    ),
    "proposition_entities.parquet": EXPECTED_COLUMNS["proposition_entities.parquet"],
    "scopes.parquet": (
        "scope_id",
        "parent_scope_id",
        "scope_type",
        "scope_key",
        "stage_rank",
        "progression_path",
        "progression_branch",
    ),
}

SEMANTIC_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    "events.parquet": (
        "event_id",
        "event_logical_eligible",
        "membership_status",
        "membership_basis",
        "tournament_part",
        "scope_id",
        "event_constraint_group_id",
        "event_constraint_kind",
        "event_constraint_complete",
    ),
    "markets.parquet": (
        "market_id",
        "condition_id",
        "primary_event_id",
        "market_family",
        "tournament_part",
        "scope_id",
        "resolution_scope",
        "resolution_period",
        "void_semantics",
        "outcome_format",
        "normalized_threshold",
        "threshold_source",
        "market_neg_risk_market_id",
        "market_neg_risk_request_id",
        "market_neg_risk_other",
        "logical_usable",
        "quarantine_reason",
        "subject_entity_ids",
        "participant_entity_ids",
        "player_national_team_entity_ids",
        "referenced_entity_ids",
    ),
    "market_events.parquet": (
        *TOPOLOGY_COLUMNS["market_events.parquet"],
        "event_membership_status",
        "fifa_match_id",
        "fixture_mapping_basis",
    ),
    "propositions.parquet": tuple(
        column
        for column in EXPECTED_COLUMNS["propositions.parquet"]
        if column not in {"clob_token_id", "statement"}
    ),
    "entities.parquet": tuple(
        column
        for column in EXPECTED_COLUMNS["entities.parquet"]
        if column not in {"display_name", "source"}
    ),
    "proposition_entities.parquet": EXPECTED_COLUMNS["proposition_entities.parquet"],
    "scopes.parquet": EXPECTED_COLUMNS["scopes.parquet"],
}

PHYSICAL_INTEGER_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "fifa_match_id",
        "source_ordinal",
        "outcome_index",
        "predicate_stage_rank",
        "score_home",
        "score_away",
        "stage_rank",
    }
)
PHYSICAL_DOUBLE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "event_volume_usd_lifetime_reported",
        "market_volume_usd_lifetime_reported",
        "line",
        "normalized_threshold",
        "interval_lower",
        "interval_upper",
        "threshold_value",
        "handicap_value",
    }
)
PHYSICAL_BOOLEAN_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "ever_eligible",
        "currently_eligible",
        "volume_unknown",
        "is_active",
        "is_closed",
        "is_archived",
        "neg_risk",
        "show_all_outcomes",
        "event_logical_eligible",
        "event_constraint_complete",
        "is_resolved",
        "logical_usable",
        "outcomes_usable",
        "tokens_usable",
        "is_enclosing_event",
        "event_ever_eligible",
        "event_volume_unknown",
        "is_primary_qualifying_event",
        "lower_inclusive",
        "upper_inclusive",
        "market_constraint_complete",
        "market_neg_risk_other",
        "semantic_usable",
    }
)
PHYSICAL_TIMESTAMP_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "event_volume_observed_at",
        "first_seen_at",
        "first_eligible_observed_at",
        "eligibility_effective_from",
        "event_created_at",
        "start_at",
        "end_at",
        "finished_at",
    }
)
PHYSICAL_VARCHAR_ARRAY_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "subject_entity_ids",
        "participant_entity_ids",
        "player_national_team_entity_ids",
        "referenced_entity_ids",
    }
)


def physical_type(column: str) -> str:
    """Return the frozen logical-v1 Parquet type for one contract column."""
    if column in PHYSICAL_INTEGER_COLUMNS:
        return "BIGINT"
    if column in PHYSICAL_DOUBLE_COLUMNS:
        return "DOUBLE"
    if column in PHYSICAL_BOOLEAN_COLUMNS:
        return "BOOLEAN"
    if column in PHYSICAL_TIMESTAMP_COLUMNS:
        return "TIMESTAMP WITH TIME ZONE"
    if column in PHYSICAL_VARCHAR_ARRAY_COLUMNS:
        return "VARCHAR[]"
    return "VARCHAR"


EXPECTED_PHYSICAL_SCHEMA: Final[dict[str, tuple[tuple[str, str, str], ...]]] = {
    filename: tuple(
        (column, physical_type(column), "YES") for column in EXPECTED_COLUMNS[filename]
    )
    for filename in BUNDLE_FILES
}


def _typed_projection(filename: str) -> str:
    return ", ".join(
        f'cast("{column}" as {physical_type(column)}) as "{column}"'
        for column in EXPECTED_COLUMNS[filename]
    )


def _validate_parquet_physical_schema(
    conn: duckdb.DuckDBPyConnection, path: Path, filename: str
) -> None:
    escaped_path = str(path).replace("'", "''")
    rows = conn.execute(
        f"describe select * from read_parquet('{escaped_path}')"
    ).fetchall()
    actual = tuple((str(row[0]), str(row[1]), str(row[2])) for row in rows)
    expected = EXPECTED_PHYSICAL_SCHEMA[filename]
    if actual != expected:
        raise RuntimeError(
            f"{filename} physical schema does not match logical-v1: "
            f"expected={expected}, actual={actual}"
        )


_sha256 = sha256_file


def _repo_sha(*, require_clean: bool = True) -> str:
    if not require_clean:
        return git_head_sha(REPO_ROOT)
    return current_clean_commit(
        REPO_ROOT,
        untracked_files="all",
        dirty_error="Refusing to publish logical-v1 from a dirty pipeline worktree",
    )


def _relation_columns(
    conn: duckdb.DuckDBPyConnection, relation: str
) -> tuple[str, ...]:
    result = conn.execute(f"select * from {relation} limit 0")
    return tuple(column[0] for column in result.description)


def _canonical_default(value: Any) -> str:
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Unsupported canonical JSON value: {type(value).__name__}")


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_canonical_default,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _relation_fingerprint(
    conn: duckdb.DuckDBPyConnection,
    relations: dict[str, str],
    projections: dict[str, tuple[str, ...]],
) -> str:
    payload: dict[str, list[dict[str, Any]]] = {}
    for filename in BUNDLE_FILES:
        columns = projections[filename]
        query = (
            f"select {', '.join(columns)} from {relations[filename]} "
            "order by all nulls first"
        )
        result = conn.execute(query)
        names = tuple(column[0] for column in result.description)
        payload[filename.removesuffix(".parquet")] = [
            dict(zip(names, row, strict=True)) for row in result.fetchall()
        ]
    return _canonical_sha256(payload)


def _query_content_hash(conn: duckdb.DuckDBPyConnection, query: str) -> str:
    result = conn.execute(f"select * from ({query}) order by all nulls first")
    names = tuple(column[0] for column in result.description)
    rows = [dict(zip(names, row, strict=True)) for row in result.fetchall()]
    return _canonical_sha256(rows)


def _semantic_input_hashes(
    conn: duckdb.DuckDBPyConnection,
) -> dict[str, str]:
    event_snapshots = qualified_mart_name(
        POLYMARKET_WC2026_STAGING_SCHEMA,
        "stg_polymarket_wc2026_event_snapshots",
    )
    event_markets = qualified_mart_name(
        POLYMARKET_WC2026_STAGING_SCHEMA,
        "stg_polymarket_wc2026_event_markets",
    )
    market_payloads = qualified_mart_name(
        POLYMARKET_WC2026_STAGING_SCHEMA,
        "stg_polymarket_wc2026_event_market_payload_latest",
    )
    knockout = qualified_mart_name(
        OPENFOOTBALL_WC2026_STAGING_SCHEMA,
        "stg_openfootball_wc2026_schedule_fixtures",
    )
    teams = qualified_mart_name(
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_logical_team_identities",
    )
    players = qualified_mart_name(WC2026_MARTS_SCHEMA, "wc2026_player_features")
    reviewed_membership = polymarket_raw_tbl("wc2026", "reviewed_event_membership")
    queries = {
        "relation/stg_polymarket_wc2026_event_snapshots/history": (
            f"select * from {event_snapshots}"
        ),
        "relation/stg_polymarket_wc2026_event_markets/history": (
            f"select * from {event_markets}"
        ),
        "relation/stg_polymarket_wc2026_event_market_payload_latest": (
            f"select * from {market_payloads}"
        ),
        "relation/stg_openfootball_wc2026_schedule_fixtures": (
            f"select * from {knockout}"
        ),
        "relation/reviewed_event_membership": (
            f"select * exclude (loaded_at) from {reviewed_membership}"
        ),
        "relation/int_polymarket_wc2026_logical_team_identities": (
            f"select * from {teams}"
        ),
        "relation/wc2026_player_features": f"select * from {players}",
    }
    return {
        name: _query_content_hash(conn, query)
        for name, query in sorted(queries.items())
    }


def _fixture_schedule_provenance(
    conn: duckdb.DuckDBPyConnection,
    relation_sha256: str,
) -> dict[str, Any]:
    relation = qualified_mart_name(
        OPENFOOTBALL_WC2026_STAGING_SCHEMA,
        "stg_openfootball_wc2026_schedule_fixtures",
    )
    row = _single_row(
        conn,
        f"""
        select
            count(*) as row_count,
            count(distinct fifa_match_id) as distinct_match_count,
            min(fifa_match_id) as match_id_min,
            max(fifa_match_id) as match_id_max
        from {relation}
        """,
    )
    if row != {
        "row_count": 104,
        "distinct_match_count": 104,
        "match_id_min": 1,
        "match_id_max": 104,
    }:
        raise RuntimeError(f"Official fixture schedule is incomplete: {row}")
    return {
        **row,
        "relation": "stg_openfootball_wc2026_schedule_fixtures",
        "relation_sha256": relation_sha256,
        "openfootball_revision": OPENFOOTBALL_REVISION,
        "openfootball_files": OPENFOOTBALL_FILES,
        "fifa_schedule": {
            "url": FIFA_SCHEDULE_URL,
            "document_title": FIFA_SCHEDULE_TITLE,
            "sha256": FIFA_SCHEDULE_SHA256,
        },
    }


def _reviewed_membership_provenance(
    conn: duckdb.DuckDBPyConnection,
    relation_sha256: str,
) -> dict[str, Any]:
    relation = polymarket_raw_tbl("wc2026", "reviewed_event_membership")
    row = _single_row(
        conn,
        f"""
        select
            count(*) as row_count,
            count(distinct source_sha256) as source_count,
            min(source_sha256) as source_sha256
        from {relation}
        """,
    )
    if row["row_count"] <= 0 or row["source_count"] != 1:
        raise RuntimeError(f"Reviewed membership inventory is invalid: {row}")
    return {
        "relation": "polymarket_wc2026_raw.reviewed_event_membership",
        "row_count": row["row_count"],
        "source_sha256": row["source_sha256"],
        "relation_sha256": relation_sha256,
    }


def _single_row(conn: duckdb.DuckDBPyConnection, query: str) -> dict[str, Any]:
    result = conn.execute(query)
    row = result.fetchone()
    if row is None:
        raise RuntimeError("Expected one row but query returned none")
    if result.fetchone() is not None:
        raise RuntimeError("Expected exactly one row")
    return dict(zip((column[0] for column in result.description), row, strict=True))


def _validate_contract_versions(
    conn: duckdb.DuckDBPyConnection,
    contract: dict[str, Any],
    events_relation: str,
) -> None:
    """Freeze logical-v1 versions and bind every event row to the manifest policy."""
    expected = {
        "contract_name": EXPECTED_CONTRACT_NAME,
        "contract_version": EXPECTED_CONTRACT_VERSION,
        "taxonomy_version": EXPECTED_TAXONOMY_VERSION,
        "policy_version": EXPECTED_MEMBERSHIP_POLICY_VERSION,
    }
    actual = {
        field: str(contract.get(field)) if contract.get(field) is not None else None
        for field in expected
    }
    if actual != expected:
        raise RuntimeError(
            f"Logical-v1 contract versions do not match: expected={expected}, "
            f"actual={actual}"
        )
    policy = _single_row(
        conn,
        f"""
        select
            count(*) as event_count,
            count(*) filter (
                where membership_policy_version is distinct from
                    '{EXPECTED_MEMBERSHIP_POLICY_VERSION}'
            ) as mismatched_event_count,
            count(distinct membership_policy_version) as policy_version_count
        from {events_relation}
        """,
    )
    if (
        policy["event_count"] <= 0
        or policy["mismatched_event_count"] != 0
        or policy["policy_version_count"] != 1
    ):
        raise RuntimeError(
            "Logical event membership-policy version does not match manifest "
            f"contract: {policy}"
        )


def _utc_timestamp(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise RuntimeError(f"Invalid {field}: {value!r}") from exc
    else:
        raise RuntimeError(f"Missing or invalid {field}: {value!r}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _scan_partitions(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    ops_relation = polymarket_ops_tbl("wc2026", "sync_run_metrics")
    row = conn.execute(
        f"select metrics_json from {ops_relation} where task_name = 'event_catalog'"
    ).fetchone()
    if row is None or row[0] is None:
        raise RuntimeError("Missing converged event_catalog sync metrics")
    metrics = json.loads(row[0])
    snapshots_relation = qualified_mart_name(
        POLYMARKET_WC2026_STAGING_SCHEMA,
        "stg_polymarket_wc2026_event_snapshots",
    )
    latest_snapshot_observed_at = conn.execute(
        f"select max(observed_at) from {snapshots_relation}"
    ).fetchone()[0]
    if latest_snapshot_observed_at is None:
        raise RuntimeError("Event catalog snapshot history is empty")
    metrics_observed_at = _utc_timestamp(
        metrics.get("observed_at"), field="event_catalog metrics observed_at"
    )
    latest_snapshot_observed_at = _utc_timestamp(
        latest_snapshot_observed_at,
        field="latest event catalog snapshot observed_at",
    )
    if metrics_observed_at != latest_snapshot_observed_at:
        raise RuntimeError(
            "Event catalog sync metrics are stale: "
            f"metrics_observed_at={metrics_observed_at.isoformat()}, "
            "latest_snapshot_observed_at="
            f"{latest_snapshot_observed_at.isoformat()}"
        )
    partitions = metrics.get("scan_partitions")
    expected = {
        f"{source}:{state}"
        for source in (
            "exact_2026_tag",
            "related_2026_tag_recall",
            "broad_fifa_world_cup_tag",
            "soccer_fifwc_series",
            "wc2026_event_slug_prefix_recall",
        )
        for state in ("open", "closed")
    }
    if not isinstance(partitions, dict) or set(partitions) != expected:
        raise RuntimeError("Event catalog scan partition inventory is incomplete")
    for name, partition in partitions.items():
        if not partition.get("complete") or not partition.get("stable"):
            raise RuntimeError(f"Event catalog partition is not stable: {name}")
        for field in (
            "event_ids_sha256",
            "membership_inventory_sha256",
            "event_payload_inventory_sha256",
        ):
            signature = str(partition.get(field) or "")
            if len(signature) != 64:
                raise RuntimeError(
                    f"Event catalog partition has invalid {field}: {name}"
                )
        for field in ("event_count", "child_market_count", "membership_count"):
            value = partition.get(field)
            if not isinstance(value, int) or value < 0:
                raise RuntimeError(
                    f"Event catalog partition has invalid {field}: {name}"
                )
    return partitions


def _data_quality(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    membership = qualified_mart_name(
        "polymarket_wc2026_intermediate",
        "int_polymarket_wc2026_event_membership",
    )
    events = qualified_mart_name(
        POLYMARKET_WC2026_MARTS_SCHEMA, "polymarket_wc2026_logical_events"
    )
    markets = qualified_mart_name(
        POLYMARKET_WC2026_MARTS_SCHEMA, "polymarket_wc2026_logical_markets"
    )
    market_events = qualified_mart_name(
        POLYMARKET_WC2026_MARTS_SCHEMA,
        "polymarket_wc2026_logical_market_events",
    )
    propositions = qualified_mart_name(
        POLYMARKET_WC2026_MARTS_SCHEMA,
        "polymarket_wc2026_logical_propositions",
    )
    entities = qualified_mart_name(
        POLYMARKET_WC2026_MARTS_SCHEMA, "polymarket_wc2026_logical_entities"
    )
    proposition_entities = qualified_mart_name(
        POLYMARKET_WC2026_MARTS_SCHEMA,
        "polymarket_wc2026_logical_proposition_entities",
    )
    scopes = qualified_mart_name(
        POLYMARKET_WC2026_MARTS_SCHEMA, "polymarket_wc2026_logical_scopes"
    )
    reviewed_membership = polymarket_raw_tbl("wc2026", "reviewed_event_membership")
    event_latest = qualified_mart_name(
        "polymarket_wc2026_intermediate",
        "int_polymarket_wc2026_event_latest",
    )
    result = conn.execute(
        f"""
        select
            (select count(*) from {membership}
                where ever_eligible and membership_status = 'review_required')
                as review_required_event_count,
            (select count(*) from {events} where volume_unknown)
                as volume_unknown_event_count,
            (select count(*) from {events} where event_logical_eligible)
                as eligible_event_count,
            (select count(*) from {events}
                where event_logical_eligible and (
                    nullif(trim(event_slug), '') is null
                    or nullif(trim(event_title), '') is null
                )) as eligible_event_display_issue_count,
            (select count(*) from {events} where not event_logical_eligible)
                as audit_only_event_count,
            (select count(*) from {reviewed_membership} as reviews
                left join {event_latest} as catalog using (event_id)
                where catalog.event_id is null)
                as orphan_reviewed_membership_count,
            (select count(*) from {market_events} where not event_logical_eligible)
                as ineligible_market_membership_count,
            (select count(*) from (
                select market_id from {market_events} group by market_id
                having not bool_or(event_logical_eligible)
            )) as only_ineligible_market_count,
            (select count(*) from {markets} where not logical_usable)
                as unusable_market_count,
            (select count(*) from {markets} where market_family = 'unclassified')
                as unclassified_market_count,
            (select count(*) from {propositions} where not semantic_usable)
                as semantic_unusable_proposition_count,
            (select count(*) from (
                select market_id from {market_events} group by market_id
                having count(*) filter (where is_primary_qualifying_event) != 1
            )) as primary_event_issue_count,
            (select count(*) from {market_events} as links
                left join {events} as events using (event_id)
                left join {markets} as markets using (market_id)
                where events.event_id is null or markets.market_id is null)
                as dangling_market_event_count,
            (
                (select count(*) from {propositions} as p
                    left join {markets} as m using (market_id)
                    where m.market_id is null)
                +
                (select count(*) from {proposition_entities} as pe
                    left join {propositions} as p using (source_proposition_id)
                    left join {entities} as e using (entity_id)
                    where p.source_proposition_id is null or e.entity_id is null)
            ) as dangling_proposition_count,
            (select count(*) from {events}) as event_count,
            (select count(*) from {markets}) as market_count,
            (select count(*) from {market_events}) as market_event_count,
            (select count(*) from {propositions}) as proposition_count,
            (select count(*) from {entities}) as entity_count,
            (select count(*) from {proposition_entities})
                as proposition_entity_count,
            (select count(*) from {scopes}) as scope_count
        """
    )
    row = result.fetchone()
    if row is None:
        raise RuntimeError("Could not compute logical bundle data quality")
    quality = {
        column[0]: int(value)
        for column, value in zip(result.description, row, strict=True)
    }
    quality["eligible_event_creation_gap_count"] = _eligible_event_creation_gap_count(
        conn
    )
    blocking = (
        quality["review_required_event_count"],
        quality["eligible_event_creation_gap_count"],
        quality["eligible_event_display_issue_count"],
        quality["orphan_reviewed_membership_count"],
        quality["only_ineligible_market_count"],
        quality["primary_event_issue_count"],
        quality["dangling_market_event_count"],
        quality["dangling_proposition_count"],
    )
    if any(blocking):
        raise RuntimeError(f"Logical bundle data quality failed: {quality}")
    if (
        min(
            quality["event_count"],
            quality["market_count"],
            quality["proposition_count"],
            quality["entity_count"],
            quality["scope_count"],
        )
        <= 0
    ):
        raise RuntimeError("Logical bundle cannot be empty")
    return quality


def _eligible_event_creation_gap_count(
    conn: duckdb.DuckDBPyConnection,
) -> int:
    """Count included, ever-eligible events lacking retroactive creation time."""
    events = qualified_mart_name(
        POLYMARKET_WC2026_MARTS_SCHEMA, "polymarket_wc2026_logical_events"
    )
    return int(
        conn.execute(
            f"""
            select count(*)
            from {events}
            where membership_status = 'included'
                and ever_eligible
                and (
                    event_created_at is null
                    or eligibility_effective_from is null
                    or eligibility_effective_from is distinct from event_created_at
                )
            """
        ).fetchone()[0]
    )


def _validate_bundle_relationships(
    conn: duckdb.DuckDBPyConnection,
    relations: dict[str, str],
) -> None:
    """Fail closed on every logical-v1 relationship used by the consumer."""
    events = relations["events.parquet"]
    markets = relations["markets.parquet"]
    market_events = relations["market_events.parquet"]
    propositions = relations["propositions.parquet"]
    entities = relations["entities.parquet"]
    proposition_entities = relations["proposition_entities.parquet"]
    scopes = relations["scopes.parquet"]

    checks = {
        "negative_volume_count": f"""
            select
                (select count(*) from {events}
                    where event_volume_usd_lifetime_reported < 0)
                +
                (select count(*) from {markets}
                    where market_volume_usd_lifetime_reported < 0)
        """,
        "eligible_event_display_reference_count": f"""
            select count(*)
            from {events}
            where event_logical_eligible and (
                nullif(trim(event_slug), '') is null
                or nullif(trim(event_title), '') is null
            )
        """,
        "logical_usable_market_display_count": f"""
            select count(*)
            from {markets}
            where logical_usable and nullif(trim(question), '') is null
        """,
        "proposition_statement_count": f"""
            select count(*)
            from {propositions}
            where nullif(trim(statement), '') is null
        """,
        "market_event_event_reference_count": f"""
            select count(*)
            from {market_events} as links
            left join {events} as events using (event_id)
            where links.event_id is null or events.event_id is null
        """,
        "market_event_market_reference_count": f"""
            select count(*)
            from {market_events} as links
            left join {markets} as markets using (market_id)
            where links.market_id is null or markets.market_id is null
        """,
        "event_scope_reference_count": f"""
            select count(*)
            from {events} as events
            left join {scopes} as scopes using (scope_id)
            where events.scope_id is not null and scopes.scope_id is null
        """,
        "market_scope_reference_count": f"""
            select count(*)
            from {markets} as markets
            left join {scopes} as scopes using (scope_id)
            where markets.scope_id is null or scopes.scope_id is null
        """,
        "proposition_scope_reference_count": f"""
            select count(*)
            from {propositions} as propositions
            left join {scopes} as scopes using (scope_id)
            where propositions.scope_id is null or scopes.scope_id is null
        """,
        "scope_parent_reference_count": f"""
            select count(*)
            from {scopes} as child
            left join {scopes} as parent
                on child.parent_scope_id = parent.scope_id
            where child.parent_scope_id is not null and parent.scope_id is null
        """,
        "proposition_market_reference_count": f"""
            select count(*)
            from {propositions} as propositions
            left join {markets} as markets using (market_id)
            where propositions.market_id is null or markets.market_id is null
        """,
        "proposition_subject_reference_count": f"""
            select count(*)
            from {propositions} as propositions
            left join {entities} as entities
                on propositions.predicate_subject_entity_id = entities.entity_id
            where propositions.predicate_subject_entity_id is not null
                and entities.entity_id is null
        """,
        "proposition_object_entity_reference_count": f"""
            select count(*)
            from {propositions} as propositions
            left join {entities} as entities
                on propositions.predicate_object = entities.entity_id
            where (
                starts_with(propositions.predicate_object, 'fixture:')
                or starts_with(propositions.predicate_object, 'team:')
                or starts_with(propositions.predicate_object, 'group:')
                or starts_with(propositions.predicate_object, 'stage:')
                or starts_with(propositions.predicate_object, 'award:')
                or starts_with(propositions.predicate_object, 'tournament:')
            ) and entities.entity_id is null
        """,
        "fixture_team_reference_count": f"""
            select count(*)
            from {entities} as fixture
            left join {entities} as home
                on fixture.home_team_entity_id = home.entity_id
            left join {entities} as away
                on fixture.away_team_entity_id = away.entity_id
            where fixture.entity_type = 'fixture'
                and (
                    home.entity_type is distinct from 'team'
                    or away.entity_type is distinct from 'team'
                    or fixture.home_team_entity_id
                        is not distinct from fixture.away_team_entity_id
                )
        """,
        "market_primary_event_reference_count": f"""
            select count(*)
            from {markets} as markets
            left join {market_events} as links
                on markets.market_id = links.market_id
                and links.is_primary_qualifying_event
            left join {events} as events
                on links.event_id = events.event_id
            where links.market_id is null
                or markets.primary_event_id is distinct from links.event_id
                or links.event_logical_eligible is distinct from true
                or links.event_membership_status is distinct from 'included'
                or links.event_ever_eligible is distinct from true
                or events.event_logical_eligible is distinct from true
        """,
        "proposition_entity_proposition_reference_count": f"""
            select count(*)
            from {proposition_entities} as links
            left join {propositions} as propositions using (source_proposition_id)
            where links.source_proposition_id is null
                or propositions.source_proposition_id is null
        """,
        "proposition_entity_entity_reference_count": f"""
            select count(*)
            from {proposition_entities} as links
            left join {entities} as entities using (entity_id)
            where links.entity_id is null or entities.entity_id is null
        """,
    }
    market_entity_roles = {
        "subject_entity_ids": ("team", "player"),
        "participant_entity_ids": ("team",),
        "player_national_team_entity_ids": ("team",),
        "referenced_entity_ids": (
            "fixture",
            "group",
            "stage",
            "award",
            "tournament",
        ),
    }
    for column, entity_types in market_entity_roles.items():
        allowed = ", ".join(f"'{entity_type}'" for entity_type in entity_types)
        checks[f"market_{column}_shape_count"] = f"""
            select count(*)
            from {markets} as markets
            where markets.{column} is null
                or markets.{column}
                    != list_sort(list_distinct(markets.{column}))
                or exists (
                    select 1
                    from unnest(markets.{column}) as ids(entity_id)
                    where ids.entity_id is null
                )
        """
        checks[f"market_{column}_reference_count"] = f"""
            select count(*)
            from {markets} as markets
            cross join unnest(markets.{column}) as ids(entity_id)
            left join {entities} as entities on ids.entity_id = entities.entity_id
            where entities.entity_id is null
                or entities.entity_type not in ({allowed})
        """
    violations = {
        name: int(conn.execute(query).fetchone()[0]) for name, query in checks.items()
    }
    blocking = {name: count for name, count in violations.items() if count}
    if blocking:
        raise RuntimeError(
            "Logical bundle relationship validation failed: "
            + ", ".join(f"{name}={count}" for name, count in blocking.items())
        )


def export_polymarket_wc2026_logical_bundle(
    conn: duckdb.DuckDBPyConnection,
    output_dir: Path,
    *,
    require_clean_repo: bool = True,
) -> dict[str, Any]:
    """Validate and atomically publish all seven logical-v1 parquet files."""
    # Raw and dbt timestamps are stored as UTC-naive TIMESTAMP values. Pin the
    # session before any TIMESTAMPTZ cast, fingerprint, comparison, or copy so
    # the host timezone cannot change exported instants or hashes.
    conn.execute("SET TimeZone='UTC'")
    output_dir = output_dir.resolve()
    if output_dir.exists():
        raise FileExistsError(f"Output directory already exists: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = output_dir.with_name(f".{output_dir.name}.{uuid4().hex}.tmp")
    temporary_dir.mkdir()

    try:
        relations: dict[str, str] = {}
        for filename, (mart_name, _) in BUNDLE_FILES.items():
            relation = qualified_mart_name(POLYMARKET_WC2026_MARTS_SCHEMA, mart_name)
            columns = _relation_columns(conn, relation)
            expected = EXPECTED_COLUMNS[filename]
            if columns != expected:
                raise RuntimeError(
                    f"{relation} columns do not match logical-v1: "
                    f"expected={list(expected)}, actual={list(columns)}"
                )
            relations[filename] = relation

        _validate_bundle_relationships(conn, relations)
        quality = _data_quality(conn)
        scan_partitions = _scan_partitions(conn)
        contract_relation = qualified_mart_name(
            POLYMARKET_WC2026_STAGING_SCHEMA,
            "polymarket_wc2026_logical_contract",
        )
        contract = _single_row(conn, f"select * from {contract_relation}")
        event_relation = relations["events.parquet"]
        _validate_contract_versions(conn, contract, event_relation)
        as_of = conn.execute(
            f"select max(event_volume_observed_at) from {event_relation}"
        ).fetchone()[0]
        if as_of is None:
            raise RuntimeError("Logical events have no observation timestamp")
        as_of_utc = _utc_timestamp(as_of, field="logical events as_of")
        topology_fingerprint = _relation_fingerprint(conn, relations, TOPOLOGY_COLUMNS)
        semantic_fingerprint = _relation_fingerprint(conn, relations, SEMANTIC_COLUMNS)

        files: dict[str, dict[str, Any]] = {}
        for filename, (mart_name, order_columns) in BUNDLE_FILES.items():
            target = temporary_dir / filename
            order_by = ", ".join(order_columns)
            conn.execute(
                f"copy (select {_typed_projection(filename)} "
                f"from {relations[filename]} order by {order_by}) "
                "to ? (format parquet, compression zstd)",
                [str(target)],
            )
            _validate_parquet_physical_schema(conn, target, filename)
            rows = int(
                conn.execute(f"select count(*) from {relations[filename]}").fetchone()[
                    0
                ]
            )
            files[filename] = {
                "sha256": sha256_file(target),
                "rows": rows,
                "bytes": target.stat().st_size,
            }

        seed_inputs = {
            "dbt/seeds/polymarket_wc2026_logical_contract.csv": sha256_file(
                REPO_ROOT / "dbt/seeds/polymarket_wc2026_logical_contract.csv"
            ),
        }
        scan_hashes = {
            f"scan/{name}/{kind}": partition[field]
            for name, partition in sorted(scan_partitions.items())
            for kind, field in (
                ("event_ids", "event_ids_sha256"),
                ("memberships", "membership_inventory_sha256"),
                ("payload", "event_payload_inventory_sha256"),
            )
        }
        semantic_input_hashes = _semantic_input_hashes(conn)
        fixture_schedule = _fixture_schedule_provenance(
            conn,
            semantic_input_hashes["relation/stg_openfootball_wc2026_schedule_fixtures"],
        )
        reviewed_membership = _reviewed_membership_provenance(
            conn,
            semantic_input_hashes["relation/reviewed_event_membership"],
        )
        generated_at = datetime.now(timezone.utc).isoformat()
        source_snapshot_fingerprint = _canonical_sha256(
            {filename: details["sha256"] for filename, details in sorted(files.items())}
        )
        manifest = {
            "schema_version": contract["contract_name"],
            "contract_version": str(contract["contract_version"]),
            "taxonomy_version": contract["taxonomy_version"],
            "membership_policy_version": contract["policy_version"],
            "source": "oddsfox-pipeline",
            "scope": "wc2026",
            "pipeline_git_sha": _repo_sha(require_clean=require_clean_repo),
            "generated_at": generated_at,
            "as_of": as_of_utc.isoformat(),
            "event_volume_field": "event.volume",
            "volume_threshold_usd": float(contract["event_volume_min_usd"]),
            "volume_comparison": contract["event_volume_comparison"],
            "eligibility_mode": contract["eligibility_mode"],
            "eligibility_history": contract["eligibility_history"],
            "child_market_volume_floor_usd": contract["child_market_volume_floor_usd"],
            "temporal_odds": bool(contract["temporal_odds"]),
            "required_event_tag": contract["required_event_tag"],
            "recall_event_tag": contract["recall_event_tag"],
            "fixture_series_slug": contract["fixture_series_slug"],
            "topology_fingerprint": topology_fingerprint,
            "semantic_fingerprint": semantic_fingerprint,
            "source_snapshot_fingerprint": source_snapshot_fingerprint,
            "scan_partitions": scan_partitions,
            "fixture_schedule": fixture_schedule,
            "reviewed_membership": reviewed_membership,
            "row_counts": {
                filename: details["rows"] for filename, details in files.items()
            },
            "data_quality": quality,
            "input_hashes": {
                **seed_inputs,
                **semantic_input_hashes,
                **scan_hashes,
            },
            "files": files,
        }
        manifest_path = temporary_dir / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        temporary_dir.replace(output_dir)
    except Exception:
        import shutil

        shutil.rmtree(temporary_dir, ignore_errors=True)
        raise
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb-path", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--output-dir", type=Path)
    mode.add_argument("--validate-only", action="store_true")
    args = parser.parse_args(argv)
    try:
        with duckdb.connect(str(args.duckdb_path), read_only=True) as conn:
            if args.validate_only:
                with tempfile.TemporaryDirectory(
                    prefix="oddsfox-wc2026-logical-validation-"
                ) as temporary_root:
                    manifest = export_polymarket_wc2026_logical_bundle(
                        conn,
                        Path(temporary_root) / "bundle",
                        require_clean_repo=False,
                    )
            else:
                manifest = export_polymarket_wc2026_logical_bundle(
                    conn, args.output_dir
                )
    except (duckdb.Error, FileExistsError, LookupError, OSError, RuntimeError) as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    if args.validate_only:
        print(
            f"Validated {len(BUNDLE_FILES)} logical-v1 parquet files "
            f"({manifest['as_of']})"
        )
    else:
        print(
            f"Exported {len(BUNDLE_FILES)} logical-v1 parquet files to "
            f"{args.output_dir} ({manifest['as_of']})"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
