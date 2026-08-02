#!/usr/bin/env python3
"""Materialize the pinned logical-v1 synthetic fixture into a strict bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Final

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from export_polymarket_wc2026_logical_bundle import (  # noqa: E402
    BUNDLE_FILES,
    EXPECTED_COLUMNS,
    FIFA_SCHEDULE_SHA256,
    FIFA_SCHEDULE_TITLE,
    FIFA_SCHEDULE_URL,
    OPENFOOTBALL_FILES,
    OPENFOOTBALL_REVISION,
    SEMANTIC_COLUMNS,
    TOPOLOGY_COLUMNS,
    _canonical_sha256,
    _relation_fingerprint,
    _sha256,
    _typed_projection,
    _validate_bundle_relationships,
    _validate_parquet_physical_schema,
    physical_type,
)

REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[1]
DEFAULT_SPEC: Final[Path] = (
    REPO_ROOT / "tests/fixtures/polymarket_wc2026_logical_v1/source_fixture.v1.json"
)
DEFAULT_LOCK: Final[Path] = DEFAULT_SPEC.with_name("fixture.lock.json")


def _source_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _row(filename: str, **values: Any) -> dict[str, Any]:
    unknown = set(values) - set(EXPECTED_COLUMNS[filename])
    if unknown:
        raise ValueError(f"{filename} fixture has unknown columns: {sorted(unknown)}")
    return {column: values.get(column) for column in EXPECTED_COLUMNS[filename]}


def _event_rows(spec: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for item in spec["events"]:
        eligible = bool(item["eligible"])
        status = item["membership_status"]
        market_set_ids = {
            market["neg_risk_market_id"]
            for market in spec["markets"]
            if market["primary_event_id"] == item["id"]
            and market.get("neg_risk_market_id")
        }
        rows.append(
            _row(
                "events.parquet",
                event_id=item["id"],
                event_slug=item["slug"],
                event_title=item["title"],
                event_description=item["title"],
                source_text=item["title"],
                resolution_source="Official FIFA results",
                source_url=f"https://polymarket.com/event/{item['slug']}",
                tags_json='["2026-fifa-world-cup"]',
                series_slugs_json="[]",
                candidate_sources_json='["synthetic_fixture"]',
                event_volume_usd_lifetime_reported=item["volume"],
                event_volume_observed_at=spec["as_of"],
                first_seen_at="2026-01-01T00:00:00+00:00",
                first_eligible_observed_at=spec["as_of"] if eligible else None,
                eligibility_effective_from=(
                    "2026-01-01T00:00:00+00:00" if eligible else None
                ),
                ever_eligible=eligible,
                currently_eligible=eligible,
                volume_unknown=False,
                event_created_at="2026-01-01T00:00:00+00:00",
                is_active=item.get("is_active", True),
                is_closed=item.get("is_closed", False),
                is_archived=item.get("is_archived", False),
                neg_risk=item["neg_risk"],
                neg_risk_market_id=(
                    f"neg-risk:{item['id']}" if item["neg_risk"] else None
                ),
                show_all_outcomes=True,
                start_at="2026-06-11T00:00:00+00:00",
                end_at="2026-07-19T23:59:59+00:00",
                finished_at=item.get("finished_at"),
                game_id=(
                    f"game:{item['fifa_match_id']}"
                    if item.get("fifa_match_id")
                    else None
                ),
                fifa_match_id=item.get("fifa_match_id"),
                fixture_group_label=item.get("fixture_group_label"),
                fixture_mapping_basis=item.get("fixture_mapping_basis"),
                membership_status=status,
                membership_class=item["membership_class"],
                tournament_part=item.get("tournament_part"),
                scope_id=item.get("scope_id"),
                membership_basis=(
                    "synthetic_reviewed_fixture"
                    if status == "included"
                    else "synthetic_excluded_fixture"
                ),
                membership_reason="Deterministic logical-v1 fixture",
                membership_policy_version=spec["membership_policy_version"],
                event_logical_eligible=eligible and status == "included",
                event_constraint_group_id=(
                    "polymarket:neg-risk-market:"
                    + next(iter(market_set_ids))
                    + ":positive-outcomes"
                    if eligible and len(market_set_ids) == 1
                    else None
                ),
                event_constraint_kind=(
                    "at_most_one" if eligible and len(market_set_ids) == 1 else None
                ),
                event_constraint_complete=False,
            )
        )
    return rows


def _constraint_id(market: dict[str, Any]) -> str:
    key = "|".join(
        (
            market["resolution_scope"],
            market["resolution_period"],
            market.get("void_semantics", "unspecified"),
        )
    )
    suffix = hashlib.md5(key.encode(), usedforsecurity=False).hexdigest()
    return (
        f"polymarket:neg-risk-market:{market['neg_risk_market_id']}:"
        f"positive-outcomes:{suffix}"
    )


def _market_rows(
    spec: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    events = {item["id"]: item for item in spec["events"]}
    entities = {item["entity_id"]: item for item in spec["entities"]}
    fixtures = {
        item["fifa_match_id"]: item
        for item in spec["entities"]
        if item["entity_type"] == "fixture"
    }
    player_teams = {
        row["player_entity_id"]: row["team_entity_id"]
        for row in spec.get("player_teams", [])
    }
    markets: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []
    propositions: list[dict[str, Any]] = []
    for market in spec["markets"]:
        propositions_spec = market["propositions"]
        subject_entity_ids = set(market.get("subject_entity_ids", []))
        if not subject_entity_ids:
            candidate_subjects = {
                proposition.get("subject") for proposition in propositions_spec
            }
            subject_entity_ids.update(
                entity_id
                for entity_id in candidate_subjects
                if entity_id in entities
                and entities[entity_id]["entity_type"] in {"team", "player"}
            )
        participant_entity_ids = set(market.get("participant_entity_ids", []))
        referenced_entity_ids = set(market.get("referenced_entity_ids", []))
        tournament_entity_id = "tournament:fifa_world_cup_2026"
        if tournament_entity_id in entities:
            referenced_entity_ids.add(tournament_entity_id)
        fixture = fixtures.get(market.get("fifa_match_id"))
        if fixture is not None:
            referenced_entity_ids.add(fixture["entity_id"])
            participant_entity_ids.update(
                (fixture["home_team_entity_id"], fixture["away_team_entity_id"])
            )
            if fixture.get("group_label"):
                group_entity_id = f"group:{fixture['group_label'].lower()}"
                if group_entity_id in entities:
                    referenced_entity_ids.add(group_entity_id)
        scope_entity_id = market["scope_id"].replace("scope:wc2026:", "", 1)
        for candidate in (
            market["resolution_scope"],
            f"stage:{market['tournament_part']}",
            f"award:{scope_entity_id.removeprefix('award:')}",
        ):
            if candidate in entities:
                referenced_entity_ids.add(candidate)
        player_national_team_entity_ids = {
            player_teams[entity_id]
            for entity_id in subject_entity_ids
            if entity_id in player_teams
        }
        markets.append(
            _row(
                "markets.parquet",
                market_id=market["id"],
                condition_id=market["condition_id"],
                market_slug=market["slug"],
                question=market["question"],
                description=market["question"],
                resolution_text=market["question"],
                resolution_source="Official FIFA results",
                outcome_format=market["outcome_format"],
                source_url=(
                    "https://polymarket.com/event/"
                    + events[market["primary_event_id"]]["slug"]
                ),
                tags_json='["2026-fifa-world-cup"]',
                market_volume_usd_lifetime_reported=market.get("volume", 1000.0),
                is_active=market.get("is_active", True),
                is_closed=market.get("is_closed", False),
                is_resolved=market.get("is_resolved", False),
                winning_outcome=market.get("winning_outcome"),
                winning_clob_token_id=market.get("winning_clob_token_id"),
                market_family=market["family"],
                tournament_part=market["tournament_part"],
                scope_id=market["scope_id"],
                resolution_scope=market["resolution_scope"],
                resolution_period=market["resolution_period"],
                void_semantics=market.get("void_semantics", "unspecified"),
                sports_market_type=market.get("sports_market_type"),
                group_item_title=market.get("group_item_title"),
                group_item_threshold=market.get("group_item_threshold"),
                line=market.get("line"),
                normalized_threshold=market.get("normalized_threshold"),
                threshold_source=market.get("threshold_source"),
                start_at="2026-06-11T00:00:00+00:00",
                end_at="2026-07-19T23:59:59+00:00",
                logical_usable=market.get("logical_usable", True),
                outcomes_usable=market.get("outcomes_usable", True),
                tokens_usable=market.get("tokens_usable", True),
                quarantine_reason=market.get("quarantine_reason"),
                market_neg_risk_market_id=market.get("neg_risk_market_id"),
                market_neg_risk_request_id=market.get("neg_risk_request_id"),
                market_neg_risk_other=market.get("neg_risk_other"),
                primary_event_id=market["primary_event_id"],
                fifa_match_id=market.get("fifa_match_id"),
                subject_entity_ids=sorted(subject_entity_ids),
                participant_entity_ids=sorted(participant_entity_ids),
                player_national_team_entity_ids=sorted(
                    player_national_team_entity_ids
                ),
                referenced_entity_ids=sorted(referenced_entity_ids),
            )
        )
        for membership in market["event_memberships"]:
            event = events[membership["event_id"]]
            links.append(
                _row(
                    "market_events.parquet",
                    event_id=event["id"],
                    market_id=market["id"],
                    source_ordinal=membership["source_ordinal"],
                    is_enclosing_event=membership["is_enclosing_event"],
                    event_logical_eligible=event["eligible"],
                    event_membership_status=event["membership_status"],
                    event_ever_eligible=event["eligible"],
                    event_volume_unknown=False,
                    fifa_match_id=event.get("fifa_match_id"),
                    fixture_mapping_basis=event.get("fixture_mapping_basis"),
                    is_primary_qualifying_event=(
                        event["id"] == market["primary_event_id"]
                    ),
                )
            )
        for outcome_index, proposition in enumerate(propositions_spec):
            has_event_constraint = bool(proposition.get("event_constraint"))
            propositions.append(
                _row(
                    "propositions.parquet",
                    source_proposition_id=(
                        f"polymarket:condition:{market['condition_id']}:"
                        f"outcome:{outcome_index}"
                    ),
                    market_id=market["id"],
                    condition_id=market["condition_id"],
                    outcome_index=outcome_index,
                    outcome_label=proposition["label"],
                    clob_token_id=proposition.get("token"),
                    statement=f"{market['question']} [{proposition['label']}]",
                    market_family=market["family"],
                    tournament_part=market["tournament_part"],
                    scope_id=market["scope_id"],
                    resolution_scope=market["resolution_scope"],
                    resolution_period=market["resolution_period"],
                    void_semantics=market.get("void_semantics", "unspecified"),
                    predicate=proposition.get("predicate"),
                    predicate_subject_entity_id=proposition.get("subject"),
                    predicate_object=proposition.get("object"),
                    predicate_stage_rank=proposition.get("predicate_stage_rank"),
                    target_stage_key=proposition.get("target_stage_key"),
                    polarity=proposition["polarity"],
                    operator=proposition.get("operator"),
                    interval_lower=proposition.get("interval_lower"),
                    interval_upper=proposition.get("interval_upper"),
                    lower_inclusive=proposition.get("lower_inclusive"),
                    upper_inclusive=proposition.get("upper_inclusive"),
                    threshold_value=proposition.get("threshold_value"),
                    threshold_source=proposition.get(
                        "threshold_source", market.get("threshold_source")
                    ),
                    handicap_value=proposition.get("handicap_value"),
                    score_home=proposition.get("score_home"),
                    score_away=proposition.get("score_away"),
                    market_constraint_group_id=(
                        f"polymarket:market:{market['id']}:outcomes"
                    ),
                    market_constraint_kind="exactly_one",
                    market_constraint_complete=True,
                    event_constraint_group_id=(
                        _constraint_id(market) if has_event_constraint else None
                    ),
                    event_constraint_kind=(
                        "at_most_one" if has_event_constraint else None
                    ),
                    event_constraint_complete=False,
                    logical_usable=market.get("logical_usable", True),
                    semantic_usable=proposition.get("semantic_usable", True),
                )
            )
    return markets, links, propositions


def _entity_rows(spec: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        _row(
            "entities.parquet",
            **item,
            source="synthetic_logical_v1_fixture",
        )
        for item in spec["entities"]
    ]


def _proposition_entity_rows(
    propositions: list[dict[str, Any]],
    entities: list[dict[str, Any]],
    spec: dict[str, Any],
) -> list[dict[str, Any]]:
    entity_ids = {row["entity_id"] for row in entities}
    fixtures = {
        row["entity_id"]: row for row in entities if row["entity_type"] == "fixture"
    }
    player_teams = {
        row["player_entity_id"]: row["team_entity_id"]
        for row in spec.get("player_teams", [])
    }
    output: set[tuple[str, str, str]] = set()
    for proposition in propositions:
        proposition_id = proposition["source_proposition_id"]
        subject = proposition["predicate_subject_entity_id"]
        object_id = proposition["predicate_object"]
        if subject in entity_ids:
            output.add((proposition_id, subject, "subject"))
        if object_id in entity_ids:
            output.add((proposition_id, object_id, "referenced"))
        team_id = player_teams.get(subject)
        if team_id in entity_ids:
            output.add((proposition_id, team_id, "player_national_team"))
        scope_id = proposition["scope_id"]
        scoped_fixture_id = (
            "fixture:" + scope_id.rsplit(":", 1)[1]
            if scope_id and scope_id.startswith("scope:wc2026:fixture:")
            else None
        )
        fixture = (
            fixtures.get(scoped_fixture_id)
            or fixtures.get(subject)
            or fixtures.get(object_id)
        )
        if fixture is not None:
            output.add((proposition_id, fixture["entity_id"], "referenced"))
            group_entity_id = (
                f"group:{fixture['group_label'].lower()}"
                if fixture.get("group_label")
                else None
            )
            if group_entity_id in entity_ids:
                output.add((proposition_id, group_entity_id, "referenced"))
            output.add((proposition_id, fixture["home_team_entity_id"], "participant"))
            output.add((proposition_id, fixture["away_team_entity_id"], "participant"))
        stage_entity_id = f"stage:{proposition['tournament_part']}"
        context_entity_id = (
            stage_entity_id
            if stage_entity_id in entity_ids
            else "tournament:fifa_world_cup_2026"
        )
        if context_entity_id in entity_ids:
            output.add((proposition_id, context_entity_id, "referenced"))
    return [
        _row(
            "proposition_entities.parquet",
            source_proposition_id=proposition_id,
            entity_id=entity_id,
            entity_role=role,
        )
        for proposition_id, entity_id, role in sorted(output)
    ]


def _scope_rows(spec: dict[str, Any]) -> list[dict[str, Any]]:
    return [_row("scopes.parquet", **item) for item in spec["scopes"]]


def _sql_type(column: str) -> str:
    return physical_type(column)


def _create_relation(
    conn: duckdb.DuckDBPyConnection,
    name: str,
    filename: str,
    rows: list[dict[str, Any]],
) -> None:
    columns = EXPECTED_COLUMNS[filename]
    definitions = ", ".join(f'"{column}" {_sql_type(column)}' for column in columns)
    conn.execute(f'create table "{name}" ({definitions})')
    placeholders = ", ".join("?" for _ in columns)
    conn.executemany(
        f'insert into "{name}" values ({placeholders})',
        [tuple(row[column] for column in columns) for row in rows],
    )


def _scan_partitions(spec_sha256: str, spec: dict[str, Any]) -> dict[str, Any]:
    names = (
        "exact_2026_tag:open",
        "exact_2026_tag:closed",
        "related_2026_tag_recall:open",
        "related_2026_tag_recall:closed",
        "broad_fifa_world_cup_tag:open",
        "broad_fifa_world_cup_tag:closed",
        "soccer_fifwc_series:open",
        "soccer_fifwc_series:closed",
        "wc2026_event_slug_prefix_recall:open",
        "wc2026_event_slug_prefix_recall:closed",
    )
    event_count = len(spec["events"])
    child_market_count = len(spec["markets"])
    membership_count = sum(
        len(market["event_memberships"]) for market in spec["markets"]
    )
    return {
        name: {
            "attempts": [
                {
                    "attempt": attempt,
                    "pages": 1,
                    "event_count": event_count,
                    "event_ids_sha256": spec_sha256,
                    "child_market_count": child_market_count,
                    "membership_count": membership_count,
                    "membership_inventory_sha256": spec_sha256,
                    "event_payload_inventory_sha256": spec_sha256,
                }
                for attempt in (1, 2)
            ],
            "event_count": event_count,
            "event_ids_sha256": spec_sha256,
            "child_market_count": child_market_count,
            "membership_count": membership_count,
            "membership_inventory_sha256": spec_sha256,
            "event_payload_inventory_sha256": spec_sha256,
            "complete": True,
            "stable": True,
        }
        for name in names
    }


def materialize_fixture(
    spec_path: Path, output_dir: Path, *, verify_lock: bool = True
) -> dict[str, Any]:
    spec_path = spec_path.resolve()
    output_dir = output_dir.resolve()
    if output_dir.exists():
        raise FileExistsError(f"Output directory already exists: {output_dir}")
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    if spec["schema_version"] != "polymarket-wc2026-logical-v1":
        raise ValueError("Fixture must target logical-v1")
    source_sha256 = _source_sha256(spec_path)

    events = _event_rows(spec)
    markets, market_events, propositions = _market_rows(spec)
    entities = _entity_rows(spec)
    proposition_entities = _proposition_entity_rows(propositions, entities, spec)
    scopes = _scope_rows(spec)
    rows_by_file = {
        "events.parquet": events,
        "markets.parquet": markets,
        "market_events.parquet": market_events,
        "propositions.parquet": propositions,
        "entities.parquet": entities,
        "proposition_entities.parquet": proposition_entities,
        "scopes.parquet": scopes,
    }

    output_dir.mkdir(parents=True)
    with duckdb.connect(":memory:") as conn:
        conn.execute("SET TimeZone='UTC'")
        relations = {}
        for index, filename in enumerate(BUNDLE_FILES):
            name = f"fixture_{index}"
            _create_relation(conn, name, filename, rows_by_file[filename])
            relations[filename] = f'"{name}"'
        _validate_bundle_relationships(conn, relations)
        topology_fingerprint = _relation_fingerprint(conn, relations, TOPOLOGY_COLUMNS)
        semantic_fingerprint = _relation_fingerprint(conn, relations, SEMANTIC_COLUMNS)
        files = {}
        for filename, (_, order_columns) in BUNDLE_FILES.items():
            target = output_dir / filename
            conn.execute(
                f"copy (select {_typed_projection(filename)} "
                f"from {relations[filename]} "
                f"order by {', '.join(order_columns)}) "
                "to ? (format parquet, compression zstd)",
                [str(target)],
            )
            _validate_parquet_physical_schema(conn, target, filename)
            files[filename] = {
                "sha256": _sha256(target),
                "rows": len(rows_by_file[filename]),
                "bytes": target.stat().st_size,
            }

    scan_partitions = _scan_partitions(source_sha256, spec)
    data_quality = {
        "review_required_event_count": 0,
        "eligible_event_creation_gap_count": 0,
        "eligible_event_count": sum(row["event_logical_eligible"] for row in events),
        "eligible_event_display_issue_count": sum(
            row["event_logical_eligible"]
            and (
                not str(row.get("event_slug") or "").strip()
                or not str(row.get("event_title") or "").strip()
            )
            for row in events
        ),
        "audit_only_event_count": sum(
            not row["event_logical_eligible"] for row in events
        ),
        "orphan_reviewed_membership_count": 0,
        "volume_unknown_event_count": 0,
        "ineligible_market_membership_count": sum(
            not row["event_logical_eligible"] for row in market_events
        ),
        "only_ineligible_market_count": sum(
            not any(
                link["event_logical_eligible"]
                for link in market_events
                if link["market_id"] == market["market_id"]
            )
            for market in markets
        ),
        "unusable_market_count": sum(not row["logical_usable"] for row in markets),
        "unclassified_market_count": sum(
            row["market_family"] == "unclassified" for row in markets
        ),
        "semantic_unusable_proposition_count": sum(
            not row["semantic_usable"] for row in propositions
        ),
        "primary_event_issue_count": 0,
        "dangling_market_event_count": 0,
        "dangling_proposition_count": 0,
        "event_count": len(events),
        "market_count": len(markets),
        "market_event_count": len(market_events),
        "proposition_count": len(propositions),
        "entity_count": len(entities),
        "proposition_entity_count": len(proposition_entities),
        "scope_count": len(scopes),
    }
    seed_hashes = {
        path: _sha256(REPO_ROOT / path)
        for path in ("dbt/seeds/polymarket_wc2026_logical_contract.csv",)
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
    semantic_input_hashes = {
        name: source_sha256
        for name in (
            "relation/stg_polymarket_wc2026_event_snapshots/history",
            "relation/stg_polymarket_wc2026_event_markets/history",
            "relation/stg_polymarket_wc2026_event_market_payload_latest",
            "relation/stg_openfootball_wc2026_schedule_fixtures",
            "relation/reviewed_event_membership",
            "relation/int_polymarket_wc2026_logical_team_identities",
            "relation/wc2026_player_features",
        )
    }
    manifest = {
        "schema_version": spec["schema_version"],
        "contract_version": spec["contract_version"],
        "taxonomy_version": spec["taxonomy_version"],
        "membership_policy_version": spec["membership_policy_version"],
        "source": "oddsfox-pipeline",
        "scope": "wc2026",
        "pipeline_git_sha": spec["pipeline_git_sha"],
        "generated_at": spec["generated_at"],
        "as_of": spec["as_of"],
        "event_volume_field": "event.volume",
        "volume_threshold_usd": 100000.0,
        "volume_comparison": "greater_than_or_equal",
        "eligibility_mode": "ever_crossed",
        "eligibility_history": "retroactive_from_event_creation",
        "child_market_volume_floor_usd": None,
        "temporal_odds": False,
        "required_event_tag": "2026-fifa-world-cup",
        "recall_event_tag": "fifa-world-cup",
        "fixture_series_slug": "soccer-fifwc",
        "topology_fingerprint": topology_fingerprint,
        "semantic_fingerprint": semantic_fingerprint,
        "source_snapshot_fingerprint": _canonical_sha256(
            {filename: details["sha256"] for filename, details in sorted(files.items())}
        ),
        "scan_partitions": scan_partitions,
        "fixture_schedule": {
            "relation": "stg_openfootball_wc2026_schedule_fixtures",
            "relation_sha256": semantic_input_hashes[
                "relation/stg_openfootball_wc2026_schedule_fixtures"
            ],
            "row_count": 104,
            "distinct_match_count": 104,
            "match_id_min": 1,
            "match_id_max": 104,
            "openfootball_revision": OPENFOOTBALL_REVISION,
            "openfootball_files": OPENFOOTBALL_FILES,
            "fifa_schedule": {
                "url": FIFA_SCHEDULE_URL,
                "document_title": FIFA_SCHEDULE_TITLE,
                "sha256": FIFA_SCHEDULE_SHA256,
            },
        },
        "reviewed_membership": {
            "relation": "polymarket_wc2026_raw.reviewed_event_membership",
            "row_count": len(spec["events"]),
            "source_sha256": source_sha256,
            "relation_sha256": semantic_input_hashes[
                "relation/reviewed_event_membership"
            ],
        },
        "row_counts": {
            filename: details["rows"] for filename, details in files.items()
        },
        "data_quality": data_quality,
        "input_hashes": {
            **seed_hashes,
            **semantic_input_hashes,
            **scan_hashes,
        },
        "files": files,
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    if verify_lock:
        lock = json.loads(DEFAULT_LOCK.read_text(encoding="utf-8"))
        actual = {
            "fixture_version": spec["fixture_version"],
            "schema_version": spec["schema_version"],
            "source_sha256": source_sha256,
            "topology_fingerprint": topology_fingerprint,
            "semantic_fingerprint": semantic_fingerprint,
            "row_counts": manifest["row_counts"],
        }
        if lock != actual:
            raise RuntimeError(
                f"Fixture lock mismatch: expected={lock}, actual={actual}"
            )
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--skip-lock-check", action="store_true")
    args = parser.parse_args(argv)
    try:
        manifest = materialize_fixture(
            args.spec, args.output_dir, verify_lock=not args.skip_lock_check
        )
    except (duckdb.Error, FileExistsError, OSError, RuntimeError, ValueError) as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    print(
        f"Materialized logical-v1 fixture with "
        f"{manifest['row_counts']['propositions.parquet']} propositions"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
