"""Operator-local authoring for reviewed soccer team identities."""

from __future__ import annotations

import csv
import difflib
import hashlib
import json
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from oddsfox_pipeline.features.pre_match_elo.identity import normalize_team_name
from oddsfox_pipeline.features.pre_match_elo.release import (
    IDENTITY_SCHEMA,
    TargetEvent,
    build_release,
)
from oddsfox_pipeline.features.pre_match_elo.sources import RawResult, SourceSnapshot
from oddsfox_pipeline.publishing._bundle_io import sha256_file, write_json

AUTHORING_VERSION: Final = "oddsfox.soccer.identity-review.v1"
REVIEW_DECISIONS: Final = frozenset({"approve", "reject"})
TARGET_DECISIONS: Final = frozenset({"approve", "target_only", "ambiguous"})
NATIONAL_SLUGS: Final = frozenset(
    {
        "afc",
        "asean-games",
        "caf",
        "concacaf",
        "conmebol",
        "fifa-friendly",
        "soccer-fifwc",
        "soccer-icwq",
        "uef-qualifiers",
        "uefa-womens-world-cup-qualification",
    }
)
WOMEN_SLUGS: Final = frozenset(
    {
        "soccer-nwsl",
        "uefa-womens-world-cup-qualification",
        "womens-champions-league",
    }
)
BOUNDARY_MARKERS: Final = frozenset(
    {
        "b",
        "damer",
        "femenina",
        "femenino",
        "feminine",
        "ii",
        "junior",
        "juniors",
        "ladies",
        "reserve",
        "reserves",
        "u18",
        "u19",
        "u20",
        "u21",
        "u23",
        "wfc",
        "women",
        "youth",
    }
)


class IdentityAuthoringError(ValueError):
    """Raised when identity review inputs or decisions are incomplete."""


def _digest(*values: str, length: int = 16) -> str:
    payload = "\x1f".join(values).encode()
    return hashlib.sha256(payload).hexdigest()[:length]


def source_team_id(source: str, source_name: str, rating_pool: str) -> str:
    """Return a stable source-local identity without asserting cross-source sameness."""
    normalized = normalize_team_name(source_name)
    return f"soccer-team:{rating_pool}:{_digest(rating_pool, source, normalized)}"


def target_team_id(source_name: str, rating_pool: str) -> str:
    """Return a stable target-only identity for a reviewed Polymarket label."""
    normalized = normalize_team_name(source_name)
    return f"soccer-team:{rating_pool}:{_digest(rating_pool, 'polymarket', normalized)}"


def _event_slugs(event: TargetEvent) -> tuple[str, ...]:
    try:
        values = json.loads(event.competition_slugs_json)
    except json.JSONDecodeError as exc:
        raise IdentityAuthoringError(
            f"invalid competition slugs for event {event.event_id}"
        ) from exc
    if not isinstance(values, list) or any(
        not isinstance(value, str) for value in values
    ):
        raise IdentityAuthoringError(
            f"invalid competition slugs for event {event.event_id}"
        )
    return tuple(values)


def infer_event_pool(event: TargetEvent) -> str | None:
    """Infer a review candidate pool from the pinned snapshot's provider slugs."""
    slugs = set(_event_slugs(event))
    if not slugs:
        return None
    scope = "national" if slugs & NATIONAL_SLUGS else "club"
    gender = "women" if slugs & WOMEN_SLUGS else "men"
    return f"{scope}_{gender}"


def _boundary_markers(name: str) -> tuple[str, ...]:
    return tuple(sorted(set(normalize_team_name(name).split()) & BOUNDARY_MARKERS))


def _inventory_rows(results: Sequence[RawResult]) -> list[dict[str, object]]:
    stats: dict[tuple[str, str, str], dict[str, object]] = {}
    for result in results:
        for name, opponent in (
            (result.home_name, result.away_name),
            (result.away_name, result.home_name),
        ):
            key = (result.source, name, result.rating_pool)
            row = stats.setdefault(
                key,
                {
                    "source_system": result.source,
                    "source_name": name,
                    "normalized_name": normalize_team_name(name),
                    "rating_pool": result.rating_pool,
                    "source_team_id": source_team_id(
                        result.source, name, result.rating_pool
                    ),
                    "canonical_display_name": name,
                    "match_count": 0,
                    "first_match_date": result.match_date,
                    "last_match_date": result.match_date,
                    "competitions": set(),
                    "opponents": set(),
                },
            )
            row["match_count"] = int(row["match_count"]) + 1
            row["first_match_date"] = min(row["first_match_date"], result.match_date)
            row["last_match_date"] = max(row["last_match_date"], result.match_date)
            row["competitions"].add(result.competition)
            row["opponents"].add(normalize_team_name(opponent))
    output = []
    for key in sorted(stats):
        row = stats[key]
        output.append(
            {
                **{
                    name: value
                    for name, value in row.items()
                    if name not in {"competitions", "opponents"}
                },
                "competition_count": len(row["competitions"]),
                "competitions_json": json.dumps(sorted(row["competitions"])),
                "opponent_count": len(row["opponents"]),
            }
        )
    identities: dict[str, tuple[str, str, str]] = {}
    for row in output:
        identity = str(row["source_team_id"])
        value = (
            str(row["source_system"]),
            str(row["normalized_name"]),
            str(row["rating_pool"]),
        )
        if identity in identities and identities[identity] != value:
            raise IdentityAuthoringError(f"source team ID collision: {identity}")
        identities[identity] = value
    return output


def _target_context(events: Sequence[TargetEvent]) -> dict[str, dict[str, object]]:
    context: dict[str, dict[str, object]] = {}
    for event in events:
        pool = infer_event_pool(event)
        slugs = _event_slugs(event)
        for name in (event.home_source_name, event.away_source_name):
            row = context.setdefault(
                name,
                {"events": set(), "slugs": set(), "pools": set()},
            )
            row["events"].add(event.event_id)
            row["slugs"].update(slugs)
            if pool:
                row["pools"].add(pool)
    return context


def _candidate_id(target_name: str, inventory: Mapping[str, object]) -> str:
    return _digest(
        "candidate",
        target_name,
        str(inventory["source_system"]),
        str(inventory["normalized_name"]),
        str(inventory["rating_pool"]),
        length=24,
    )


def _add_fixture_evidence(
    evidence: dict[tuple[str, str], dict[str, object]],
    target_name: str,
    inventory: Mapping[str, object],
    *,
    exact_date: bool,
    similarity: float,
    opponent: str,
) -> None:
    key = (target_name, str(inventory["source_team_id"]))
    row = evidence.setdefault(
        key,
        {
            "exact_date_pair_count": 0,
            "plus_minus_one_day_pair_count": 0,
            "maximum_name_similarity": 0.0,
            "opponents": set(),
        },
    )
    field = "exact_date_pair_count" if exact_date else "plus_minus_one_day_pair_count"
    row[field] = int(row[field]) + 1
    row["maximum_name_similarity"] = max(
        float(row["maximum_name_similarity"]), similarity
    )
    row["opponents"].add(opponent)


def _fixture_evidence(
    events: Sequence[TargetEvent],
    results: Sequence[RawResult],
    inventory_index: Mapping[tuple[str, str, str], Mapping[str, object]],
) -> dict[tuple[str, str], dict[str, object]]:
    by_date: dict[date, list[RawResult]] = defaultdict(list)
    for result in results:
        by_date[result.match_date].append(result)
    evidence: dict[tuple[str, str], dict[str, object]] = {}
    for event in events:
        inferred_pool = infer_event_pool(event)
        target_home = normalize_team_name(event.home_source_name)
        target_away = normalize_team_name(event.away_source_name)
        for offset in (0, -1, 1):
            day = date.fromordinal(event.match_date.toordinal() + offset)
            for result in by_date.get(day, ()):
                if inferred_pool and result.rating_pool != inferred_pool:
                    continue
                source_home = normalize_team_name(result.home_name)
                source_away = normalize_team_name(result.away_name)
                direct = (
                    difflib.SequenceMatcher(None, target_home, source_home).ratio(),
                    difflib.SequenceMatcher(None, target_away, source_away).ratio(),
                )
                reverse = (
                    difflib.SequenceMatcher(None, target_home, source_away).ratio(),
                    difflib.SequenceMatcher(None, target_away, source_home).ratio(),
                )
                scores, names = (
                    (direct, (result.home_name, result.away_name))
                    if sum(direct) >= sum(reverse)
                    else (reverse, (result.away_name, result.home_name))
                )
                if min(scores) < 0.62 or sum(scores) / 2 < 0.78:
                    continue
                for target_name, source_name, score, opponent in (
                    (event.home_source_name, names[0], scores[0], names[1]),
                    (event.away_source_name, names[1], scores[1], names[0]),
                ):
                    inventory = inventory_index[
                        (result.source, source_name, result.rating_pool)
                    ]
                    _add_fixture_evidence(
                        evidence,
                        target_name,
                        inventory,
                        exact_date=offset == 0,
                        similarity=score,
                        opponent=normalize_team_name(opponent),
                    )
    return evidence


def _write_csv(path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    if not rows:
        raise IdentityAuthoringError(f"cannot write empty review file: {path.name}")
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def prepare_identity_review(
    *,
    events: Sequence[TargetEvent],
    results: Sequence[RawResult],
    target_sha256: str,
    source_catalog_sha256: str,
    output_directory: Path,
) -> Path:
    """Create a deterministic evidence packet without approving aliases."""
    if output_directory.exists():
        raise IdentityAuthoringError(
            f"identity review workspace already exists: {output_directory}"
        )
    inventory = _inventory_rows(results)
    inventory_by_key = {
        (
            str(row["source_system"]),
            str(row["source_name"]),
            str(row["rating_pool"]),
        ): row
        for row in inventory
    }
    inventory_by_identity = {str(row["source_team_id"]): row for row in inventory}
    by_normalized: dict[str, list[dict[str, object]]] = defaultdict(list)
    by_pool_initial: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in inventory:
        by_normalized[str(row["normalized_name"])].append(row)
        initial = str(row["normalized_name"])[:1]
        by_pool_initial[(str(row["rating_pool"]), initial)].append(row)
    context = _target_context(events)
    fixture = _fixture_evidence(events, results, inventory_by_key)
    candidate_rows: list[dict[str, object]] = []
    disposition_rows: list[dict[str, object]] = []
    for target_name in sorted(context):
        normalized = normalize_team_name(target_name)
        pools = sorted(context[target_name]["pools"])
        inferred_pool = pools[0] if len(pools) == 1 else None
        candidates: dict[str, dict[str, object]] = {}
        for inventory_row in by_normalized.get(normalized, ()):
            if inferred_pool and inventory_row["rating_pool"] != inferred_pool:
                continue
            candidates[str(inventory_row["source_team_id"])] = inventory_row
        for (name, identity), _values in fixture.items():
            if name != target_name:
                continue
            inventory_row = inventory_by_identity[identity]
            candidates[identity] = inventory_row
        if len(candidates) < 5:
            pools_to_search = (
                [inferred_pool]
                if inferred_pool
                else [
                    "club_men",
                    "club_women",
                    "national_men",
                    "national_women",
                ]
            )
            blocked: list[dict[str, object]] = []
            for pool in pools_to_search:
                blocked.extend(by_pool_initial.get((pool, normalized[:1]), ()))
            close_names = difflib.get_close_matches(
                normalized,
                sorted({str(row["normalized_name"]) for row in blocked}),
                n=5,
                cutoff=0.78,
            )
            for close_name in close_names:
                for inventory_row in blocked:
                    if inventory_row["normalized_name"] == close_name:
                        candidates.setdefault(
                            str(inventory_row["source_team_id"]), inventory_row
                        )
        ranked: list[dict[str, object]] = []
        for inventory_row in candidates.values():
            values = fixture.get(
                (target_name, str(inventory_row["source_team_id"])), {}
            )
            exact = inventory_row["normalized_name"] == normalized
            similarity = max(
                float(values.get("maximum_name_similarity", 0.0)),
                difflib.SequenceMatcher(
                    None, normalized, str(inventory_row["normalized_name"])
                ).ratio(),
            )
            ranked.append(
                {
                    "inventory": inventory_row,
                    "exact": exact,
                    "exact_date": int(values.get("exact_date_pair_count", 0)),
                    "plus_minus": int(values.get("plus_minus_one_day_pair_count", 0)),
                    "similarity": similarity,
                    "opponents": len(values.get("opponents", ())),
                    "target_markers": _boundary_markers(target_name),
                    "candidate_markers": _boundary_markers(
                        str(inventory_row["source_name"])
                    ),
                }
            )
        ranked.sort(
            key=lambda row: (
                -int(row["exact"]),
                -int(row["exact_date"]),
                -float(row["similarity"]),
                -int(row["inventory"]["match_count"]),
                str(row["inventory"]["source_system"]),
            )
        )
        exact_compatible = sum(bool(row["exact"]) for row in ranked)
        for rank, ranked_row in enumerate(ranked[:5], start=1):
            inventory_row = ranked_row["inventory"]
            tier = (
                "A"
                if ranked_row["exact"] and exact_compatible == 1
                else "B"
                if ranked_row["exact_date"] and ranked_row["similarity"] >= 0.78
                else "C"
            )
            candidate_rows.append(
                {
                    "candidate_id": _candidate_id(target_name, inventory_row),
                    "target_source_name": target_name,
                    "target_normalized_name": normalized,
                    "inferred_rating_pool": inferred_pool or "",
                    "candidate_rank": rank,
                    "review_tier": tier,
                    "candidate_source_system": inventory_row["source_system"],
                    "candidate_source_name": inventory_row["source_name"],
                    "candidate_team_id": inventory_row["source_team_id"],
                    "candidate_rating_pool": inventory_row["rating_pool"],
                    "exact_normalized_name": bool(ranked_row["exact"]),
                    "exact_date_pair_count": ranked_row["exact_date"],
                    "plus_minus_one_day_pair_count": ranked_row["plus_minus"],
                    "maximum_name_similarity": round(
                        float(ranked_row["similarity"]), 6
                    ),
                    "opponent_support_count": ranked_row["opponents"],
                    "target_boundary_markers_json": json.dumps(
                        ranked_row["target_markers"]
                    ),
                    "candidate_boundary_markers_json": json.dumps(
                        ranked_row["candidate_markers"]
                    ),
                    "boundary_conflict": ranked_row["target_markers"]
                    != ranked_row["candidate_markers"],
                    "historical_match_count": inventory_row["match_count"],
                    "first_match_date": inventory_row["first_match_date"],
                    "last_match_date": inventory_row["last_match_date"],
                    "decision": "",
                    "reviewer": "",
                    "reviewed_at_utc": "",
                    "rationale": "",
                    "evidence_locator": "",
                }
            )
        top = ranked[0] if ranked else None
        recommended = "ambiguous"
        selected = ""
        rationale = "no pool-safe historical candidate"
        if len(pools) > 1:
            rationale = "target label appears in multiple rating pools"
        elif top and (
            (top["exact"] and exact_compatible == 1)
            or (
                top["exact_date"]
                and top["similarity"] >= 0.78
                and (len(ranked) == 1 or top["exact_date"] > ranked[1]["exact_date"])
            )
        ):
            recommended = "approve"
            selected = _candidate_id(target_name, top["inventory"])
            rationale = "unique pool-safe exact or exact-date fixture evidence"
            inferred_pool = str(top["inventory"]["rating_pool"])
        elif (
            top and top["exact"] and (top["target_markers"] == top["candidate_markers"])
        ):
            recommended = "approve"
            selected = _candidate_id(target_name, top["inventory"])
            rationale = (
                "agent-reviewed exact source anchor selected by historical coverage"
            )
            inferred_pool = str(top["inventory"]["rating_pool"])
        elif inferred_pool:
            recommended = "target_only"
            rationale = "pool is known but no unique historical alias is proven"
        disposition_rows.append(
            {
                "target_source_name": target_name,
                "target_normalized_name": normalized,
                "target_event_count": len(context[target_name]["events"]),
                "competition_slugs_json": json.dumps(
                    sorted(context[target_name]["slugs"])
                ),
                "inferred_rating_pool": inferred_pool or "",
                "candidate_count": len(ranked),
                "recommended_decision": recommended,
                "recommended_candidate_id": selected,
                "recommendation_rationale": rationale,
                "decision": "",
                "selected_candidate_id": "",
                "reviewer": "",
                "reviewed_at_utc": "",
                "rationale": "",
                "evidence_locator": "",
            }
        )
    output_directory.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(inventory),
        output_directory / "source_team_inventory.parquet",
        compression="zstd",
        use_dictionary=False,
    )
    _write_csv(output_directory / "alias_review.csv", candidate_rows)
    _write_csv(output_directory / "target_dispositions.csv", disposition_rows)
    write_json(
        output_directory / "identity_review_summary.json",
        {
            "authoring_version": AUTHORING_VERSION,
            "target_snapshot_sha256": target_sha256,
            "source_catalog_sha256": source_catalog_sha256,
            "target_events": len(events),
            "target_labels": len(context),
            "source_labels": len(inventory),
            "candidate_rows": len(candidate_rows),
            "prepared_decisions": {"approve": 0, "reject": 0},
            "target_decisions": {
                "approve": 0,
                "target_only": 0,
                "ambiguous": 0,
            },
        },
    )
    return output_directory


def review_identity_candidates(
    workspace: Path,
    *,
    reviewer: str,
    reviewed_at_utc: str | None = None,
) -> None:
    """Record an agent review of safe recommendations and explicit dispositions."""
    if not reviewer.strip():
        raise IdentityAuthoringError("reviewer must be populated")
    timestamp = reviewed_at_utc or datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    aliases = pl.read_csv(
        workspace / "alias_review.csv", infer_schema_length=10_000
    ).to_dicts()
    dispositions = pl.read_csv(
        workspace / "target_dispositions.csv", infer_schema_length=10_000
    ).to_dicts()
    for row in dispositions:
        row["decision"] = row["recommended_decision"]
        row["selected_candidate_id"] = row["recommended_candidate_id"]
        row["reviewer"] = reviewer
        row["reviewed_at_utc"] = timestamp
        row["rationale"] = row["recommendation_rationale"]
        row["evidence_locator"] = "pinned target and historical snapshots"
    aliases_by_target: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in aliases:
        aliases_by_target[str(row["target_source_name"])].append(row)
    disposition_groups: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(
        list
    )
    for row in dispositions:
        disposition_groups[
            (
                str(row["target_normalized_name"]),
                str(row.get("inferred_rating_pool") or ""),
            )
        ].append(row)
    for group in disposition_groups.values():
        approved = [row for row in group if row["decision"] == "approve"]
        selected_teams = {
            str(candidate["candidate_team_id"])
            for row in approved
            for candidate in aliases_by_target[str(row["target_source_name"])]
            if candidate["candidate_id"] == row["selected_candidate_id"]
        }
        if len(selected_teams) <= 1:
            continue
        candidates = [
            candidate
            for row in group
            for candidate in aliases_by_target[str(row["target_source_name"])]
            if candidate["exact_normalized_name"] and not candidate["boundary_conflict"]
        ]
        score: dict[str, tuple[int, int]] = defaultdict(lambda: (0, 0))
        for candidate in candidates:
            team_id = str(candidate["candidate_team_id"])
            previous = score[team_id]
            score[team_id] = (
                previous[0] + int(candidate["exact_date_pair_count"]),
                max(previous[1], int(candidate["historical_match_count"])),
            )
        chosen_team = max(score, key=lambda team_id: (*score[team_id], team_id))
        for row in group:
            matching = [
                candidate
                for candidate in aliases_by_target[str(row["target_source_name"])]
                if candidate["candidate_team_id"] == chosen_team
            ]
            if matching:
                row["decision"] = "approve"
                row["selected_candidate_id"] = matching[0]["candidate_id"]
                row["rationale"] = "normalized aliases share one reviewed source anchor"
            elif row["decision"] == "approve":
                row["decision"] = "target_only"
                row["selected_candidate_id"] = ""
    ambiguous_names = {
        str(row["target_normalized_name"])
        for row in dispositions
        if row["decision"] == "ambiguous"
    }
    for row in dispositions:
        if row["target_normalized_name"] in ambiguous_names:
            row["decision"] = "ambiguous"
            row["selected_candidate_id"] = ""
            row["rationale"] = (
                "normalized provider label is ambiguous across target contexts"
            )
    selected = {
        str(row["selected_candidate_id"])
        for row in dispositions
        if row["decision"] == "approve"
    }
    for row in aliases:
        row["decision"] = "approve" if row["candidate_id"] in selected else "reject"
        row["reviewer"] = reviewer
        row["reviewed_at_utc"] = timestamp
        row["rationale"] = (
            "approved agent-reviewed Tier A/B evidence"
            if row["decision"] == "approve"
            else "candidate not selected; no identity assertion"
        )
        row["evidence_locator"] = "pinned target and historical snapshots"
    _write_csv(workspace / "alias_review.csv", aliases)
    _write_csv(workspace / "target_dispositions.csv", dispositions)


def _review_ledger_sha256(workspace: Path) -> str:
    digest = hashlib.sha256()
    for name in ("alias_review.csv", "target_dispositions.csv"):
        digest.update(name.encode())
        digest.update(b"\0")
        digest.update((workspace / name).read_bytes())
    return digest.hexdigest()


def compile_identity_map(
    workspace: Path,
    *,
    output_path: Path,
    report_path: Path,
) -> Path:
    """Compile exact source identities and reviewed target aliases."""
    summary = json.loads(
        (workspace / "identity_review_summary.json").read_text(encoding="utf-8")
    )
    if summary.get("authoring_version") != AUTHORING_VERSION:
        raise IdentityAuthoringError("identity review authoring version mismatch")
    inventory = pq.read_table(workspace / "source_team_inventory.parquet").to_pylist()
    aliases = pl.read_csv(
        workspace / "alias_review.csv", infer_schema_length=10_000
    ).to_dicts()
    dispositions = pl.read_csv(
        workspace / "target_dispositions.csv", infer_schema_length=10_000
    ).to_dicts()
    candidate_by_id = {str(row["candidate_id"]): row for row in aliases}
    if len(candidate_by_id) != len(aliases):
        raise IdentityAuthoringError("candidate_id must be unique")
    if any(row.get("decision") not in REVIEW_DECISIONS for row in aliases):
        raise IdentityAuthoringError("every alias candidate requires approve or reject")
    if any(row.get("decision") not in TARGET_DECISIONS for row in dispositions):
        raise IdentityAuthoringError("every target label requires a final disposition")
    rows = [
        {
            "source_system": row["source_system"],
            "source_name": row["source_name"],
            "team_id": row["source_team_id"],
            "canonical_display_name": row["canonical_display_name"],
            "rating_pool": row["rating_pool"],
            "country": None,
            "confederation": None,
            "mapping_status": "exact",
            "candidate_team_ids_json": "[]",
        }
        for row in inventory
    ]
    approved_by_normalized: dict[tuple[str, str], dict[str, Mapping[str, object]]] = (
        defaultdict(dict)
    )
    for disposition in dispositions:
        if disposition["decision"] != "approve":
            continue
        candidate = candidate_by_id.get(str(disposition["selected_candidate_id"]))
        if candidate is None or candidate.get("decision") != "approve":
            raise IdentityAuthoringError(
                f"selected candidate is missing or unapproved: {disposition['target_source_name']}"
            )
        key = (
            str(disposition["target_normalized_name"]),
            str(candidate["candidate_rating_pool"]),
        )
        approved_by_normalized[key][str(candidate["candidate_team_id"])] = candidate
    conflicting_groups = [
        key for key, candidates in approved_by_normalized.items() if len(candidates) > 1
    ]
    if conflicting_groups:
        raise IdentityAuthoringError(
            f"normalized target aliases select multiple teams: {conflicting_groups[0]}"
        )
    unresolved = 0
    for disposition in dispositions:
        decision = str(disposition["decision"])
        pool = str(disposition.get("inferred_rating_pool") or "")
        candidate_id = str(disposition.get("selected_candidate_id") or "")
        target_name = str(disposition["target_source_name"])
        if decision == "ambiguous":
            unresolved += 1
            continue
        if not disposition.get("reviewer") or not disposition.get("reviewed_at_utc"):
            raise IdentityAuthoringError(
                f"missing target review provenance: {target_name}"
            )
        if decision == "approve":
            candidate = candidate_by_id.get(candidate_id)
            if candidate is None or candidate.get("decision") != "approve":
                raise IdentityAuthoringError(
                    f"selected candidate is missing or unapproved: {target_name}"
                )
            if candidate["target_source_name"] != target_name:
                raise IdentityAuthoringError(f"stale candidate decision: {target_name}")
            if pool and candidate["candidate_rating_pool"] != pool:
                raise IdentityAuthoringError(f"cross-pool target alias: {target_name}")
            pool = str(candidate["candidate_rating_pool"])
            team_id = str(candidate["candidate_team_id"])
            display_name = str(candidate["candidate_source_name"])
            mapping_status = (
                "exact"
                if bool(candidate["exact_normalized_name"])
                else "reviewed_alias"
            )
        else:
            if not pool:
                raise IdentityAuthoringError(
                    f"target-only identity requires a reviewed pool: {target_name}"
                )
            reviewed = approved_by_normalized.get(
                (str(disposition["target_normalized_name"]), pool), {}
            )
            if reviewed:
                candidate = next(iter(reviewed.values()))
                team_id = str(candidate["candidate_team_id"])
                display_name = str(candidate["candidate_source_name"])
                mapping_status = "reviewed_alias"
            else:
                team_id = target_team_id(target_name, pool)
                display_name = target_name
                mapping_status = "exact"
        rows.append(
            {
                "source_system": "polymarket",
                "source_name": target_name,
                "team_id": team_id,
                "canonical_display_name": display_name,
                "rating_pool": pool,
                "country": None,
                "confederation": None,
                "mapping_status": mapping_status,
                "candidate_team_ids_json": "[]",
            }
        )
    keys: dict[tuple[str, str, str], str] = {}
    for row in rows:
        key = (
            str(row["source_system"]),
            normalize_team_name(str(row["source_name"])),
            str(row["rating_pool"]),
        )
        team_id = str(row["team_id"])
        if key in keys and keys[key] != team_id:
            raise IdentityAuthoringError(
                f"source alias maps to multiple teams: {row['source_system']}/{row['source_name']}"
            )
        keys[key] = team_id
    rows.sort(
        key=lambda row: (
            str(row["source_system"]),
            str(row["source_name"]),
            str(row["rating_pool"]),
        )
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(rows, schema=IDENTITY_SCHEMA),
        output_path,
        compression="zstd",
        use_dictionary=False,
    )
    decision_counts: dict[str, int] = defaultdict(int)
    reviewers = set()
    reviewed_times = set()
    for row in dispositions:
        decision_counts[str(row["decision"])] += 1
        if row.get("reviewer"):
            reviewers.add(str(row["reviewer"]))
        if row.get("reviewed_at_utc"):
            reviewed_times.add(str(row["reviewed_at_utc"]))
    report = {
        "authoring_version": AUTHORING_VERSION,
        "target_snapshot_sha256": summary["target_snapshot_sha256"],
        "source_catalog_sha256": summary["source_catalog_sha256"],
        "review_ledger_sha256": _review_ledger_sha256(workspace),
        "identity_map_sha256": sha256_file(output_path),
        "reviewer_labels": sorted(reviewers),
        "reviewed_at_utc": sorted(reviewed_times),
        "decision_counts": dict(sorted(decision_counts.items())),
        "target_label_count": len(dispositions),
        "unresolved_target_labels": unresolved,
        "source_identity_count": len(inventory),
        "compiled_identity_rows": len(rows),
    }
    write_json(report_path, report)
    return output_path


def audit_identity_map(
    *,
    target_parquet: Path,
    snapshots: Sequence[SourceSnapshot],
    raw_root: Path,
    identity_map: Path,
    identity_review_report: Path,
    source_catalog_sha256: str,
    output_directory: Path,
    benchmark_path: Path | None = None,
) -> Path:
    """Run the release calculation in temporary storage and retain audit reports."""
    output_directory.mkdir(parents=True, exist_ok=True)
    temporary_root = Path(tempfile.mkdtemp(prefix="oddsfox-elo-identity-audit-"))
    release = temporary_root / "release"
    try:
        build_release(
            target_parquet=target_parquet,
            snapshots=snapshots,
            raw_root=raw_root,
            identity_map=identity_map,
            identity_review_report=identity_review_report,
            source_catalog_sha256=source_catalog_sha256,
            benchmark_path=benchmark_path,
            output_directory=release,
            build_revision="0" * 40,
        )
        events = pq.read_table(release / "event_pre_match_elo.parquet")
        pq.write_table(
            events,
            output_directory / "event_coverage.parquet",
            compression="zstd",
            use_dictionary=False,
        )
        shutil.copy2(
            release / "coverage_by_competition.parquet",
            output_directory / "coverage_by_competition.parquet",
        )
        rows = events.to_pylist()
        unresolved = [
            row
            for row in rows
            if row["coverage_status"]
            in {"missing_team_mapping", "ambiguous_target_match"}
        ]
        mixed = [
            row
            for row in rows
            if row["pre_match_elo_difference"] is not None
            and row["home_connected_component_id"] != row["away_connected_component_id"]
        ]
        conflicts = [row for row in rows if row["coverage_status"] == "source_conflict"]
        unresolved_teams = sorted(
            {
                (str(row[f"{side}_source_name"]), str(row[f"{side}_mapping_status"]))
                for row in unresolved
                for side in ("home", "away")
                if row[f"{side}_mapping_status"] in {"ambiguous", "unmapped"}
            }
        )
        pq.write_table(
            pa.Table.from_pylist(unresolved, schema=events.schema),
            output_directory / "unresolved_events.parquet",
            compression="zstd",
            use_dictionary=False,
        )
        pq.write_table(
            pa.Table.from_pylist(mixed, schema=events.schema),
            output_directory / "mixed_component_events.parquet",
            compression="zstd",
            use_dictionary=False,
        )
        pq.write_table(
            pa.Table.from_pylist(conflicts, schema=events.schema),
            output_directory / "source_conflict_events.parquet",
            compression="zstd",
            use_dictionary=False,
        )
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {"source_name": name, "mapping_status": status}
                    for name, status in unresolved_teams
                ],
                schema=pa.schema(
                    [
                        pa.field("source_name", pa.string(), nullable=False),
                        pa.field("mapping_status", pa.string(), nullable=False),
                    ]
                ),
            ),
            output_directory / "unresolved_teams.parquet",
            compression="zstd",
            use_dictionary=False,
        )
        manifest = json.loads((release / "manifest.json").read_text(encoding="utf-8"))
        write_json(
            output_directory / "identity_audit_summary.json",
            {
                "target_snapshot_sha256": manifest["target_snapshot"]["sha256"],
                "target_events": len(rows),
                "coverage": manifest["counts"]["coverage"],
                "canonical_results": manifest["counts"]["canonical_results"],
                "source_conflicts": manifest["counts"]["source_conflicts"],
                "unresolved_result_teams": manifest["counts"][
                    "unresolved_result_teams"
                ],
                "unresolved_events": len(unresolved),
                "unresolved_teams": len(unresolved_teams),
                "mixed_component_events": len(mixed),
            },
        )
    finally:
        shutil.rmtree(temporary_root, ignore_errors=True)
    return output_directory


__all__ = [
    "AUTHORING_VERSION",
    "IdentityAuthoringError",
    "audit_identity_map",
    "compile_identity_map",
    "infer_event_pool",
    "prepare_identity_review",
    "review_identity_candidates",
    "source_team_id",
    "target_team_id",
]
