"""Build the immutable WC2026 stage-market minute strategy input release."""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Final, Mapping

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from oddsfox_pipeline.config.settings_warehouse import BASE_DIR
from oddsfox_pipeline.contracts.raw_snapshots import schema_fingerprint
from oddsfox_pipeline.publishing._bundle_io import (
    COMMIT_RE,
    current_clean_commit,
    sha256_file,
    validate_dataset_version,
    write_checksums,
    write_json,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import polymarket_ops_tbl
from oddsfox_pipeline.storage.minute_odds_snapshots import (
    active_snapshot_dir,
    minute_odds_snapshot_root,
    validate_minute_odds_snapshot,
)

CONTRACT_VERSION: Final = "oddsfox.polymarket_wc2026.stage_minute.v1"
RELEASE_VERSION: Final = "1.0.0"
DEFAULT_OUTPUT_ROOT: Final = (
    BASE_DIR / "artifacts" / "strategy-inputs" / "polymarket_wc2026_stage_minute"
)
FAMILY_BY_TITLE: Final = {
    "World Cup Winner": ("champion", "Champion"),
    "World Cup: Nation to Reach Final": ("final", "Final"),
    "World Cup: Nation To Reach Semifinals": ("semifinals", "Semifinals"),
    "World Cup: Nation To Reach Quarterfinals": (
        "quarterfinals",
        "Quarterfinals",
    ),
    "World Cup: Nation To Reach Round of 16": ("round_of_16", "Round of 16"),
    "World Cup: Team to advance to Knockout Stages": (
        "round_of_32",
        "Round of 32",
    ),
}
EXPECTED_TEAMS: Final = 48
EXPECTED_MARKETS: Final = 288
EXPECTED_TOKENS: Final = 576
EXPECTED_IMPLICATIONS: Final = 528
ALLOWED_RULES: Final = {
    "wc.stage_monotonicity",
    "wc.champion_reaches_final",
}
DETERMINISTIC_NODE_METHODS: Final = {
    "deterministic",
    "official_bracket",
    "proposition_compiler",
}
OUTPUT_FILES: Final = {
    "token_minute_ohlc.parquet",
    "outcomes.parquet",
    "implications.parquet",
    "coverage.parquet",
    "SCHEMA.json",
    "MANIFEST.json",
    "CHECKSUMS.sha256",
}

_NODE_COLUMNS: Final = {
    "canonical_id",
    "type",
    "label",
    "confidence",
    "evidence_market_ids",
    "inference_method",
    "proposition_json",
}
_EDGE_COLUMNS: Final = {
    "source_id",
    "target_id",
    "edge_type",
    "confidence",
    "inference_method",
    "derivation_type",
    "rule_id",
    "rule_version",
}


@dataclass(frozen=True)
class StageMinuteReleaseSpec:
    dataset_version: str
    graph_revision: str

    def __post_init__(self) -> None:
        validate_dataset_version(self.dataset_version)
        if self.dataset_version != RELEASE_VERSION:
            raise ValueError(f"dataset_version must be {RELEASE_VERSION}")
        if not COMMIT_RE.fullmatch(self.graph_revision):
            raise ValueError("graph_revision must be a lowercase 40-character Git SHA")


def _json_list(value: Any, *, field: str) -> list[str]:
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{field} must be a JSON string array") from exc
    if not isinstance(parsed, list) or any(
        not isinstance(item, str) for item in parsed
    ):
        raise ValueError(f"{field} must be a JSON string array")
    return parsed


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _parquet_columns(path: Path) -> set[str]:
    return set(pq.ParquetFile(path).schema_arrow.names)


def _market_id_from_outcome(node: Mapping[str, Any]) -> str:
    evidence = node.get("evidence_market_ids")
    if not isinstance(evidence, list) or len(evidence) != 1:
        raise ValueError(
            f"outcome {node.get('canonical_id')} must evidence exactly one market"
        )
    return str(evidence[0])


def _read_semantic_inputs(
    nodes_path: Path,
    edges_path: Path,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    nodes_path = nodes_path.resolve()
    edges_path = edges_path.resolve()
    for path, required in ((nodes_path, _NODE_COLUMNS), (edges_path, _EDGE_COLUMNS)):
        if not path.is_file():
            raise FileNotFoundError(path)
        missing = required - _parquet_columns(path)
        if missing:
            raise ValueError(f"{path.name} is missing columns: {sorted(missing)}")

    node_rows = pq.read_table(nodes_path).to_pylist()
    entities = {
        str(node.get("canonical_id")): node
        for node in node_rows
        if str(node.get("type")) in {"TEAM", "STAGE"}
    }
    outcomes: dict[str, dict[str, Any]] = {}
    for node in node_rows:
        if str(node.get("type")) != "OUTCOME":
            continue
        if float(node.get("confidence") or 0) != 1.0:
            raise ValueError(f"outcome {node.get('canonical_id')} confidence must be 1")
        if node.get("inference_method") not in DETERMINISTIC_NODE_METHODS:
            raise ValueError(f"outcome {node.get('canonical_id')} is not deterministic")
        raw_prop = node.get("proposition_json")
        if raw_prop is None:
            raise ValueError(
                f"outcome {node.get('canonical_id')} has no proposition_json"
            )
        # Resolution keeps the exact-ID outcome's original method, so the
        # proposition payload is the authoritative compiler boundary.
        try:
            proposition = json.loads(str(raw_prop))
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"outcome {node.get('canonical_id')} has invalid proposition_json"
            ) from exc
        if not isinstance(proposition, dict):
            raise ValueError("proposition_json must contain an object")
        predicate = proposition.get("predicate")
        arguments = proposition.get("arguments")
        polarity = proposition.get("polarity")
        if (
            predicate not in {"wins_competition", "reaches_stage"}
            or not isinstance(arguments, dict)
            or not isinstance(polarity, bool)
            or not arguments.get("team")
            or not arguments.get("competition")
        ):
            raise ValueError(
                f"outcome {node.get('canonical_id')} has unsupported proposition"
            )
        if arguments["competition"] != "competition:world-cup-2026":
            raise ValueError(
                f"outcome {node.get('canonical_id')} targets another competition"
            )
        if predicate == "reaches_stage" and not arguments.get("stage"):
            raise ValueError(f"outcome {node.get('canonical_id')} has no stage")
        team_node = entities.get(str(arguments["team"]))
        if (
            team_node is None
            or team_node.get("type") != "TEAM"
            or float(team_node.get("confidence") or 0) != 1.0
            or team_node.get("inference_method") not in DETERMINISTIC_NODE_METHODS
        ):
            raise ValueError(
                f"outcome {node.get('canonical_id')} has no deterministic TEAM endpoint"
            )
        stage_node = None
        if predicate == "reaches_stage":
            stage_node = entities.get(str(arguments["stage"]))
            if (
                stage_node is None
                or stage_node.get("type") != "STAGE"
                or float(stage_node.get("confidence") or 0) != 1.0
                or stage_node.get("inference_method") not in DETERMINISTIC_NODE_METHODS
            ):
                raise ValueError(
                    f"outcome {node.get('canonical_id')} has no deterministic STAGE endpoint"
                )
        canonical_id = str(node.get("canonical_id"))
        if canonical_id in outcomes:
            raise ValueError(f"duplicate outcome canonical_id: {canonical_id}")
        outcomes[canonical_id] = {
            **node,
            "market_id": _market_id_from_outcome(node),
            "proposition": proposition,
            "graph_team_label": str(team_node["label"]),
            "graph_stage_label": (
                str(stage_node["label"]) if stage_node is not None else None
            ),
        }

    edge_rows = pq.read_table(edges_path).to_pylist()
    implications: list[dict[str, Any]] = []
    for edge in edge_rows:
        if str(edge.get("edge_type")) != "IMPLIES":
            continue
        rule_id = str(edge.get("rule_id") or "")
        if (
            float(edge.get("confidence") or 0) != 1.0
            or edge.get("inference_method") != "rule_engine"
            or edge.get("derivation_type") != "rule"
            or rule_id not in ALLOWED_RULES
            or not isinstance(edge.get("rule_version"), int)
        ):
            raise ValueError(
                f"non-deterministic or unsupported implication: "
                f"{edge.get('source_id')}->{edge.get('target_id')}"
            )
        source_id = str(edge.get("source_id"))
        target_id = str(edge.get("target_id"))
        if source_id not in outcomes or target_id not in outcomes:
            raise ValueError("implication endpoint lacks a compiled outcome")
        implications.append(dict(edge))
    return outcomes, implications


def _read_candidate_markets(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    titles = list(FAMILY_BY_TITLE)
    cursor = conn.execute(
        """
        SELECT
            market_id, event_id, event_title, question, market_slug,
            condition_id, group_item_title, outcomes, clob_token_ids,
            game_start_time, end_time, created_at, is_active, is_closed,
            is_resolved, winning_outcome, winning_clob_token_id
        FROM polymarket_wc2026_intermediate.int_polymarket_wc2026_markets AS markets
        WHERE markets.event_title IN (SELECT unnest(?::VARCHAR[]))
          AND EXISTS (
              SELECT 1
              FROM polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds
                  AS hourly
              WHERE hourly.market_id = markets.market_id
          )
        ORDER BY event_title, market_id
        """,
        [titles],
    )
    columns = [item[0] for item in cursor.description]
    rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
    if len(rows) != 290:
        raise ValueError(
            f"expected 290 stage-family candidate markets, got {len(rows)}"
        )
    return rows


def _official_team_name(
    raw_name: str,
    aliases: Mapping[str, str],
    official: set[str],
) -> str:
    aliased = aliases.get(raw_name.casefold(), raw_name)
    official_by_fold = {team.casefold(): team for team in official}
    matches = {
        official_by_fold[value.casefold()]
        for value in (raw_name, aliased)
        if value.casefold() in official_by_fold
    }
    for alias_source, alias_target in aliases.items():
        if (
            alias_target.casefold() == raw_name.casefold()
            and alias_source in official_by_fold
        ):
            matches.add(official_by_fold[alias_source])
    if len(matches) > 1:
        raise ValueError(f"ambiguous official-team alias for {raw_name!r}: {matches}")
    if matches:
        return matches.pop()
    return aliased


def _build_dimensions(
    conn: duckdb.DuckDBPyConnection,
    semantic_outcomes: Mapping[str, dict[str, Any]],
    implications: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    official = {
        str(row[0])
        for row in conn.execute(
            """
            SELECT team_name
            FROM international_results_wc2026_marts
                .international_results_wc2026_team_status
            """
        ).fetchall()
    }
    if len(official) != EXPECTED_TEAMS:
        raise ValueError(
            f"expected {EXPECTED_TEAMS} official teams, got {len(official)}"
        )
    aliases: dict[str, str] = {}
    alias_rows = conn.execute(
        """
        SELECT market_team_name, canonical_team_name
        FROM international_results_wc2026_staging
            .international_results_wc2026_team_aliases
        """
    ).fetchall()
    for raw_name, canonical_name in alias_rows:
        key = str(raw_name).casefold()
        value = str(canonical_name)
        if key in aliases and aliases[key] != value:
            raise ValueError(f"ambiguous team alias for {raw_name!r}")
        aliases[key] = value
    candidates = _read_candidate_markets(conn)
    coverage: list[dict[str, Any]] = []
    outcomes: list[dict[str, Any]] = []
    market_keys: set[tuple[str, str]] = set()
    token_ids: set[str] = set()
    outcome_ids: set[str] = set()
    graph_by_market_label: dict[tuple[str, str], dict[str, Any]] = {}
    for node in semantic_outcomes.values():
        key = (str(node["market_id"]), str(node["label"]).casefold())
        if key in graph_by_market_label:
            raise ValueError(f"duplicate compiled graph outcome: {key}")
        graph_by_market_label[key] = node
    for candidate in candidates:
        market_team = str(candidate["group_item_title"] or "").strip()
        team = _official_team_name(market_team, aliases, official)
        stage_key, stage_label = FAMILY_BY_TITLE[str(candidate["event_title"])]
        included = team in official
        reason = None if included else "not_official_wc2026_roster"
        coverage.append(
            {
                "market_id": str(candidate["market_id"]),
                "event_title": candidate["event_title"],
                "market_team_name": market_team,
                "canonical_team_name": team,
                "stage_key": stage_key,
                "included": included,
                "coverage_reason": (
                    "official_wc2026_roster"
                    if included
                    else "not_official_wc2026_roster"
                ),
                "exclusion_reason": reason,
            }
        )
        if not included:
            if team not in {"Italy", "Peru"}:
                raise ValueError(f"unexpected non-roster stage market: {team}")
            continue
        key = (team, stage_key)
        if key in market_keys:
            raise ValueError(f"duplicate team/stage market: {key}")
        market_keys.add(key)
        labels = _json_list(candidate["outcomes"], field="outcomes")
        tokens = _json_list(candidate["clob_token_ids"], field="clob_token_ids")
        if labels != ["Yes", "No"] or len(tokens) != 2 or len(set(tokens)) != 2:
            raise ValueError(
                f"market {candidate['market_id']} must contain ordered Yes/No"
            )
        for index, (label, token_id) in enumerate(zip(labels, tokens, strict=True)):
            graph_node = graph_by_market_label.get(
                (str(candidate["market_id"]), label.casefold())
            )
            if graph_node is None:
                raise ValueError(
                    f"missing compiled graph outcome for market {candidate['market_id']} {label}"
                )
            proposition = graph_node["proposition"]
            expected_predicate = (
                "wins_competition" if stage_key == "champion" else "reaches_stage"
            )
            if proposition["predicate"] != expected_predicate or proposition[
                "polarity"
            ] is not (label == "Yes"):
                raise ValueError(
                    f"graph proposition mismatch for {graph_node['canonical_id']}"
                )
            graph_team = _official_team_name(
                str(graph_node["graph_team_label"]), aliases, official
            )
            if graph_team != team:
                raise ValueError(
                    f"graph team mismatch for {graph_node['canonical_id']}: "
                    f"{graph_team!r} != {team!r}"
                )
            proposition_stage = str(proposition["arguments"].get("stage") or "")
            expected_stage_suffix = stage_label.casefold().replace(" ", "-")
            if stage_key != "champion" and not proposition_stage.endswith(
                f":{expected_stage_suffix}"
            ):
                raise ValueError(
                    f"graph stage mismatch for {graph_node['canonical_id']}"
                )
            if (
                stage_key != "champion"
                and graph_node["graph_stage_label"] != stage_label
            ):
                raise ValueError(
                    f"graph stage label mismatch for {graph_node['canonical_id']}"
                )
            if token_id in token_ids:
                raise ValueError(f"CLOB token reused across markets: {token_id}")
            token_ids.add(token_id)
            outcome_id = str(graph_node["canonical_id"])
            outcome_ids.add(outcome_id)
            outcomes.append(
                {
                    "market_id": str(candidate["market_id"]),
                    "event_id": str(candidate["event_id"]),
                    "condition_id": str(candidate["condition_id"]),
                    "question": candidate["question"],
                    "market_slug": candidate["market_slug"],
                    "market_team_name": market_team,
                    "team_name": team,
                    "team_id": proposition["arguments"]["team"],
                    "stage_key": stage_key,
                    "stage_label": stage_label,
                    "stage_id": proposition["arguments"].get("stage"),
                    "outcome_index": index,
                    "outcome_label": label,
                    "clob_token_id": token_id,
                    "proposition_id": outcome_id,
                    "predicate": proposition["predicate"],
                    "polarity": proposition["polarity"],
                    "proposition_json": json.dumps(
                        proposition, sort_keys=True, separators=(",", ":")
                    ),
                    "game_start_time": candidate["game_start_time"],
                    "end_time": candidate["end_time"],
                    "created_at": candidate["created_at"],
                    "is_active": candidate["is_active"],
                    "is_closed": candidate["is_closed"],
                    "is_resolved": candidate["is_resolved"],
                    "winning_outcome": candidate["winning_outcome"],
                    "winning_clob_token_id": candidate["winning_clob_token_id"],
                }
            )
    actual_exclusions = {
        (row["canonical_team_name"], row["event_title"], row["coverage_reason"])
        for row in coverage
        if not row["included"]
    }
    expected_exclusions = {
        ("Italy", "World Cup Winner", "not_official_wc2026_roster"),
        ("Peru", "World Cup Winner", "not_official_wc2026_roster"),
    }
    if actual_exclusions != expected_exclusions:
        raise ValueError(f"unexpected stage-market exclusions: {actual_exclusions}")
    expected_keys = {
        (team, family[0]) for team in official for family in FAMILY_BY_TITLE.values()
    }
    if market_keys != expected_keys:
        raise ValueError("included markets do not equal the official 48x6 matrix")
    if len(outcomes) != EXPECTED_TOKENS or len(token_ids) != EXPECTED_TOKENS:
        raise ValueError(f"expected {EXPECTED_TOKENS} unique outcome tokens")

    token_by_outcome = {row["proposition_id"]: row for row in outcomes}
    selected_implications: list[dict[str, Any]] = []
    seen_edges: set[tuple[str, str, str, int]] = set()
    for edge in implications:
        source_id = str(edge["source_id"])
        target_id = str(edge["target_id"])
        if source_id not in outcome_ids and target_id not in outcome_ids:
            continue
        if source_id not in outcome_ids or target_id not in outcome_ids:
            raise ValueError(
                "selected implication crosses the release outcome boundary"
            )
        source = token_by_outcome[source_id]
        target = token_by_outcome[target_id]
        if not source["polarity"] or not target["polarity"]:
            raise ValueError("stage implications must connect positive outcomes")
        if source["team_name"] != target["team_name"]:
            raise ValueError("stage implication crosses teams")
        key = (
            source_id,
            target_id,
            str(edge["rule_id"]),
            int(edge["rule_version"]),
        )
        if key in seen_edges:
            raise ValueError(f"duplicate implication: {key}")
        seen_edges.add(key)
        identity = json.dumps(key, separators=(",", ":"), ensure_ascii=True)
        selected_implications.append(
            {
                "implication_id": hashlib.sha256(identity.encode()).hexdigest(),
                "source_proposition_id": source_id,
                "target_proposition_id": target_id,
                "source_clob_token_id": source["clob_token_id"],
                "target_clob_token_id": target["clob_token_id"],
                "team_name": source["team_name"],
                "source_stage_key": source["stage_key"],
                "target_stage_key": target["stage_key"],
                "rule_id": edge["rule_id"],
                "rule_version": int(edge["rule_version"]),
                "confidence": float(edge["confidence"]),
            }
        )
    if len(selected_implications) != EXPECTED_IMPLICATIONS:
        raise ValueError(
            f"expected {EXPECTED_IMPLICATIONS} direct implications, "
            f"got {len(selected_implications)}"
        )
    reach = ["round_of_32", "round_of_16", "quarterfinals", "semifinals", "final"]
    expected_topology = {
        (team, later, earlier, "wc.stage_monotonicity")
        for team in official
        for later_index, later in enumerate(reach)
        for earlier in reach[:later_index]
    } | {(team, "champion", "final", "wc.champion_reaches_final") for team in official}
    actual_topology = {
        (
            row["team_name"],
            row["source_stage_key"],
            row["target_stage_key"],
            row["rule_id"],
        )
        for row in selected_implications
    }
    if actual_topology != expected_topology:
        raise ValueError(
            "deterministic implications do not match the 11-per-team topology"
        )
    return outcomes, selected_implications, coverage


def _write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"refusing to write empty {path.name}")
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path, compression="zstd", write_statistics=True)


def _write_minute_fact(
    conn: duckdb.DuckDBPyConnection,
    path: Path,
    outcomes: list[dict[str, Any]],
) -> dict[str, int]:
    selected_table = f"_stage_minute_release_tokens_{uuid.uuid4().hex}"
    conn.execute(
        f"CREATE TEMP TABLE {selected_table}("
        "token VARCHAR PRIMARY KEY, market_id VARCHAR NOT NULL)"
    )
    try:
        conn.executemany(
            f"INSERT INTO {selected_table} VALUES (?, ?)",
            [(str(row["clob_token_id"]), str(row["market_id"])) for row in outcomes],
        )
        source_quality = conn.execute(
            f"""
            SELECT
                count(*) - count(DISTINCT (raw."clobTokenId", raw.timestamp)),
                count(*) FILTER (WHERE raw.market_id != selected.market_id)
            FROM polymarket_wc2026_raw.futures_minute_odds_history AS raw
            INNER JOIN {selected_table} AS selected
                ON raw."clobTokenId" = selected.token
            WHERE raw.fidelity_minutes = 1
            """
        ).fetchone()
        if source_quality is None or int(source_quality[0]):
            duplicates = None if source_quality is None else int(source_quality[0])
            raise ValueError(
                f"selected stage-minute source has {duplicates} duplicate points"
            )
        if int(source_quality[1]):
            raise ValueError(
                "selected stage-minute tokens do not match their market metadata"
            )
        quoted_path = str(path.resolve()).replace("'", "''")
        conn.execute(
            f"""
            COPY (
                WITH points AS (
                    SELECT
                        raw.market_id,
                        raw."clobTokenId" AS clob_token_id,
                        raw.timestamp AS source_epoch,
                        raw.price,
                        (raw.timestamp // 60) * 60 AS odds_minute_epoch
                    FROM polymarket_wc2026_raw.futures_minute_odds_history AS raw
                    INNER JOIN {selected_table} AS selected
                        ON raw."clobTokenId" = selected.token
                    WHERE raw.fidelity_minutes = 1
                )
                SELECT
                    market_id,
                    clob_token_id,
                    odds_minute_epoch,
                    to_timestamp(odds_minute_epoch) AS odds_minute_utc,
                    arg_min(price, source_epoch) AS open_price,
                    max(price) AS high_price,
                    min(price) AS low_price,
                    arg_max(price, source_epoch) AS close_price,
                    round(avg(price), 8) AS avg_price,
                    count(*) AS observed_points,
                    min(source_epoch) AS first_source_epoch,
                    to_timestamp(min(source_epoch)) AS first_source_utc,
                    max(source_epoch) AS last_source_epoch,
                    to_timestamp(max(source_epoch)) AS last_source_utc,
                    odds_minute_epoch + 60 - max(source_epoch)
                        AS age_at_minute_close_seconds
                FROM points
                GROUP BY market_id, clob_token_id, odds_minute_epoch
                ORDER BY clob_token_id, odds_minute_epoch
            ) TO '{quoted_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    finally:
        conn.execute(f"DROP TABLE {selected_table}")
    row = conn.execute(
        """
        SELECT
            count(*),
            count(*) - count(DISTINCT (clob_token_id, odds_minute_epoch)),
            count(*) FILTER (
                WHERE open_price < 0 OR high_price > 1
                    OR low_price > open_price OR open_price > high_price
                    OR low_price > close_price OR close_price > high_price
                    OR observed_points < 1
                    OR age_at_minute_close_seconds < 1
                    OR age_at_minute_close_seconds > 60
            ),
            count(DISTINCT market_id),
            count(DISTINCT clob_token_id),
            sum(observed_points)
        FROM read_parquet(?)
        """,
        [str(path.resolve())],
    ).fetchone()
    if row is None or int(row[0]) < 1 or int(row[1]) or int(row[2]):
        raise ValueError(f"invalid token-minute OHLC fact: {row}")
    if (int(row[3]), int(row[4])) != (EXPECTED_MARKETS, EXPECTED_TOKENS):
        raise ValueError(f"minute fact does not cover the selected universe: {row}")
    return {
        "rows": int(row[0]),
        "markets": int(row[3]),
        "tokens": int(row[4]),
        "observed_points": int(row[5]),
    }


def _relation_type(conn: duckdb.DuckDBPyConnection, name: str) -> str:
    row = conn.execute(
        """
        SELECT table_type FROM information_schema.tables
        WHERE table_schema = 'polymarket_wc2026_raw' AND table_name = ?
        """,
        [name],
    ).fetchone()
    return str(row[0]) if row else ""


def _published_audit_summary(
    conn: duckdb.DuckDBPyConnection,
    *,
    leg: str,
    expected_tokens: int,
    expected_rows: int,
) -> dict[str, Any]:
    audit_name = (
        "match_minute_odds_fetch_audit"
        if leg == "match"
        else "futures_minute_odds_fetch_audit"
    )
    row_count_column = "in_game_row_count" if leg == "match" else "window_row_count"
    audit = polymarket_ops_tbl("wc2026", audit_name)
    published_runs = conn.execute(
        f"SELECT DISTINCT fetch_run_id FROM {audit} WHERE raw_published"
    ).fetchall()
    if len(published_runs) != 1:
        raise ValueError(
            f"expected one published {leg} audit run, got {published_runs}"
        )
    fetch_run_id = str(published_runs[0][0])
    summary = conn.execute(
        f"""
        SELECT
            count(*) FILTER (WHERE fetch_status = 'success'),
            count(*) FILTER (WHERE fetch_status = 'empty'),
            sum({row_count_column}) FILTER (WHERE fetch_status = 'success'),
            count(*) FILTER (WHERE fetch_status = 'success' AND raw_published),
            count(*) FILTER (WHERE fetch_status != 'success' AND raw_published)
        FROM {audit}
        WHERE fetch_run_id = ?
        """,
        [fetch_run_id],
    ).fetchone()
    if summary is None or (
        int(summary[0]),
        int(summary[2]),
        int(summary[3]),
        int(summary[4]),
    ) != (expected_tokens, expected_rows, expected_tokens, 0):
        raise ValueError(f"published {leg} audit does not match snapshot: {summary}")
    return {
        "fetch_run_id": fetch_run_id,
        "successful_tokens": int(summary[0]),
        "empty_tokens": int(summary[1]),
    }


def _schema_payload(directory: Path) -> dict[str, Any]:
    payload: dict[str, Any] = {"contract_version": CONTRACT_VERSION, "files": {}}
    for name in sorted(OUTPUT_FILES):
        if not name.endswith(".parquet"):
            continue
        parquet = pq.ParquetFile(directory / name)
        payload["files"][name] = {
            "schema_fingerprint": schema_fingerprint(parquet.schema_arrow),
            "columns": [
                {
                    "name": field.name,
                    "type": str(field.type),
                    "nullable": field.nullable,
                }
                for field in parquet.schema_arrow
            ],
        }
    return payload


def build_stage_minute_release(
    conn: duckdb.DuckDBPyConnection,
    output_root: Path,
    spec: StageMinuteReleaseSpec,
    *,
    nodes_path: Path,
    edges_path: Path,
    generator_commit: str,
    runtime_root: Path | None = None,
) -> dict[str, Any]:
    if not COMMIT_RE.fullmatch(generator_commit):
        raise ValueError("generator_commit must be a lowercase 40-character Git SHA")
    for relation in ("match_minute_odds_history", "futures_minute_odds_history"):
        if _relation_type(conn, relation) != "VIEW":
            raise ValueError(
                f"canonical raw relation is not snapshot-backed: {relation}"
            )
    quality_cursor = conn.execute(
        """
        SELECT *
        FROM polymarket_wc2026_observability
            .polymarket_wc2026_market_minute_odds_data_quality
        """
    )
    quality_columns = [item[0] for item in quality_cursor.description]
    quality_row = quality_cursor.fetchone()
    if quality_row is None:
        raise ValueError("minute-odds data quality result is missing")
    quality = dict(zip(quality_columns, quality_row, strict=True))
    if quality["blocking_issue_keys"] is not None:
        raise ValueError(f"minute-odds data quality is blocked: {quality}")

    semantic_outcomes, semantic_implications = _read_semantic_inputs(
        nodes_path, edges_path
    )
    outcomes, implications, coverage = _build_dimensions(
        conn, semantic_outcomes, semantic_implications
    )
    release_root = output_root.resolve() / "releases"
    release_dir = release_root / spec.dataset_version
    if release_dir.exists() or release_dir.is_symlink():
        raise FileExistsError(f"release already exists: {release_dir}")
    release_root.mkdir(parents=True, exist_ok=True)
    temporary_dir = Path(
        tempfile.mkdtemp(prefix=f".{spec.dataset_version}.", dir=release_root)
    )
    try:
        _write_rows(temporary_dir / "outcomes.parquet", outcomes)
        _write_rows(temporary_dir / "implications.parquet", implications)
        _write_rows(temporary_dir / "coverage.parquet", coverage)
        fact = _write_minute_fact(
            conn, temporary_dir / "token_minute_ohlc.parquet", outcomes
        )
        write_json(temporary_dir / "SCHEMA.json", _schema_payload(temporary_dir))
        snapshots = {}
        for leg in ("match", "futures"):
            root = minute_odds_snapshot_root(leg=leg, runtime_root=runtime_root)
            directory = active_snapshot_dir(root)
            if directory is None:
                raise ValueError(f"missing active {leg} minute snapshot")
            snapshot = validate_minute_odds_snapshot(directory)
            audit = _published_audit_summary(
                conn,
                leg=leg,
                expected_tokens=len(snapshot.token_ids),
                expected_rows=snapshot.raw_row_count,
            )
            snapshots[leg] = {
                "snapshot_id": snapshot.snapshot_id,
                "manifest_sha256": sha256_file(directory / "manifest.json"),
                "raw_row_count": snapshot.raw_row_count,
                "primary_row_count": snapshot.primary_row_count,
                "audit": audit,
            }
        manifest = {
            "contract_version": CONTRACT_VERSION,
            "dataset_version": spec.dataset_version,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "pipeline_revision": generator_commit,
            "graph_revision": spec.graph_revision,
            "sources": {
                "nodes": {"sha256": sha256_file(nodes_path.resolve())},
                "edges": {"sha256": sha256_file(edges_path.resolve())},
                "snapshots": snapshots,
            },
            "data_quality": quality,
            "counts": {
                "candidate_markets": len(coverage),
                "included_markets": sum(bool(row["included"]) for row in coverage),
                "excluded_markets": sum(not row["included"] for row in coverage),
                "official_teams": EXPECTED_TEAMS,
                "outcome_tokens": len(outcomes),
                "implications": len(implications),
                **{f"token_minute_{key}": value for key, value in fact.items()},
            },
            "exclusions": [row for row in coverage if not row["included"]],
            "files": {},
        }
        for name in sorted(OUTPUT_FILES - {"MANIFEST.json", "CHECKSUMS.sha256"}):
            path = temporary_dir / name
            manifest["files"][name] = {
                "sha256": sha256_file(path),
                "byte_size": path.stat().st_size,
            }
            if name.endswith(".parquet"):
                parquet = pq.ParquetFile(path)
                manifest["files"][name].update(
                    {
                        "row_count": parquet.metadata.num_rows,
                        "schema_fingerprint": schema_fingerprint(parquet.schema_arrow),
                    }
                )
        write_json(temporary_dir / "MANIFEST.json", manifest, jsonable=_jsonable)
        write_checksums(temporary_dir, file_names=OUTPUT_FILES)
        if {path.name for path in temporary_dir.iterdir()} != OUTPUT_FILES:
            raise ValueError("release file inventory is incomplete")
        temporary_dir.rename(release_dir)
    except BaseException:
        shutil.rmtree(temporary_dir, ignore_errors=True)
        raise
    return {**manifest["counts"], "release_dir": str(release_dir)}


def current_generator_commit(repo_root: Path = BASE_DIR) -> str:
    return current_clean_commit(repo_root, untracked_files="no")


__all__ = [
    "DEFAULT_OUTPUT_ROOT",
    "StageMinuteReleaseSpec",
    "build_stage_minute_release",
    "current_generator_commit",
]
