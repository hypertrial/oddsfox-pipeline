"""Tests for the immutable WC2026 stage-minute strategy input release."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from oddsfox_pipeline.publishing.stage_minute_inputs import (
    FAMILY_BY_TITLE,
    StageMinuteReleaseSpec,
    _official_team_name,
    _read_semantic_inputs,
    build_stage_minute_release,
)
from oddsfox_pipeline.storage.minute_odds_snapshots import (
    build_and_publish_snapshot_from_shards,
)

TEAMS = [f"Team {index:02d}" for index in range(48)]


def _semantic_files(tmp_path: Path) -> tuple[Path, Path]:
    nodes = [
        {
            "canonical_id": f"team:{team.lower().replace(' ', '-')}",
            "type": "TEAM",
            "label": team,
            "aliases": [],
            "confidence": 1.0,
            "evidence_market_ids": [],
            "resolution_method": "new_entity",
            "inference_method": "proposition_compiler",
            "proposition_json": None,
        }
        for team in TEAMS
    ]
    nodes.extend(
        {
            "canonical_id": "stage:world-cup-2026:"
            + stage_label.lower().replace(" ", "-"),
            "type": "STAGE",
            "label": stage_label,
            "aliases": [],
            "confidence": 1.0,
            "evidence_market_ids": [],
            "resolution_method": "new_entity",
            "inference_method": "proposition_compiler",
            "proposition_json": None,
        }
        for stage_key, stage_label in FAMILY_BY_TITLE.values()
        if stage_key != "champion"
    )
    positive: dict[tuple[str, str], str] = {}
    market_id = 1
    for team in TEAMS:
        for _title, (stage_key, stage_label) in FAMILY_BY_TITLE.items():
            for label, polarity in (("Yes", True), ("No", False)):
                proposition = {
                    "predicate": (
                        "wins_competition"
                        if stage_key == "champion"
                        else "reaches_stage"
                    ),
                    "arguments": {
                        "team": f"team:{team.lower().replace(' ', '-')}",
                        "competition": "competition:world-cup-2026",
                        **(
                            {}
                            if stage_key == "champion"
                            else {
                                "stage": "stage:world-cup-2026:"
                                + stage_label.lower().replace(" ", "-")
                            }
                        ),
                    },
                    "polarity": polarity,
                }
                canonical_id = f"outcome:{market_id}:{label.lower()}"
                if polarity:
                    positive[(team, stage_key)] = canonical_id
                nodes.append(
                    {
                        "canonical_id": canonical_id,
                        "type": "OUTCOME",
                        "label": label,
                        "aliases": [],
                        "confidence": 1.0,
                        "evidence_market_ids": [str(market_id)],
                        "resolution_method": "exact_id",
                        "inference_method": "deterministic",
                        "proposition_json": json.dumps(proposition),
                    }
                )
            market_id += 1
    edges = []
    reach = ["round_of_32", "round_of_16", "quarterfinals", "semifinals", "final"]
    for team in TEAMS:
        for later_index, later in enumerate(reach):
            for earlier in reach[:later_index]:
                edges.append(
                    {
                        "source_id": positive[(team, later)],
                        "target_id": positive[(team, earlier)],
                        "edge_type": "IMPLIES",
                        "confidence": 1.0,
                        "evidence_market_ids": [],
                        "evidence_text": "wc.stage_monotonicity",
                        "inference_method": "rule_engine",
                        "derivation_type": "rule",
                        "rule_id": "wc.stage_monotonicity",
                        "rule_version": 1,
                        "premises": [],
                    }
                )
        edges.append(
            {
                "source_id": positive[(team, "champion")],
                "target_id": positive[(team, "final")],
                "edge_type": "IMPLIES",
                "confidence": 1.0,
                "evidence_market_ids": [],
                "evidence_text": "wc.champion_reaches_final",
                "inference_method": "rule_engine",
                "derivation_type": "rule",
                "rule_id": "wc.champion_reaches_final",
                "rule_version": 1,
                "premises": [],
            }
        )
    nodes_path = tmp_path / "nodes.parquet"
    edges_path = tmp_path / "edges.parquet"
    pq.write_table(pa.Table.from_pylist(nodes), nodes_path)
    pq.write_table(pa.Table.from_pylist(edges), edges_path)
    return nodes_path, edges_path


def _publish_snapshots(
    tmp_path: Path,
    conn: duckdb.DuckDBPyConnection,
    *,
    mismatched_market: bool = False,
) -> None:
    start = datetime(2026, 7, 1)
    end = datetime(2026, 7, 2)
    ts = int(datetime(2026, 7, 1, 12, 0, 17, tzinfo=timezone.utc).timestamp())
    futures_rows = []
    for market_id in range(1, 289):
        futures_rows.extend(
            [
                {
                    "market_id": str(market_id),
                    "clobTokenId": f"token-{market_id}-{label}",
                    "timestamp": ts + index,
                    "price": 0.2 if label == "yes" else 0.8,
                    "fidelity_minutes": 1,
                    "window_start_at": start,
                    "window_end_at": end,
                    "ingested_at": end,
                }
                for index, label in enumerate(("yes", "no"))
            ]
        )
    if mismatched_market:
        futures_rows[0]["market_id"] = "wrong-market"
    for leg, rows in (
        ("futures", futures_rows),
        (
            "match",
            [
                {
                    "market_id": "match",
                    "clobTokenId": "match-token",
                    "timestamp": ts,
                    "price": 0.5,
                    "fidelity_minutes": 1,
                    "window_start_at": start,
                    "window_end_at": end,
                    "ingested_at": end,
                }
            ],
        ),
    ):
        shard = tmp_path / f"{leg}.parquet"
        pq.write_table(pa.Table.from_pylist(rows), shard)
        primary = {
            str(row["clobTokenId"])
            for row in rows
            if str(row["clobTokenId"]).endswith("yes")
            or str(row["clobTokenId"]) == "match-token"
        }
        build_and_publish_snapshot_from_shards(
            leg=leg,
            fetch_run_id=f"{leg}-run",
            shard_paths=[shard],
            primary_token_ids=primary,
            conn=conn,
            register=True,
        )
        row_count_column = "in_game_row_count" if leg == "match" else "window_row_count"
        conn.execute(
            f"""
            CREATE TABLE polymarket_wc2026_ops.{leg}_minute_odds_fetch_audit (
                fetch_run_id VARCHAR,
                fetch_status VARCHAR,
                raw_published BOOLEAN,
                {row_count_column} BIGINT
            )
            """
        )
        conn.executemany(
            f"INSERT INTO polymarket_wc2026_ops.{leg}_minute_odds_fetch_audit "
            f"VALUES (?, 'success', TRUE, 1)",
            [(f"{leg}-audit-run",) for _row in rows],
        )


def _warehouse(
    tmp_path: Path, *, mismatched_market: bool = False
) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    for schema in (
        "polymarket_wc2026_raw",
        "polymarket_wc2026_intermediate",
        "polymarket_wc2026_marts",
        "polymarket_wc2026_observability",
        "polymarket_wc2026_ops",
        "international_results_wc2026_marts",
        "international_results_wc2026_staging",
    ):
        conn.execute(f"CREATE SCHEMA {schema}")
    conn.execute(
        "CREATE TABLE international_results_wc2026_marts."
        "international_results_wc2026_team_status(team_name VARCHAR)"
    )
    conn.executemany(
        "INSERT INTO international_results_wc2026_marts."
        "international_results_wc2026_team_status VALUES (?)",
        [(team,) for team in TEAMS],
    )
    conn.execute(
        "CREATE TABLE international_results_wc2026_staging."
        "international_results_wc2026_team_aliases("
        "market_team_name VARCHAR, canonical_team_name VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE polymarket_wc2026_observability."
        "polymarket_wc2026_market_minute_odds_data_quality("
        "blocking_issue_keys VARCHAR)"
    )
    conn.execute(
        "INSERT INTO polymarket_wc2026_observability."
        "polymarket_wc2026_market_minute_odds_data_quality VALUES (NULL)"
    )
    conn.execute(
        """
        CREATE TABLE polymarket_wc2026_intermediate.int_polymarket_wc2026_markets (
            market_id VARCHAR, event_id VARCHAR, event_title VARCHAR,
            question VARCHAR, market_slug VARCHAR, condition_id VARCHAR,
            group_item_title VARCHAR, outcomes VARCHAR, clob_token_ids VARCHAR,
            game_start_time TIMESTAMP, end_time TIMESTAMP, created_at TIMESTAMP,
            is_active BOOLEAN, is_closed BOOLEAN, is_resolved BOOLEAN,
            winning_outcome VARCHAR, winning_clob_token_id VARCHAR
        )
        """
    )
    conn.execute(
        "CREATE TABLE polymarket_wc2026_marts."
        "polymarket_wc2026_market_hourly_odds(market_id VARCHAR)"
    )
    candidates = []
    market_id = 1
    for team in TEAMS:
        for title in FAMILY_BY_TITLE:
            candidates.append(
                (
                    str(market_id),
                    f"event-{title}",
                    title,
                    f"Will {team} qualify?",
                    f"market-{market_id}",
                    f"condition-{market_id}",
                    team,
                    '["Yes", "No"]',
                    json.dumps([f"token-{market_id}-yes", f"token-{market_id}-no"]),
                    datetime(2026, 6, 1),
                    datetime(2026, 7, 20),
                    datetime(2025, 7, 2),
                    True,
                    False,
                    False,
                    None,
                    None,
                )
            )
            market_id += 1
    for team in ("Italy", "Peru"):
        title = "World Cup Winner"
        candidates.append(
            (
                str(market_id),
                f"event-{title}",
                title,
                f"Will {team} qualify?",
                f"market-{market_id}",
                f"condition-{market_id}",
                team,
                '["Yes", "No"]',
                json.dumps([f"token-{market_id}-yes", f"token-{market_id}-no"]),
                datetime(2026, 6, 1),
                datetime(2026, 7, 20),
                datetime(2025, 7, 2),
                True,
                False,
                False,
                None,
                None,
            )
        )
        market_id += 1
    conn.executemany(
        "INSERT INTO polymarket_wc2026_intermediate."
        "int_polymarket_wc2026_markets VALUES "
        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        candidates,
    )
    conn.executemany(
        "INSERT INTO polymarket_wc2026_marts."
        "polymarket_wc2026_market_hourly_odds VALUES (?)",
        [(str(index),) for index in range(1, 291)],
    )
    _publish_snapshots(tmp_path, conn, mismatched_market=mismatched_market)
    return conn


def test_build_stage_minute_release_is_complete_atomic_and_immutable(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path))
    nodes, edges = _semantic_files(tmp_path)
    conn = _warehouse(tmp_path)
    output = tmp_path / "output"
    try:
        summary = build_stage_minute_release(
            conn,
            output,
            StageMinuteReleaseSpec("1.0.0", "a" * 40),
            nodes_path=nodes,
            edges_path=edges,
            generator_commit="b" * 40,
            runtime_root=tmp_path,
        )
        release = Path(summary["release_dir"])
        assert summary["included_markets"] == 288
        assert summary["outcome_tokens"] == 576
        assert summary["implications"] == 528
        assert summary["token_minute_rows"] == 576
        assert pq.ParquetFile(release / "coverage.parquet").metadata.num_rows == 290
        exclusions = [
            row
            for row in pq.read_table(release / "coverage.parquet").to_pylist()
            if not row["included"]
        ]
        assert {row["canonical_team_name"] for row in exclusions} == {"Italy", "Peru"}
        assert {row["coverage_reason"] for row in exclusions} == {
            "not_official_wc2026_roster"
        }
        manifest = json.loads((release / "MANIFEST.json").read_text())
        assert manifest["sources"]["snapshots"]["futures"]["audit"] == {
            "empty_tokens": 0,
            "fetch_run_id": "futures-audit-run",
            "successful_tokens": 576,
        }
        assert manifest["data_quality"]["blocking_issue_keys"] is None
        checksums = (release / "CHECKSUMS.sha256").read_text().splitlines()
        assert len(checksums) == 6
        with pytest.raises(FileExistsError, match="release already exists"):
            build_stage_minute_release(
                conn,
                output,
                StageMinuteReleaseSpec("1.0.0", "a" * 40),
                nodes_path=nodes,
                edges_path=edges,
                generator_commit="b" * 40,
                runtime_root=tmp_path,
            )
    finally:
        conn.close()


def test_semantic_reader_rejects_llm_outcome(tmp_path):
    nodes, edges = _semantic_files(tmp_path)
    table = pq.read_table(nodes)
    rows = table.to_pylist()
    next(row for row in rows if row["type"] == "OUTCOME")["inference_method"] = "llm"
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), nodes)
    with pytest.raises(ValueError, match="is not deterministic"):
        _read_semantic_inputs(nodes, edges)


def test_semantic_reader_rejects_missing_proposition(tmp_path):
    nodes, edges = _semantic_files(tmp_path)
    table = pq.read_table(nodes)
    rows = table.to_pylist()
    next(row for row in rows if row["type"] == "OUTCOME")["proposition_json"] = None
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), nodes)
    with pytest.raises(ValueError, match="has no proposition_json"):
        _read_semantic_inputs(nodes, edges)


def test_release_rejects_token_market_mismatch_atomically(tmp_path, monkeypatch):
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path))
    nodes, edges = _semantic_files(tmp_path)
    conn = _warehouse(tmp_path, mismatched_market=True)
    output = tmp_path / "output"
    try:
        with pytest.raises(ValueError, match="do not match their market metadata"):
            build_stage_minute_release(
                conn,
                output,
                StageMinuteReleaseSpec("1.0.0", "a" * 40),
                nodes_path=nodes,
                edges_path=edges,
                generator_commit="b" * 40,
                runtime_root=tmp_path,
            )
        assert not (output / "releases" / "1.0.0").exists()
        assert not list((output / "releases").glob(".1.0.0.*"))
    finally:
        conn.close()


def test_release_version_is_fixed():
    with pytest.raises(ValueError, match="dataset_version must be 1.0.0"):
        StageMinuteReleaseSpec("1.0.1", "a" * 40)


def test_official_team_alias_rejects_ambiguous_resolution():
    with pytest.raises(ValueError, match="ambiguous official-team alias"):
        _official_team_name(
            "Team A",
            {"team a": "Team B"},
            {"Team A", "Team B"},
        )


def test_semantic_reader_rejects_wrong_rule(tmp_path):
    nodes, edges = _semantic_files(tmp_path)
    table = pq.read_table(edges)
    rows = table.to_pylist()
    rows[0]["rule_id"] = "llm.inferred"
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), edges)
    with pytest.raises(ValueError, match="unsupported implication"):
        _read_semantic_inputs(nodes, edges)
