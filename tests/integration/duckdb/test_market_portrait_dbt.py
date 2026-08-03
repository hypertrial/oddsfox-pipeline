"""Integration coverage for the isolated market-portrait bundle path."""

from __future__ import annotations

import gzip
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import yaml
from tests.integration.conftest import dbt_subprocess_env, write_dbt_profile
from tests.integration.dbt_cli import run_dbt
from tests.integration.duckdb.match_analysis_seed import (
    seed_order_book_contract,
    seed_portrait_alignment_contract,
)

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.publishing.market_portrait import (
    MatchFacts,
    build_market_portrait_bundle,
)

UTC = timezone.utc
PORTRAIT_TARGET_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "market_portrait"
    / "match-95-target.yml"
)


def _run_dbt(args: list[str], *, profiles_dir: Path, env: dict[str, str]) -> None:
    run_dbt(args, profiles_dir=profiles_dir, env=env)


def _match_facts() -> MatchFacts:
    kickoff = datetime(2026, 7, 4, 10, 34, 2, tzinfo=UTC)
    first_half = datetime(2026, 7, 4, 10, 35, tzinfo=UTC)
    return MatchFacts(
        fifa_match_id=95,
        stage="round_of_16",
        home_team="Argentina",
        away_team="Egypt",
        kickoff_at_utc=kickoff,
        first_half_started_at=first_half,
        first_half_ended_at=datetime(2026, 7, 4, 11, 23, tzinfo=UTC),
        second_half_started_at=datetime(2026, 7, 4, 11, 38, tzinfo=UTC),
        second_half_ended_at=datetime(2026, 7, 4, 12, 27, tzinfo=UTC),
        match_ended_at=datetime(2026, 7, 4, 12, 27, tzinfo=UTC),
        source_provenance_sha256="a" * 64,
    )


def test_portrait_fixture_manifest_hash_is_stable():
    payload = yaml.safe_load(PORTRAIT_TARGET_FIXTURE.read_text(encoding="utf-8"))
    declared = str(payload.pop("content_sha256"))
    actual = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assert declared == actual


def test_market_portrait_graph_builds_bundle_from_dbt_marts(
    tmp_path, monkeypatch, dbt_profiles_dir, dbt_target_dir
):
    facts = _match_facts()
    db_path = tmp_path / "market_portrait.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    with duckdb.connect(str(db_path)) as conn:
        seed_order_book_contract(conn)

    write_dbt_profile(dbt_profiles_dir, db_path, threads=1)
    env = dbt_subprocess_env(
        db_path=db_path,
        profiles_dir=dbt_profiles_dir,
        target_dir=dbt_target_dir,
        dbt_threads=1,
    )
    _run_dbt(
        [
            "build",
            "--select",
            "+tag:pmxt_order_book +tag:market_portrait",
            "--exclude",
            "tag:polygon_settlement tag:match_minute",
        ],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )

    with duckdb.connect(str(db_path)) as conn:
        seed_portrait_alignment_contract(
            conn,
            kickoff_at_utc=facts.kickoff_at_utc.isoformat(),
            match_started_at_utc=facts.kickoff_at_utc.isoformat(),
        )
        first = build_market_portrait_bundle(
            conn,
            fifa_match_id=95,
            match_facts=facts,
            football_events=[],
            output_root=tmp_path / "bundles",
            pipeline_revision="pipeline",
            scraper_revision="scraper",
        )
        second = build_market_portrait_bundle(
            conn,
            fifa_match_id=95,
            match_facts=facts,
            football_events=[],
            output_root=tmp_path / "bundles",
            pipeline_revision="pipeline",
            scraper_revision="scraper",
        )

    assert first["bundle_id"] == second["bundle_id"]
    assert second["noop"] is True
    bundle = tmp_path / "bundles" / "95" / first["bundle_id"]
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    trades = [
        json.loads(line)
        for line in gzip.decompress(
            (bundle / "trades.ndjson.gz").read_bytes()
        ).splitlines()
    ]
    assert manifest["contract_version"] == "oddsfox.market-portrait.v1"
    assert manifest["landscape_roles"] == ["home", "away"]
    assert len(trades) == 2
