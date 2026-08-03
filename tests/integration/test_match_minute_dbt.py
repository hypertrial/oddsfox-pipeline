"""Integration coverage for the isolated match-minute dbt graph."""

from __future__ import annotations

from pathlib import Path

import duckdb
from tests.integration.conftest import dbt_subprocess_env, write_dbt_profile
from tests.integration.dbt_cli import run_dbt
from tests.integration.match_minute_seed import (
    EXPECTED_DATA_QUALITY_ROW,
    EXPECTED_GAMES,
    EXPECTED_MARKETS,
    EXPECTED_MART_ROW_COUNT,
    seed_match_minute_contract,
    seed_wc2026_schedule_matches,
)

import oddsfox_pipeline.storage.duckdb.connection as connection


def _run_dbt(args: list[str], *, profiles_dir: Path, env: dict[str, str]) -> None:
    run_dbt(args, profiles_dir=profiles_dir, env=env)


def test_match_minute_graph_builds_published_mart(
    tmp_path, monkeypatch, dbt_profiles_dir, dbt_target_dir
):
    db_path = tmp_path / "match_minute.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    with duckdb.connect(str(db_path)) as conn:
        seed_match_minute_contract(conn)

    write_dbt_profile(dbt_profiles_dir, db_path, threads=1)
    env = dbt_subprocess_env(
        db_path=db_path,
        profiles_dir=dbt_profiles_dir,
        target_dir=dbt_target_dir,
        dbt_threads=1,
    )
    _run_dbt(
        [
            "seed",
            "--exclude",
            "tag:polygon_settlement",
            "tag:pmxt_order_book",
        ],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    with duckdb.connect(str(db_path)) as conn:
        seed_wc2026_schedule_matches(conn)

    _run_dbt(
        [
            "build",
            "--select",
            "+polymarket_wc2026_match_minute_odds",
            "--exclude",
            "tag:polygon_settlement",
            "tag:pmxt_order_book",
            "resource_type:seed",
        ],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )

    with duckdb.connect(str(db_path)) as conn:
        mart = conn.execute(
            """
            select
                count(*) as row_count,
                count(distinct fifa_match_id) as games,
                count(distinct market_id) as markets,
                count(*) - count(distinct (odds_minute_epoch, market_id)) as duplicate_grain
            from polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds
            """
        ).fetchone()
        quality = conn.execute(
            """
            select
                mapped_games,
                mapped_markets,
                mapped_group_markets,
                mapped_knockout_markets,
                mapped_tokens,
                international_results_games,
                international_results_mapped_games,
                international_results_mapped_source_games,
                international_results_revisions,
                international_results_payload_hashes,
                international_results_provenance_issues,
                latest_fetch_run_status,
                latest_fetch_audited_tokens,
                latest_fetch_success_tokens,
                latest_fetch_empty_tokens,
                latest_fetch_error_tokens,
                latest_fetch_cancelled_tokens,
                latest_fetch_published_tokens,
                latest_fetch_hash_issues,
                elapsed_axis_issue_markets,
                error_issue_count,
                blocking_issue_keys
            from polymarket_wc2026_observability
                .polymarket_wc2026_match_minute_odds_data_quality
            """
        ).fetchone()

    assert mart == (EXPECTED_MART_ROW_COUNT, EXPECTED_GAMES, EXPECTED_MARKETS, 0)
    assert quality == EXPECTED_DATA_QUALITY_ROW
