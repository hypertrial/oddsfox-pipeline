"""Integration coverage for the isolated PMXT order-book dbt graph."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import duckdb
from tests.integration.conftest import dbt_subprocess_env, write_dbt_profile
from tests.integration.dbt_cli import run_dbt
from tests.integration.duckdb.match_analysis_seed import seed_order_book_contract

import oddsfox_pipeline.storage.duckdb.connection as connection


def _run_dbt(args: list[str], *, profiles_dir: Path, env: dict[str, str]) -> None:
    run_dbt(args, profiles_dir=profiles_dir, env=env)


def _run_dbt_fails(args: list[str], *, profiles_dir: Path, env: dict[str, str]) -> str:
    completed = run_dbt(args, profiles_dir=profiles_dir, env=env, expect_fail=True)
    return completed.stdout + completed.stderr


def test_order_book_graph_expands_levels_and_blocks_fixture_mismatch(
    tmp_path, monkeypatch, dbt_profiles_dir, dbt_target_dir
):
    db_path = tmp_path / "order_book.duckdb"
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
            "+tag:pmxt_order_book",
            "--exclude",
            "tag:polygon_settlement tag:match_minute",
        ],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )

    with duckdb.connect(str(db_path)) as conn:
        levels = conn.execute(
            """
            select
                book_side, level_rank, price, size, cumulative_size,
                best_bid_price, best_ask_price, spread, midpoint
            from polymarket_wc2026_marts.polymarket_wc2026_match_order_book
            order by book_side, level_rank
            """
        ).fetchall()
        quality = conn.execute(
            """
            select
                snapshot_count, level_count, error_issue_count,
                warning_issue_count
            from polymarket_wc2026_observability
                .polymarket_wc2026_match_order_book_data_quality
            """
        ).fetchone()
        source_labels = conn.execute(
            """
            select distinct source_label
            from polymarket_wc2026_marts.polymarket_wc2026_match_order_book
            """
        ).fetchall()
    assert [row[:5] for row in levels] == [
        ("ask", 1, Decimal("0.6"), Decimal("4"), Decimal("4")),
        ("ask", 2, Decimal("0.7"), Decimal("3"), Decimal("7")),
        ("bid", 1, Decimal("0.4"), Decimal("10"), Decimal("10")),
        ("bid", 2, Decimal("0.3"), Decimal("5"), Decimal("15")),
    ]
    assert all(
        row[5:]
        == (
            Decimal("0.4"),
            Decimal("0.6"),
            Decimal("0.2"),
            Decimal("0.5"),
        )
        for row in levels
    )
    assert quality == (2, 4, 0, 1)
    assert source_labels == [
        ("api.pmxt.dev/api/polymarket/fetchOrderBook",),
    ]

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            update oddsfox_reference.openfootball_wc2026_schedule_fixtures
            set away_team = 'Morocco'
            where fifa_match_id = 104
            """
        )
    failure = _run_dbt_fails(
        [
            "run",
            "--select",
            "+tag:pmxt_order_book",
            "--exclude",
            "tag:polygon_settlement tag:match_minute",
        ],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    with duckdb.connect(str(db_path), read_only=True) as conn:
        mart_count = conn.execute(
            """
            select count(*)
            from polymarket_wc2026_marts.polymarket_wc2026_match_order_book
            """
        ).fetchone()[0]

    assert "WC2026 PMXT order-book publication blocked: fixture_identity" in failure
    assert mart_count == 4


def test_order_book_graph_blocks_malformed_optional_numerics(
    tmp_path, monkeypatch, dbt_profiles_dir, dbt_target_dir
):
    db_path = tmp_path / "order_book_invalid_optional.duckdb"
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
            "+tag:pmxt_order_book",
            "--exclude",
            "tag:polygon_settlement tag:match_minute",
        ],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            update polymarket_wc2026_raw.match_order_book_snapshots
            set bids_json = replace(
                    bids_json,
                    '"order_count":2',
                    '"order_count":"bad"'
                ),
                last_trade_price = 'bad'
            where outcome_label = 'Spain'
            """
        )
    failure = _run_dbt_fails(
        [
            "run",
            "--select",
            "+tag:pmxt_order_book",
            "--exclude",
            "tag:polygon_settlement tag:match_minute",
        ],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )

    with duckdb.connect(str(db_path), read_only=True) as conn:
        issues = {
            row[0]
            for row in conn.execute(
                """
                select issue_key
                from polymarket_wc2026_observability
                    .polymarket_wc2026_match_order_book_quality_issues
                where severity = 'error'
                """
            ).fetchall()
        }
        mart_count = conn.execute(
            """
            select count(*)
            from polymarket_wc2026_marts.polymarket_wc2026_match_order_book
            """
        ).fetchone()[0]

    assert "WC2026 PMXT order-book publication blocked:" in failure
    assert {"invalid_last_trade_price", "invalid_level"} <= issues
    assert mart_count == 4
