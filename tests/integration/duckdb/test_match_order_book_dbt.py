"""Integration coverage for the isolated PMXT order-book dbt graph."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path

import duckdb
from tests.integration.conftest import dbt_subprocess_env, write_dbt_profile
from tests.integration.dbt_cli import run_dbt

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.ingestion.polymarket.match_order_book import (
    load_order_book_manifest,
)


def _run_dbt(args: list[str], *, profiles_dir: Path, env: dict[str, str]) -> None:
    run_dbt(args, profiles_dir=profiles_dir, env=env)


def _run_dbt_fails(args: list[str], *, profiles_dir: Path, env: dict[str, str]) -> str:
    completed = run_dbt(args, profiles_dir=profiles_dir, env=env, expect_fail=True)
    return completed.stdout + completed.stderr


def _seed_order_book_contract(conn: duckdb.DuckDBPyConnection) -> None:
    manifest = load_order_book_manifest()
    target = manifest.targets[0]
    scan_id = "published-scan"
    conn.execute(
        """
        insert into openfootball_wc2026_raw.schedule_fixtures values (
            95, 'round_of_16', 2, null, timestamp '2026-07-07 17:00:00',
            'Argentina', 'Egypt', 'Test Venue', 'completed',
            'https://example.com/fixture', 95, 'fixture-hash',
            timestamp '2026-07-07 19:00:00'
        )
        """
    )
    conn.execute(
        """
        insert into polymarket_wc2026_ops.match_order_book_scan_runs (
            scan_id, manifest_version, manifest_sha256, target_count,
            token_count, status, raw_published, api_attempt_count,
            snapshot_count, aggregate_sha256, started_at,
            last_checkpoint_at, finished_at
        ) values (
            ?, ?, ?, 1, 2, 'published', true, 2, 2, ?,
            timestamp '2026-07-07 18:00:00',
            timestamp '2026-07-07 18:20:00',
            timestamp '2026-07-07 18:20:00'
        )
        """,
        [scan_id, manifest.version, manifest.sha256, "f" * 64],
    )
    for outcome in target.outcomes:
        conn.execute(
            """
            insert into polymarket_wc2026_ops.match_order_book_scan_windows (
                scan_id, fifa_match_id, market_id, condition_id,
                outcome_label, clob_token_id, window_start_ms,
                window_end_ms, depth, status, api_attempt_count,
                snapshot_count, content_sha256, updated_at
            ) values (?, 95, ?, ?, ?, ?, ?, ?, 0, 'loaded', 1, 1, ?,
                timestamp '2026-07-07 18:20:00')
            """,
            [
                scan_id,
                target.market_id,
                target.condition_id,
                outcome.label,
                outcome.clob_token_id,
                target.window_start_ms,
                target.window_end_ms,
                "e" * 64,
            ],
        )

    rows = [
        (
            target.outcomes[0],
            target.window_start_ms + 1_000,
            "a" * 64,
            (
                '[{"order_count":2,"price":"0.4","size":"10"},'
                '{"order_count":1,"price":"0.3","size":"5"}]'
            ),
            (
                '[{"order_count":1,"price":"0.6","size":"4"},'
                '{"order_count":null,"price":"0.7","size":"3"}]'
            ),
        ),
        (
            target.outcomes[1],
            target.window_start_ms + 2_000,
            "b" * 64,
            "[]",
            "[]",
        ),
    ]
    for outcome, timestamp_ms, snapshot_hash, bids_json, asks_json in rows:
        conn.execute(
            """
            insert into polymarket_wc2026_raw.match_order_book_snapshots (
                scan_id, manifest_sha256, fifa_match_id, stage, home_team,
                away_team, event_id, event_slug, market_id, market_slug,
                market_type, condition_id, outcome_label, clob_token_id,
                window_start_ms, window_end_ms, snapshot_timestamp_ms,
                snapshot_at, snapshot_sha256, provider_sequence,
                landscape_role, bids_json, asks_json,
                is_neg_risk, last_trade_price, source_endpoint, ingested_at
            ) values (
                ?, ?, 95, 'round_of_16', 'Argentina', 'Egypt', ?, ?, ?, ?,
                'soccer_team_to_advance', ?, ?, ?, ?, ?, ?,
                to_timestamp(? / 1000.0), ?, 0, ?, ?, ?, false, '0.5',
                'api.pmxt.dev/api/polymarket/fetchOrderBook',
                timestamp '2026-07-07 18:20:00'
            )
            """,
            [
                scan_id,
                manifest.sha256,
                target.event_id,
                target.event_slug,
                target.market_id,
                target.market_slug,
                target.condition_id,
                outcome.label,
                outcome.clob_token_id,
                target.window_start_ms,
                target.window_end_ms,
                timestamp_ms,
                timestamp_ms,
                snapshot_hash,
                outcome.role,
                bids_json,
                asks_json,
            ],
        )
    trade_rows = [
        (
            outcome,
            f"trade-{outcome.role}",
            target.window_start_ms + 3_000 + index,
        )
        for index, outcome in enumerate(target.outcomes)
    ]
    aggregate = hashlib.sha256(
        "\n".join(
            trade_id
            for outcome, trade_id, _ in sorted(
                trade_rows, key=lambda row: (row[0].clob_token_id, row[2], row[1])
            )
        ).encode()
    ).hexdigest()
    conn.execute(
        """
        insert into polymarket_wc2026_ops.match_trade_scan_runs (
            scan_id, manifest_sha256, status, trade_count, aggregate_sha256,
            started_at, finished_at
        ) values (
            ?, ?, 'published', 2, ?,
            timestamp '2026-07-07 18:20:00',
            timestamp '2026-07-07 18:25:00'
        )
        """,
        [scan_id, manifest.sha256, aggregate],
    )
    for outcome, trade_id, timestamp_ms in trade_rows:
        ids_sha256 = hashlib.sha256(
            json.dumps([trade_id], separators=(",", ":")).encode()
        ).hexdigest()
        conn.execute(
            """
            insert into polymarket_wc2026_ops.match_trade_scan_windows (
                scan_id, fifa_match_id, market_id, clob_token_id,
                landscape_role, window_start_ms, window_end_ms, depth, status,
                api_attempt_count, trade_count, trade_ids_sha256, updated_at
            ) values (
                ?, 95, ?, ?, ?, ?, ?, 0, 'loaded', 1, 1, ?,
                timestamp '2026-07-07 18:25:00'
            )
            """,
            [
                scan_id,
                target.market_id,
                outcome.clob_token_id,
                outcome.role,
                target.window_start_ms,
                target.window_end_ms,
                ids_sha256,
            ],
        )
        conn.execute(
            """
            insert into polymarket_wc2026_raw.match_trades (
                scan_id, manifest_sha256, fifa_match_id, market_id,
                clob_token_id, landscape_role, trade_id, trade_timestamp_ms,
                event_sequence, price, amount, source_endpoint, ingested_at
            ) values (
                ?, ?, 95, ?, ?, ?, ?, ?, 0, '0.6', '3',
                'api.pmxt.dev/api/polymarket/fetchTrades',
                timestamp '2026-07-07 18:25:00'
            )
            """,
            [
                scan_id,
                manifest.sha256,
                target.market_id,
                outcome.clob_token_id,
                outcome.role,
                trade_id,
                timestamp_ms,
            ],
        )


def test_order_book_graph_expands_levels_and_blocks_fixture_mismatch(
    tmp_path, monkeypatch, dbt_profiles_dir, dbt_target_dir
):
    db_path = tmp_path / "order_book.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    with duckdb.connect(str(db_path)) as conn:
        _seed_order_book_contract(conn)

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
            "tag:polygon_settlement tag:wc2026_logical_atlas",
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
        trade_count = conn.execute(
            """
            select count(*)
            from polymarket_wc2026_marts.polymarket_wc2026_match_trades
            """
        ).fetchone()[0]

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
    assert trade_count == 2

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            update openfootball_wc2026_raw.schedule_fixtures
            set away_team = 'Morocco'
            where fifa_match_id = 95
            """
        )
    failure = _run_dbt_fails(
        [
            "run",
            "--select",
            "+tag:pmxt_order_book",
            "--exclude",
            "tag:polygon_settlement tag:wc2026_logical_atlas",
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
        _seed_order_book_contract(conn)

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
            "tag:polygon_settlement tag:wc2026_logical_atlas",
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
            where outcome_label = 'Argentina'
            """
        )
    failure = _run_dbt_fails(
        [
            "run",
            "--select",
            "+tag:pmxt_order_book",
            "--exclude",
            "tag:polygon_settlement tag:wc2026_logical_atlas",
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
