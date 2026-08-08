"""Integration coverage for the unified minute-odds dbt graph."""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
from tests.integration.conftest import dbt_subprocess_env, write_dbt_profile
from tests.integration.dbt_cli import run_dbt
from tests.integration.match_minute_seed import (
    seed_match_minute_contract,
    seed_wc2026_schedule_matches,
)

import oddsfox_pipeline.storage.duckdb.connection as connection


def _seed_futures_minute_rows(conn: duckdb.DuckDBPyConnection) -> None:
    now = datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)
    created = datetime(2026, 5, 1, tzinfo=timezone.utc)
    end_date = datetime(2026, 7, 15, tzinfo=timezone.utc)
    observed = datetime(2026, 7, 1, tzinfo=timezone.utc)
    question = "Who wins the tournament?"
    outcomes = '["Yes", "No"]'
    token_ids = '["futures-yes", "futures-no"]'
    # One futures market admitted beside the match-minute inventory.
    # stg_polymarket_wc2026_markets reads payload snapshots, not markets.
    conn.execute(
        """
        insert into polymarket_wc2026_raw.markets (
            id, question, category, description, outcomes, volume, active, closed,
            created_at, scraped_at, end_date, slug, event_slug, event_id,
            event_title, condition_id, sports_market_type, group_item_title,
            clob_token_ids, is_resolved, tags
        ) values (
            'futures-winner', ?, 'sports', '',
            ?, 200000, false, true,
            ?, ?,
            ?, 'futures-winner', 'wc-winner',
            'wc-winner', 'WC Winner', 'condition-futures-winner',
            'tournament_winner', 'Winner', ?, false, '[]'
        )
        """,
        [question, outcomes, created, observed, end_date, token_ids],
    )
    conn.execute(
        """
        insert into polymarket_wc2026_ops.market_scope_registry (
            scope_name, market_id, event_slug, event_id, source, refreshed_at,
            event_volume_usd_lifetime_reported, is_event_volume_eligible,
            first_eligible_at
        ) values (
            'wc2026', 'futures-winner', 'wc-winner', 'wc-winner', 'test',
            ?, 200000, true, ?
        )
        """,
        [observed, created],
    )
    # Unified mart joins registry-eligible int_markets; match-minute seed omits
    # registry rows because its mart does not go through that gate.
    conn.execute(
        """
        insert into polymarket_wc2026_ops.market_scope_registry (
            scope_name, market_id, event_slug, event_id, source, refreshed_at,
            event_volume_usd_lifetime_reported, is_event_volume_eligible,
            first_eligible_at
        )
        select
            'wc2026',
            m.id,
            m.event_slug,
            m.event_id,
            'test',
            ?,
            coalesce(m.volume, 1000.0),
            true,
            coalesce(m.created_at, ?)
        from polymarket_wc2026_raw.markets as m
        where m.id != 'futures-winner'
          and not exists (
              select 1
              from polymarket_wc2026_ops.market_scope_registry as r
              where r.scope_name = 'wc2026' and r.market_id = m.id
          )
        """,
        [observed, created],
    )
    conn.execute(
        """
        insert into polymarket_wc2026_raw.event_market_payload_snapshots (
            market_id, question, category, description, outcomes, volume,
            active, closed, created_at, scraped_at, end_date, slug, event_slug,
            event_id, event_title, condition_id, sports_market_type,
            group_item_title, clob_token_ids, is_resolved, tags, observed_at
        ) values (
            'futures-winner', ?, 'sports', '', ?, 200000,
            false, true, ?, ?, ?, 'futures-winner', 'wc-winner',
            'wc-winner', 'WC Winner', 'condition-futures-winner',
            'tournament_winner', 'Winner', ?, false, '[]', ?
        )
        """,
        [question, outcomes, created, observed, end_date, token_ids, observed],
    )
    # Primary Yes token minute observations inside the tournament window.
    rows = []
    for minute in range(3):
        ts = int(datetime(2026, 6, 12, 0, minute, tzinfo=timezone.utc).timestamp())
        rows.append(
            (
                "futures-winner",
                "futures-yes",
                ts,
                0.4 + minute * 0.05,
                1,
                datetime(2026, 6, 11, tzinfo=timezone.utc),
                datetime(2026, 7, 15, tzinfo=timezone.utc),
                now,
            )
        )
    conn.executemany(
        """
        insert into polymarket_wc2026_raw.futures_minute_odds_history (
            market_id, clobTokenId, timestamp, price, fidelity_minutes,
            window_start_at, window_end_at, ingested_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.execute(
        """
        insert into polymarket_wc2026_ops.futures_minute_odds_fetch_audit (
            fetch_run_id, market_id, clobTokenId, fetch_status, raw_published,
            fidelity_minutes, exact_window_start_at, exact_window_end_at,
            request_start_epoch, request_end_epoch, source_row_count,
            window_row_count, window_history_sha256, source_endpoint,
            fetch_started_at, fetch_finished_at
        ) values (
            'ci-futures-minute', 'futures-winner', 'futures-yes', 'success', true,
            1, timestamp '2026-06-11 00:00:00', timestamp '2026-07-15 00:00:00',
            1749600000, 1752537600, 3, 3, ?,
            'https://clob.polymarket.com/prices-history',
            timestamp '2026-07-01 12:00:00', timestamp '2026-07-01 12:01:00'
        )
        """,
        ["c" * 64],
    )


def test_minute_odds_graph_builds_unified_mart(
    tmp_path, monkeypatch, dbt_profiles_dir, dbt_target_dir
):
    db_path = tmp_path / "minute_odds.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    with duckdb.connect(str(db_path)) as conn:
        seed_match_minute_contract(conn)
        _seed_futures_minute_rows(conn)

    write_dbt_profile(dbt_profiles_dir, db_path, threads=1)
    env = dbt_subprocess_env(
        db_path=db_path,
        profiles_dir=dbt_profiles_dir,
        target_dir=dbt_target_dir,
        dbt_threads=1,
    )
    run_dbt(
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

    run_dbt(
        [
            "build",
            "--select",
            "+polymarket_wc2026_market_minute_odds_data_quality",
            "--exclude",
            "tag:polygon_settlement",
            "tag:pmxt_order_book",
            "resource_type:seed",
        ],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )

    with duckdb.connect(str(db_path)) as conn:
        sources = conn.execute(
            """
            select minute_source, count(*)
            from polymarket_wc2026_marts.polymarket_wc2026_market_minute_odds
            group by 1
            order by 1
            """
        ).fetchall()
        assert ("futures", 3) in sources
        assert any(source == "match" and count > 0 for source, count in sources)

        dq = conn.execute(
            """
            select mart_rows > 0, has_match_rows, blocking_issue_keys
            from polymarket_wc2026_observability.polymarket_wc2026_market_minute_odds_data_quality
            """
        ).fetchone()
        assert dq == (True, True, None)


def test_minute_odds_graph_accepts_partial_match_with_futures(
    tmp_path, monkeypatch, dbt_profiles_dir, dbt_target_dir
):
    """Smoke-shaped warehouse: partial match history + futures still passes unified DQ."""
    db_path = tmp_path / "minute_odds_partial.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    with duckdb.connect(str(db_path)) as conn:
        seed_match_minute_contract(conn)
        _seed_futures_minute_rows(conn)
        # Keep games 1-5 only (~5% of 104), mirroring per-leg smoke sampling.
        conn.execute(
            """
            delete from polymarket_wc2026_raw.match_minute_odds_history
            where market_id not like 'ml-1-%'
              and market_id not like 'ml-2-%'
              and market_id not like 'ml-3-%'
              and market_id not like 'ml-4-%'
              and market_id not like 'ml-5-%'
            """
        )
        conn.execute(
            """
            delete from polymarket_wc2026_ops.match_minute_odds_fetch_audit
            where market_id not like 'ml-1-%'
              and market_id not like 'ml-2-%'
              and market_id not like 'ml-3-%'
              and market_id not like 'ml-4-%'
              and market_id not like 'ml-5-%'
            """
        )

    write_dbt_profile(dbt_profiles_dir, db_path, threads=1)
    env = dbt_subprocess_env(
        db_path=db_path,
        profiles_dir=dbt_profiles_dir,
        target_dir=dbt_target_dir,
        dbt_threads=1,
    )
    run_dbt(
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

    run_dbt(
        [
            "build",
            "--select",
            "+polymarket_wc2026_market_minute_odds_data_quality",
            "--exclude",
            "tag:polygon_settlement",
            "tag:pmxt_order_book",
            "resource_type:seed",
        ],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )

    with duckdb.connect(str(db_path)) as conn:
        sources = dict(
            conn.execute(
                """
                select minute_source, count(*)
                from polymarket_wc2026_marts.polymarket_wc2026_market_minute_odds
                group by 1
                """
            ).fetchall()
        )
        assert sources.get("futures", 0) == 3
        assert sources.get("match", 0) > 0
        match_markets = conn.execute(
            """
            select count(distinct market_id)
            from polymarket_wc2026_marts.polymarket_wc2026_market_minute_odds
            where minute_source = 'match'
            """
        ).fetchone()[0]
        assert match_markets == 15  # 5 games × 3 moneyline markets
        dq = conn.execute(
            """
            select has_match_rows, has_futures_rows, blocking_issue_keys
            from polymarket_wc2026_observability.polymarket_wc2026_market_minute_odds_data_quality
            """
        ).fetchone()
        assert dq == (True, True, None)
