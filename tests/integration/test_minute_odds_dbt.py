"""Integration coverage for the unified minute-odds dbt graph.

Uses a one-game match fixture plus one futures market so the graph stays small.
dbt unit tests cover OHLC/precedence SQL; this file proves end-to-end wiring.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import duckdb
from tests.integration.conftest import dbt_subprocess_env, write_dbt_profile
from tests.integration.dbt_cli import run_dbt
from tests.integration.match_minute_seed import (
    FETCH_RUN_ID,
    INGESTED_AT,
    KICKOFF_UTC,
    SOURCE_PAYLOAD_SHA256,
    SOURCE_REVISION,
    SOURCE_URL,
    WC2026_SCHEDULE_TABLE,
    _insert_market,
)

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.naming import SCOPE_WC2026
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    international_results_wc2026_raw_tbl,
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)
from oddsfox_pipeline.storage.duckdb.schemas.openfootball import (
    seed_test_openfootball_schedule_fixtures,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    create_all_scope_test_markets_tables,
)


def _seed_slim_match_leg(conn: duckdb.DuckDBPyConnection) -> None:
    """One group moneyline triple with three in-window minutes (not the 104/98 spine)."""
    create_all_scope_test_markets_tables(conn)
    seed_test_openfootball_schedule_fixtures(conn)

    game_id = 1
    home, away = f"Home {game_id}", f"Away {game_id}"
    started = KICKOFF_UTC + timedelta(minutes=game_id)
    finished = started + timedelta(minutes=5)
    event_title = f"{home} vs. {away}"
    token_rows: list[tuple[str, str, str]] = []
    for prop_idx, title in (
        (0, home),
        (1, f"Draw ({home} vs. {away})"),
        (2, away),
    ):
        market_id = f"ml-{game_id}-{prop_idx}"
        yes_token = f"{market_id}-yes"
        no_token = f"{market_id}-no"
        _insert_market(
            conn,
            market_id=market_id,
            event_id=f"primary-{game_id}",
            event_slug=f"primary-{game_id}",
            event_title=event_title,
            started=started,
            finished=finished,
            sports_market_type="moneyline",
            group_item_title=title,
            outcomes=["Yes", "No"],
            yes_token=yes_token,
            no_token=no_token,
        )
        token_rows.append((market_id, yes_token, no_token))

    ir = international_results_wc2026_raw_tbl("match_results")
    conn.execute(
        f"""
        INSERT INTO {ir} (
            match_id, match_date, home_team, away_team, home_score, away_score,
            tournament, city, country, neutral, match_status, source_url,
            source_row_number, source_row_hash, source_revision,
            source_payload_sha256, source_loaded_at
        ) VALUES (?, ?, ?, ?, 1, 0, 'FIFA World Cup', 'Venue', 'United States',
                  true, 'completed', ?, 1, 'row-hash-001', ?, ?, ?)
        """,
        [
            "match-1",
            date(2026, 6, 11),
            home,
            away,
            SOURCE_URL,
            SOURCE_REVISION,
            SOURCE_PAYLOAD_SHA256,
            INGESTED_AT,
        ],
    )

    history = polymarket_raw_tbl(SCOPE_WC2026, "match_minute_odds_history")
    audit = polymarket_ops_tbl(SCOPE_WC2026, "match_minute_odds_fetch_audit")
    history_rows = []
    audit_rows = []
    for market_id, yes_token, no_token in token_rows:
        for minute_offset in range(3):
            minute = (started + timedelta(minutes=minute_offset)).replace(
                second=0, microsecond=0
            )
            epoch = int(minute.replace(tzinfo=timezone.utc).timestamp())
            for token_id in (yes_token, no_token):
                history_rows.append(
                    (
                        market_id,
                        token_id,
                        epoch,
                        0.55,
                        1,
                        started,
                        finished,
                        INGESTED_AT,
                    )
                )
        for token_id in (yes_token, no_token):
            audit_rows.append(
                (
                    FETCH_RUN_ID,
                    market_id,
                    token_id,
                    "success",
                    True,
                    1,
                    started,
                    finished,
                    int(started.replace(tzinfo=timezone.utc).timestamp()),
                    int(finished.replace(tzinfo=timezone.utc).timestamp()),
                    3,
                    3,
                    "c" * 64,
                    "https://clob.polymarket.com/prices-history",
                    INGESTED_AT,
                    INGESTED_AT + timedelta(minutes=1),
                    None,
                    None,
                )
            )
    conn.executemany(
        f"""
        INSERT INTO {history} (
            market_id, clobTokenId, timestamp, price, fidelity_minutes,
            window_start_at, window_end_at, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        history_rows,
    )
    conn.executemany(
        f"""
        INSERT INTO {audit} (
            fetch_run_id, market_id, clobTokenId, fetch_status, raw_published,
            fidelity_minutes, exact_window_start_at, exact_window_end_at,
            request_start_epoch, request_end_epoch, source_row_count,
            in_game_row_count, in_game_history_sha256, source_endpoint,
            fetch_started_at, fetch_finished_at, error_type, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        audit_rows,
    )


def _seed_futures_minute_rows(conn: duckdb.DuckDBPyConnection) -> None:
    created_naive = datetime(2026, 5, 1)
    observed_naive = datetime(2026, 7, 1)
    end_naive = datetime(2026, 7, 15)
    now_naive = datetime(2026, 7, 1, 12, 0, 0)
    window_start = datetime(2026, 6, 11)
    window_end = datetime(2026, 7, 15)
    question = "Who wins the tournament?"
    outcomes = '["Yes", "No"]'
    token_ids = '["futures-yes", "futures-no"]'
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
        [question, outcomes, created_naive, observed_naive, end_naive, token_ids],
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
        [observed_naive, created_naive],
    )
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
        [observed_naive, created_naive],
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
        [
            question,
            outcomes,
            created_naive,
            observed_naive,
            end_naive,
            token_ids,
            observed_naive,
        ],
    )
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
                window_start,
                window_end,
                now_naive,
            )
        )
        rows.append(
            (
                "futures-winner",
                "futures-no",
                ts,
                0.6 - minute * 0.05,
                1,
                window_start,
                window_end,
                now_naive,
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
    for token_id, points in (("futures-yes", 3), ("futures-no", 3)):
        conn.execute(
            """
            insert into polymarket_wc2026_ops.futures_minute_odds_fetch_audit (
                fetch_run_id, market_id, clobTokenId, fetch_status, raw_published,
                fidelity_minutes, exact_window_start_at, exact_window_end_at,
                request_start_epoch, request_end_epoch, source_row_count,
                window_row_count, window_history_sha256, source_endpoint,
                fetch_started_at, fetch_finished_at
            ) values (
                'ci-futures-minute', 'futures-winner', ?, 'success', true,
                1, timestamp '2026-06-11 00:00:00', timestamp '2026-07-15 00:00:00',
                1749600000, 1752537600, ?, ?, ?,
                'https://clob.polymarket.com/prices-history',
                timestamp '2026-07-01 12:00:00', timestamp '2026-07-01 12:01:00'
            )
            """,
            [token_id, points, points, "c" * 64],
        )


def _seed_slim_schedule(conn: duckdb.DuckDBPyConnection) -> None:
    """One group-stage schedule row (full helper inserts 72; too heavy here)."""
    conn.execute(
        f"""
        INSERT INTO {WC2026_SCHEDULE_TABLE} (
            match_id, stage, group_label, matchday, match_date, kickoff_time_et,
            venue, home_slot, away_slot, home_team, away_team, status, source
        ) VALUES (
            '1', 'Group Stage', 'A', '1', '2026-06-11', '12:00 PM',
            'Venue 1', 'slot-home-1', 'slot-away-1', 'Home 1', 'Away 1',
            'scheduled', 'synthetic-minute-odds-ci'
        )
        """
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
        _seed_slim_match_leg(conn)
        _seed_futures_minute_rows(conn)

    write_dbt_profile(dbt_profiles_dir, db_path, threads=1)
    env = dbt_subprocess_env(
        db_path=db_path,
        profiles_dir=dbt_profiles_dir,
        target_dir=dbt_target_dir,
        dbt_threads=1,
    )
    # Seeds needed by match_working_set; polygon/pmxt stay excluded.
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
        _seed_slim_schedule(conn)

    # run (not build): dbt unit/data tests for this tag live in dbt-minute-odds-ci.
    run_dbt(
        [
            "run",
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
        assert sources.get("futures") == 3
        assert sources.get("match", 0) > 0

        raw_tokens = dict(
            conn.execute(
                """
                select clobTokenId, count(*)
                from polymarket_wc2026_raw.futures_minute_odds_history
                group by 1
                """
            ).fetchall()
        )
        assert raw_tokens == {"futures-no": 3, "futures-yes": 3}

        mart_futures_tokens = conn.execute(
            """
            select distinct clob_token_id
            from polymarket_wc2026_marts.polymarket_wc2026_market_minute_odds
            where minute_source = 'futures'
            """
        ).fetchall()
        assert mart_futures_tokens == [("futures-yes",)]

        match_markets = conn.execute(
            """
            select count(distinct market_id)
            from polymarket_wc2026_marts.polymarket_wc2026_market_minute_odds
            where minute_source = 'match'
            """
        ).fetchone()[0]
        assert match_markets == 3

        dupes = conn.execute(
            """
            select count(*) from (
                select market_id, odds_minute_epoch, count(*) as n
                from polymarket_wc2026_marts.polymarket_wc2026_market_minute_odds
                group by 1, 2
                having count(*) > 1
            )
            """
        ).fetchone()[0]
        assert dupes == 0

        dq = conn.execute(
            """
            select has_match_rows, has_futures_rows, blocking_issue_keys,
                   futures_tokens_with_prices
            from polymarket_wc2026_observability.polymarket_wc2026_market_minute_odds_data_quality
            """
        ).fetchone()
        assert dq == (True, True, None, 1)
