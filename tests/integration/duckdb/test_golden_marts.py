"""Golden-row regression coverage for shipped public marts."""

from __future__ import annotations

import csv
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from tests.integration.conftest import write_dbt_profile

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.naming import SCOPE_US_MIDTERMS_2026, SCOPE_WC2026
from oddsfox_pipeline.storage.duckdb.connection import init_duck_db
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    international_results_wc2026_raw_tbl,
    kalshi_ops_tbl,
    kalshi_raw_tbl,
    openfootball_wc2026_raw_tbl,
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)
from oddsfox_pipeline.storage.duckdb.schemas.kalshi import (
    create_all_kalshi_test_raw_tables,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    create_all_scope_test_markets_tables,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_ROOT = REPO_ROOT / "dbt"
GOLDEN_ROOT = REPO_ROOT / "tests" / "fixtures" / "golden"
ODDS_HOUR = datetime(2099, 1, 1, 10, tzinfo=timezone.utc)
ODDS_HOUR_EPOCH = int(ODDS_HOUR.timestamp())
MATCH_ODDS_HOUR = datetime(2026, 7, 14, 16, tzinfo=timezone.utc)
MATCH_ODDS_HOUR_EPOCH = int(MATCH_ODDS_HOUR.timestamp())


def _run_dbt(args: list[str], *, profiles_dir: Path, env: dict[str, str]) -> None:
    cmd = [
        sys.executable,
        "-m",
        "dbt.cli.main",
        *args,
        "--project-dir",
        str(DBT_ROOT),
        "--profiles-dir",
        str(profiles_dir),
    ]
    proc = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def _expected(name: str) -> list[dict[str, str]]:
    with (GOLDEN_ROOT / name).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _dict_rows(conn: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, str]]:
    cursor = conn.execute(query)
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, map(str, row))) for row in cursor.fetchall()]


def _seed_international_results(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        f"""
        insert into {international_results_wc2026_raw_tbl("match_results")}
        (
            match_id,
            match_date,
            home_team,
            away_team,
            home_score,
            away_score,
            tournament,
            city,
            country,
            neutral,
            match_status,
            source_url,
            source_row_number,
            source_row_hash,
            source_revision,
            source_payload_sha256,
            source_loaded_at
        )
        values (
            'golden-arg-mex',
            date '2026-06-12',
            'Argentina',
            'Mexico',
            null,
            null,
            'FIFA World Cup',
            'Mexico City',
            'Mexico',
            true,
            'scheduled',
            'https://example.com/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/results.csv',
            1,
            'golden-hash',
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
            timestamp '2099-01-01 00:00:00'
        )
        """
    )


def _seed_polymarket_scope(
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str,
    market_id: str,
    token_id: str,
    question: str,
    event_slug: str,
    close_price: float,
) -> None:
    conn.execute(
        f"""
        insert into {polymarket_raw_tbl(scope_name, "markets")}
        (
            id,
            question,
            category,
            description,
            outcomes,
            volume,
            active,
            closed,
            created_at,
            scraped_at,
            end_date,
            slug,
            event_slug,
            event_id,
            condition_id,
            sports_market_type,
            game_start_time,
            group_item_title,
            tags,
            clob_token_ids,
            is_resolved,
            winning_outcome,
            winning_clob_token_id
        )
        values (?, ?, 'sports', '', '["Yes","No"]', 10000.0, true, false,
            timestamp '2099-01-01 00:00:00', timestamp '2099-01-01 00:00:00',
            timestamp '2099-07-19 00:00:00', ?, ?, 'event-1',
            'condition-1', 'outright', null, null, '[]', ?, false, null, null)
        """,
        [market_id, question, market_id, event_slug, f'["{token_id}","{token_id}-no"]'],
    )
    conn.execute(
        f"""
        insert into {polymarket_raw_tbl(scope_name, "market_tokens")}
        (market_id, clobTokenIds, updated_at)
        values (?, ?, timestamp '2099-01-01 00:00:00')
        """,
        [market_id, f'["{token_id}","{token_id}-no"]'],
    )
    conn.execute(
        f"""
        insert into {polymarket_ops_tbl(scope_name, "market_scope_registry")}
        (scope_name, market_id, event_slug, event_id, source, refreshed_at)
        values (?, ?, ?, 'event-1', 'golden', timestamp '2099-01-01 00:00:00')
        """,
        [scope_name, market_id, event_slug],
    )
    conn.executemany(
        f"""
        insert into {polymarket_raw_tbl(scope_name, "odds_history")}
        (clobTokenId, timestamp, price, ingested_at)
        values (?, ?, ?, timestamp '2099-01-01 11:00:00')
        """,
        [
            (token_id, ODDS_HOUR_EPOCH + 300, close_price - 0.1),
            (token_id, ODDS_HOUR_EPOCH + 2700, close_price),
        ],
    )


def _seed_kalshi(conn: duckdb.DuckDBPyConnection) -> None:
    rows = [
        (
            "KXWCSTAGEOFELIM-26ARG-R16",
            "KXWCSTAGEOFELIM-26ARG",
            "KXWCSTAGEOFELIM",
            "Argentina",
            "",
            "Argentina",
            "",
            "active",
            "binary",
            100,
            10,
            "0.32",
        ),
        (
            "KXWCGROUPWIN-26A-MEX",
            "KXWCGROUPWIN-26A",
            "KXWCGROUPWIN",
            "Mexico",
            "",
            "Mexico",
            "",
            "active",
            "binary",
            100,
            10,
            "0.41",
        ),
    ]
    conn.executemany(
        f"""
        insert into {kalshi_raw_tbl(SCOPE_WC2026, "markets")}
        (
            market_ticker,
            event_ticker,
            series_ticker,
            title,
            subtitle,
            yes_sub_title,
            no_sub_title,
            status,
            market_type,
            open_time,
            close_time,
            expiration_time,
            volume,
            open_interest,
            last_price_dollars,
            scraped_at
        )
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, timestamp '2099-01-01 00:00:00',
            timestamp '2099-07-19 00:00:00', timestamp '2099-07-19 00:00:00',
            ?, ?, ?, timestamp '2099-01-01 00:00:00')
        """,
        rows,
    )
    conn.executemany(
        f"""
        insert into {kalshi_ops_tbl(SCOPE_WC2026, "market_scope_registry")}
        (scope_name, market_ticker, event_ticker, series_ticker, source, refreshed_at)
        values ('wc2026', ?, ?, ?, 'golden', timestamp '2099-01-01 00:00:00')
        """,
        [(row[0], row[1], row[2]) for row in rows],
    )
    conn.executemany(
        f"""
        insert into {kalshi_raw_tbl(SCOPE_WC2026, "market_candlesticks_hourly")}
        (
            market_ticker,
            hour_start_utc,
            open_price,
            high_price,
            low_price,
            close_price,
            avg_price,
            volume,
            refreshed_at
        )
        values (?, timestamp '2099-01-01 10:00:00', ?, ?, ?, ?, ?, 100,
            timestamp '2099-01-01 11:00:00')
        """,
        [
            ("KXWCSTAGEOFELIM-26ARG-R16", 0.30, 0.34, 0.29, 0.32, 0.31),
            ("KXWCGROUPWIN-26A-MEX", 0.39, 0.42, 0.38, 0.41, 0.40),
        ],
    )


def _seed_wc2026_match_odds(conn: duckdb.DuckDBPyConnection) -> None:
    conn.executemany(
        f"""
        insert into {openfootball_wc2026_raw_tbl("knockout_fixtures")}
        (
            fifa_match_id,
            stage_key,
            stage_rank,
            kickoff_at_utc,
            home_team,
            away_team,
            venue,
            match_status,
            source_url,
            source_line_number,
            source_line_hash,
            source_loaded_at
        )
        values (?, ?, ?, ?, ?, ?, 'Test Venue', 'scheduled',
            'https://example.com/cup_finals.txt', ?, ?, timestamp '2026-07-13 00:00:00')
        """,
        [
            (
                101,
                "semifinal",
                4,
                datetime(2026, 7, 14, 19),
                "France",
                "Spain",
                101,
                "fixture-101",
            ),
            (
                103,
                "third_place",
                0,
                datetime(2026, 7, 18, 21),
                "L101",
                "L102",
                103,
                "fixture-103",
            ),
        ],
    )
    conn.execute(
        f"""
        insert into {polymarket_raw_tbl(SCOPE_WC2026, "markets")}
        (
            id,
            question,
            category,
            description,
            outcomes,
            volume,
            active,
            closed,
            created_at,
            scraped_at,
            end_date,
            slug,
            event_slug,
            event_id,
            condition_id,
            sports_market_type,
            game_start_time,
            group_item_title,
            tags,
            clob_token_ids,
            is_resolved,
            winning_outcome,
            winning_clob_token_id
        )
        values (
            'pm-match-101',
            'France vs. Spain: Team to Advance',
            'sports',
            '',
            '["Spain","France"]',
            1.0,
            true,
            false,
            timestamp '2026-07-11 00:00:00',
            timestamp '2026-07-13 00:00:00',
            timestamp '2026-07-14 19:00:00',
            'fifwc-fra-esp-2026-07-14-team-to-advance',
            'fifwc-fra-esp-2026-07-14-more-markets',
            'event-101',
            'condition-101',
            'soccer_team_to_advance',
            timestamp '2026-07-14 19:00:00',
            'Team to Advance',
            '[]',
            '["pm-spain","pm-france"]',
            false,
            null,
            null
        )
        """
    )
    conn.execute(
        f"""
        insert into {polymarket_raw_tbl(SCOPE_WC2026, "market_tokens")}
        (market_id, clobTokenIds, updated_at)
        values ('pm-match-101', '["pm-spain","pm-france"]',
            timestamp '2026-07-13 00:00:00')
        """
    )
    conn.execute(
        f"""
        insert into {polymarket_ops_tbl(SCOPE_WC2026, "market_scope_registry")}
        (scope_name, market_id, event_slug, event_id, source, refreshed_at)
        values ('wc2026', 'pm-match-101',
            'fifwc-fra-esp-2026-07-14-more-markets', 'event-101', 'golden',
            timestamp '2026-07-13 00:00:00')
        """
    )
    conn.executemany(
        f"""
        insert into {polymarket_raw_tbl(SCOPE_WC2026, "odds_history")}
        (clobTokenId, timestamp, price, ingested_at)
        values (?, ?, ?, timestamp '2026-07-14 21:00:00')
        """,
        [
            ("pm-spain", MATCH_ODDS_HOUR_EPOCH + 600, 0.40),
            ("pm-france", MATCH_ODDS_HOUR_EPOCH + 600, 0.60),
            ("pm-france", MATCH_ODDS_HOUR_EPOCH + 2 * 3600 + 600, 0.65),
        ],
    )
    kalshi_markets = [
        ("KXWCADVANCE-101-ESP", "Spain advances"),
        ("KXWCADVANCE-101-FRA", "France advances"),
    ]
    conn.executemany(
        f"""
        insert into {kalshi_raw_tbl(SCOPE_WC2026, "markets")}
        (
            market_ticker,
            event_ticker,
            series_ticker,
            title,
            subtitle,
            yes_sub_title,
            no_sub_title,
            status,
            market_type,
            open_time,
            close_time,
            expiration_time,
            occurrence_datetime,
            volume,
            open_interest,
            last_price_dollars,
            scraped_at
        )
        values (?, 'KXWCADVANCE-101', 'KXWCADVANCE',
            'France vs. Spain', '', ?, '', 'active', 'binary',
            timestamp '2026-07-11 00:00:00', timestamp '2026-07-14 23:00:00',
            timestamp '2026-07-14 23:00:00', timestamp '2026-07-14 19:00:00',
            1, 1, '0.50', timestamp '2026-07-13 00:00:00')
        """,
        kalshi_markets,
    )
    conn.executemany(
        f"""
        insert into {kalshi_ops_tbl(SCOPE_WC2026, "market_scope_registry")}
        (scope_name, market_ticker, event_ticker, series_ticker, source, refreshed_at)
        values ('wc2026', ?, 'KXWCADVANCE-101', 'KXWCADVANCE', 'golden',
            timestamp '2026-07-13 00:00:00')
        """,
        [(market_ticker,) for market_ticker, _ in kalshi_markets],
    )
    conn.executemany(
        f"""
        insert into {kalshi_raw_tbl(SCOPE_WC2026, "market_candlesticks_hourly")}
        (
            market_ticker,
            hour_start_utc,
            open_price,
            high_price,
            low_price,
            close_price,
            avg_price,
            volume,
            refreshed_at
        )
        values (?, timestamp '2026-07-14 17:00:00', ?, ?, ?, ?, ?, 1,
            timestamp '2026-07-14 21:00:00')
        """,
        [
            ("KXWCADVANCE-101-ESP", 0.44, 0.46, 0.44, 0.45, 0.45),
            ("KXWCADVANCE-101-FRA", 0.54, 0.56, 0.54, 0.55, 0.55),
        ],
    )


def test_public_marts_match_golden_rows(tmp_path: Path, monkeypatch, dbt_profiles_dir):
    db_path = tmp_path / "golden.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    write_dbt_profile(dbt_profiles_dir, db_path)
    connection.reset_duckdb_connection_state()
    init_duck_db()

    with duckdb.connect(str(db_path)) as conn:
        create_all_scope_test_markets_tables(conn)
        create_all_kalshi_test_raw_tables(conn)
        _seed_international_results(conn)
        _seed_polymarket_scope(
            conn,
            scope_name=SCOPE_WC2026,
            market_id="pm-wc-arg-win",
            token_id="pm-wc-arg-yes",
            question="Will Argentina win the 2026 FIFA World Cup?",
            event_slug="world-cup",
            close_price=0.70,
        )
        _seed_polymarket_scope(
            conn,
            scope_name=SCOPE_US_MIDTERMS_2026,
            market_id="pm-mid-house",
            token_id="pm-mid-house-yes",
            question="Which party will win the 2026 House popular vote?",
            event_slug="us-midterms-2026",
            close_price=0.54,
        )
        _seed_kalshi(conn)
        _seed_wc2026_match_odds(conn)

    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(db_path)
    env["DUCKDB_NAME"] = str(db_path)
    _run_dbt(["seed"], profiles_dir=dbt_profiles_dir, env=env)
    _run_dbt(
        [
            "run",
            "--full-refresh",
            "--select",
            "+international_results_wc2026_team_status",
            "+polymarket_wc2026_knockout_token_hourly_odds",
            "+polymarket_us_midterms_2026_market_token_hourly_odds",
            "+kalshi_wc2026_stage_market_hourly_odds",
            "+kalshi_wc2026_group_winner_market_hourly_odds",
            "+wc2026_knockout_match_hourly_odds",
        ],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )

    with duckdb.connect(str(db_path), read_only=True) as conn:
        assert _dict_rows(
            conn,
            """
            select
                team_name,
                tournament_status,
                case when is_still_alive then 'true' else 'false' end
                    as is_still_alive,
                cast(matches_played as varchar) as matches_played,
                next_stage_key
            from international_results_wc2026_marts.international_results_wc2026_team_status
            order by team_name
            """,
        ) == _expected("international_results_wc2026_team_status.csv")
        assert _dict_rows(
            conn,
            """
            select
                market_id,
                clob_token_id,
                stage_key,
                market_direction,
                canonical_team_name,
                cast(odds_hour_epoch as varchar) as odds_hour_epoch,
                cast(close_price as varchar) as close_price,
                cast(observed_points as varchar) as observed_points
            from polymarket_wc2026_marts.polymarket_wc2026_knockout_token_hourly_odds
            order by market_id, clob_token_id, odds_hour_epoch
            """,
        ) == _expected("polymarket_wc2026_knockout_token_hourly_odds.csv")
        assert _dict_rows(
            conn,
            """
            select
                market_id,
                clob_token_id,
                question,
                event_slug,
                cast(odds_hour_epoch as varchar) as odds_hour_epoch,
                cast(close_price as varchar) as close_price,
                cast(observed_points as varchar) as observed_points
            from polymarket_us_midterms_2026_marts.polymarket_us_midterms_2026_market_token_hourly_odds
            order by market_id, clob_token_id, odds_hour_epoch
            """,
        ) == _expected("polymarket_us_midterms_2026_market_token_hourly_odds.csv")
        assert _dict_rows(
            conn,
            """
            select
                'kalshi_wc2026_stage_market_hourly_odds' as model_name,
                market_ticker,
                progression_outcome_label as semantic_key,
                canonical_team_name,
                cast(odds_hour_epoch as varchar) as odds_hour_epoch,
                cast(round(progression_close_price, 6) as varchar) as close_price
            from kalshi_wc2026_marts.kalshi_wc2026_stage_market_hourly_odds
            union all
            select
                'kalshi_wc2026_group_winner_market_hourly_odds' as model_name,
                market_ticker,
                group_letter as semantic_key,
                canonical_team_name,
                cast(odds_hour_epoch as varchar) as odds_hour_epoch,
                cast(round(close_price, 6) as varchar) as close_price
            from kalshi_wc2026_marts.kalshi_wc2026_group_winner_market_hourly_odds
            order by model_name desc, market_ticker
            """,
        ) == _expected("kalshi_wc2026_hourly_odds.csv")
        assert _dict_rows(
            conn,
            """
            select
                cast(fifa_match_id as varchar) as fifa_match_id,
                cast(odds_hour_utc as varchar) as odds_hour_utc,
                stage_key,
                home_team,
                away_team,
                coalesce(cast(polymarket_home_advance_price as varchar), '')
                    as polymarket_home_advance_price,
                coalesce(cast(polymarket_away_advance_price as varchar), '')
                    as polymarket_away_advance_price,
                coalesce(cast(kalshi_home_advance_price as varchar), '')
                    as kalshi_home_advance_price,
                coalesce(cast(kalshi_away_advance_price as varchar), '')
                    as kalshi_away_advance_price,
                polymarket_market_id,
                kalshi_event_ticker,
                cast(polymarket_hour_complete as varchar) as polymarket_hour_complete,
                cast(kalshi_hour_complete as varchar) as kalshi_hour_complete,
                cast(both_sources_complete as varchar) as both_sources_complete,
                cast(is_pre_kickoff as varchar) as is_pre_kickoff
            from wc2026_marts.wc2026_knockout_match_hourly_odds
            order by fifa_match_id, odds_hour_epoch
            """,
        ) == _expected("wc2026_knockout_match_hourly_odds.csv")

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            f"""
            delete from {polymarket_raw_tbl(SCOPE_WC2026, "odds_history")}
            where clobTokenId in ('pm-france', 'pm-spain')
                and timestamp < ?
            """,
            [MATCH_ODDS_HOUR_EPOCH + 3600],
        )
        conn.execute(
            f"""
            delete from {kalshi_raw_tbl(SCOPE_WC2026, "market_candlesticks_hourly")}
            where market_ticker like 'KXWCADVANCE-101-%'
            """
        )
        conn.execute(
            f"""
            insert into {polymarket_raw_tbl(SCOPE_WC2026, "odds_history")}
            (clobTokenId, timestamp, price, ingested_at)
            values ('pm-spain', ?, 0.35, timestamp '2026-07-14 22:00:00')
            """,
            [MATCH_ODDS_HOUR_EPOCH + 2 * 3600 + 1200],
        )

    _run_dbt(
        [
            "run",
            "--select",
            "int_polymarket_wc2026_match_hourly_odds",
            "int_kalshi_wc2026_match_hourly_odds",
            "wc2026_knockout_match_hourly_odds",
        ],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )

    with duckdb.connect(str(db_path), read_only=True) as conn:
        assert conn.execute(
            """
            select count(*)
            from polymarket_wc2026_intermediate.int_polymarket_wc2026_match_hourly_odds
            where odds_hour_epoch = ?
            """,
            [MATCH_ODDS_HOUR_EPOCH],
        ).fetchone() == (2,)
        assert conn.execute(
            """
            select count(*)
            from kalshi_wc2026_intermediate.int_kalshi_wc2026_match_hourly_odds
            where odds_hour_epoch = ?
            """,
            [MATCH_ODDS_HOUR_EPOCH + 3600],
        ).fetchone() == (2,)
        assert conn.execute(
            """
            select
                polymarket_home_advance_price,
                polymarket_away_advance_price,
                polymarket_hour_complete
            from wc2026_marts.wc2026_knockout_match_hourly_odds
            where fifa_match_id = 101 and odds_hour_epoch = ?
            """,
            [MATCH_ODDS_HOUR_EPOCH + 2 * 3600],
        ).fetchone() == (0.65, 0.35, True)
