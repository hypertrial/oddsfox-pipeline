"""Synthetic 104/248/496 contract for isolated match-minute dbt integration."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone

import duckdb

from oddsfox_pipeline.naming import SCOPE_WC2026
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    create_all_scope_test_markets_tables,
)

SOURCE_REVISION = "a" * 40
SOURCE_PAYLOAD_SHA256 = "b" * 64


def reference_tbl(name: str) -> str:
    """Qualified synthetic Scraper reference table used by integration tests."""
    return f'"oddsfox_reference"."{name}"'


def create_test_reference_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create only the final Scraper tables consumed by Pipeline models."""
    conn.execute("create schema if not exists oddsfox_reference")
    conn.execute(
        """
        create table if not exists oddsfox_reference.international_results_wc2026_matches (
            match_id varchar, match_date date, stage_key varchar, stage_rank integer,
            home_team varchar, away_team varchar, home_score integer,
            away_score integer, tournament varchar, city varchar, country varchar,
            neutral boolean, match_status varchar, is_knockout boolean,
            source_url varchar, source_row_number integer, source_row_hash varchar,
            source_revision varchar, source_payload_sha256 varchar,
            source_loaded_at timestamp
        )
        """
    )
    conn.execute(
        """
        create table if not exists oddsfox_reference.wc2026_fixtures (
            match_id integer, stage varchar, group_label varchar, match_date date,
            kickoff_time_et varchar, venue varchar, home_team varchar,
            away_team varchar, home_slot varchar, away_slot varchar, status varchar,
            source_provenance varchar, matchday integer, kickoff_at_et timestamp,
            stage_order integer, is_knockout boolean
        )
        """
    )
    conn.execute(
        """
        create table if not exists oddsfox_reference.international_results_wc2026_team_aliases (
            market_team_name varchar, canonical_team_name varchar
        )
        """
    )
    conn.execute(
        """
        create table if not exists oddsfox_reference.international_results_wc2026_team_status (
            team_name varchar, tournament_status varchar, is_still_alive boolean,
            eliminated_stage_key varchar, eliminated_match_date date,
            next_match_date date, next_stage_key varchar, matches_played integer,
            wins integer, draws integer, losses integer, goals_for integer,
            goals_against integer, latest_completed_match_date date,
            latest_completed_stage_key varchar
        )
        """
    )
    conn.execute(
        """
        create table if not exists oddsfox_reference.wc2026_team_canonical_aliases (
            variant_match_key varchar, canonical_match_key varchar
        )
        """
    )
    conn.execute(
        """
        create table if not exists oddsfox_reference.openfootball_wc2026_schedule_fixtures (
            fifa_match_id integer, stage_key varchar, stage_rank integer,
            group_label varchar, kickoff_at_utc timestamp, home_team varchar,
            away_team varchar, venue varchar, match_status varchar,
            source_url varchar, source_line_number integer,
            source_line_hash varchar, source_loaded_at timestamp
        )
        """
    )


def seed_test_openfootball_schedule_fixtures(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Seed the final Scraper fixture table used by isolated dbt tests."""
    create_test_reference_tables(conn)
    conn.execute("delete from oddsfox_reference.openfootball_wc2026_schedule_fixtures")
    conn.executemany(
        """
        insert into oddsfox_reference.openfootball_wc2026_schedule_fixtures
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                match_id,
                "group_stage" if match_id <= 72 else "round_of_32",
                0 if match_id <= 72 else 1,
                "ABCDEFGHIJKL"[(match_id - 1) // 6] if match_id <= 72 else None,
                datetime(2026, 6, 11, 16, tzinfo=timezone.utc).replace(tzinfo=None),
                f"Home {match_id}",
                f"Away {match_id}",
                f"Venue {match_id}",
                "scheduled",
                "https://example.invalid/reference",
                match_id,
                f"{match_id:064x}",
                INGESTED_AT.replace(tzinfo=None),
            )
            for match_id in range(1, 105)
        ],
    )


SOURCE_URL = "https://example.com/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/results.csv"
FETCH_RUN_ID = "ci-match-minute-fetch"
# ponytail: 97m30s + minute truncation yields 98 inclusive spine buckets per market;
# changing duration without updating EXPECTED_MART_ROW_COUNT breaks the contract.
GAME_DURATION = timedelta(minutes=97, seconds=30)
KICKOFF_UTC = datetime(2026, 6, 11, 16, 0, 0)
INGESTED_AT = datetime(2026, 7, 1, 12, 0, 0)
MINUTES_PER_GAME = 98
EXPECTED_GAMES = 104
EXPECTED_MARKETS = 248
EXPECTED_GROUP_MARKETS = 216
EXPECTED_KNOCKOUT_MARKETS = 32
EXPECTED_TOKENS = 496
EXPECTED_MART_ROW_COUNT = EXPECTED_MARKETS * MINUTES_PER_GAME
EXPECTED_DATA_QUALITY_ROW = (
    EXPECTED_GAMES,
    EXPECTED_MARKETS,
    EXPECTED_GROUP_MARKETS,
    EXPECTED_KNOCKOUT_MARKETS,
    EXPECTED_TOKENS,
    EXPECTED_GAMES,
    EXPECTED_GAMES,
    EXPECTED_GAMES,
    1,
    1,
    0,
    "published",
    EXPECTED_TOKENS,
    EXPECTED_TOKENS,
    0,
    0,
    0,
    EXPECTED_TOKENS,
    0,
    0,
    0,
    None,
)
WC2026_SCHEDULE_TABLE = reference_tbl("wc2026_fixtures")


def _game_times(game_id: int) -> tuple[datetime, datetime]:
    started = KICKOFF_UTC + timedelta(minutes=game_id)
    finished = started + GAME_DURATION
    return started, finished


def _insert_market(
    conn: duckdb.DuckDBPyConnection,
    *,
    market_id: str,
    event_id: str,
    event_slug: str,
    event_title: str,
    started: datetime,
    finished: datetime,
    sports_market_type: str,
    group_item_title: str,
    outcomes: list[str],
    yes_token: str,
    no_token: str,
) -> tuple[str, str]:
    markets = polymarket_raw_tbl(SCOPE_WC2026, "markets")
    payloads = polymarket_raw_tbl(SCOPE_WC2026, "event_market_payload_snapshots")
    question = f"{event_title} - {group_item_title}"
    outcomes_json = json.dumps(outcomes)
    token_ids = json.dumps([yes_token, no_token])
    condition_id = f"condition-{market_id}"
    conn.execute(
        f"""
        INSERT INTO {markets} (
            id, question, category, description, outcomes, volume, active, closed,
            created_at, scraped_at, end_date, slug, event_slug, event_id,
            event_title, event_start_time, event_finished_time, event_ended,
            condition_id, sports_market_type, group_item_title, clob_token_ids,
            is_resolved, winning_outcome, winning_clob_token_id, tags
        ) VALUES (
            ?, ?, 'sports', '', ?, 1000.0, false, true,
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, true,
            ?, ?, ?, ?,
            false, null, null, '[]'
        )
        """,
        [
            market_id,
            question,
            outcomes_json,
            started,
            started,
            finished,
            market_id,
            event_slug,
            event_id,
            event_title,
            started,
            finished,
            condition_id,
            sports_market_type,
            group_item_title,
            token_ids,
        ],
    )
    # stg_polymarket_wc2026_markets reads payload snapshots, not markets.
    conn.execute(
        f"""
        INSERT INTO {payloads} (
            market_id, question, category, description, outcomes, volume,
            active, closed, created_at, scraped_at, end_date, slug, event_slug,
            event_id, event_title, event_start_time, event_finished_time,
            event_ended, condition_id, sports_market_type, group_item_title,
            clob_token_ids, is_resolved, tags, observed_at
        ) VALUES (
            ?, ?, 'sports', '', ?, 1000.0, false, true,
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, true,
            ?, ?, ?, ?,
            false, '[]', ?
        )
        """,
        [
            market_id,
            question,
            outcomes_json,
            started,
            started,
            finished,
            market_id,
            event_slug,
            event_id,
            event_title,
            started,
            finished,
            condition_id,
            sports_market_type,
            group_item_title,
            token_ids,
            started,
        ],
    )
    return yes_token, no_token


def _seed_international_results(conn: duckdb.DuckDBPyConnection) -> None:
    table = reference_tbl("international_results_wc2026_matches")
    rows = []
    for game_id in range(1, EXPECTED_GAMES + 1):
        rows.append(
            (
                f"match-{game_id}",
                date(2026, 6, 11),
                f"Home {game_id}",
                f"Away {game_id}",
                1,
                0,
                "group_stage" if game_id <= 72 else "round_of_32",
                1 if game_id <= 72 else 2,
                "FIFA World Cup",
                f"Venue {game_id}",
                "United States",
                True,
                "completed",
                SOURCE_URL,
                game_id,
                f"row-hash-{game_id:03d}",
                SOURCE_REVISION,
                SOURCE_PAYLOAD_SHA256,
                INGESTED_AT,
            )
        )
    conn.executemany(
        f"""
        INSERT INTO {table} (
            match_id, match_date, home_team, away_team, home_score, away_score,
            stage_key, stage_rank, tournament, city, country, neutral, match_status, source_url,
            source_row_number, source_row_hash, source_revision,
            source_payload_sha256, source_loaded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def _seed_markets(
    conn: duckdb.DuckDBPyConnection,
) -> list[tuple[str, str, str, datetime, datetime]]:
    token_rows: list[tuple[str, str, str, datetime, datetime]] = []
    for game_id in range(1, EXPECTED_GAMES + 1):
        home = f"Home {game_id}"
        away = f"Away {game_id}"
        started, finished = _game_times(game_id)
        event_title = f"{home} vs. {away}"
        primary_event_id = f"primary-{game_id}"
        primary_slug = f"primary-{game_id}"

        moneyline_titles = (
            (0, home, home),
            (1, f"Draw ({home} vs. {away})", "draw"),
            (2, away, away),
        )
        for prop_idx, title, _team_key in moneyline_titles:
            market_id = f"ml-{game_id}-{prop_idx}"
            yes_token = f"{market_id}-yes"
            no_token = f"{market_id}-no"
            _insert_market(
                conn,
                market_id=market_id,
                event_id=primary_event_id,
                event_slug=primary_slug,
                event_title=event_title,
                started=started,
                finished=finished,
                sports_market_type="moneyline",
                group_item_title=title,
                outcomes=["Yes", "No"],
                yes_token=yes_token,
                no_token=no_token,
            )
            if game_id <= 72:
                token_rows.append((market_id, yes_token, no_token, started, finished))

        if game_id >= 73:
            market_id = f"adv-{game_id}"
            yes_token = f"{market_id}-home"
            no_token = f"{market_id}-away"
            _insert_market(
                conn,
                market_id=market_id,
                event_id=f"advance-{game_id}",
                event_slug=f"advance-{game_id}",
                event_title=f"{event_title} - More Markets",
                started=started,
                finished=finished,
                sports_market_type="soccer_team_to_advance",
                group_item_title="Team to Advance",
                outcomes=[home, away],
                yes_token=yes_token,
                no_token=no_token,
            )
            token_rows.append((market_id, yes_token, no_token, started, finished))

    return token_rows


def _seed_minute_history(
    conn: duckdb.DuckDBPyConnection,
    token_rows: list[tuple[str, str, str, datetime, datetime]],
) -> None:
    history = polymarket_raw_tbl(SCOPE_WC2026, "match_minute_odds_history")
    rows: list[tuple[object, ...]] = []
    for market_id, yes_token, no_token, started, finished in token_rows:
        minute = started.replace(second=0, microsecond=0)
        end_minute = finished.replace(second=0, microsecond=0)
        while minute <= end_minute:
            epoch = int(minute.replace(tzinfo=timezone.utc).timestamp())
            for token_id in (yes_token, no_token):
                rows.append(
                    (
                        market_id,
                        token_id,
                        epoch,
                        0.5,
                        1,
                        started,
                        finished,
                        INGESTED_AT,
                    )
                )
            minute += timedelta(minutes=1)

    conn.executemany(
        f"""
        INSERT INTO {history} (
            market_id, clobTokenId, timestamp, price, fidelity_minutes,
            window_start_at, window_end_at, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def _seed_fetch_audit(
    conn: duckdb.DuckDBPyConnection,
    token_rows: list[tuple[str, str, str, datetime, datetime]],
) -> None:
    audit = polymarket_ops_tbl(SCOPE_WC2026, "match_minute_odds_fetch_audit")
    rows: list[tuple[object, ...]] = []
    for market_id, yes_token, no_token, started, finished in token_rows:
        for token_id in (yes_token, no_token):
            in_game_rows = MINUTES_PER_GAME
            rows.append(
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
                    in_game_rows,
                    in_game_rows,
                    hashlib.sha256(token_id.encode()).hexdigest(),
                    "https://clob.polymarket.com/prices-history",
                    INGESTED_AT,
                    INGESTED_AT + timedelta(minutes=1),
                    None,
                    None,
                )
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
        rows,
    )


def seed_wc2026_schedule_matches(conn: duckdb.DuckDBPyConnection) -> None:
    """Populate the final Scraper fixture contract used by market models.

    Group-stage FIFA 1..72 only; knockout 73..104 mapping uses OpenFootball
    fixtures from seed_test_openfootball_schedule_fixtures.
    """
    rows = []
    for match_id in range(1, 73):
        group_label = "ABCDEFGHIJKL"[(match_id - 1) // 6]
        rows.append(
            (
                str(match_id),
                "Group Stage",
                group_label,
                date(2026, 6, 11),
                "12:00 PM",
                f"Venue {match_id}",
                f"Home {match_id}",
                f"Away {match_id}",
                f"slot-home-{match_id}",
                f"slot-away-{match_id}",
                "scheduled",
                "synthetic-match-minute-ci",
                1,
                datetime(2026, 6, 11, 12),
                1,
                False,
            )
        )
    conn.executemany(
        f"""
        INSERT INTO {WC2026_SCHEDULE_TABLE} (
            match_id, stage, group_label, match_date, kickoff_time_et,
            venue, home_team, away_team, home_slot, away_slot, status,
            source_provenance, matchday, kickoff_at_et, stage_order, is_knockout
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def seed_match_minute_contract(conn: duckdb.DuckDBPyConnection) -> None:
    create_all_scope_test_markets_tables(conn)
    seed_test_openfootball_schedule_fixtures(conn)
    _seed_international_results(conn)
    token_rows = _seed_markets(conn)
    _seed_minute_history(conn, token_rows)
    _seed_fetch_audit(conn, token_rows)
