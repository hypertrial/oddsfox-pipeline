"""Synthetic 104/248/496 contract for isolated match-minute dbt integration."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone

import duckdb

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

SOURCE_REVISION = "a" * 40
SOURCE_PAYLOAD_SHA256 = "b" * 64
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
WC2026_SCHEDULE_TABLE = '"wc2026_staging"."wc2026_schedule_matches"'


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
            f"{event_title} - {group_item_title}",
            json.dumps(outcomes),
            started,
            started,
            finished,
            market_id,
            event_slug,
            event_id,
            event_title,
            started,
            finished,
            f"condition-{market_id}",
            sports_market_type,
            group_item_title,
            json.dumps([yes_token, no_token]),
        ],
    )
    return yes_token, no_token


def _seed_international_results(conn: duckdb.DuckDBPyConnection) -> None:
    table = international_results_wc2026_raw_tbl("match_results")
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
            tournament, city, country, neutral, match_status, source_url,
            source_row_number, source_row_hash, source_revision,
            source_payload_sha256, source_loaded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    """Populate the operator-local schedule shell used by wc2026_fixtures.

    Group-stage FIFA 1..72 only; knockout 73..104 mapping uses OpenFootball
    fixtures from seed_test_openfootball_schedule_fixtures. Call after dbt seed
    so the header-only CSV seed does not wipe these rows.
    """
    rows = []
    for match_id in range(1, 73):
        group_label = "ABCDEFGHIJKL"[(match_id - 1) // 6]
        rows.append(
            (
                str(match_id),
                "Group Stage",
                group_label,
                "1",
                "2026-06-11",
                "12:00 PM",
                f"Venue {match_id}",
                f"slot-home-{match_id}",
                f"slot-away-{match_id}",
                f"Home {match_id}",
                f"Away {match_id}",
                "scheduled",
                "synthetic-match-minute-ci",
            )
        )
    conn.executemany(
        f"""
        INSERT INTO {WC2026_SCHEDULE_TABLE} (
            match_id, stage, group_label, matchday, match_date, kickoff_time_et,
            venue, home_slot, away_slot, home_team, away_team, status, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
