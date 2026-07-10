#!/usr/bin/env python3
"""Seed a disposable DuckDB database for deterministic dbt source freshness."""

from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

ensure_src_on_path()

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.naming import SCOPE_US_MIDTERMS_2026, SCOPE_WC2026
from oddsfox_pipeline.storage.duckdb.connection import init_duck_db
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    international_results_wc2026_raw_tbl,
    kalshi_ops_tbl,
    kalshi_raw_tbl,
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)
from oddsfox_pipeline.storage.duckdb.schemas.kalshi import (
    create_all_kalshi_test_raw_tables,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    create_all_scope_test_markets_tables,
)

FRESHNESS_SOURCE_TABLES: frozenset[tuple[str, str]] = frozenset(
    {
        ("international_results_wc2026_raw", "match_results"),
        ("kalshi_wc2026_ops", "pipeline_run_events"),
        ("kalshi_wc2026_raw", "events"),
        ("kalshi_wc2026_raw", "market_candlesticks_hourly"),
        ("kalshi_wc2026_raw", "markets"),
        ("polymarket_us_midterms_2026_ops", "pipeline_run_events"),
        ("polymarket_us_midterms_2026_raw", "market_tokens"),
        ("polymarket_us_midterms_2026_raw", "markets"),
        ("polymarket_us_midterms_2026_raw", "odds_history"),
        ("polymarket_us_midterms_2026_raw", "token_odds_daily"),
        ("polymarket_wc2026_ops", "pipeline_run_events"),
        ("polymarket_wc2026_raw", "market_tokens"),
        ("polymarket_wc2026_raw", "markets"),
        ("polymarket_wc2026_raw", "odds_history"),
        ("polymarket_wc2026_raw", "token_odds_daily"),
    }
)


def _seed_international_results(conn, now: datetime) -> None:
    conn.execute(
        f"""
        insert or replace into {international_results_wc2026_raw_tbl("match_results")}
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
            source_loaded_at
        )
        values (
            'freshness-match',
            current_date,
            'Argentina',
            'Mexico',
            null,
            null,
            'FIFA World Cup',
            'Mexico City',
            'Mexico',
            true,
            'scheduled',
            'https://example.com/results.csv',
            1,
            'freshness-hash',
            ?
        )
        """,
        [now],
    )


def _seed_polymarket_scope(conn, *, scope_name: str, now: datetime) -> None:
    raw_markets = polymarket_raw_tbl(scope_name, "markets")
    raw_tokens = polymarket_raw_tbl(scope_name, "market_tokens")
    raw_odds = polymarket_raw_tbl(scope_name, "odds_history")
    raw_daily = polymarket_raw_tbl(scope_name, "token_odds_daily")
    ops_events = polymarket_ops_tbl(scope_name, "pipeline_run_events")
    market_id = f"freshness-{scope_name}"
    token_id = f"freshness-{scope_name}-yes"
    conn.execute(
        f"""
        insert or replace into {raw_markets}
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
        values (?, 'Freshness market?', 'test', '', '["Yes","No"]', 1.0,
            true, false, ?, ?, ?, ?, 'freshness-event', 'event-1',
            'condition-1', 'test', null, null, '[]', ?, false, null, null)
        """,
        [market_id, now, now, now, market_id, f'["{token_id}","{token_id}-no"]'],
    )
    conn.execute(
        f"""
        insert or replace into {raw_tokens}
        (market_id, clobTokenIds, updated_at)
        values (?, ?, ?)
        """,
        [market_id, f'["{token_id}","{token_id}-no"]', now],
    )
    conn.execute(
        f"""
        insert or replace into {raw_odds}
        (clobTokenId, timestamp, price, ingested_at)
        values (?, ?, 0.5, ?)
        """,
        [token_id, int(now.timestamp()), now],
    )
    conn.execute(
        f"""
        insert or replace into {raw_daily}
        (
            clobTokenId,
            odds_date_utc,
            open_price,
            high_price,
            low_price,
            close_price,
            avg_price,
            observed_points,
            first_timestamp,
            last_timestamp,
            refreshed_at
        )
        values (?, current_date, 0.5, 0.5, 0.5, 0.5, 0.5, 1, ?, ?, ?)
        """,
        [token_id, int(now.timestamp()), int(now.timestamp()), now],
    )
    conn.execute(
        f"""
        insert or replace into {ops_events}
        (run_id, task_name, recorded_at, metrics_json)
        values (?, 'freshness', ?, ?)
        """,
        [str(uuid.uuid4()), now, json.dumps({"rows": 1}, sort_keys=True)],
    )


def _seed_kalshi(conn, now: datetime) -> None:
    conn.execute(
        f"""
        insert or replace into {kalshi_raw_tbl(SCOPE_WC2026, "events")}
        (
            event_ticker,
            series_ticker,
            title,
            sub_title,
            category,
            status,
            open_time,
            close_time,
            scraped_at
        )
        values ('FRESH-EVENT', 'FRESH-SERIES', 'Freshness event', '',
            'test', 'active', ?, ?, ?)
        """,
        [now, now, now],
    )
    conn.execute(
        f"""
        insert or replace into {kalshi_raw_tbl(SCOPE_WC2026, "markets")}
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
        values ('FRESH-MARKET', 'FRESH-EVENT', 'FRESH-SERIES',
            'Freshness market', '', 'Yes', 'No', 'active', 'binary',
            ?, ?, ?, 1, 1, '0.5', ?)
        """,
        [now, now, now, now],
    )
    conn.execute(
        f"""
        insert or replace into {kalshi_raw_tbl(SCOPE_WC2026, "market_candlesticks_hourly")}
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
        values ('FRESH-MARKET', date_trunc('hour', ?), 0.5, 0.5, 0.5, 0.5,
            0.5, 1, ?)
        """,
        [now, now],
    )
    conn.execute(
        f"""
        insert or replace into {kalshi_ops_tbl(SCOPE_WC2026, "pipeline_run_events")}
        (run_id, task_name, recorded_at, metrics_json)
        values (?, 'freshness', ?, ?)
        """,
        [str(uuid.uuid4()), now, json.dumps({"rows": 1}, sort_keys=True)],
    )


def main() -> None:
    now = datetime.now(timezone.utc)
    connection.reset_duckdb_connection_state()
    init_duck_db()
    conn = connection.get_persistent_connection()
    try:
        create_all_scope_test_markets_tables(conn)
        create_all_kalshi_test_raw_tables(conn)
        _seed_international_results(conn, now)
        _seed_polymarket_scope(conn, scope_name=SCOPE_WC2026, now=now)
        _seed_polymarket_scope(conn, scope_name=SCOPE_US_MIDTERMS_2026, now=now)
        _seed_kalshi(conn, now)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
