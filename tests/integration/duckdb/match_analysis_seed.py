"""Shared synthetic seed helpers for isolated match-analysis dbt integration."""

from __future__ import annotations

import hashlib
import json

import duckdb
from tests.integration.match_minute_seed import create_test_reference_tables

from oddsfox_pipeline.ingestion.polymarket.match_order_book import (
    load_order_book_manifest,
)


def seed_order_book_contract(conn: duckdb.DuckDBPyConnection) -> None:
    create_test_reference_tables(conn)
    manifest = load_order_book_manifest()
    target = manifest.targets[0]
    scan_id = "published-scan"
    conn.execute(
        """
        insert into oddsfox_reference.openfootball_wc2026_schedule_fixtures values (
            104, 'final', 5, null, timestamp '2026-07-19 19:00:00',
            'Spain', 'Argentina', 'Test Venue', 'completed',
            'https://example.com/fixture', 104, 'fixture-hash',
            timestamp '2026-07-19 22:30:00'
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
            timestamp '2026-07-19 22:00:00',
            timestamp '2026-07-19 22:20:00',
            timestamp '2026-07-19 22:20:00'
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
            ) values (?, 104, ?, ?, ?, ?, ?, ?, 0, 'loaded', 1, 1, ?,
                timestamp '2026-07-19 22:20:00')
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
                ?, ?, 104, 'final', 'Spain', 'Argentina', ?, ?, ?, ?,
                'soccer_team_to_advance', ?, ?, ?, ?, ?, ?,
                to_timestamp(? / 1000.0), ?, 0, ?, ?, ?, false, '0.5',
                'api.pmxt.dev/api/polymarket/fetchOrderBook',
                timestamp '2026-07-19 22:20:00'
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
            timestamp '2026-07-19 22:20:00',
            timestamp '2026-07-19 22:25:00'
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
                ?, 104, ?, ?, ?, ?, ?, 0, 'loaded', 1, 1, ?,
                timestamp '2026-07-19 22:25:00'
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
                ?, ?, 104, ?, ?, ?, ?, ?, 0, '0.6', '3',
                'api.pmxt.dev/api/polymarket/fetchTrades',
                timestamp '2026-07-19 22:25:00'
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


def seed_portrait_alignment_contract(
    conn: duckdb.DuckDBPyConnection,
    *,
    kickoff_at_utc: str,
    match_started_at_utc: str,
) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS polymarket_wc2026_intermediate")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS polymarket_wc2026_intermediate
            .int_polymarket_wc2026_match_working_set (
            fifa_match_id BIGINT,
            scheduled_kickoff_at_utc TIMESTAMPTZ,
            match_started_at_utc TIMESTAMPTZ,
            fixture_mapping_count BIGINT,
            primary_mapping_count BIGINT
        )
        """
    )
    conn.execute(
        """
        DELETE FROM polymarket_wc2026_intermediate
            .int_polymarket_wc2026_match_working_set
        WHERE fifa_match_id = 104
        """
    )
    conn.execute(
        """
        INSERT INTO polymarket_wc2026_intermediate
            .int_polymarket_wc2026_match_working_set (
            fifa_match_id, scheduled_kickoff_at_utc, match_started_at_utc,
            fixture_mapping_count, primary_mapping_count
        ) VALUES (104, ?, ?, 1, 1)
        """,
        [kickoff_at_utc, match_started_at_utc],
    )
