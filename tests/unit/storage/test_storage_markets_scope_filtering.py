"""Unit tests for DuckDB market scope filtering."""

from __future__ import annotations

from tests.unit.storage.duckdb_storage_test_support import (
    T_LED,
    T_SK,
    _seed_markets,
)

import oddsfox_pipeline.storage.duckdb.markets as markets
from oddsfox_pipeline.ingestion.polymarket.odds import sync as odds_sync
from oddsfox_pipeline.ingestion.polymarket.odds.support import OddsSyncOptions
from oddsfox_pipeline.orchestration.config import HourlyOddsSyncConfig
from oddsfox_pipeline.storage.duckdb.market_scope_registry import (
    RegistryRow,
    upsert_registry_rows,
)


def test_iter_due_market_tokens_filters_due_closed_and_skipped(duck):
    _seed_markets(
        duck,
        [
            (
                "due_market",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
            (
                "future_market",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
            (
                "closed_market",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                True,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
            (
                "skip_market",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
        ],
        [
            ("due_market", '["tok_due"]'),
            ("future_market", '["tok_future"]'),
            ("closed_market", '["tok_closed"]'),
            ("skip_market", '["tok_skip"]'),
        ],
    )
    with markets.get_connection() as conn:
        conn.execute(
            f"""
            INSERT INTO {T_LED}
            (clobTokenId, next_check_at, fully_checked)
            VALUES
            ('tok_future', CURRENT_TIMESTAMP + INTERVAL 1 DAY, FALSE),
            ('tok_closed', CURRENT_TIMESTAMP - INTERVAL 1 DAY, TRUE)
            """
        )
        conn.execute(
            f"INSERT INTO {T_SK} (clobTokenId, reason) VALUES ('tok_skip', 'bad token')"
        )
    pages = list(
        markets.iter_due_market_tokens(
            page_size=10,
            cutoff_created_at="2024-01-01 00:00:00",
        )
    )
    flat = [row for page in pages for row in page]
    assert len(flat) == 1
    assert flat[0][0] == "due_market"
    assert flat[0][1] == "tok_due"
    assert flat[0][3] is False


def test_iter_due_market_tokens_without_cutoff(duck):
    _seed_markets(
        duck,
        [
            (
                "no_cutoff_market",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            )
        ],
        [("no_cutoff_market", '["tok_nocutoff"]')],
    )
    pages = list(markets.iter_due_market_tokens(page_size=10))
    flat = [row for page in pages for row in page]
    assert any(row[1] == "tok_nocutoff" for row in flat)


def test_iter_due_market_tokens_scopes_market_scope_and_counts_scope_skip(duck):
    _seed_markets(
        duck,
        [
            (
                "wc_market",
                "FIFA World Cup 2026 winner",
                "sports",
                "World Cup 2026",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                "world-cup-2026-winner",
                "2026-fifa-world-cup-winner-595",
                None,
            ),
            (
                "other_market",
                "Premier League winner",
                "sports",
                "Club market",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                "premier-league-winner",
                None,
                None,
            ),
        ],
        [("wc_market", '["tok_wc"]'), ("other_market", '["tok_other"]')],
        register_scope=False,
    )
    upsert_registry_rows(
        [
            RegistryRow(
                "wc_market",
                "2026-fifa-world-cup-winner-595",
                None,
                "seed",
            )
        ]
    )
    pages = list(
        markets.iter_due_market_tokens(
            page_size=10,
            cutoff_created_at="2024-01-01 00:00:00",
            market_scope="wc2026",
        )
    )
    flat = [row for page in pages for row in page]
    assert [row[1] for row in flat] == ["tok_wc"]
    counts = markets.count_due_market_token_exclusions(
        cutoff_created_at="2024-01-01 00:00:00",
        market_scope="wc2026",
    )
    assert counts["scope_skip"] == 1


def test_iter_due_market_tokens_skips_ended_markets_after_grace(duck):
    _seed_markets(
        duck,
        [
            (
                "ended_old",
                "FIFA World Cup 2026 old",
                "sports",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                "2000-01-01 00:00:00",
                "world-cup-2026-old",
                "2026-fifa-world-cup-winner-595",
                None,
            ),
            (
                "future_end",
                "FIFA World Cup 2026 future",
                "sports",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                "2999-01-01 00:00:00",
                "world-cup-2026-future",
                "2026-fifa-world-cup-winner-595",
                None,
            ),
        ],
        [("ended_old", '["tok_old"]'), ("future_end", '["tok_future_end"]')],
    )
    upsert_registry_rows(
        [
            RegistryRow("ended_old", "2026-fifa-world-cup-winner-595", None, "seed"),
            RegistryRow("future_end", "2026-fifa-world-cup-winner-595", None, "seed"),
        ]
    )
    pages = list(
        markets.iter_due_market_tokens(
            page_size=10,
            market_scope="wc2026",
            ended_market_grace_days=7,
        )
    )
    flat = [row for page in pages for row in page]
    assert [row[1] for row in flat] == ["tok_future_end"]
    counts = markets.count_due_market_token_exclusions(
        market_scope="wc2026",
        ended_market_grace_days=7,
    )
    assert counts["ended_market_skip"] == 1


def test_iter_due_market_tokens_requires_latest_payload_tokens(duck):
    """Odds planning must ignore raw market_tokens without payload coverage."""
    _seed_markets(
        duck,
        [
            (
                "with_payload",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
            (
                "raw_only",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
        ],
        [("with_payload", '["tok_payload"]')],
    )
    # Divergent enrichment-style raw tokens: market exists in registry/raw but
    # has no event_market_payload_snapshots row (and an extra token on the
    # payload market that staging would not expand).
    markets.save_market_tokens_batch(
        [
            ("with_payload", '["tok_payload", "tok_extra_raw"]'),
            ("raw_only", '["tok_raw_only"]'),
        ]
    )

    pages = list(markets.iter_due_market_tokens(page_size=10))
    tokens = {(row[0], row[1]) for page in pages for row in page}
    assert tokens == {("with_payload", "tok_payload")}

    force_pages = list(markets.iter_markets_with_tokens(page_size=10, json_array_only=True))
    force_markets = {row[0]: row[1] for page in force_pages for row in page}
    assert set(force_markets) == {"with_payload"}
    assert "tok_extra_raw" not in force_markets["with_payload"]
    assert "tok_payload" in force_markets["with_payload"]


def test_staging_odds_relationship_excludes_raw_only_tokens(duck):
    """Mirror the dbt odds→market_tokens relationship against payload SoT.

    Reproduces the full-pipeline failure mode: odds_history rows for a
    registry market that has raw market_tokens but no payload snapshot.
    """
    from tests.unit.storage.duckdb_storage_test_support import T_OH, T_PAYLOAD

    payload_tid = "111111111111111111111111111111111"
    orphan_tid = "222222222222222222222222222222222"
    _seed_markets(
        duck,
        [
            (
                "payload_mkt",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
            (
                "orphan_mkt",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
        ],
        [("payload_mkt", f'["{payload_tid}"]')],
        seed_payloads=True,
    )
    markets.save_market_tokens_batch(
        [
            ("payload_mkt", f'["{payload_tid}"]'),
            ("orphan_mkt", f'["{orphan_tid}"]'),
        ]
    )
    with markets.get_connection() as conn:
        conn.execute(
            f"""
            INSERT INTO {T_OH} (clobTokenId, timestamp, price)
            VALUES (?, 100, 0.4), (?, 100, 0.6), (?, 101, 0.55)
            """,
            [payload_tid, orphan_tid, orphan_tid],
        )
        # Staging-equivalent: expand latest payloads, then filter odds.
        orphan_rows = conn.execute(
            f"""
            WITH latest_payloads AS (
              SELECT market_id, clob_token_ids, scraped_at
              FROM {T_PAYLOAD}
              QUALIFY ROW_NUMBER() OVER (
                PARTITION BY market_id ORDER BY observed_at DESC
              ) = 1
            ),
            stg_tokens AS (
              SELECT json_extract_string(je.value, '$') AS clob_token_id
              FROM latest_payloads AS markets
              CROSS JOIN LATERAL json_each(markets.clob_token_ids) AS je
              WHERE markets.clob_token_ids IS NOT NULL
                AND left(trim(markets.clob_token_ids), 1) = '['
            ),
            stg_odds AS (
              SELECT o.clobTokenId AS clob_token_id
              FROM {T_OH} AS o
              INNER JOIN stg_tokens AS tokens
                ON o.clobTokenId = tokens.clob_token_id
            )
            SELECT COUNT(*) FROM {T_OH} o
            ANTI JOIN stg_odds s ON o.clobTokenId = s.clob_token_id
            """
        ).fetchone()[0]
        kept = conn.execute(
            f"""
            WITH latest_payloads AS (
              SELECT market_id, clob_token_ids
              FROM {T_PAYLOAD}
              QUALIFY ROW_NUMBER() OVER (
                PARTITION BY market_id ORDER BY observed_at DESC
              ) = 1
            ),
            stg_tokens AS (
              SELECT json_extract_string(je.value, '$') AS clob_token_id
              FROM latest_payloads AS markets
              CROSS JOIN LATERAL json_each(markets.clob_token_ids) AS je
            )
            SELECT COUNT(*) FROM {T_OH} o
            INNER JOIN stg_tokens t ON o.clobTokenId = t.clob_token_id
            """
        ).fetchone()[0]

    assert orphan_rows == 2  # both orphan_tid history points filtered out
    assert kept == 1


def test_wc2026_hourly_planning_keeps_zero_volume_ended_event_child(duck):
    token_id = "1234567890abcdef1234567890abcdef12"
    _seed_markets(
        duck,
        [
            (
                "zero_volume_child",
                "World Cup event child",
                "sports",
                "d",
                "[]",
                0.0,
                True,
                True,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                "2026-06-12 00:00:00",
                "world-cup-child",
                "world-cup-event",
                "event-eligible",
            )
        ],
        [("zero_volume_child", f'["{token_id}"]')],
    )
    config = HourlyOddsSyncConfig()
    options = OddsSyncOptions(
        clob_cutoff_date=config.clob_cutoff,
        fidelity=config.fidelity,
        force=config.force,
        rebuild_history=config.rebuild_history,
        overlap_minutes=config.overlap_minutes,
        skip_recent_minutes=config.skip_recent_minutes,
        market_page_size=config.market_page_size,
        reconcile_ledger=config.reconcile_ledger,
        short_range_first=config.short_range_first,
        market_scope="wc2026",
        ended_market_grace_days=config.ended_market_grace_days,
        min_volume=config.min_volume,
        max_volume=config.max_volume,
        history_backfill_days=config.history_backfill_days,
        empty_token_skip_runs=config.empty_skip_runs,
    )

    groups = list(
        odds_sync.iter_token_plans_paged(now_ts=1_900_000_000, options=options)
    )
    plans = [plan for group in groups for plan in group.token_plans]

    assert [(plan.market_id, plan.token_id) for plan in plans] == [
        ("zero_volume_child", token_id)
    ]
    assert plans[0].start_ts == 1_704_153_600
