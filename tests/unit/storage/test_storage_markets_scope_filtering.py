"""Unit tests for DuckDB market scope filtering."""

from __future__ import annotations

from tests.unit.storage.duckdb_storage_test_support import (
    T_LED,
    T_SK,
    _seed_markets,
)

import oddsfox.storage.duckdb.markets as markets
from oddsfox.storage.duckdb.market_scope_registry import (
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
