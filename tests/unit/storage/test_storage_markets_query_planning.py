"""Unit tests for DuckDB market query and planning helpers."""

from __future__ import annotations

import pytest
from tests.unit.storage.duckdb_storage_test_support import (
    T_LED,
    T_M,
    T_SK,
    T_UNR,
    _insert_minimal_market,
    _seed_markets,
)

import oddsfox_pipeline.storage.duckdb as pkg
import oddsfox_pipeline.storage.duckdb.markets as markets
from oddsfox_pipeline.storage.duckdb.market_scope_registry import (
    RegistryRow,
    upsert_registry_rows,
)


def test_markets_count_and_ids(duck):
    assert markets.get_market_count() == 0
    with duck.get_connection() as conn:
        _insert_minimal_market(conn, "a")
    assert markets.get_market_count() == 1
    assert "a" in markets.get_all_market_ids()


def test_get_markets_with_tokens_and_iter(duck):
    _seed_markets(
        duck,
        [
            (
                "gw",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-01 00:00:00",
                "2024-01-01 00:00:00",
                None,
                None,
                None,
            )
        ],
        [("gw", '["a","b"]')],
    )
    rows = markets.get_markets_with_tokens()
    assert len(rows) >= 1
    pages = list(
        markets.iter_markets_with_tokens(
            page_size=1, cutoff_created_at="2020-01-01 00:00:00", json_array_only=True
        )
    )
    assert pages


def test_slug_event_end_date_queries(duck):
    with markets.get_connection() as conn:
        conn.execute(
            f"""INSERT OR REPLACE INTO {T_M}
            (id, question, category, description, outcomes, volume, active, closed,
             created_at, scraped_at, end_date, slug, event_slug)
            VALUES ('99', 'q','c','d','[]',1.0,1,0,
             '2024-01-01','2024-01-01', NULL, '', '')"""
        )
    assert "99" in markets.get_markets_without_slugs(limit=10)
    assert "99" in markets.get_markets_without_event_slugs(limit=10)
    assert "99" in markets.get_markets_without_end_date(limit=10)
    markets.save_slugs_batch([("slug99", "99")])
    markets.save_event_slugs_batch([("es99", "99")])
    markets.save_end_dates_batch([("2025-12-31 00:00:00", "99")])


def test_iter_markets_json_array_only_no_cutoff(duck):
    _seed_markets(
        duck,
        [
            (
                "ja",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-01 00:00:00",
                "2024-01-01 00:00:00",
                None,
                None,
                None,
            )
        ],
        [("ja", '["tok"]')],
    )
    pages = list(markets.iter_markets_with_tokens(page_size=10, json_array_only=True))
    assert pages


def test_iter_markets_with_cutoff_without_json_array_filter(duck):
    _seed_markets(
        duck,
        [
            (
                "jb",
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
        [("jb", '["tok"]')],
    )
    pages = list(
        markets.iter_markets_with_tokens(
            page_size=10,
            cutoff_created_at="2024-01-01 00:00:00",
            json_array_only=False,
        )
    )
    assert pages


def test_count_candidate_market_tokens_due_only(duck):
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
    counts = markets.count_candidate_market_tokens(
        cutoff_created_at="2024-01-01 00:00:00",
        due_only=True,
    )
    assert counts == {"candidate_tokens": 1, "candidate_markets": 1}


def test_due_token_iterator_count_parity(duck):
    """count_candidate_market_tokens(due_only=True) matches iter_due_market_tokens rows."""
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
    kwargs = {"cutoff_created_at": "2024-01-01 00:00:00"}
    iter_count = sum(
        len(page) for page in markets.iter_due_market_tokens(page_size=10, **kwargs)
    )
    counts = markets.count_candidate_market_tokens(due_only=True, **kwargs)
    assert iter_count == counts["candidate_tokens"]


def test_count_candidate_market_tokens_force_mode(duck):
    _seed_markets(
        duck,
        [
            (
                "m_two_tok",
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
                "m_one_tok",
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
            ("m_two_tok", '["tok_a", "tok_b"]'),
            ("m_one_tok", '["tok_c"]'),
        ],
    )
    counts = markets.count_candidate_market_tokens(
        cutoff_created_at="2024-01-01 00:00:00",
        due_only=False,
    )
    assert counts == {"candidate_tokens": 3, "candidate_markets": 2}


def test_count_candidate_market_tokens_force_mode_honors_ended_market_grace(duck):
    _seed_markets(
        duck,
        [
            (
                "live_market",
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
                "old_ended_market",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                True,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                "2000-01-01 00:00:00",
                None,
                None,
            ),
        ],
        [
            ("live_market", '["tok_live"]'),
            ("old_ended_market", '["tok_old_a", "tok_old_b"]'),
        ],
    )

    counts = markets.count_candidate_market_tokens(
        cutoff_created_at="2024-01-01 00:00:00",
        due_only=False,
        ended_market_grace_days=7,
    )
    all_counts = markets.count_candidate_market_tokens(
        cutoff_created_at="2024-01-01 00:00:00",
        due_only=False,
        ended_market_grace_days=None,
    )

    assert counts == {"candidate_tokens": 1, "candidate_markets": 1}
    assert all_counts == {"candidate_tokens": 3, "candidate_markets": 2}


def test_count_candidate_market_tokens_without_cutoff(duck):
    _seed_markets(
        duck,
        [
            (
                "m_nocutoff",
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
        [("m_nocutoff", '["tok_nocutoff"]')],
    )
    counts = markets.count_candidate_market_tokens(due_only=True)
    assert counts == {"candidate_tokens": 1, "candidate_markets": 1}


def test_volume_where_clause_helpers():
    assert markets._volume_where_clause(None, None) == ""
    clause = markets._volume_where_clause(100_000.0, None)
    assert "COALESCE(m.volume, 0) >= 100000.0" in clause
    clause = markets._volume_where_clause(None, 100_000.0)
    assert "COALESCE(m.volume, 0) < 100000.0" in clause
    with pytest.raises(ValueError, match="min_volume"):
        markets._validate_volume_bound("bad", name="min_volume")
    with pytest.raises(ValueError, match="max_volume"):
        markets._volume_where_clause(None, -5.0)


def test_iter_markets_with_tokens_volume_bounds(duck):
    _seed_markets(
        duck,
        [
            (
                "m_whale",
                "q",
                "c",
                "d",
                "[]",
                150_000.0,
                True,
                False,
                "2024-01-01 00:00:00",
                "2024-01-01 00:00:00",
                None,
                None,
                None,
                None,
            ),
            (
                "m_small",
                "q",
                "c",
                "d",
                "[]",
                500.0,
                True,
                False,
                "2024-01-01 00:00:00",
                "2024-01-01 00:00:00",
                None,
                None,
                None,
                None,
            ),
        ],
        [
            ("m_whale", '["tok_whale"]'),
            ("m_small", '["tok_small"]'),
        ],
    )
    whale_pages = list(
        markets.iter_markets_with_tokens(
            page_size=10,
            json_array_only=True,
            min_volume=100_000.0,
        )
    )
    whale_ids = {row[0] for page in whale_pages for row in page}
    assert whale_ids == {"m_whale"}

    daily_pages = list(
        markets.iter_markets_with_tokens(
            page_size=10,
            json_array_only=True,
            max_volume=100_000.0,
        )
    )
    daily_ids = {row[0] for page in daily_pages for row in page}
    assert daily_ids == {"m_small"}


def test_count_candidate_market_tokens_force_mode_without_cutoff(duck):
    _seed_markets(
        duck,
        [
            (
                "m_force_nocutoff",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-01 00:00:00",
                "2024-01-01 00:00:00",
                None,
                None,
                None,
                None,
            ),
        ],
        [("m_force_nocutoff", '["tok_force"]')],
    )
    counts = markets.count_candidate_market_tokens(due_only=False)
    assert counts == {"candidate_tokens": 1, "candidate_markets": 1}


def test_get_markets_missing_any_metadata_individual_field_predicates(duck):
    with duck.get_connection() as conn:
        _insert_minimal_market(
            conn,
            "missing-all",
            slug="",
            event_slug="event-present",
            end_date=None,
        )
    upsert_registry_rows([RegistryRow("missing-all", None, None, "test")])

    assert markets.get_markets_missing_any_metadata(
        include_tokens=True,
        include_slugs=False,
        include_event_slugs=False,
        include_end_dates=False,
    ) == ["missing-all"]
    assert markets.get_markets_missing_any_metadata(
        include_tokens=False,
        include_slugs=True,
        include_event_slugs=False,
        include_end_dates=False,
    ) == ["missing-all"]
    assert markets.get_markets_missing_any_metadata(
        include_tokens=False,
        include_slugs=False,
        include_event_slugs=False,
        include_end_dates=True,
    ) == ["missing-all"]


def test_market_metadata_unresolved_cooldown_suppresses_event_slug_retry(duck):
    with duck.get_connection() as conn:
        _insert_minimal_market(
            conn,
            "cooldown",
            question="World Cup 2026 winner",
            slug="world-cup-2026-winner",
            event_slug="2026-fifa-world-cup-winner-595",
        )
        conn.execute(f"UPDATE {T_M} SET event_slug = NULL WHERE id = 'cooldown'")
    upsert_registry_rows([RegistryRow("cooldown", None, None, "seed")])
    markets.save_tokens_batch([("cooldown", '["tok"]')])

    assert (
        markets.get_markets_missing_any_metadata(
            include_tokens=False,
            include_slugs=False,
            include_event_slugs=False,
            include_end_dates=False,
        )
        == []
    )
    markets.mark_market_metadata_unresolved([])

    assert markets.get_markets_missing_any_metadata(
        include_tokens=False,
        include_slugs=False,
        include_event_slugs=True,
        include_end_dates=False,
        market_scope="wc2026",
        limit=10,
    ) == ["cooldown"]

    markets.mark_market_metadata_unresolved(
        [("cooldown", "event_slug", "missing")],
        retry_after_hours=168,
    )
    markets.mark_market_metadata_unresolved(
        [("cooldown", "event_slug", "still missing")],
        retry_after_hours=168,
    )
    with markets.get_connection() as conn:
        attempts, reason = conn.execute(
            f"SELECT attempts, reason FROM {T_UNR} WHERE market_id = 'cooldown'"
        ).fetchone()
    assert attempts == 2
    assert reason == "still missing"
    assert (
        markets.get_markets_missing_any_metadata(
            include_tokens=False,
            include_slugs=False,
            include_event_slugs=True,
            include_end_dates=False,
            market_scope="wc2026",
        )
        == []
    )

    with markets.get_connection() as conn:
        conn.execute(
            f"UPDATE {T_UNR} SET next_retry_at = TIMESTAMP '2000-01-01 00:00:00'"
        )
    assert markets.get_markets_missing_any_metadata(
        include_tokens=False,
        include_slugs=False,
        include_event_slugs=True,
        include_end_dates=False,
        market_scope="wc2026",
    ) == ["cooldown"]

    markets.save_event_slugs_batch([("world-cup-2026", "cooldown")])
    with markets.get_connection() as conn:
        remaining = conn.execute(f"SELECT count(*) FROM {T_UNR}").fetchone()[0]
    assert remaining == 0


def test_fetch_market_ids_no_limit(duck):
    assert isinstance(markets._fetch_market_ids("SELECT 1 WHERE 1=0", limit=None), list)


def test_storage_duckdb_package_import():

    assert pkg.ensure_duck_db
    assert pkg.save_market_tokens_batch
