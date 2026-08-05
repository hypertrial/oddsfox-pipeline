"""Storage tests for polymarket_wc2026_ops.market_scope_registry."""

from __future__ import annotations

import pytest

from oddsfox_pipeline.storage.duckdb import (
    kalshi_market_scope_registry as kalshi_registry,
)
from oddsfox_pipeline.storage.duckdb.market_scope_registry import (
    RegistryRow,
    clear_registry,
    get_registry_event_slugs,
    get_registry_market_ids,
    prune_stale_event_catalog_registry_rows,
    registry_market_count,
    upsert_registry_rows,
)


def test_upsert_empty_rows_is_noop(duck):
    assert upsert_registry_rows([]) == 0


def test_prune_event_catalog_registry_with_no_active_markets(duck):
    upsert_registry_rows(
        [
            RegistryRow(
                "m1",
                "event-a",
                "e1",
                "event_catalog",
                scope_name="wc2026",
            )
        ]
    )

    assert (
        prune_stale_event_catalog_registry_rows(
            scope_name="wc2026", active_market_ids=[]
        )
        == 1
    )
    assert get_registry_market_ids() == []


def test_clear_registry_default_scope(duck):
    upsert_registry_rows(
        [RegistryRow("m1", "event-a", "e1", "seed", scope_name="wc2026")]
    )
    kalshi_registry.upsert_registry_rows(
        [
            kalshi_registry.KalshiRegistryRow(
                "KXTEST-1",
                "KXTEST-EVT",
                "KXTEST",
                "seed",
                scope_name="wc2026",
            )
        ]
    )
    clear_registry()
    assert registry_market_count("wc2026") == 0
    assert kalshi_registry.registry_market_count("wc2026") == 1


def test_upsert_and_query_registry(duck):
    n = upsert_registry_rows(
        [
            RegistryRow("m1", "2026-fifa-world-cup-winner-595", "e1", "events_api"),
            RegistryRow("m2", "2026-fifa-world-cup-winner-595", "e1", "events_api"),
        ]
    )
    assert n == 2
    assert registry_market_count() == 2
    assert get_registry_market_ids() == ["m1", "m2"]


def test_registry_grain_includes_scope_name(duck):
    upsert_registry_rows(
        [RegistryRow("m1", "event-a", "e1", "seed", scope_name="wc2026")]
    )
    with duck.get_connection() as conn:
        conn.execute(
            """
            INSERT INTO polymarket_wc2026_ops.market_scope_registry
            (scope_name, market_id, event_slug, event_id, source, refreshed_at)
            VALUES ('legacy-scope', 'm1', 'event-b', 'e2', 'seed', current_timestamp)
            """
        )

    assert registry_market_count("wc2026") == 1
    with duck.get_connection() as conn:
        legacy_count = conn.execute(
            """
            SELECT count(*)
            FROM polymarket_wc2026_ops.market_scope_registry
            WHERE scope_name = 'legacy-scope'
            """
        ).fetchone()[0]
    assert legacy_count == 1
    assert get_registry_market_ids("wc2026") == ["m1"]


def test_registry_helpers_are_scope_aware(duck):
    upsert_registry_rows(
        [RegistryRow("m1", "event-a", "e1", "seed", scope_name="wc2026")]
    )
    with duck.get_connection() as conn:
        conn.execute(
            """
            INSERT INTO polymarket_wc2026_ops.market_scope_registry
            (scope_name, market_id, event_slug, event_id, source, refreshed_at)
            VALUES ('legacy-scope', 'm2', 'event-b', 'e2', 'seed', current_timestamp)
            """
        )

    assert get_registry_event_slugs("wc2026") == ["event-a"]
    clear_registry("wc2026")
    assert registry_market_count("wc2026") == 0
    with duck.get_connection() as conn:
        legacy_count = conn.execute(
            """
            SELECT count(*)
            FROM polymarket_wc2026_ops.market_scope_registry
            WHERE scope_name = 'legacy-scope'
            """
        ).fetchone()[0]
    assert legacy_count == 1
    with pytest.raises(ValueError, match="scope_name"):
        get_registry_market_ids("")


def test_registry_upsert_preserves_existing_event_fields_when_new_values_null(duck):
    upsert_registry_rows(
        [RegistryRow("m1", "2026-fifa-world-cup-winner-595", "e1", "events_api")]
    )
    upsert_registry_rows([RegistryRow("m1", None, None, "seed")])

    with duck.get_connection() as conn:
        row = conn.execute(
            """
            SELECT event_slug, event_id, source
            FROM polymarket_wc2026_ops.market_scope_registry
            WHERE market_id = 'm1'
            """
        ).fetchone()

    assert row == ("2026-fifa-world-cup-winner-595", "e1", "seed")


def test_market_scope_discovery_metadata_rejects_blank_scope(duck):
    from oddsfox_pipeline.storage.duckdb.metadata import (
        get_market_scope_discovery_fully_checked,
    )

    with pytest.raises(ValueError, match="scope_name"):
        get_market_scope_discovery_fully_checked("")
