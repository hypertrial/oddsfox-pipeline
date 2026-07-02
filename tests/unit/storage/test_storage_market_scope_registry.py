"""Storage tests for polymarket_ops.market_scope_registry."""

from __future__ import annotations

import importlib

import pytest

from oddsfox.config._reload_settings import reload_all_settings_modules
from oddsfox.storage.duckdb.market_scope_registry import (
    RegistryRow,
    clear_registry,
    get_registry_event_slugs,
    get_registry_market_ids,
    registry_market_count,
    upsert_registry_rows,
)


@pytest.fixture
def duck(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "registry.duckdb"))
    import oddsfox.storage.duckdb.connection as connection

    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()
    clear_registry()
    yield connection
    clear_registry()


def test_upsert_empty_rows_is_noop(duck):
    assert upsert_registry_rows([]) == 0


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
    n = upsert_registry_rows(
        [
            RegistryRow("m1", "event-a", "e1", "seed", scope_name="scope-a"),
            RegistryRow("m1", "event-b", "e2", "seed", scope_name="scope-b"),
        ]
    )

    assert n == 2
    assert registry_market_count() == 2
    assert registry_market_count("scope-a") == 1
    assert get_registry_market_ids("scope-a") == ["m1"]
    assert get_registry_market_ids("scope-b") == ["m1"]


def test_registry_helpers_are_scope_aware(duck):
    upsert_registry_rows(
        [
            RegistryRow("m1", "event-a", "e1", "seed", scope_name="scope-a"),
            RegistryRow("m2", "event-b", "e2", "seed", scope_name="scope-b"),
        ]
    )

    assert get_registry_event_slugs("scope-a") == ["event-a"]
    clear_registry("scope-a")
    assert registry_market_count("scope-a") == 0
    assert registry_market_count("scope-b") == 1
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
            FROM polymarket_ops.market_scope_registry
            WHERE market_id = 'm1'
            """
        ).fetchone()

    assert row == ("2026-fifa-world-cup-winner-595", "e1", "seed")


def test_market_scope_discovery_metadata_rejects_blank_scope(duck):
    from oddsfox.storage.duckdb.metadata import (
        get_market_scope_discovery_fully_checked,
    )

    with pytest.raises(ValueError, match="scope_name"):
        get_market_scope_discovery_fully_checked("")
