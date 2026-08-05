"""Tests for scripts/cleanup_polymarket_wc2026_registry_hygiene.py."""

from __future__ import annotations

import importlib.util
from datetime import datetime, timezone
from pathlib import Path

from tests.unit.storage.duckdb_storage_test_support import initialize_isolated_duckdb

from oddsfox_pipeline.storage.duckdb.connection import get_connection
from oddsfox_pipeline.storage.duckdb.market_scope_registry import (
    RegistryRow,
    clear_registry,
    get_registry_market_ids,
    upsert_registry_rows,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    bootstrap_polymarket_tables,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "scripts" / "cleanup_polymarket_wc2026_registry_hygiene.py"


def _load_cleanup():
    spec = importlib.util.spec_from_file_location(
        "cleanup_polymarket_wc2026_registry_hygiene", SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.cleanup_registry_hygiene


def test_cleanup_registry_hygiene_removes_synthetic_and_api_orphans(
    monkeypatch, tmp_path
) -> None:
    cleanup_registry_hygiene = _load_cleanup()
    connection = initialize_isolated_duckdb(monkeypatch, tmp_path / "cleanup.duckdb")
    observed = datetime(2026, 8, 1, tzinfo=timezone.utc)
    with get_connection() as conn:
        bootstrap_polymarket_tables(conn)
        conn.execute(
            f"""
            insert into {polymarket_raw_tbl("wc2026", "event_snapshots")} (
                event_id, event_slug, event_volume_usd_lifetime_reported,
                tags_json, series_slugs_json, candidate_sources_json,
                source_market_count, observed_at, source_endpoint
            ) values
                ('evt-A', 'evt-A', 150000, '[]', '[]', '[]', 1, ?, '/events/keyset'),
                ('evt-B', 'evt-B', 150000, '[]', '[]', '[]', 1, ?, '/events/keyset')
            """,
            [observed, observed],
        )
        conn.execute(
            f"""
            insert into {polymarket_raw_tbl("wc2026", "event_market_snapshots")} (
                event_id, market_id, source_ordinal, is_enclosing_event, observed_at
            ) values
                ('evt-A', 'm-shared', 0, true, ?),
                ('evt-B', 'm-shared', 0, false, ?)
            """,
            [observed, observed],
        )
        # Bypass numeric guard via direct SQL for synthetic registry row.
        conn.execute(
            f"""
            insert into {polymarket_ops_tbl("wc2026", "market_scope_registry")} (
                scope_name, market_id, event_slug, event_id, source,
                refreshed_at, event_volume_usd_lifetime_reported,
                is_event_volume_eligible, first_eligible_at
            ) values (
                'wc2026', 'm-shared', 'evt-A', 'evt-A', 'event_catalog',
                ?, 150000, true, ?
            )
            """,
            [observed, observed],
        )
        upsert_registry_rows(
            [
                RegistryRow(
                    market_id="200100",
                    event_slug="noise",
                    event_id="100100",
                    source="events_api",
                    is_event_volume_eligible=False,
                ),
                RegistryRow(
                    market_id="200101",
                    event_slug="keep",
                    event_id="100101",
                    source="event_catalog",
                    is_event_volume_eligible=True,
                    event_volume_usd_lifetime_reported=150_000.0,
                ),
            ]
        )
        dry = cleanup_registry_hygiene(conn, apply=False)
        assert dry["would_delete_registry_synthetic"] == 1
        assert dry["would_delete_registry_ineligible_api"] == 1
        assert get_registry_market_ids() == ["200100", "200101", "m-shared"]

        applied = cleanup_registry_hygiene(conn, apply=True)
        assert applied["deleted_registry_synthetic"] == 1
        assert applied["deleted_registry_ineligible_api"] == 1
        assert applied["remaining_registry_synthetic"] == 0
        assert applied["remaining_event_snapshots_synthetic"] == 0
        assert get_registry_market_ids() == ["200101"]

    connection.reset_duckdb_connection_state()
    clear_registry()
