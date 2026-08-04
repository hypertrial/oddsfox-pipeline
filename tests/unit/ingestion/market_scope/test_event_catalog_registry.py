"""Sticky event-volume admission via event catalog snapshots."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from tests.unit.storage.duckdb_storage_test_support import initialize_isolated_duckdb

from oddsfox_pipeline.config.settings_polymarket import (
    POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
)
from oddsfox_pipeline.storage.duckdb.connection import get_connection
from oddsfox_pipeline.storage.duckdb.market_scope_registry import (
    build_registry_rows_from_event_catalog,
    clear_registry,
    upsert_registry_rows,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import polymarket_raw_tbl
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    bootstrap_polymarket_tables,
)

T_EVENT_SNAPSHOTS = polymarket_raw_tbl("wc2026", "event_snapshots")
T_EVENT_MARKET_SNAPSHOTS = polymarket_raw_tbl("wc2026", "event_market_snapshots")
OBSERVED_1 = datetime(2026, 8, 1, tzinfo=timezone.utc)
OBSERVED_2 = datetime(2026, 8, 2, tzinfo=timezone.utc)


@pytest.fixture
def event_catalog_db(monkeypatch, tmp_path):
    connection = initialize_isolated_duckdb(monkeypatch, tmp_path / "registry.duckdb")
    with get_connection() as conn:
        bootstrap_polymarket_tables(conn)
    yield connection
    connection.reset_duckdb_connection_state()
    clear_registry()


def _insert_event_snapshot(
    conn,
    *,
    event_id: str,
    volume: float,
    observed_at: datetime,
    row_order: int = 0,
) -> None:
    conn.execute(
        f"""
        insert into {T_EVENT_SNAPSHOTS} (
            event_id,
            event_slug,
            event_volume_usd_lifetime_reported,
            tags_json,
            series_slugs_json,
            candidate_sources_json,
            source_market_count,
            observed_at,
            source_endpoint
        ) values (?, ?, ?, '[]', '[]', '[]', 1, ?, '/events/keyset')
        """,
        [event_id, event_id, volume, observed_at],
    )


def _insert_event_market_snapshot(
    conn,
    *,
    event_id: str,
    market_id: str,
    observed_at: datetime,
    row_order: int = 0,
) -> None:
    conn.execute(
        f"""
        insert into {T_EVENT_MARKET_SNAPSHOTS} (
            event_id,
            market_id,
            source_ordinal,
            is_enclosing_event,
            observed_at
        ) values (?, ?, 0, true, ?)
        """,
        [event_id, market_id, observed_at],
    )


def test_build_registry_rows_skips_events_below_volume_floor(event_catalog_db) -> None:
    with event_catalog_db.get_connection() as conn:
        _insert_event_snapshot(
            conn,
            event_id="evt-low",
            volume=50_000.0,
            observed_at=OBSERVED_1,
        )
        _insert_event_market_snapshot(
            conn,
            event_id="evt-low",
            market_id="m-low",
            observed_at=OBSERVED_1,
        )
        rows = build_registry_rows_from_event_catalog(
            event_min_volume_usd=POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
        )

    assert rows == []


def test_build_registry_rows_admits_eligible_event_markets(event_catalog_db) -> None:
    with event_catalog_db.get_connection() as conn:
        _insert_event_snapshot(
            conn,
            event_id="evt-high",
            volume=150_000.0,
            observed_at=OBSERVED_1,
        )
        _insert_event_market_snapshot(
            conn,
            event_id="evt-high",
            market_id="m-1",
            observed_at=OBSERVED_1,
        )
        _insert_event_market_snapshot(
            conn,
            event_id="evt-high",
            market_id="m-2",
            observed_at=OBSERVED_1,
            row_order=1,
        )
        rows = build_registry_rows_from_event_catalog(
            event_min_volume_usd=POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
        )

    assert {row.market_id for row in rows} == {"m-1", "m-2"}
    assert all(row.source == "event_catalog" for row in rows)
    assert all(row.is_event_volume_eligible for row in rows)


def test_build_registry_rows_keeps_sticky_eligibility_after_volume_drops(
    event_catalog_db,
) -> None:
    with event_catalog_db.get_connection() as conn:
        _insert_event_snapshot(
            conn,
            event_id="evt-sticky",
            volume=150_000.0,
            observed_at=OBSERVED_1,
        )
        _insert_event_market_snapshot(
            conn,
            event_id="evt-sticky",
            market_id="m-sticky",
            observed_at=OBSERVED_1,
        )
        upsert_registry_rows(
            build_registry_rows_from_event_catalog(
                event_min_volume_usd=POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
            )
        )
        _insert_event_snapshot(
            conn,
            event_id="evt-sticky",
            volume=25_000.0,
            observed_at=OBSERVED_2,
            row_order=1,
        )
        rows = build_registry_rows_from_event_catalog(
            event_min_volume_usd=POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
        )

    assert [row.market_id for row in rows] == ["m-sticky"]
    assert rows[0].is_event_volume_eligible
