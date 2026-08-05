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
    RegistryRow,
    build_registry_rows_from_event_catalog,
    clear_registry,
    get_registry_market_ids,
    prune_ineligible_api_registry_rows,
    prune_stale_event_catalog_registry_rows,
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
    is_enclosing_event: bool = True,
) -> None:
    conn.execute(
        f"""
        insert into {T_EVENT_MARKET_SNAPSHOTS} (
            event_id,
            market_id,
            source_ordinal,
            is_enclosing_event,
            observed_at
        ) values (?, ?, 0, ?, ?)
        """,
        [event_id, market_id, is_enclosing_event, observed_at],
    )


def test_build_registry_rows_skips_events_below_volume_floor(event_catalog_db) -> None:
    with event_catalog_db.get_connection() as conn:
        _insert_event_snapshot(
            conn,
            event_id="100001",
            volume=50_000.0,
            observed_at=OBSERVED_1,
        )
        _insert_event_market_snapshot(
            conn,
            event_id="100001",
            market_id="200001",
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
            event_id="100002",
            volume=150_000.0,
            observed_at=OBSERVED_1,
        )
        _insert_event_market_snapshot(
            conn,
            event_id="100002",
            market_id="200002",
            observed_at=OBSERVED_1,
        )
        _insert_event_market_snapshot(
            conn,
            event_id="100002",
            market_id="200003",
            observed_at=OBSERVED_1,
            row_order=1,
        )
        rows = build_registry_rows_from_event_catalog(
            event_min_volume_usd=POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
        )

    assert {row.market_id for row in rows} == {"200002", "200003"}
    assert all(row.source == "event_catalog" for row in rows)
    assert all(row.is_event_volume_eligible for row in rows)


def test_build_registry_rows_keeps_sticky_eligibility_after_volume_drops(
    event_catalog_db,
) -> None:
    with event_catalog_db.get_connection() as conn:
        _insert_event_snapshot(
            conn,
            event_id="100003",
            volume=150_000.0,
            observed_at=OBSERVED_1,
        )
        _insert_event_market_snapshot(
            conn,
            event_id="100003",
            market_id="200004",
            observed_at=OBSERVED_1,
        )
        upsert_registry_rows(
            build_registry_rows_from_event_catalog(
                event_min_volume_usd=POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
            )
        )
        _insert_event_snapshot(
            conn,
            event_id="100003",
            volume=25_000.0,
            observed_at=OBSERVED_2,
            row_order=1,
        )
        rows = build_registry_rows_from_event_catalog(
            event_min_volume_usd=POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
        )

    assert [row.market_id for row in rows] == ["200004"]
    assert rows[0].is_event_volume_eligible


def test_build_registry_rows_excludes_markets_that_left_enclosing_event(
    event_catalog_db,
) -> None:
    with event_catalog_db.get_connection() as conn:
        _insert_event_snapshot(
            conn,
            event_id="100004",
            volume=150_000.0,
            observed_at=OBSERVED_1,
        )
        _insert_event_market_snapshot(
            conn,
            event_id="100004",
            market_id="200005",
            observed_at=OBSERVED_1,
            is_enclosing_event=True,
        )
        _insert_event_market_snapshot(
            conn,
            event_id="100004",
            market_id="200005",
            observed_at=OBSERVED_2,
            is_enclosing_event=False,
        )
        rows = build_registry_rows_from_event_catalog(
            event_min_volume_usd=POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
        )

    assert rows == []


def test_build_registry_rows_keeps_enclosing_when_newer_related_bridge_exists(
    event_catalog_db,
) -> None:
    with event_catalog_db.get_connection() as conn:
        _insert_event_snapshot(
            conn,
            event_id="100005",
            volume=150_000.0,
            observed_at=OBSERVED_1,
        )
        _insert_event_snapshot(
            conn,
            event_id="100006",
            volume=150_000.0,
            observed_at=OBSERVED_1,
        )
        _insert_event_market_snapshot(
            conn,
            event_id="100005",
            market_id="200006",
            observed_at=OBSERVED_1,
            is_enclosing_event=True,
        )
        _insert_event_market_snapshot(
            conn,
            event_id="100006",
            market_id="200006",
            observed_at=OBSERVED_2,
            is_enclosing_event=False,
        )
        rows = build_registry_rows_from_event_catalog(
            event_min_volume_usd=POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
        )

    assert {row.market_id for row in rows} == {"200006"}
    assert rows[0].event_id == "100005"


def test_build_registry_rows_skips_non_numeric_synthetic_ids(event_catalog_db) -> None:
    with event_catalog_db.get_connection() as conn:
        _insert_event_snapshot(
            conn,
            event_id="evt-A",
            volume=150_000.0,
            observed_at=OBSERVED_1,
        )
        _insert_event_market_snapshot(
            conn,
            event_id="evt-A",
            market_id="m-shared",
            observed_at=OBSERVED_1,
        )
        rows = build_registry_rows_from_event_catalog(
            event_min_volume_usd=POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
        )

    assert rows == []


def test_prune_ineligible_api_registry_rows_keeps_eligible_catalog(
    event_catalog_db,
) -> None:
    upsert_registry_rows(
        [
            RegistryRow(
                market_id="200010",
                event_slug="fifwc-ok",
                event_id="100010",
                source="event_catalog",
                is_event_volume_eligible=True,
                event_volume_usd_lifetime_reported=150_000.0,
            ),
            RegistryRow(
                market_id="200011",
                event_slug="bieber-noise",
                event_id="100011",
                source="events_api",
                is_event_volume_eligible=False,
            ),
        ]
    )
    pruned = prune_ineligible_api_registry_rows(scope_name="wc2026")
    assert pruned == 1
    assert get_registry_market_ids() == ["200010"]


def test_prune_stale_event_catalog_registry_rows_removes_vanished_markets(
    event_catalog_db,
) -> None:
    with event_catalog_db.get_connection() as conn:
        _insert_event_snapshot(
            conn,
            event_id="100007",
            volume=150_000.0,
            observed_at=OBSERVED_1,
        )
        _insert_event_market_snapshot(
            conn,
            event_id="100007",
            market_id="200007",
            observed_at=OBSERVED_1,
        )
        _insert_event_market_snapshot(
            conn,
            event_id="100007",
            market_id="200008",
            observed_at=OBSERVED_1,
            row_order=1,
        )
        upsert_registry_rows(
            build_registry_rows_from_event_catalog(
                event_min_volume_usd=POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
            )
        )
        conn.execute(
            f"delete from {T_EVENT_MARKET_SNAPSHOTS} where market_id = '200008'"
        )
        active_rows = build_registry_rows_from_event_catalog(
            event_min_volume_usd=POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
        )
        upsert_registry_rows(active_rows)
        pruned = prune_stale_event_catalog_registry_rows(
            scope_name="wc2026",
            active_market_ids=[row.market_id for row in active_rows],
        )

    assert pruned == 1
    assert get_registry_market_ids() == ["200007"]
