"""Shared fixtures for storage/duckdb unit tests."""

from __future__ import annotations

import importlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

import oddsfox_pipeline.storage.duckdb.markets as markets
from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules
from oddsfox_pipeline.storage.duckdb.connection import (
    get_connection,
    polymarket_wc2026_ops_tbl,
    polymarket_wc2026_raw_tbl,
)
from oddsfox_pipeline.storage.duckdb.market_scope_registry import (
    RegistryRow,
    upsert_registry_rows,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import create_test_markets_table

T_M = polymarket_wc2026_raw_tbl("markets")
T_MT = polymarket_wc2026_raw_tbl("market_tokens")
T_PAYLOAD = polymarket_wc2026_raw_tbl("event_market_payload_snapshots")
T_OH = polymarket_wc2026_raw_tbl("odds_history")
T_TOD = polymarket_wc2026_raw_tbl("token_odds_daily")
T_LED = polymarket_wc2026_ops_tbl("token_sync_ledger")
T_SK = polymarket_wc2026_ops_tbl("token_sync_skips")
T_PRE = polymarket_wc2026_ops_tbl("ingestion_run_events")
T_UNR = polymarket_wc2026_ops_tbl("market_metadata_unresolved")


def isolate_duckdb_test_env(monkeypatch, db_path: str | Path) -> None:
    """Point tests at an isolated tmp DuckDB; block repo `.env` ``DUCKDB_PATH`` leak."""
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    reload_all_settings_modules()
    monkeypatch.delenv("DUCKDB_PATH", raising=False)


def reload_settings_and_connection(monkeypatch, db_path: str | Path):
    """Reload settings + connection for an isolated DuckDB path and reset caches."""
    isolate_duckdb_test_env(monkeypatch, db_path)
    import oddsfox_pipeline.storage.duckdb.connection as connection

    connection.reset_duckdb_connection_state()
    connection = importlib.reload(connection)
    return connection


def initialize_isolated_duckdb(
    monkeypatch, db_path: str | Path, *, ensure: bool = True
):
    """Canonical isolated DuckDB bootstrap used by storage and orchestration fixtures."""
    connection = reload_settings_and_connection(monkeypatch, db_path)
    if ensure:
        connection.ensure_duck_db()
    return connection


@pytest.fixture
def duck(monkeypatch, tmp_path):
    connection = initialize_isolated_duckdb(monkeypatch, tmp_path / "unit.duckdb")
    with get_connection() as conn:
        create_test_markets_table(conn)
    yield connection
    connection.reset_duckdb_connection_state()


def _insert_minimal_market(conn, mid="m1", **kwargs):
    defaults = dict(
        id=mid,
        question="Q",
        category="c",
        description="d",
        outcomes="[]",
        volume=1.0,
        active=True,
        closed=False,
        created_at=datetime.now(timezone.utc),
        scraped_at=datetime.now(timezone.utc),
        end_date=None,
        slug=None,
        event_slug=None,
        event_id=None,
    )
    defaults.update(kwargs)
    conn.execute(
        f"""INSERT OR REPLACE INTO {T_M}
        (id, question, category, description, outcomes, volume, active, closed,
         created_at, scraped_at, end_date, slug, event_slug, event_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            defaults["id"],
            defaults["question"],
            defaults["category"],
            defaults["description"],
            defaults["outcomes"],
            defaults["volume"],
            defaults["active"],
            defaults["closed"],
            defaults["created_at"],
            defaults["scraped_at"],
            defaults["end_date"],
            defaults["slug"],
            defaults["event_slug"],
            defaults["event_id"],
        ],
    )


def _normalize_market_tuple(row: tuple) -> tuple:
    if len(row) == 14:
        normalized = row
    elif len(row) == 13:
        normalized = (*row, None)
    elif len(row) == 12:
        expanded = list(row)
        expanded.insert(10, None)
        normalized = (*expanded, None)
    elif len(row) == 11:
        expanded = list(row)
        expanded.insert(10, None)
        normalized = (*expanded, None, None)
    elif len(row) == 10:
        normalized = (*row, None, None, None, None)
    else:
        raise ValueError(f"Expected 10-14 columns for markets insert, got {len(row)}")

    rec = list(normalized)
    end_val = rec[10]
    if not end_val or (isinstance(end_val, str) and not end_val.strip()):
        rec[10] = None
    return tuple(rec)


def _insert_market_tuple(conn, row: tuple) -> None:
    rec = _normalize_market_tuple(row)
    conn.execute(
        f"""INSERT OR REPLACE INTO {T_M}
        (id, question, category, description, outcomes, volume, active, closed,
         created_at, scraped_at, end_date, slug, event_slug, event_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        list(rec),
    )


def _seed_payload_tokens(
    conn,
    token_rows: list[tuple[str, str]],
    *,
    observed_at: datetime | None = None,
) -> None:
    """Seed latest-style payload snapshots so odds planning matches staging SoT."""
    stamp = observed_at or datetime.now(timezone.utc)
    for market_id, clob_token_ids in token_rows:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {T_PAYLOAD}
            (
                market_id,
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
                clob_token_ids,
                is_resolved,
                observed_at
            )
            VALUES (
                ?, 'Q', 'c', 'd', '["Yes","No"]', 1.0, TRUE, FALSE,
                ?, ?, NULL, ?, NULL, NULL, 'condition', 'outright',
                ?, FALSE, ?
            )
            """,
            [
                str(market_id),
                stamp,
                stamp,
                str(market_id),
                clob_token_ids,
                stamp,
            ],
        )


def _seed_markets(
    duck,
    market_rows=None,
    token_rows=None,
    *,
    register_scope: bool = True,
    seed_payloads: bool = True,
) -> None:
    """Seed markets via direct insert; persist tokens through save_market_tokens_batch."""
    normalized_rows = [_normalize_market_tuple(row) for row in market_rows or ()]
    with duck.get_connection() as conn:
        for row in normalized_rows:
            _insert_market_tuple(conn, row)
        if token_rows and seed_payloads:
            _seed_payload_tokens(conn, list(token_rows))
    if token_rows:
        markets.save_market_tokens_batch(token_rows)
    if register_scope and normalized_rows:
        upsert_registry_rows(
            [
                RegistryRow(
                    market_id=str(row[0]),
                    event_slug=row[12],
                    event_id=row[13],
                    source="test",
                )
                for row in normalized_rows
            ]
        )
