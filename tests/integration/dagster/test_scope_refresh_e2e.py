from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")

from dagster import materialize
from dagster_dbt import DbtCliResource

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.config.settings import resolve_dbt_executable
from oddsfox_pipeline.ingestion.polymarket.markets.persistence import (
    MARKET_RECORD_COLUMNS,
    prepare_batch_for_db,
)
from oddsfox_pipeline.ingestion.polymarket.markets.transform import (
    process_markets_dataframe,
)
from oddsfox_pipeline.orchestration import (
    assets_kalshi_wc2026 as kalshi_assets_mod,
)
from oddsfox_pipeline.orchestration.assets import (
    DBT_PROJECT,
    international_results_wc2026_raw_match_results,
    kalshi_wc2026_ops_market_scope_registry,
    kalshi_wc2026_raw_market_candlesticks_hourly,
    kalshi_wc2026_raw_markets,
    kalshi_wc2026_raw_markets_snapshot,
    oddsfox_dbt,
    polymarket_us_midterms_2026_ops_market_scope_registry,
    polymarket_us_midterms_2026_raw_market_metadata_enrichment,
    polymarket_us_midterms_2026_raw_markets,
    polymarket_us_midterms_2026_raw_markets_snapshot,
    polymarket_us_midterms_2026_raw_token_odds_history_hourly,
    polymarket_wc2026_ops_market_scope_registry,
    polymarket_wc2026_raw_market_metadata_enrichment,
    polymarket_wc2026_raw_markets,
    polymarket_wc2026_raw_markets_snapshot,
    polymarket_wc2026_raw_token_odds_history_hourly,
)
from oddsfox_pipeline.orchestration.shipped_scopes import (
    KALSHI_WC2026_SCOPE,
    POLYMARKET_US_MIDTERMS_2026_SCOPE,
    POLYMARKET_WC2026_SCOPE,
)
from oddsfox_pipeline.storage.duckdb.kalshi_market_scope_registry import (
    KalshiRegistryRow,
)
from oddsfox_pipeline.storage.duckdb.kalshi_market_scope_registry import (
    upsert_registry_rows as upsert_kalshi_registry_rows,
)
from oddsfox_pipeline.storage.duckdb.market_scope_registry import (
    RegistryRow,
    upsert_registry_rows,
)
from oddsfox_pipeline.storage.duckdb.schemas.kalshi import (
    bootstrap_kalshi_tables,
    create_all_kalshi_test_raw_tables,
)
from oddsfox_pipeline.storage.duckdb.schemas.openfootball import (
    seed_test_openfootball_schedule_fixtures,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    create_all_scope_test_markets_tables,
    seed_test_ingestion_run_event,
)

_EMPTY_RESULTS_SUMMARY = {
    "rows": 0,
    "completed_rows": 0,
    "scheduled_rows": 0,
    "source_url": "https://example.com/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/results.csv",
    "source_revision": "a" * 40,
    "source_payload_sha256": "b" * 64,
}


def _fake_sync_market_scope_registry(**kwargs):
    del kwargs
    upsert_registry_rows(
        [
            RegistryRow(
                "m1",
                "2026-fifa-world-cup-winner-595",
                "ev-smoke",
                "seed",
            )
        ]
    )
    return {"registry_rows_upserted": 1, "discovered_event_slugs": []}


def _seed_dlt_owned_markets(
    market_page: list[dict],
    *,
    raw_schema: str = "polymarket_wc2026_raw",
) -> None:
    """dlt owns polymarket_*_raw.markets; this test's dlt resource is a noop."""
    df = process_markets_dataframe(market_page)
    market_data, _token_data = prepare_batch_for_db(df)
    if not market_data:
        return
    connection.ensure_duck_db()
    with connection.get_connection() as conn:
        create_all_scope_test_markets_tables(conn)
        columns = ", ".join(f'"{column}"' for column in MARKET_RECORD_COLUMNS)
        placeholders = ", ".join("?" for _column in MARKET_RECORD_COLUMNS)
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO "{raw_schema}"."markets"
                ({columns})
            VALUES ({placeholders})
            """,
            market_data,
        )
        conn.execute(
            f'ALTER TABLE "{raw_schema}"."markets" '
            "ADD COLUMN IF NOT EXISTS _dlt_id TEXT"
        )
        conn.execute(
            f'UPDATE "{raw_schema}"."markets" SET _dlt_id = id WHERE _dlt_id IS NULL'
        )


def _ingestion_run_counts(conn, *, ops_schema: str) -> dict[str, int]:
    return dict(
        conn.execute(
            f"""
            select task_name, count(*)
            from "{ops_schema}"."ingestion_run_events"
            group by task_name
            order by task_name
            """
        ).fetchall()
    )


def _polymarket_business_state(conn, *, scope_name: str) -> dict[str, list[tuple]]:
    raw = f"polymarket_{scope_name}_raw"
    ops = f"polymarket_{scope_name}_ops"
    intermediate = f"polymarket_{scope_name}_intermediate"
    model = f"int_polymarket_{scope_name}_token_hourly_odds"
    return {
        "markets": conn.execute(
            f"""
            select id, question, volume, active, closed, slug, event_slug,
                clob_token_ids
            from "{raw}"."markets"
            order by id
            """
        ).fetchall(),
        "market_tokens": conn.execute(
            f"""
            select market_id, clobTokenIds
            from "{raw}"."market_tokens"
            order by market_id
            """
        ).fetchall(),
        "registry": conn.execute(
            f"""
            select scope_name, market_id, event_slug, event_id, source
            from "{ops}"."market_scope_registry"
            order by scope_name, market_id
            """
        ).fetchall(),
        "odds_history": conn.execute(
            f"""
            select clobTokenId, timestamp, price
            from "{raw}"."odds_history"
            order by clobTokenId, timestamp
            """
        ).fetchall(),
        "daily": conn.execute(
            f"""
            select clobTokenId, odds_date_utc, open_price, high_price, low_price,
                close_price, avg_price, observed_points, first_timestamp,
                last_timestamp
            from "{raw}"."token_odds_daily"
            order by clobTokenId, odds_date_utc
            """
        ).fetchall(),
        "ledger": conn.execute(
            f"""
            select clobTokenId, last_sync_timestamp, fully_checked,
                empty_run_streak
            from "{ops}"."token_sync_ledger"
            order by clobTokenId
            """
        ).fetchall(),
        "skips": conn.execute(
            f"""
            select clobTokenId, reason
            from "{ops}"."token_sync_skips"
            order by clobTokenId
            """
        ).fetchall(),
        "hourly_model": (
            conn.execute(
                f"""
                select clob_token_id, odds_hour_epoch, open_price, high_price,
                    low_price, close_price, avg_price, observed_points,
                    first_timestamp, last_timestamp
                from "{intermediate}"."{model}"
                order by clob_token_id, odds_hour_epoch
                """
            ).fetchall()
            if conn.execute(
                """
                select count(*) from information_schema.schemata
                where schema_name = ?
                """,
                [intermediate],
            ).fetchone()[0]
            else []
        ),
    }


def _kalshi_business_state(conn) -> dict[str, list[tuple]]:
    return {
        "events": conn.execute(
            """
            select event_ticker, series_ticker, title, status
            from "kalshi_wc2026_raw"."events"
            order by event_ticker
            """
        ).fetchall(),
        "markets": conn.execute(
            """
            select market_ticker, event_ticker, series_ticker, title, status,
                volume, open_interest, last_price_dollars
            from "kalshi_wc2026_raw"."markets"
            order by market_ticker
            """
        ).fetchall(),
        "registry": conn.execute(
            """
            select scope_name, market_ticker, event_ticker, series_ticker, source
            from "kalshi_wc2026_ops"."market_scope_registry"
            order by scope_name, market_ticker
            """
        ).fetchall(),
        "candlesticks": conn.execute(
            """
            select market_ticker, hour_start_utc, open_price, high_price,
                low_price, close_price, avg_price, volume
            from "kalshi_wc2026_raw"."market_candlesticks_hourly"
            order by market_ticker, hour_start_utc
            """
        ).fetchall(),
        "hourly_model": conn.execute(
            """
            select market_ticker, odds_hour_epoch, open_price, high_price,
                low_price, close_price, avg_price, volume
            from "kalshi_wc2026_intermediate".
                "int_kalshi_wc2026_market_hourly_odds"
            order by market_ticker, odds_hour_epoch
            """
        ).fetchall(),
    }


def _materialize_refresh_path(
    monkeypatch,
    tmp_path: Path,
    *,
    db_name: str,
    slug: str,
    question: str,
    transient_token: str | None,
    fail_second_writer_flush: bool = False,
    one_point_history: bool = False,
    run_dbt: bool = True,
) -> Path:
    db_path = tmp_path / db_name
    profiles_dir = tmp_path / f"profiles-{db_name}"
    profiles_dir.mkdir(exist_ok=True)
    (profiles_dir / "profiles.yml").write_text(
        f"""
oddsfox:
  outputs:
    dev:
      type: duckdb
      path: {db_path}
      schema: dbt
      threads: 2
  target: dev
"""
    )

    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles_dir))
    connection.reset_duckdb_connection_state()

    connection.ensure_duck_db()
    with connection.get_connection() as conn:
        create_all_scope_test_markets_tables(conn)
        seed_test_ingestion_run_event(conn)
        create_all_kalshi_test_raw_tables(conn)
        seed_test_openfootball_schedule_fixtures(conn)

    subprocess.run(
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "seed",
            "--exclude",
            "tag:polygon_settlement",
            "tag:pmxt_order_book",
            "--project-dir",
            str(DBT_PROJECT.project_dir),
            "--profiles-dir",
            str(profiles_dir),
        ],
        check=True,
    )

    market_page = [
        {
            "id": "m1",
            "question": question,
            "category": "World Cup 2026 Testing",
            "description": "Synthetic World Cup 2026 end-to-end run",
            "outcomes": ["Yes", "No"],
            "volumeNum": 123.45,
            "active": True,
            "closed": False,
            "createdAt": "2026-04-13T10:00:00.000Z",
            "endDate": "2026-07-19T10:00:00.000Z",
            "clobTokenIds": ["t1", "t2"],
            "slug": slug,
            "events": [{"slug": "2026-fifa-world-cup-winner-595", "id": "ev-smoke"}],
        }
    ]

    def fake_refresh_registry_and_collect_markets_targeted(
        client, config, progress_callback=None
    ):
        del client
        if progress_callback:
            progress_callback(
                "market_scope_event_by_slug",
                {"slug": "2026-fifa-world-cup-winner-595", "found": True},
            )
        upsert_registry_rows(
            [
                RegistryRow(
                    "m1",
                    "2026-fifa-world-cup-winner-595",
                    "ev-smoke",
                    "events_api",
                    scope_name=config.scope_name,
                )
            ]
        )
        return (
            {
                "scope_name": config.scope_name,
                "registry_rows_upserted": 1,
                "discovered_event_slugs": ["2026-fifa-world-cup-winner-595"],
                "registry_refreshed": True,
            },
            market_page,
            {
                "scope_name": config.scope_name,
                "events_pages": 0,
                "markets_collected": len(market_page),
                "registry_refreshed": True,
                "api_requests": 2,
            },
        )

    def fake_fetch_token_history_with_retry(
        client,
        token_id,
        start_ts=None,
        end_ts=None,
        fidelity=1440,
        now_ts=None,
        **kwargs,
    ):
        del client, start_ts, end_ts, fidelity, now_ts, kwargs
        if transient_token is not None and str(token_id) == transient_token:
            return None
        rows = [
            (str(token_id), 1_784_851_260, 0.55),
            (str(token_id), 1_784_851_320, 0.60),
        ]
        return rows[:1] if one_point_history else rows

    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.markets.sync.refresh_registry_and_collect_markets_targeted",
        fake_refresh_registry_and_collect_markets_targeted,
    )

    def _skip_backfill(task_name: str):
        def _skip(**kwargs):
            del kwargs
            return {"task": task_name, "skipped": True}

        return _skip

    for task in (
        "backfill_tokens",
        "backfill_slugs",
        "backfill_event_slugs",
        "backfill_end_dates",
    ):
        monkeypatch.setattr(
            f"oddsfox_pipeline.orchestration.polymarket_ops.{task}",
            _skip_backfill(task),
        )
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.odds.sync.fetch_token_history_with_retry",
        fake_fetch_token_history_with_retry,
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.orchestration.polymarket_ops.sync_market_scope_registry",
        _fake_sync_market_scope_registry,
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.orchestration.assets_international_results.sync_wc2026_match_results",
        lambda: dict(_EMPTY_RESULTS_SUMMARY),
    )

    _seed_dlt_owned_markets(market_page)

    noop_dlt = MagicMock()
    noop_dlt.run.return_value = iter([])

    ingest_result = materialize(
        [
            international_results_wc2026_raw_match_results,
            polymarket_wc2026_raw_markets,
            polymarket_wc2026_raw_markets_snapshot,
            polymarket_wc2026_ops_market_scope_registry,
            polymarket_wc2026_raw_market_metadata_enrichment,
        ],
        resources={
            "dlt": noop_dlt,
        },
        run_config={
            "ops": {
                "polymarket_wc2026_raw_markets": {
                    "config": {
                        "discovery_mode": "targeted",
                    }
                },
            }
        },
    )
    assert ingest_result.success is True

    with monkeypatch.context() as writer_patch:
        if fail_second_writer_flush:
            from oddsfox_pipeline.ingestion.polymarket.odds import sync as odds_sync

            original_flush = odds_sync._flush_writer_buffers
            flush_calls = 0

            def fail_write(*_args, **_kwargs):
                raise RuntimeError("injected second writer flush failure")

            def flaky_flush(*args, **kwargs):
                nonlocal flush_calls
                flush_calls += 1
                if flush_calls == 2:
                    kwargs["save_odds_bulk_upsert_fn"] = fail_write
                    kwargs["upsert_token_sync_state_batch_fn"] = fail_write
                return original_flush(*args, **kwargs)

            writer_patch.setattr(
                odds_sync, "_dynamic_writer_flush_rows", lambda *_args: 1
            )
            writer_patch.setattr(odds_sync, "_flush_writer_buffers", flaky_flush)
        odds_result = materialize(
            [polymarket_wc2026_raw_token_odds_history_hourly],
            run_config={
                "ops": {
                    "polymarket_wc2026_raw_token_odds_history_hourly": {
                        "config": {
                            "workers": 1,
                            "batch_size": 1000,
                            "requests_per_second": 1,
                            "skip_recent_minutes": 0,
                            "overlap_minutes": 0,
                            "window_hours": 1,
                            "market_page_size": 100,
                            "min_volume": 0,
                            # Keep this synthetic refresh fixture valid after WC2026.
                            "ended_market_grace_days": None,
                            "progress_log_interval_tokens": 1,
                            "progress_log_interval_seconds": 1,
                            "no_progress_soft_timeout_seconds": 120,
                            "no_progress_hard_timeout_seconds": 600,
                            "progress_poll_seconds": 1,
                        }
                    },
                }
            },
            raise_on_error=False,
        )
    if fail_second_writer_flush:
        assert odds_result.success is False
        assert flush_calls == 2
        return db_path
    assert odds_result.success is True

    if run_dbt:
        dbt_result = materialize(
            [oddsfox_dbt],
            resources={
                "dbt": DbtCliResource(
                    project_dir=DBT_PROJECT,
                    profiles_dir=str(profiles_dir),
                    dbt_executable=resolve_dbt_executable(),
                ),
            },
            run_config={
                "ops": {
                    "oddsfox_dbt": {
                        "config": {
                            "full_refresh": True,
                            "dbt_select": POLYMARKET_WC2026_SCOPE.dbt_select,
                            "dbt_exclude": (
                                f"{POLYMARKET_WC2026_SCOPE.dbt_exclude} "
                                "tag:wc2026_logical_atlas"
                            ),
                            "progress_log_interval_events": 1,
                            "progress_log_interval_seconds": 1,
                            "no_progress_soft_timeout_seconds": 120,
                            "no_progress_hard_timeout_seconds": 600,
                            "progress_poll_seconds": 1,
                        }
                    },
                }
            },
        )
        assert dbt_result.success is True
    return db_path


def test_refresh_path_materializes(
    monkeypatch,
    tmp_path,
    reset_connection_globals,
    no_sleep,
) -> None:
    slug = "world-cup-2026-smoke-pipeline-pass"
    question = "Will the World Cup 2026 smoke pipeline pass?"
    db_path = _materialize_refresh_path(
        monkeypatch,
        tmp_path,
        db_name=f"pipeline-{slug}.duckdb",
        slug=slug,
        question=question,
        transient_token=None,
    )
    with connection.get_connection() as conn:
        first_state = _polymarket_business_state(conn, scope_name="wc2026")
        first_run_counts = _ingestion_run_counts(
            conn, ops_schema="polymarket_wc2026_ops"
        )
        checks = (
            conn.execute(
                'select count(*) from "polymarket_wc2026_raw"."markets"'
            ).fetchone()
            == (1,),
            conn.execute(
                'select count(*) from "polymarket_wc2026_raw"."market_tokens"'
            ).fetchone()
            == (1,),
            conn.execute(
                'select count(*) from "polymarket_wc2026_raw"."odds_history"'
            ).fetchone()[0]
            > 0,
            conn.execute(
                'select count(*) from "polymarket_wc2026_raw"."token_odds_daily"'
            ).fetchone()[0]
            > 0,
            conn.execute(
                "select count(*) from polymarket_wc2026_staging.stg_polymarket_wc2026_markets"
            ).fetchone()
            == (1,),
            conn.execute(
                "select count(*) from polymarket_wc2026_staging.stg_polymarket_wc2026_market_tokens"
            ).fetchone()
            == (2,),
        )
        assert all(checks)
    assert db_path.exists()

    first_raw_state = {k: v for k, v in first_state.items() if k != "hourly_model"}
    _materialize_refresh_path(
        monkeypatch,
        tmp_path,
        db_name=f"pipeline-{slug}.duckdb",
        slug=slug,
        question=question,
        transient_token=None,
        run_dbt=False,
    )
    with connection.get_connection() as conn:
        second_state = _polymarket_business_state(conn, scope_name="wc2026")
        second_raw = {k: v for k, v in second_state.items() if k != "hourly_model"}
        assert second_raw == first_raw_state
        assert _ingestion_run_counts(conn, ops_schema="polymarket_wc2026_ops") == {
            task: count * 2 for task, count in first_run_counts.items()
        }


_MIDTERMS_EVENT_SLUG = "balance-of-power-2026-midterms"
_MIDTERMS_SCOPE = "us_midterms_2026"
_MIDTERMS_RAW_SCHEMA = "polymarket_us_midterms_2026_raw"
_MIDTERMS_VALID_TOKEN_YES = "m" * 33 + "01"
_MIDTERMS_VALID_TOKEN_NO = "m" * 33 + "02"


def _patch_midterms_refresh_externals(
    monkeypatch, *, transient_token: str | None
) -> None:
    market_page = [
        {
            "id": "m-midterms-1",
            "question": "Will Democrats control the House after the 2026 midterms?",
            "category": "US Politics",
            "description": "Synthetic US midterms 2026 end-to-end run",
            "outcomes": ["Yes", "No"],
            "volumeNum": 12_345.67,
            "active": True,
            "closed": False,
            "createdAt": "2026-01-15T10:00:00.000Z",
            "endDate": "2026-11-04T10:00:00.000Z",
            "clobTokenIds": [_MIDTERMS_VALID_TOKEN_YES, _MIDTERMS_VALID_TOKEN_NO],
            "slug": "us-midterms-2026-smoke-pipeline-pass",
            "events": [{"slug": _MIDTERMS_EVENT_SLUG, "id": "ev-midterms-smoke"}],
        }
    ]

    def fake_refresh_registry_and_collect_markets_targeted(
        client, config, progress_callback=None
    ):
        del client
        if progress_callback:
            progress_callback(
                "market_scope_event_by_slug",
                {"slug": _MIDTERMS_EVENT_SLUG, "found": True},
            )
        upsert_registry_rows(
            [
                RegistryRow(
                    "m-midterms-1",
                    _MIDTERMS_EVENT_SLUG,
                    "ev-midterms-smoke",
                    "events_api",
                    scope_name=config.scope_name,
                )
            ]
        )
        return (
            {
                "scope_name": config.scope_name,
                "registry_rows_upserted": 1,
                "discovered_event_slugs": [_MIDTERMS_EVENT_SLUG],
                "registry_refreshed": True,
            },
            market_page,
            {
                "scope_name": config.scope_name,
                "events_pages": 0,
                "markets_collected": len(market_page),
                "registry_refreshed": True,
                "api_requests": 2,
            },
        )

    def fake_fetch_token_history_with_retry(
        client,
        token_id,
        start_ts=None,
        end_ts=None,
        fidelity=1440,
        now_ts=None,
        **kwargs,
    ):
        del client, start_ts, end_ts, fidelity, now_ts, kwargs
        if transient_token is not None and str(token_id) == transient_token:
            return None
        return [
            (str(token_id), 1_784_851_260, 0.52),
            (str(token_id), 1_784_851_320, 0.54),
        ]

    def fake_sync_market_scope_registry(**kwargs):
        del kwargs
        upsert_registry_rows(
            [
                RegistryRow(
                    "m-midterms-1",
                    _MIDTERMS_EVENT_SLUG,
                    "ev-midterms-smoke",
                    "seed",
                    scope_name=_MIDTERMS_SCOPE,
                )
            ]
        )
        return {"registry_rows_upserted": 1, "discovered_event_slugs": []}

    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.markets.sync.refresh_registry_and_collect_markets_targeted",
        fake_refresh_registry_and_collect_markets_targeted,
    )

    def _skip_backfill(task_name: str):
        def _skip(**kwargs):
            del kwargs
            return {"task": task_name, "skipped": True}

        return _skip

    for task in (
        "backfill_tokens",
        "backfill_slugs",
        "backfill_event_slugs",
        "backfill_end_dates",
    ):
        monkeypatch.setattr(
            f"oddsfox_pipeline.orchestration.polymarket_ops.{task}",
            _skip_backfill(task),
        )
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.odds.sync.fetch_token_history_with_retry",
        fake_fetch_token_history_with_retry,
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.orchestration.polymarket_ops.sync_market_scope_registry",
        fake_sync_market_scope_registry,
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.orchestration.assets_international_results.sync_wc2026_match_results",
        lambda: dict(_EMPTY_RESULTS_SUMMARY),
    )
    return market_page


def _configure_midterms_smoke_env(monkeypatch, tmp_path: Path, db_name: str) -> Path:
    db_path = tmp_path / db_name
    profiles_dir = tmp_path / f"profiles-{db_name}"
    profiles_dir.mkdir(exist_ok=True)
    (profiles_dir / "profiles.yml").write_text(
        f"""
oddsfox:
  outputs:
    dev:
      type: duckdb
      path: {db_path}
      schema: dbt
      threads: 2
  target: dev
"""
    )
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles_dir))
    connection.reset_duckdb_connection_state()
    connection.ensure_duck_db()
    with connection.get_connection() as conn:
        create_all_scope_test_markets_tables(conn)
        create_all_kalshi_test_raw_tables(conn)
    return profiles_dir


def _materialize_midterms_refresh_path(
    monkeypatch,
    tmp_path: Path,
    *,
    db_name: str,
    transient_token: str | None,
) -> Path:
    profiles_dir = _configure_midterms_smoke_env(monkeypatch, tmp_path, db_name)
    market_page = _patch_midterms_refresh_externals(
        monkeypatch, transient_token=transient_token
    )
    _seed_dlt_owned_markets(market_page, raw_schema=_MIDTERMS_RAW_SCHEMA)

    noop_dlt = MagicMock()
    noop_dlt.run.return_value = iter([])

    ingest_result = materialize(
        [
            polymarket_us_midterms_2026_raw_markets,
            polymarket_us_midterms_2026_raw_markets_snapshot,
            polymarket_us_midterms_2026_ops_market_scope_registry,
            polymarket_us_midterms_2026_raw_market_metadata_enrichment,
        ],
        resources={"dlt": noop_dlt},
        run_config={
            "ops": {
                "polymarket_us_midterms_2026_raw_markets": {
                    "config": {"discovery_mode": "targeted"}
                },
            }
        },
    )
    assert ingest_result.success is True

    odds_result = materialize(
        [polymarket_us_midterms_2026_raw_token_odds_history_hourly],
        run_config={
            "ops": {
                "polymarket_us_midterms_2026_raw_token_odds_history_hourly": {
                    "config": {
                        "workers": 1,
                        "batch_size": 1000,
                        "requests_per_second": 1,
                        "skip_recent_minutes": 0,
                        "overlap_minutes": 0,
                        "window_hours": 1,
                        "market_page_size": 100,
                        "min_volume": 0,
                        "progress_log_interval_tokens": 1,
                        "progress_log_interval_seconds": 1,
                        "no_progress_soft_timeout_seconds": 120,
                        "no_progress_hard_timeout_seconds": 600,
                        "progress_poll_seconds": 1,
                    }
                },
            }
        },
    )
    assert odds_result.success is True

    dbt_result = materialize(
        [oddsfox_dbt],
        resources={
            "dbt": DbtCliResource(
                project_dir=DBT_PROJECT,
                profiles_dir=str(profiles_dir),
                dbt_executable=resolve_dbt_executable(),
            ),
        },
        run_config={
            "ops": {
                "oddsfox_dbt": {
                    "config": {
                        "full_refresh": True,
                        "dbt_select": POLYMARKET_US_MIDTERMS_2026_SCOPE.dbt_select,
                        "dbt_exclude": POLYMARKET_US_MIDTERMS_2026_SCOPE.dbt_exclude,
                        "progress_log_interval_events": 1,
                        "progress_log_interval_seconds": 1,
                        "no_progress_soft_timeout_seconds": 120,
                        "no_progress_hard_timeout_seconds": 600,
                        "progress_poll_seconds": 1,
                    }
                },
            }
        },
    )
    assert dbt_result.success is True
    return profiles_dir.parent / db_name


def test_midterms_refresh_path_materializes(
    monkeypatch,
    tmp_path,
    reset_connection_globals,
    no_sleep,
) -> None:
    db_path = _materialize_midterms_refresh_path(
        monkeypatch,
        tmp_path,
        db_name="pipeline-us-midterms-2026-smoke.duckdb",
        transient_token=None,
    )
    with connection.get_connection() as conn:
        first_state = _polymarket_business_state(conn, scope_name=_MIDTERMS_SCOPE)
        first_run_counts = _ingestion_run_counts(
            conn, ops_schema="polymarket_us_midterms_2026_ops"
        )
        checks = {
            "markets": conn.execute(
                f'select count(*) from "{_MIDTERMS_RAW_SCHEMA}"."markets"'
            ).fetchone()
            == (1,),
            "market_tokens": conn.execute(
                f'select count(*) from "{_MIDTERMS_RAW_SCHEMA}"."market_tokens"'
            ).fetchone()
            == (1,),
            "odds_history": conn.execute(
                f'select count(*) from "{_MIDTERMS_RAW_SCHEMA}"."odds_history"'
            ).fetchone()[0]
            > 0,
            "token_odds_daily": conn.execute(
                f'select count(*) from "{_MIDTERMS_RAW_SCHEMA}"."token_odds_daily"'
            ).fetchone()[0]
            > 0,
            "staging_markets": conn.execute(
                "select count(*) from "
                "polymarket_us_midterms_2026_staging.stg_polymarket_us_midterms_2026_markets"
            ).fetchone()
            == (1,),
            "staging_market_tokens": conn.execute(
                "select count(*) from "
                "polymarket_us_midterms_2026_staging.stg_polymarket_us_midterms_2026_market_tokens"
            ).fetchone()
            == (2,),
            "mart_hourly_odds": conn.execute(
                "select count(*) from "
                "polymarket_us_midterms_2026_marts.polymarket_us_midterms_2026_market_token_hourly_odds"
            ).fetchone()[0]
            > 0,
        }
        assert all(checks.values()), checks
    assert db_path.exists()

    _materialize_midterms_refresh_path(
        monkeypatch,
        tmp_path,
        db_name="pipeline-us-midterms-2026-smoke.duckdb",
        transient_token=None,
    )
    with connection.get_connection() as conn:
        assert (
            _polymarket_business_state(conn, scope_name=_MIDTERMS_SCOPE) == first_state
        )
        assert _ingestion_run_counts(
            conn, ops_schema="polymarket_us_midterms_2026_ops"
        ) == {task: count * 2 for task, count in first_run_counts.items()}


_KALSHI_SCOPE = "wc2026"
_KALSHI_RAW_SCHEMA = "kalshi_wc2026_raw"
_KALSHI_EVENT_TICKER = "KXMENWORLDCUP-WINNER"
_KALSHI_MARKET_TICKER = "KXMENWORLDCUP-WINNER-USA"
_KALSHI_SERIES = "KXMENWORLDCUP"


def _seed_kalshi_smoke_raw_rows(conn) -> None:
    scraped_at = "2026-01-15 10:00:00"
    conn.execute(
        f"""
        INSERT OR REPLACE INTO "{_KALSHI_RAW_SCHEMA}"."events" (
            event_ticker,
            series_ticker,
            title,
            sub_title,
            category,
            status,
            open_time,
            close_time,
            scraped_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            _KALSHI_EVENT_TICKER,
            _KALSHI_SERIES,
            "Men's World Cup Winner",
            "",
            "Sports",
            "open",
            scraped_at,
            "2026-07-19 10:00:00",
            scraped_at,
        ],
    )
    conn.execute(
        f"""
        INSERT OR REPLACE INTO "{_KALSHI_RAW_SCHEMA}"."markets" (
            market_ticker,
            event_ticker,
            series_ticker,
            title,
            subtitle,
            yes_sub_title,
            no_sub_title,
            status,
            market_type,
            open_time,
            close_time,
            expiration_time,
            volume,
            open_interest,
            last_price_dollars,
            scraped_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            _KALSHI_MARKET_TICKER,
            _KALSHI_EVENT_TICKER,
            _KALSHI_SERIES,
            "Will United States win the Men's World Cup?",
            "",
            "United States",
            "",
            "open",
            "binary",
            scraped_at,
            "2026-07-19 10:00:00",
            "2026-07-19 10:00:00",
            1000,
            100,
            "0.12",
            scraped_at,
        ],
    )


def _patch_kalshi_refresh_externals(monkeypatch) -> dict[str, list[dict]]:
    events = [
        {
            "event_ticker": _KALSHI_EVENT_TICKER,
            "series_ticker": _KALSHI_SERIES,
            "title": "Men's World Cup Winner",
            "sub_title": "",
            "category": "Sports",
            "status": "open",
            "open_time": "2026-01-15T10:00:00Z",
            "close_time": "2026-07-19T10:00:00Z",
            "scraped_at": "2026-01-15T10:00:00Z",
        }
    ]
    markets = [
        {
            "market_ticker": _KALSHI_MARKET_TICKER,
            "event_ticker": _KALSHI_EVENT_TICKER,
            "series_ticker": _KALSHI_SERIES,
            "title": "Will United States win the Men's World Cup?",
            "subtitle": "",
            "yes_sub_title": "United States",
            "no_sub_title": "",
            "status": "open",
            "market_type": "binary",
            "open_time": "2026-01-15T10:00:00Z",
            "close_time": "2026-07-19T10:00:00Z",
            "expiration_time": "2026-07-19T10:00:00Z",
            "volume": 1000,
            "open_interest": 100,
            "last_price_dollars": "0.12",
            "scraped_at": "2026-01-15T10:00:00Z",
        }
    ]

    def fake_collect_market_scope_payload(**_kwargs):
        return {
            "scope_name": _KALSHI_SCOPE,
            "events": events,
            "markets": markets,
            "total_events": len(events),
            "total_markets": len(markets),
            "registry_summary": {"registry_rows_upserted": 1},
        }

    def fake_sync_kalshi_market_scope_registry(**_kwargs):
        upsert_kalshi_registry_rows(
            [
                KalshiRegistryRow(
                    _KALSHI_MARKET_TICKER,
                    _KALSHI_EVENT_TICKER,
                    _KALSHI_SERIES,
                    "seed",
                    scope_name=_KALSHI_SCOPE,
                )
            ]
        )
        return {"registry_rows_upserted": 1, "discovered_event_slugs": []}

    def fake_sync_kalshi_candlesticks(**_kwargs):
        from oddsfox_pipeline.storage.duckdb import kalshi_candlesticks

        rows_written = kalshi_candlesticks.save_candlesticks_batch(
            [
                {
                    "market_ticker": _KALSHI_MARKET_TICKER,
                    "hour_start_utc": "2026-01-15 11:00:00",
                    "open_price": 0.10,
                    "high_price": 0.12,
                    "low_price": 0.09,
                    "close_price": 0.11,
                    "avg_price": 0.105,
                    "volume": 25,
                }
            ],
        )
        return {
            "task": "sync_kalshi_candlesticks",
            "scope_name": _KALSHI_SCOPE,
            "markets_synced": 1,
            "rows_written": rows_written,
            "window_hours": 1,
        }

    monkeypatch.setattr(
        kalshi_assets_mod,
        "collect_market_scope_payload",
        fake_collect_market_scope_payload,
    )
    monkeypatch.setattr(
        kalshi_assets_mod.ops,
        "sync_kalshi_market_scope_registry",
        fake_sync_kalshi_market_scope_registry,
    )
    original_materialize = (
        kalshi_assets_mod.asset_helpers.materialize_kalshi_candlesticks_sync
    )

    def fake_materialize_kalshi_candlesticks_sync(
        context, config, *, scope_name, **kwargs
    ):
        return original_materialize(
            context,
            config,
            scope_name=scope_name,
            sync_fn=fake_sync_kalshi_candlesticks,
            **kwargs,
        )

    monkeypatch.setattr(
        kalshi_assets_mod.asset_helpers,
        "materialize_kalshi_candlesticks_sync",
        fake_materialize_kalshi_candlesticks_sync,
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.orchestration.kalshi_ops.sync_kalshi_candlesticks",
        fake_sync_kalshi_candlesticks,
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.orchestration.assets_international_results.sync_wc2026_match_results",
        lambda: dict(_EMPTY_RESULTS_SUMMARY),
    )
    return {"events": events, "markets": markets}


def _configure_kalshi_smoke_env(monkeypatch, tmp_path: Path, db_name: str) -> Path:
    db_path = tmp_path / db_name
    profiles_dir = tmp_path / f"profiles-{db_name}"
    profiles_dir.mkdir(exist_ok=True)
    (profiles_dir / "profiles.yml").write_text(
        f"""
oddsfox:
  outputs:
    dev:
      type: duckdb
      path: {db_path}
      schema: dbt
      threads: 2
  target: dev
"""
    )
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles_dir))
    connection.reset_duckdb_connection_state()
    connection.ensure_duck_db()
    with connection.get_connection() as conn:
        create_all_scope_test_markets_tables(conn)
        bootstrap_kalshi_tables(conn, scope_name=_KALSHI_SCOPE)
        create_all_kalshi_test_raw_tables(conn)
        _seed_kalshi_smoke_raw_rows(conn)
        upsert_kalshi_registry_rows(
            [
                KalshiRegistryRow(
                    _KALSHI_MARKET_TICKER,
                    _KALSHI_EVENT_TICKER,
                    _KALSHI_SERIES,
                    "seed",
                    scope_name=_KALSHI_SCOPE,
                )
            ]
        )
    return profiles_dir


def _materialize_kalshi_refresh_path(
    monkeypatch,
    tmp_path: Path,
    *,
    db_name: str,
    patch_externals: bool = True,
) -> Path:
    profiles_dir = _configure_kalshi_smoke_env(monkeypatch, tmp_path, db_name)
    if patch_externals:
        _patch_kalshi_refresh_externals(monkeypatch)

    noop_dlt = MagicMock()
    noop_dlt.run.return_value = iter([])

    ingest_result = materialize(
        [
            international_results_wc2026_raw_match_results,
            kalshi_wc2026_raw_markets,
            kalshi_wc2026_raw_markets_snapshot,
            kalshi_wc2026_ops_market_scope_registry,
        ],
        resources={"dlt": noop_dlt},
    )
    assert ingest_result.success is True

    odds_result = materialize(
        [kalshi_wc2026_raw_market_candlesticks_hourly],
        run_config={
            "ops": {
                "kalshi_wc2026_raw_market_candlesticks_hourly": {
                    "config": {
                        "window_hours": 1,
                        "force": True,
                        "progress_log_interval_markets": 1,
                        "progress_log_interval_seconds": 1,
                        "no_progress_soft_timeout_seconds": 120,
                        "no_progress_hard_timeout_seconds": 600,
                        "progress_poll_seconds": 1,
                    }
                },
            }
        },
    )
    assert odds_result.success is True

    dbt_build = subprocess.run(
        [
            resolve_dbt_executable(),
            "build",
            "--project-dir",
            str(DBT_PROJECT.project_dir),
            "--profiles-dir",
            str(profiles_dir),
            "--select",
            KALSHI_WC2026_SCOPE.dbt_select,
            "--exclude",
            KALSHI_WC2026_SCOPE.dbt_exclude,
        ],
        env=os.environ.copy(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert dbt_build.returncode == 0, dbt_build.stdout + dbt_build.stderr
    return profiles_dir.parent / db_name


def test_kalshi_refresh_path_materializes(
    monkeypatch,
    tmp_path,
    reset_connection_globals,
    no_sleep,
) -> None:
    db_path = _materialize_kalshi_refresh_path(
        monkeypatch,
        tmp_path,
        db_name="pipeline-kalshi-wc2026-smoke.duckdb",
    )
    with connection.get_connection() as conn:
        first_state = _kalshi_business_state(conn)
        first_run_counts = _ingestion_run_counts(conn, ops_schema="kalshi_wc2026_ops")
        checks = {
            "raw_markets": conn.execute(
                f'select count(*) from "{_KALSHI_RAW_SCHEMA}"."markets"'
            ).fetchone()
            == (1,),
            "raw_candlesticks": conn.execute(
                'select count(*) from "kalshi_wc2026_raw"."market_candlesticks_hourly"'
            ).fetchone()[0]
            > 0,
            "staging_markets": conn.execute(
                "select count(*) from kalshi_wc2026_staging.stg_kalshi_wc2026_markets"
            ).fetchone()
            == (1,),
            "intermediate_markets": conn.execute(
                "select count(*) from "
                "kalshi_wc2026_intermediate.int_kalshi_wc2026_markets"
            ).fetchone()
            == (1,),
        }
        assert all(checks.values()), checks
    assert db_path.exists()

    _materialize_kalshi_refresh_path(
        monkeypatch,
        tmp_path,
        db_name="pipeline-kalshi-wc2026-smoke.duckdb",
        patch_externals=False,
    )
    with connection.get_connection() as conn:
        assert _kalshi_business_state(conn) == first_state
        assert _ingestion_run_counts(conn, ops_schema="kalshi_wc2026_ops") == {
            task: count * 2 for task, count in first_run_counts.items()
        }
