from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")

from dagster import materialize
from dagster_dbt import DbtCliResource

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.config.settings import resolve_dbt_executable
from oddsfox_pipeline.ingestion.polymarket.markets.persistence import (
    prepare_batch_for_db,
)
from oddsfox_pipeline.ingestion.polymarket.markets.transform import (
    process_markets_dataframe,
)
from oddsfox_pipeline.orchestration import (
    assets_polymarket_us_midterms_2026 as midterms_assets_mod,
)
from oddsfox_pipeline.orchestration.assets import (
    DBT_PROJECT,
    international_results_wc2026_raw_match_results,
    polymarket_us_midterms_2026_ops_market_scope_registry,
    polymarket_us_midterms_2026_raw_market_metadata_backfill,
    polymarket_us_midterms_2026_raw_markets,
    polymarket_us_midterms_2026_raw_markets_snapshot,
    polymarket_us_midterms_2026_raw_token_odds_history_hourly,
    polymarket_wc2026_dbt,
    polymarket_wc2026_ops_market_scope_registry,
    polymarket_wc2026_raw_market_metadata_backfill,
    polymarket_wc2026_raw_markets,
    polymarket_wc2026_raw_markets_snapshot,
    polymarket_wc2026_raw_token_odds_history_hourly,
)
from oddsfox_pipeline.orchestration.definitions import defs
from oddsfox_pipeline.storage.duckdb.market_scope_registry import (
    RegistryRow,
    upsert_registry_rows,
)
from oddsfox_pipeline.storage.duckdb.markets import save_market_tokens_batch
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    create_all_scope_test_markets_tables,
)


@pytest.fixture
def reset_connection_globals():
    connection.reset_duckdb_connection_state()
    yield
    connection.reset_duckdb_connection_state()


@pytest.fixture
def no_sleep():
    with patch("time.sleep", lambda *_args, **_kwargs: None):
        yield


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
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO "{raw_schema}"."markets"
                (
                    id, question, category, description, outcomes, volume, active, closed,
                    created_at, scraped_at, end_date, slug, event_slug, event_id,
                    condition_id, sports_market_type, game_start_time, group_item_title,
                    tags, clob_token_ids, is_resolved, winning_outcome,
                    winning_clob_token_id
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
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


def _materialize_refresh_path(
    monkeypatch,
    tmp_path: Path,
    *,
    db_name: str,
    slug: str,
    question: str,
    transient_token: str | None,
) -> Path:
    db_path = tmp_path / db_name
    profiles_dir = tmp_path / f"profiles-{db_name}"
    profiles_dir.mkdir()
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
        del client, fidelity, kwargs
        if transient_token is not None and str(token_id) == transient_token:
            return None
        base_ts = int(
            start_ts if start_ts is not None else now_ts if now_ts is not None else 0
        )
        return [
            (str(token_id), base_ts + 60, 0.55),
            (str(token_id), base_ts + 120, 0.60),
        ]

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
        lambda: {"rows": 0, "completed_rows": 0, "scheduled_rows": 0},
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
            polymarket_wc2026_raw_market_metadata_backfill,
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
        [polymarket_wc2026_dbt],
        resources={
            "dbt": DbtCliResource(
                project_dir=DBT_PROJECT,
                profiles_dir=str(profiles_dir),
                dbt_executable=resolve_dbt_executable(),
            ),
        },
        run_config={
            "ops": {
                "polymarket_wc2026_dbt": {
                    "config": {
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
    _materialize_refresh_path(
        monkeypatch,
        tmp_path,
        db_name=f"pipeline-{slug}.duckdb",
        slug=slug,
        question=question,
        transient_token=None,
    )
    with connection.get_connection() as conn:
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


_MIDTERMS_EVENT_SLUG = "balance-of-power-2026-midterms"
_MIDTERMS_SCOPE = "us_midterms_2026"
_MIDTERMS_RAW_SCHEMA = "polymarket_us_midterms_2026_raw"
_MIDTERMS_VALID_TOKEN_YES = "m" * 33 + "01"
_MIDTERMS_VALID_TOKEN_NO = "m" * 33 + "02"
_MIDTERMS_JOBS = (
    "polymarket_us_midterms_2026_market_registry_refresh",
    "polymarket_us_midterms_2026_hourly_odds_ingest",
    "polymarket_us_midterms_2026_full_pipeline",
)


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
        del client, fidelity, kwargs
        if transient_token is not None and str(token_id) == transient_token:
            return None
        base_ts = int(
            start_ts if start_ts is not None else now_ts if now_ts is not None else 0
        )
        return [
            (str(token_id), base_ts + 60, 0.52),
            (str(token_id), base_ts + 120, 0.54),
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
        lambda: {"rows": 0, "completed_rows": 0, "scheduled_rows": 0},
    )
    return market_page


def _patch_midterms_job_externals(monkeypatch, market_page: list[dict]) -> None:
    """Job configs force keyset discovery; stub collect + dlt for in-process runs."""
    monkeypatch.setattr(
        midterms_assets_mod,
        "collect_market_scope_payload",
        lambda **_kwargs: {
            "market_rows": market_page,
            "token_rows": [
                (
                    "m-midterms-1",
                    f'["{_MIDTERMS_VALID_TOKEN_YES}", "{_MIDTERMS_VALID_TOKEN_NO}"]',
                )
            ],
            "run_summary": {
                "task": "sync_markets",
                "total_fetched": len(market_page),
                "scope_name": _MIDTERMS_SCOPE,
            },
        },
    )
    pipeline = MagicMock(has_pending_data=False)
    conn = MagicMock()

    @contextmanager
    def connection_ctx():
        yield conn

    monkeypatch.setattr(
        midterms_assets_mod.asset_helpers,
        "get_polymarket_dlt_pipeline",
        lambda **_kwargs: pipeline,
    )
    monkeypatch.setattr(midterms_assets_mod, "get_connection", connection_ctx)
    monkeypatch.setattr(
        midterms_assets_mod,
        "ensure_polymarket_indexes",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        midterms_assets_mod, "save_sync_run_metrics", lambda *_args, **_kwargs: None
    )


def _configure_midterms_smoke_env(monkeypatch, tmp_path: Path, db_name: str) -> Path:
    db_path = tmp_path / db_name
    profiles_dir = tmp_path / f"profiles-{db_name}"
    profiles_dir.mkdir()
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
            polymarket_us_midterms_2026_raw_market_metadata_backfill,
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
        [polymarket_wc2026_dbt],
        resources={
            "dbt": DbtCliResource(
                project_dir=DBT_PROJECT,
                profiles_dir=str(profiles_dir),
                dbt_executable=resolve_dbt_executable(),
            ),
        },
        run_config={
            "ops": {
                "polymarket_wc2026_dbt": {
                    "config": {
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


@pytest.mark.parametrize("job_name", _MIDTERMS_JOBS)
def test_midterms_job_executes_in_process(
    job_name,
    monkeypatch,
    tmp_path,
    reset_connection_globals,
    no_sleep,
) -> None:
    profiles_dir = _configure_midterms_smoke_env(
        monkeypatch,
        tmp_path,
        db_name=f"job-{job_name}.duckdb",
    )
    market_page = _patch_midterms_refresh_externals(monkeypatch, transient_token=None)
    _patch_midterms_job_externals(monkeypatch, market_page)
    _seed_dlt_owned_markets(market_page, raw_schema=_MIDTERMS_RAW_SCHEMA)
    save_market_tokens_batch(
        [
            (
                "m-midterms-1",
                f'["{_MIDTERMS_VALID_TOKEN_YES}", "{_MIDTERMS_VALID_TOKEN_NO}"]',
            )
        ],
        scope_name=_MIDTERMS_SCOPE,
    )
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

    noop_dlt = MagicMock()
    noop_dlt.run.return_value = iter([])
    result = defs.resolve_job_def(job_name).execute_in_process(
        resources={
            "dbt": DbtCliResource(
                project_dir=DBT_PROJECT,
                profiles_dir=str(profiles_dir),
                dbt_executable=resolve_dbt_executable(),
            ),
            "dlt": noop_dlt,
        },
    )
    assert result.success is True
