import importlib
from pathlib import Path

import yaml
from dagster import DefaultScheduleStatus, build_schedule_context

from oddsfox.orchestration.definitions import defs
from oddsfox.orchestration.schedules import (
    polymarket_minutely_odds_cold_schedule,
    polymarket_minutely_odds_live_schedule,
    polymarket_minutely_odds_schedule,
)


def _polymarket_sources_path() -> Path:
    return (
        Path(__file__).resolve().parents[3]
        / "dbt"
        / "models"
        / "sources"
        / "polymarket_sources.yml"
    )


def _reload_schedules_module(monkeypatch, *, standard: bool, live: bool):
    monkeypatch.setenv(
        "POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED", "true" if standard else "false"
    )
    monkeypatch.setenv(
        "POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED", "true" if live else "false"
    )
    from oddsfox.config._reload_settings import reload_all_settings_modules

    reload_all_settings_modules()
    import oddsfox.orchestration.schedules as schedules_mod

    return importlib.reload(schedules_mod)


def test_definitions_expose_v010_jobs_only():
    expected = {
        "polymarket_ingest_full_refresh_events",
        "polymarket_ingest_incremental",
        "polymarket_minutely_odds_ingest",
        "dbt_full_refresh",
        "polymarket_selected_scope_full_pipeline",
    }

    assert {
        job.name for job in defs.resolve_all_job_defs() if job.name != "__ASSET_JOB"
    } == expected


def test_definitions_expose_v010_asset_keys():
    expected = {
        "dlt_polymarket_markets",
        "polymarket_markets_snapshot",
        "polymarket_market_scope_registry",
        "polymarket_market_metadata_backfill",
        "polymarket_token_odds_history",
        "polymarket_token_odds_history_minutely",
        "polymarket_odds_repair",
        "polymarket_stg_markets",
        "polymarket_stg_market_tokens",
        "polymarket_stg_odds",
        "polymarket_stg_odds_daily",
        "polymarket_stg_pipeline_run_events",
        "polymarket_stg_sync_ledger",
        "polymarket_stg_token_sync_skips",
        "polymarket_int_selected_markets",
        "polymarket_int_token_universe",
        "polymarket_int_selected_token_universe",
        "polymarket_int_token_daily_timeseries",
        "polymarket_market_coverage",
        "polymarket_selected_markets",
        "polymarket_token_coverage",
        "polymarket_selected_token_minutely_odds",
        "polymarket_selected_token_daily_odds",
        "polymarket_selected_whale_minutely_odds",
        "polymarket_sync_run_observability",
    }

    asset_keys = {key.path[-1] for key in defs.resolve_all_asset_keys()}
    assert expected <= asset_keys
    excluded_source_slug = "fifa" + "index"
    assert not any(excluded_source_slug in key for key in asset_keys)


def test_polymarket_source_dagster_asset_keys_exist_in_definitions():
    data = yaml.safe_load(_polymarket_sources_path().read_text())
    yaml_asset_keys = {
        tuple(table["meta"]["dagster"]["asset_key"])
        for source in data["sources"]
        for table in source["tables"]
    }
    defs_asset_keys = {tuple(key.path) for key in defs.resolve_all_asset_keys()}
    missing = yaml_asset_keys - defs_asset_keys
    assert not missing, f"missing Dagster assets for dbt source metadata: {missing}"


def test_minutely_schedules_default_stopped():
    schedules = (
        polymarket_minutely_odds_schedule,
        polymarket_minutely_odds_cold_schedule,
        polymarket_minutely_odds_live_schedule,
    )
    assert all(
        schedule.default_status == DefaultScheduleStatus.STOPPED
        for schedule in schedules
    )


def test_minutely_schedules_mutually_exclusive_when_both_flags_true(
    monkeypatch, caplog
):
    schedules_mod = _reload_schedules_module(monkeypatch, standard=True, live=True)

    assert schedules_mod.polymarket_minutely_odds_live_schedule.default_status == (
        DefaultScheduleStatus.RUNNING
    )
    assert schedules_mod.polymarket_minutely_odds_schedule.default_status == (
        DefaultScheduleStatus.STOPPED
    )
    assert schedules_mod.polymarket_minutely_odds_cold_schedule.default_status == (
        DefaultScheduleStatus.STOPPED
    )
    assert "keeping only polymarket_minutely_odds_live_schedule RUNNING" in caplog.text


def test_minutely_schedules_share_job_and_cold_config():
    schedules = (
        polymarket_minutely_odds_schedule,
        polymarket_minutely_odds_cold_schedule,
        polymarket_minutely_odds_live_schedule,
    )
    assert {schedule.job_name for schedule in schedules} == {
        "polymarket_minutely_odds_ingest"
    }

    context = build_schedule_context()
    cold_run_config = (
        polymarket_minutely_odds_cold_schedule.evaluate_tick(context)
        .run_requests[0]
        .run_config
    )
    cold_config = cold_run_config["ops"]["polymarket_token_odds_history_minutely"][
        "config"
    ]
    assert cold_config["force"] is False
    assert cold_config["overlap_minutes"] == 2

    assert (
        polymarket_minutely_odds_schedule.evaluate_tick(context)
        .run_requests[0]
        .run_config
        == {}
    )
    assert (
        polymarket_minutely_odds_live_schedule.evaluate_tick(context)
        .run_requests[0]
        .run_config
        == {}
    )
