import importlib
from pathlib import Path

import yaml
from dagster import DefaultScheduleStatus, build_schedule_context

from oddsfox_pipeline.orchestration.config import (
    wc2026_full_refresh_events_run_config,
    wc2026_hourly_odds_run_config,
)
from oddsfox_pipeline.orchestration.definitions import defs
from oddsfox_pipeline.orchestration.jobs import _merge_run_configs
from oddsfox_pipeline.orchestration.schedules import (
    wc2026_hourly_odds_schedule,
    wc2026_minutely_odds_cold_schedule,
    wc2026_minutely_odds_live_schedule,
    wc2026_minutely_odds_schedule,
)


def _polymarket_sources_path() -> Path:
    return (
        Path(__file__).resolve().parents[3]
        / "dbt"
        / "models"
        / "sources"
        / "wc2026_polymarket_sources.yml"
    )


def _reload_schedules_module(
    monkeypatch, *, standard: bool, live: bool, hourly: bool = False
):
    monkeypatch.setenv(
        "WC2026_POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED",
        "true" if standard else "false",
    )
    monkeypatch.setenv(
        "WC2026_POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED",
        "true" if live else "false",
    )
    monkeypatch.setenv(
        "WC2026_POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED", "true" if hourly else "false"
    )
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules

    reload_all_settings_modules()
    import oddsfox_pipeline.orchestration.schedules as schedules_mod

    return importlib.reload(schedules_mod)


def test_definitions_expose_v010_jobs_only():
    expected = {
        "wc2026_hourly_odds_ingest",
        "wc2026_market_registry_refresh",
        "wc2026_minutely_odds_ingest",
        "wc2026_dbt_build",
        "wc2026_knockout_export",
        "wc2026_full_pipeline",
    }

    assert {
        job.name for job in defs.resolve_all_job_defs() if job.name != "__ASSET_JOB"
    } == expected


def test_definitions_expose_v010_asset_keys():
    expected = {
        "wc2026_polymarket_raw_markets",
        "wc2026_polymarket_markets_snapshot",
        "wc2026_polymarket_market_registry",
        "wc2026_polymarket_market_metadata_backfill",
        "wc2026_polymarket_token_odds_history_hourly",
        "wc2026_polymarket_token_odds_history_minutely",
        "wc2026_polymarket_odds_repair",
        "wc2026_polymarket_stg_markets",
        "wc2026_polymarket_stg_market_tokens",
        "wc2026_polymarket_stg_odds",
        "wc2026_polymarket_stg_odds_daily",
        "wc2026_polymarket_stg_pipeline_run_events",
        "wc2026_polymarket_stg_sync_ledger",
        "wc2026_polymarket_stg_token_sync_skips",
        "wc2026_polymarket_int_markets",
        "wc2026_polymarket_int_market_tokens",
        "wc2026_polymarket_int_token_universe",
        "wc2026_polymarket_int_token_daily_timeseries",
        "wc2026_polymarket_market_coverage",
        "wc2026_polymarket_markets",
        "wc2026_polymarket_token_coverage",
        "wc2026_polymarket_token_hourly_odds",
        "wc2026_polymarket_token_minutely_odds",
        "wc2026_polymarket_token_daily_odds",
        "wc2026_polymarket_whale_minutely_odds",
        "wc2026_polymarket_market_tokens",
        "wc2026_polymarket_knockout_market_tokens",
        "wc2026_polymarket_knockout_markets",
        "wc2026_polymarket_knockout_token_hourly_odds",
        "wc2026_polymarket_sync_run_observability",
    }

    asset_keys = {key.path[-1] for key in defs.resolve_all_asset_keys()}
    assert expected <= asset_keys
    assert not any(key.startswith("polymarket_") for key in asset_keys)
    assert not any("selected" in key for key in asset_keys)
    excluded_source_slug = "fifa" + "index"
    assert not any(excluded_source_slug in key for key in asset_keys)


def test_wc2026_jobs_pin_scope_config():
    registry_config = wc2026_full_refresh_events_run_config()["ops"]
    hourly_config = wc2026_hourly_odds_run_config()["ops"]
    full_config = _merge_run_configs(
        wc2026_full_refresh_events_run_config(),
        wc2026_hourly_odds_run_config(),
        {"ops": {"wc2026_polymarket_dbt": {"config": {"full_refresh": True}}}},
    )["ops"]

    assert registry_config["wc2026_polymarket_markets_snapshot"]["config"][
        "scope_names"
    ] == ["wc2026"]
    assert registry_config["wc2026_polymarket_market_registry"]["config"][
        "scope_names"
    ] == ["wc2026"]
    assert hourly_config["wc2026_polymarket_token_odds_history_hourly"]["config"][
        "scope_names"
    ] == ["wc2026"]
    assert full_config["wc2026_polymarket_markets_snapshot"]["config"][
        "scope_names"
    ] == ["wc2026"]
    assert full_config["wc2026_polymarket_token_odds_history_hourly"]["config"][
        "scope_names"
    ] == ["wc2026"]
    assert "wc2026_polymarket_dbt" in full_config


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
        wc2026_minutely_odds_schedule,
        wc2026_minutely_odds_cold_schedule,
        wc2026_minutely_odds_live_schedule,
        wc2026_hourly_odds_schedule,
    )
    assert all(
        schedule.default_status == DefaultScheduleStatus.STOPPED
        for schedule in schedules
    )


def test_minutely_schedules_mutually_exclusive_when_both_flags_true(
    monkeypatch, caplog
):
    schedules_mod = _reload_schedules_module(monkeypatch, standard=True, live=True)

    assert schedules_mod.wc2026_minutely_odds_live_schedule.default_status == (
        DefaultScheduleStatus.RUNNING
    )
    assert schedules_mod.wc2026_minutely_odds_schedule.default_status == (
        DefaultScheduleStatus.STOPPED
    )
    assert schedules_mod.wc2026_minutely_odds_cold_schedule.default_status == (
        DefaultScheduleStatus.STOPPED
    )
    assert "keeping only wc2026_minutely_odds_live_schedule RUNNING" in caplog.text


def test_minutely_schedules_share_job_and_cold_config():
    schedules = (
        wc2026_minutely_odds_schedule,
        wc2026_minutely_odds_cold_schedule,
        wc2026_minutely_odds_live_schedule,
    )
    assert {schedule.job_name for schedule in schedules} == {
        "wc2026_minutely_odds_ingest"
    }

    context = build_schedule_context()
    cold_run_config = (
        wc2026_minutely_odds_cold_schedule.evaluate_tick(context)
        .run_requests[0]
        .run_config
    )
    cold_config = cold_run_config["ops"][
        "wc2026_polymarket_token_odds_history_minutely"
    ]["config"]
    assert cold_config["force"] is False
    assert cold_config["overlap_minutes"] == 2

    assert (
        wc2026_minutely_odds_schedule.evaluate_tick(context).run_requests[0].run_config
        == {}
    )
    assert (
        wc2026_minutely_odds_live_schedule.evaluate_tick(context)
        .run_requests[0]
        .run_config
        == {}
    )


def test_hourly_schedule_targets_hourly_job_and_config():
    assert wc2026_hourly_odds_schedule.default_status == (DefaultScheduleStatus.STOPPED)
    assert wc2026_hourly_odds_schedule.job_name == "wc2026_hourly_odds_ingest"

    context = build_schedule_context()
    run_config = (
        wc2026_hourly_odds_schedule.evaluate_tick(context).run_requests[0].run_config
    )
    cfg = run_config["ops"]["wc2026_polymarket_token_odds_history_hourly"]["config"]
    assert cfg["fidelity"] == 60
    assert cfg["overlap_minutes"] == 60
    assert cfg["min_volume"] == 100000.0
    assert cfg["max_volume"] is None


def test_hourly_schedule_enabled_by_env(monkeypatch):
    schedules_mod = _reload_schedules_module(
        monkeypatch, standard=False, live=False, hourly=True
    )

    assert schedules_mod.wc2026_hourly_odds_schedule.default_status == (
        DefaultScheduleStatus.RUNNING
    )
