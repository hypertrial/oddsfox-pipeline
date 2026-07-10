import importlib
from pathlib import Path

import yaml
from dagster import DefaultScheduleStatus, build_schedule_context

from oddsfox_pipeline.orchestration.config import (
    polymarket_wc2026_full_refresh_events_run_config,
    polymarket_wc2026_hourly_odds_run_config,
)
from oddsfox_pipeline.orchestration.definitions import defs
from oddsfox_pipeline.orchestration.jobs import _merge_run_configs
from oddsfox_pipeline.orchestration.schedules import (
    polymarket_wc2026_hourly_odds_schedule,
)


def _polymarket_sources_paths() -> list[Path]:
    sources_dir = Path(__file__).resolve().parents[3] / "dbt" / "models" / "sources"
    return [
        sources_dir / "polymarket_wc2026_sources.yml",
        sources_dir / "polymarket_us_midterms_2026_sources.yml",
        sources_dir / "international_results_wc2026_sources.yml",
        sources_dir / "kalshi_wc2026_sources.yml",
    ]


def _polymarket_sources_path() -> Path:
    return _polymarket_sources_paths()[0]


def _reload_schedules_module(monkeypatch, *, hourly: bool = False):
    monkeypatch.setenv(
        "POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED", "true" if hourly else "false"
    )
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules

    reload_all_settings_modules()
    import oddsfox_pipeline.orchestration.schedules as schedules_mod

    return importlib.reload(schedules_mod)


def test_definitions_expose_v010_jobs_only():
    expected = {
        "international_results_wc2026_match_results_ingest",
        "kalshi_wc2026_full_pipeline",
        "kalshi_wc2026_hourly_odds_ingest",
        "kalshi_wc2026_market_registry_refresh",
        "polymarket_us_midterms_2026_full_pipeline",
        "polymarket_us_midterms_2026_hourly_odds_ingest",
        "polymarket_us_midterms_2026_market_registry_refresh",
        "polymarket_wc2026_hourly_odds_ingest",
        "polymarket_wc2026_market_registry_refresh",
        "polymarket_wc2026_dbt_build",
        "polymarket_wc2026_full_pipeline",
    }

    assert {
        job.name for job in defs.resolve_all_job_defs() if job.name != "__ASSET_JOB"
    } == expected


def test_definitions_expose_v010_asset_keys():
    expected = {
        ("international_results", "wc2026", "raw", "match_results"),
        ("international_results", "wc2026", "staging", "match_results"),
        ("international_results", "wc2026", "staging", "team_aliases"),
        ("international_results", "wc2026", "intermediate", "match_teams"),
        ("international_results", "wc2026", "marts", "matches"),
        ("international_results", "wc2026", "marts", "team_status"),
        ("international_results", "wc2026", "observability", "data_quality"),
        ("kalshi", "wc2026", "raw", "events"),
        ("kalshi", "wc2026", "raw", "markets"),
        ("kalshi", "wc2026", "raw", "markets_snapshot"),
        ("kalshi", "wc2026", "ops", "market_scope_registry"),
        ("kalshi", "wc2026", "raw", "market_candlesticks_hourly"),
        ("kalshi", "wc2026", "staging", "events"),
        ("kalshi", "wc2026", "staging", "markets"),
        ("kalshi", "wc2026", "staging", "market_candlesticks_hourly"),
        ("kalshi", "wc2026", "intermediate", "markets"),
        ("kalshi", "wc2026", "intermediate", "market_hourly_odds"),
        ("kalshi", "wc2026", "intermediate", "stage_classification"),
        ("kalshi", "wc2026", "intermediate", "group_winner_classification"),
        ("kalshi", "wc2026", "marts", "contract"),
        ("kalshi", "wc2026", "marts", "stage_markets"),
        ("kalshi", "wc2026", "marts", "stage_market_hourly_odds"),
        ("kalshi", "wc2026", "marts", "group_winner_markets"),
        ("kalshi", "wc2026", "marts", "group_winner_market_hourly_odds"),
        ("kalshi", "wc2026", "observability", "sync_run_observability"),
        ("kalshi", "wc2026", "observability", "stage_coverage"),
        ("kalshi", "wc2026", "observability", "data_quality"),
        ("polymarket", "us_midterms_2026", "raw", "markets"),
        ("polymarket", "us_midterms_2026", "raw", "markets_snapshot"),
        ("polymarket", "us_midterms_2026", "ops", "market_scope_registry"),
        ("polymarket", "us_midterms_2026", "raw", "market_metadata_backfill"),
        ("polymarket", "us_midterms_2026", "raw", "token_odds_history_hourly"),
        ("polymarket", "us_midterms_2026", "staging", "markets"),
        ("polymarket", "us_midterms_2026", "staging", "market_tokens"),
        ("polymarket", "us_midterms_2026", "staging", "odds"),
        ("polymarket", "us_midterms_2026", "staging", "odds_daily"),
        ("polymarket", "us_midterms_2026", "staging", "pipeline_run_events"),
        ("polymarket", "us_midterms_2026", "staging", "sync_ledger"),
        ("polymarket", "us_midterms_2026", "staging", "token_sync_skips"),
        ("polymarket", "us_midterms_2026", "intermediate", "markets"),
        ("polymarket", "us_midterms_2026", "intermediate", "market_tokens"),
        ("polymarket", "us_midterms_2026", "intermediate", "token_universe"),
        ("polymarket", "us_midterms_2026", "intermediate", "token_hourly_odds"),
        ("polymarket", "us_midterms_2026", "marts", "market_token_hourly_odds"),
        ("polymarket", "us_midterms_2026", "observability", "sync_run_observability"),
        ("polymarket", "wc2026", "raw", "markets"),
        ("polymarket", "wc2026", "raw", "markets_snapshot"),
        ("polymarket", "wc2026", "ops", "market_scope_registry"),
        ("polymarket", "wc2026", "raw", "market_metadata_backfill"),
        ("polymarket", "wc2026", "raw", "token_odds_history_hourly"),
        ("polymarket", "wc2026", "staging", "markets"),
        ("polymarket", "wc2026", "staging", "market_tokens"),
        ("polymarket", "wc2026", "staging", "odds"),
        ("polymarket", "wc2026", "staging", "odds_daily"),
        ("polymarket", "wc2026", "staging", "pipeline_run_events"),
        ("polymarket", "wc2026", "staging", "sync_ledger"),
        ("polymarket", "wc2026", "staging", "token_sync_skips"),
        ("polymarket", "wc2026", "intermediate", "markets"),
        ("polymarket", "wc2026", "intermediate", "market_tokens"),
        ("polymarket", "wc2026", "intermediate", "knockout_market_classification"),
        ("polymarket", "wc2026", "intermediate", "token_universe"),
        ("polymarket", "wc2026", "marts", "knockout_market_tokens"),
        ("polymarket", "wc2026", "marts", "knockout_markets"),
        ("polymarket", "wc2026", "marts", "knockout_token_hourly_odds"),
        ("polymarket", "wc2026", "observability", "sync_run_observability"),
    }

    asset_keys = {tuple(key.path) for key in defs.resolve_all_asset_keys()}
    assert expected <= asset_keys
    assert all(
        key[:2]
        in {
            ("polymarket", "wc2026"),
            ("polymarket", "us_midterms_2026"),
            ("international_results", "wc2026"),
            ("kalshi", "wc2026"),
        }
        for key in asset_keys
    )
    assert not any("selected" in part for key in asset_keys for part in key)
    excluded_source_slug = "fifa" + "index"
    assert not any(excluded_source_slug in part for key in asset_keys for part in key)


def _nested_keys(payload):
    if isinstance(payload, dict):
        for key, value in payload.items():
            yield key
            yield from _nested_keys(value)
    elif isinstance(payload, list):
        for value in payload:
            yield from _nested_keys(value)


def test_wc2026_jobs_do_not_expose_scope_config():
    legacy_key = "scope" + "_names"
    registry_config = polymarket_wc2026_full_refresh_events_run_config()["ops"]
    hourly_config = polymarket_wc2026_hourly_odds_run_config()["ops"]
    full_config = _merge_run_configs(
        polymarket_wc2026_full_refresh_events_run_config(),
        polymarket_wc2026_hourly_odds_run_config(),
        {"ops": {"oddsfox_dbt": {"config": {"full_refresh": True}}}},
    )["ops"]

    assert legacy_key not in set(_nested_keys(registry_config))
    assert legacy_key not in set(_nested_keys(hourly_config))
    assert legacy_key not in set(_nested_keys(full_config))
    assert "oddsfox_dbt" in full_config


def test_polymarket_source_dagster_asset_keys_exist_in_definitions():
    source_paths = _polymarket_sources_paths()
    yaml_asset_keys = set()
    for path in source_paths:
        data = yaml.safe_load(path.read_text())
        yaml_asset_keys.update(
            tuple(table["meta"]["dagster"]["asset_key"])
            for source in data["sources"]
            for table in source["tables"]
        )
    defs_asset_keys = {tuple(key.path) for key in defs.resolve_all_asset_keys()}
    missing = yaml_asset_keys - defs_asset_keys
    assert not missing, f"missing Dagster assets for dbt source metadata: {missing}"


def test_hourly_schedule_targets_hourly_job_and_config():
    assert polymarket_wc2026_hourly_odds_schedule.default_status == (
        DefaultScheduleStatus.STOPPED
    )
    assert (
        polymarket_wc2026_hourly_odds_schedule.job_name
        == "polymarket_wc2026_hourly_odds_ingest"
    )

    context = build_schedule_context()
    run_config = (
        polymarket_wc2026_hourly_odds_schedule.evaluate_tick(context)
        .run_requests[0]
        .run_config
    )
    cfg = run_config["ops"]["polymarket_wc2026_raw_token_odds_history_hourly"]["config"]
    assert cfg["fidelity"] == 60
    assert cfg["overlap_minutes"] == 60
    assert cfg["window_hours"] == 720
    assert cfg["history_backfill_days"] == 30
    assert cfg["min_volume"] == 5000.0
    assert cfg["max_volume"] is None


def test_hourly_schedule_enabled_by_env(monkeypatch):
    schedules_mod = _reload_schedules_module(monkeypatch, hourly=True)

    assert schedules_mod.polymarket_wc2026_hourly_odds_schedule.default_status == (
        DefaultScheduleStatus.RUNNING
    )


def test_midterms_full_pipeline_excludes_wc2026_and_results_assets():
    job = defs.resolve_job_def("polymarket_us_midterms_2026_full_pipeline")
    selected = {tuple(key.path) for key in job.asset_layer.selected_asset_keys}

    assert not any(key[:2] == ("international_results", "wc2026") for key in selected)
    assert not any(key[:2] == ("polymarket", "wc2026") for key in selected)
    assert all(key[:2] == ("polymarket", "us_midterms_2026") for key in selected)
