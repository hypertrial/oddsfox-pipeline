import importlib
from pathlib import Path

import yaml
from dagster import AssetKey, DefaultScheduleStatus, build_schedule_context

from oddsfox_pipeline.orchestration.config import (
    polymarket_wc2026_full_refresh_events_run_config,
    polymarket_wc2026_hourly_odds_run_config,
    polymarket_wc2026_match_minute_odds_run_config,
    polymarket_wc2026_polygon_settlement_backfill_run_config,
    wc2026_knockout_match_odds_full_pipeline_run_config,
)
from oddsfox_pipeline.orchestration.definitions import defs
from oddsfox_pipeline.orchestration.jobs import (
    POLYMARKET_WC2026_MATCH_MINUTE_DBT_SELECTION,
    POLYMARKET_WC2026_POLYGON_SETTLEMENT_DBT_SELECTION,
    _merge_run_configs,
)
from oddsfox_pipeline.orchestration.schedules import (
    polymarket_wc2026_hourly_odds_schedule,
    wc2026_knockout_match_odds_hourly_schedule,
)


def _polymarket_sources_paths() -> list[Path]:
    sources_dir = Path(__file__).resolve().parents[3] / "dbt" / "models" / "sources"
    return [
        sources_dir / "polymarket_wc2026_sources.yml",
        sources_dir / "polymarket_us_midterms_2026_sources.yml",
        sources_dir / "international_results_wc2026_sources.yml",
        sources_dir / "kalshi_wc2026_sources.yml",
        sources_dir / "openfootball_wc2026_sources.yml",
    ]


def _polymarket_sources_path() -> Path:
    return _polymarket_sources_paths()[0]


def _reload_schedules_module(
    monkeypatch,
    *,
    hourly: bool = False,
    combined_hourly: bool = False,
):
    monkeypatch.setenv(
        "POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED", "true" if hourly else "false"
    )
    monkeypatch.setenv(
        "WC2026_KNOCKOUT_MATCH_ODDS_HOURLY_SCHEDULE_ENABLED",
        "true" if combined_hourly else "false",
    )
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules

    reload_all_settings_modules()
    import oddsfox_pipeline.orchestration.schedules as schedules_mod

    return importlib.reload(schedules_mod)


def test_definitions_expose_v010_jobs_only():
    expected = {
        "international_results_historical_ingest",
        "international_results_wc2026_match_results_ingest",
        "kalshi_wc2026_dbt_build",
        "kalshi_wc2026_full_pipeline",
        "kalshi_wc2026_hourly_odds_ingest",
        "kalshi_wc2026_market_registry_refresh",
        "polymarket_us_midterms_2026_dbt_build",
        "polymarket_us_midterms_2026_full_pipeline",
        "polymarket_us_midterms_2026_hourly_odds_ingest",
        "polymarket_us_midterms_2026_market_registry_refresh",
        "polymarket_wc2026_hourly_odds_ingest",
        "polymarket_wc2026_market_registry_refresh",
        "polymarket_wc2026_match_minute_odds_backfill",
        "polymarket_wc2026_polygon_settlement_backfill",
        "polymarket_wc2026_polygon_settlement_release",
        "polymarket_wc2026_dbt_build",
        "polymarket_wc2026_full_pipeline",
        "wc2026_knockout_match_odds_full_pipeline",
    }

    assert {
        job.name for job in defs.resolve_all_job_defs() if job.name != "__ASSET_JOB"
    } == expected


def test_definitions_expose_v010_asset_keys():
    expected = {
        ("international_results", "historical", "raw", "snapshot"),
        ("international_results", "wc2026", "raw", "match_results"),
        ("international_results", "wc2026", "staging", "match_results"),
        ("international_results", "wc2026", "staging", "team_aliases"),
        ("international_results", "wc2026", "intermediate", "match_teams"),
        ("international_results", "wc2026", "marts", "matches"),
        ("international_results", "wc2026", "marts", "team_status"),
        ("international_results", "wc2026", "observability", "data_quality"),
        ("openfootball", "wc2026", "raw", "knockout_fixtures"),
        ("openfootball", "wc2026", "staging", "knockout_fixtures"),
        ("wc2026", "intermediate", "knockout_fixtures"),
        ("wc2026", "marts", "knockout_match_hourly_odds"),
        ("wc2026", "observability", "knockout_match_odds_coverage"),
        ("wc2026", "observability", "knockout_match_odds_data_quality"),
        ("wc2026", "raw", "clubelo"),
        ("wc2026", "raw", "eloratings"),
        ("wc2026", "raw", "fifaindex"),
        ("wc2026", "raw", "fotmob"),
        ("wc2026", "raw", "wikipedia_squads"),
        ("wc2026", "ops", "raw_snapshot_ledger"),
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
        ("kalshi", "wc2026", "intermediate", "match_advance_markets"),
        ("kalshi", "wc2026", "intermediate", "match_hourly_odds"),
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
        ("polymarket", "wc2026", "raw", "polygon_settlement_fills"),
        ("polymarket", "wc2026", "release", "polygon_settlement_odds_bundle"),
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
        ("polymarket", "wc2026", "intermediate", "match_advance_tokens"),
        ("polymarket", "wc2026", "intermediate", "match_hourly_odds"),
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
            ("international_results", "historical"),
            ("international_results", "wc2026"),
            ("openfootball", "wc2026"),
            ("kalshi", "wc2026"),
        }
        or key[0] == "wc2026"
        for key in asset_keys
    )
    assert not any("selected" in part for key in asset_keys for part in key)


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


def test_match_minute_job_is_closed_untruncated_and_unscheduled():
    config = polymarket_wc2026_match_minute_odds_run_config()["ops"]
    markets = config["polymarket_wc2026_raw_markets"]["config"]
    registry = config["polymarket_wc2026_ops_market_scope_registry"]["config"]
    minute = config["polymarket_wc2026_raw_match_token_odds_history_minute"]["config"]
    dbt = config["oddsfox_dbt"]["config"]

    assert markets["keyset_closed"] is True
    assert registry["keyset_closed"] is True
    assert markets["keyset_volume_min"] == 0.0
    assert registry["keyset_volume_min"] == 0.0
    assert markets["max_event_pages"] is None
    assert markets["max_pages_without_progress"] is None
    assert minute["requests_per_second"] > 0
    assert dbt["dbt_select"] == "+polymarket_wc2026_match_minute_odds"
    selected = defs.resolve_job_def(
        "polymarket_wc2026_match_minute_odds_backfill"
    ).asset_layer.selected_asset_keys
    assert (
        AssetKey(["international_results", "wc2026", "raw", "match_results"])
        in selected
    )
    assert all(
        schedule.job_name != "polymarket_wc2026_match_minute_odds_backfill"
        for schedule in defs.schedules
    )


def test_match_minute_dbt_selection_does_not_leak_sibling_model_checks():
    graph = defs.resolve_asset_graph()
    selected_assets = POLYMARKET_WC2026_MATCH_MINUTE_DBT_SELECTION.resolve(graph)
    selected_checks = POLYMARKET_WC2026_MATCH_MINUTE_DBT_SELECTION.resolve_checks(graph)

    assert selected_checks
    assert {check.asset_key for check in selected_checks} <= selected_assets


def test_polygon_settlement_jobs_are_isolated_and_unscheduled():
    config = polymarket_wc2026_polygon_settlement_backfill_run_config()["ops"]
    assert set(config) == {
        "polymarket_wc2026_raw_polygon_settlement_fills",
        "oddsfox_dbt",
    }
    assert config["oddsfox_dbt"]["config"]["dbt_select"] == (
        "+polymarket_wc2026_polygon_settlement_minute_odds"
    )

    backfill = defs.resolve_job_def("polymarket_wc2026_polygon_settlement_backfill")
    selected = backfill.asset_layer.selected_asset_keys
    assert (
        AssetKey(["polymarket", "wc2026", "raw", "polygon_settlement_fills"])
        in selected
    )
    assert (
        AssetKey(["polymarket", "wc2026", "marts", "polygon_settlement_minute_odds"])
        in selected
    )
    assert AssetKey(["polymarket", "wc2026", "raw", "markets"]) not in selected

    release = defs.resolve_job_def("polymarket_wc2026_polygon_settlement_release")
    assert release.asset_layer.selected_asset_keys == {
        AssetKey(["polymarket", "wc2026", "release", "polygon_settlement_odds_bundle"])
    }
    assert all(
        "polygon_settlement" not in schedule.job_name for schedule in defs.schedules
    )

    graph = defs.resolve_asset_graph()
    dbt_assets = POLYMARKET_WC2026_POLYGON_SETTLEMENT_DBT_SELECTION.resolve(graph)
    dbt_checks = POLYMARKET_WC2026_POLYGON_SETTLEMENT_DBT_SELECTION.resolve_checks(
        graph
    )
    assert dbt_checks
    assert {check.asset_key for check in dbt_checks} <= dbt_assets

    ordinary = defs.resolve_job_def("polymarket_wc2026_dbt_build")
    assert all(
        "polygon_settlement" not in key.path[-1]
        for key in ordinary.asset_layer.selected_asset_keys
    )


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


def test_combined_schedule_is_atomic_stopped_and_uses_unfiltered_prices():
    assert wc2026_knockout_match_odds_hourly_schedule.default_status == (
        DefaultScheduleStatus.STOPPED
    )
    assert (
        wc2026_knockout_match_odds_hourly_schedule.job_name
        == "wc2026_knockout_match_odds_full_pipeline"
    )
    run_config = (
        wc2026_knockout_match_odds_hourly_schedule.evaluate_tick(
            build_schedule_context()
        )
        .run_requests[0]
        .run_config
    )
    assert run_config == wc2026_knockout_match_odds_full_pipeline_run_config()
    assert (
        run_config["ops"]["polymarket_wc2026_raw_markets"]["config"][
            "keyset_volume_min"
        ]
        == 0.0
    )
    assert (
        run_config["ops"]["polymarket_wc2026_ops_market_scope_registry"]["config"][
            "keyset_volume_min"
        ]
        == 0.0
    )
    assert (
        run_config["ops"]["polymarket_wc2026_raw_token_odds_history_hourly"]["config"][
            "min_volume"
        ]
        is None
    )
    dbt_config = run_config["ops"]["oddsfox_dbt"]["config"]
    assert dbt_config["full_refresh"] is False
    assert dbt_config["dbt_select"] == "+tag:cross_domain"
    assert dbt_config["dbt_exclude"] == "tag:polygon_settlement"


def test_combined_hourly_schedule_can_be_enabled(monkeypatch):
    schedules_mod = _reload_schedules_module(monkeypatch, combined_hourly=True)

    assert (
        schedules_mod.wc2026_knockout_match_odds_hourly_schedule.default_status
        == DefaultScheduleStatus.RUNNING
    )


def test_midterms_full_pipeline_excludes_wc2026_and_results_assets():
    job = defs.resolve_job_def("polymarket_us_midterms_2026_full_pipeline")
    selected = {tuple(key.path) for key in job.asset_layer.selected_asset_keys}

    assert not any(key[:2] == ("international_results", "wc2026") for key in selected)
    assert not any(key[:2] == ("polymarket", "wc2026") for key in selected)
    assert all(key[:2] == ("polymarket", "us_midterms_2026") for key in selected)


def test_scoped_dbt_jobs_select_only_their_expected_scope_assets():
    midterms = {
        tuple(key.path)
        for key in defs.resolve_job_def(
            "polymarket_us_midterms_2026_dbt_build"
        ).asset_layer.selected_asset_keys
    }
    kalshi = {
        tuple(key.path)
        for key in defs.resolve_job_def(
            "kalshi_wc2026_dbt_build"
        ).asset_layer.selected_asset_keys
    }
    wc2026 = {
        tuple(key.path)
        for key in defs.resolve_job_def(
            "polymarket_wc2026_dbt_build"
        ).asset_layer.selected_asset_keys
    }

    assert midterms
    assert all(key[:2] == ("polymarket", "us_midterms_2026") for key in midterms)

    assert kalshi
    assert any(key[:2] == ("international_results", "wc2026") for key in kalshi)
    assert any(key[:2] == ("kalshi", "wc2026") for key in kalshi)
    assert not any(key[:2] == ("wc2026", "marts") for key in kalshi)
    assert not any(
        key[:4] == ("kalshi", "wc2026", "intermediate", "match_hourly_odds")
        for key in kalshi
    )
    assert not any(
        key[:4] == ("kalshi", "wc2026", "intermediate", "match_advance_markets")
        for key in kalshi
    )
    assert not any(key[:2] == ("polymarket", "wc2026") for key in kalshi)
    assert not any(key[:2] == ("polymarket", "us_midterms_2026") for key in kalshi)

    assert wc2026
    assert any(key[:2] == ("international_results", "wc2026") for key in wc2026)
    assert any(key[:2] == ("polymarket", "wc2026") for key in wc2026)
    assert not any(key[:2] == ("wc2026", "marts") for key in wc2026)
    assert not any(
        key[:4] == ("polymarket", "wc2026", "intermediate", "match_hourly_odds")
        for key in wc2026
    )
    assert not any(
        key[:4] == ("polymarket", "wc2026", "intermediate", "match_advance_tokens")
        for key in wc2026
    )
    assert not any(key[:2] == ("kalshi", "wc2026") for key in wc2026)
    assert not any(key[:2] == ("polymarket", "us_midterms_2026") for key in wc2026)


def test_combined_job_selects_both_sources_fixture_and_cross_domain_models():
    selected = {
        tuple(key.path)
        for key in defs.resolve_job_def(
            "wc2026_knockout_match_odds_full_pipeline"
        ).asset_layer.selected_asset_keys
    }

    assert ("openfootball", "wc2026", "raw", "knockout_fixtures") in selected
    assert any(key[:2] == ("polymarket", "wc2026") for key in selected)
    assert any(key[:2] == ("kalshi", "wc2026") for key in selected)
    assert (
        "polymarket",
        "wc2026",
        "intermediate",
        "match_hourly_odds",
    ) in selected
    assert (
        "kalshi",
        "wc2026",
        "intermediate",
        "match_hourly_odds",
    ) in selected
    assert ("wc2026", "marts", "knockout_match_hourly_odds") in selected


def test_combined_job_leaves_indirect_dbt_checks_to_buildable_selection():
    from oddsfox_pipeline.orchestration.jobs import (
        WC2026_KNOCKOUT_MATCH_ODDS_DBT_SELECTION,
    )

    graph = defs.resolve_asset_graph()

    assert WC2026_KNOCKOUT_MATCH_ODDS_DBT_SELECTION.resolve(graph)
    assert not WC2026_KNOCKOUT_MATCH_ODDS_DBT_SELECTION.resolve_checks(graph)


def test_combined_job_refreshes_fixture_before_both_vendor_registry_paths():
    graph = defs.resolve_asset_graph()
    fixture = AssetKey(["openfootball", "wc2026", "raw", "knockout_fixtures"])

    for vendor_asset in (
        AssetKey(["polymarket", "wc2026", "raw", "markets"]),
        AssetKey(["kalshi", "wc2026", "raw", "events"]),
        AssetKey(["kalshi", "wc2026", "raw", "markets"]),
    ):
        assert fixture in graph.get(vendor_asset).parent_keys
