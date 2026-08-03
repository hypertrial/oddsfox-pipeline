import importlib
from pathlib import Path

import yaml
from dagster import AssetKey, DefaultScheduleStatus, build_schedule_context

from oddsfox_pipeline.orchestration.config import (
    polymarket_wc2026_dbt_build_run_config,
    polymarket_wc2026_full_refresh_events_run_config,
    polymarket_wc2026_hourly_odds_run_config,
    polymarket_wc2026_logical_atlas_run_config,
    polymarket_wc2026_market_portrait_run_config,
    polymarket_wc2026_match_minute_odds_run_config,
    polymarket_wc2026_match_order_book_run_config,
    polymarket_wc2026_polygon_settlement_backfill_run_config,
)
from oddsfox_pipeline.orchestration.definitions import defs
from oddsfox_pipeline.orchestration.jobs import (
    POLYMARKET_WC2026_MATCH_MINUTE_DBT_SELECTION,
    POLYMARKET_WC2026_MATCH_ORDER_BOOK_DBT_SELECTION,
    POLYMARKET_WC2026_POLYGON_SETTLEMENT_DBT_SELECTION,
    _merge_run_configs,
)
from oddsfox_pipeline.orchestration.schedules import (
    polymarket_wc2026_hourly_odds_schedule,
)


def _polymarket_sources_paths() -> list[Path]:
    sources_dir = Path(__file__).resolve().parents[3] / "dbt" / "models" / "sources"
    return [
        sources_dir / "polymarket_wc2026_sources.yml",
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
    kalshi_hourly: bool = False,
):
    monkeypatch.setenv(
        "POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED", "true" if hourly else "false"
    )
    monkeypatch.setenv(
        "KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED",
        "true" if kalshi_hourly else "false",
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
        "kalshi_wc2026_market_scope_registry_refresh",
        "polymarket_wc2026_hourly_odds_ingest",
        "polymarket_wc2026_logical_atlas",
        "polymarket_wc2026_market_scope_registry_refresh",
        "polymarket_wc2026_match_minute_odds_backfill",
        "polymarket_wc2026_match_order_book_backfill",
        "polymarket_wc2026_market_portrait_backfill",
        "polymarket_wc2026_polygon_settlement_backfill",
        "polymarket_wc2026_polygon_settlement_release",
        "polymarket_wc2026_dbt_build",
        "polymarket_wc2026_full_pipeline",
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
        ("openfootball", "wc2026", "raw", "schedule_fixtures"),
        ("openfootball", "wc2026", "staging", "schedule_fixtures"),
        ("wc2026", "raw", "clubelo"),
        ("wc2026", "raw", "eloratings"),
        ("wc2026", "raw", "fifaindex"),
        ("wc2026", "raw", "private_match_events"),
        ("wc2026", "raw", "wikipedia_squads"),
        ("wc2026", "ops", "raw_snapshot_ledger"),
        ("polymarket", "wc2026", "raw", "event_snapshots"),
        ("polymarket", "wc2026", "raw", "event_market_memberships"),
        ("polymarket", "wc2026", "intermediate", "event_membership"),
        ("polymarket", "wc2026", "release", "logical_bundle"),
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
        ("kalshi", "wc2026", "marts", "pipeline_policy"),
        ("kalshi", "wc2026", "marts", "stage_markets"),
        ("kalshi", "wc2026", "marts", "stage_market_hourly_odds"),
        ("kalshi", "wc2026", "marts", "group_winner_markets"),
        ("kalshi", "wc2026", "marts", "group_winner_market_hourly_odds"),
        ("kalshi", "wc2026", "observability", "ingestion_run_observability"),
        ("kalshi", "wc2026", "observability", "stage_coverage"),
        ("kalshi", "wc2026", "observability", "data_quality"),
        ("polymarket", "catalog", "raw", "markets"),
        ("polymarket", "catalog", "staging", "markets"),
        ("polymarket", "wc2026", "raw", "markets"),
        ("polymarket", "wc2026", "raw", "markets_snapshot"),
        ("polymarket", "wc2026", "ops", "market_scope_registry"),
        ("polymarket", "wc2026", "raw", "market_metadata_enrichment"),
        ("polymarket", "wc2026", "raw", "token_odds_history_hourly"),
        ("polymarket", "wc2026", "raw", "match_order_book_snapshots"),
        ("polymarket", "wc2026", "raw", "polygon_settlement_fills"),
        ("polymarket", "wc2026", "release", "polygon_settlement_odds_bundle"),
        ("polymarket", "wc2026", "staging", "markets"),
        ("polymarket", "wc2026", "marts", "markets"),
        ("polymarket", "wc2026", "staging", "market_tokens"),
        ("polymarket", "wc2026", "staging", "odds"),
        ("polymarket", "wc2026", "staging", "odds_daily"),
        ("polymarket", "wc2026", "staging", "ingestion_run_events"),
        ("polymarket", "wc2026", "staging", "sync_ledger"),
        ("polymarket", "wc2026", "staging", "token_sync_skips"),
        ("polymarket", "wc2026", "intermediate", "markets"),
        ("polymarket", "wc2026", "intermediate", "market_tokens"),
        ("polymarket", "wc2026", "intermediate", "knockout_market_classification"),
        ("polymarket", "wc2026", "intermediate", "token_working_set"),
        ("polymarket", "wc2026", "staging", "match_order_book_snapshots"),
        ("polymarket", "wc2026", "intermediate", "match_order_book_levels"),
        (
            "polymarket",
            "wc2026",
            "intermediate",
            "match_order_book_publication_gate",
        ),
        ("polymarket", "wc2026", "marts", "match_order_book"),
        (
            "polymarket",
            "wc2026",
            "observability",
            "match_order_book_quality_issues",
        ),
        (
            "polymarket",
            "wc2026",
            "observability",
            "match_order_book_data_quality",
        ),
        ("polymarket", "wc2026", "marts", "knockout_market_tokens"),
        ("polymarket", "wc2026", "marts", "knockout_markets"),
        ("polymarket", "wc2026", "marts", "knockout_token_hourly_odds"),
        ("polymarket", "wc2026", "observability", "ingestion_run_observability"),
    }

    asset_keys = {tuple(key.path) for key in defs.resolve_all_asset_keys()}
    assert expected <= asset_keys
    shipped_asset_keys = {
        key for key in asset_keys if "us_midterms_2026" not in "/".join(key)
    }
    assert all(
        key[:2]
        in {
            ("polymarket", "wc2026"),
            ("polymarket", "catalog"),
            ("international_results", "historical"),
            ("international_results", "wc2026"),
            ("openfootball", "wc2026"),
            ("kalshi", "wc2026"),
        }
        or key[0] == "wc2026"
        for key in shipped_asset_keys
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


def test_full_pipeline_merge_retains_logical_atlas_and_scope_dbt_selects():
    merged = _merge_run_configs(
        polymarket_wc2026_logical_atlas_run_config(),
        polymarket_wc2026_dbt_build_run_config(),
    )["ops"]["oddsfox_dbt"]["config"]

    assert "+tag:wc2026_logical_atlas" in merged["dbt_select"]
    assert "+tag:polymarket,tag:wc2026" in merged["dbt_select"]
    assert "tag:polygon_settlement" in merged["dbt_exclude"]
    assert "tag:pmxt_order_book" in merged["dbt_exclude"]


def test_match_minute_job_is_closed_untruncated_and_unscheduled():
    config = polymarket_wc2026_match_minute_odds_run_config()["ops"]
    markets = config["polymarket_wc2026_raw_markets"]["config"]
    registry = config["polymarket_wc2026_ops_market_scope_registry"]["config"]
    event_catalog = config["polymarket_wc2026_raw_event_catalog"]["config"]
    minute = config["polymarket_wc2026_raw_match_token_odds_history_minute"]["config"]
    dbt = config["oddsfox_dbt"]["config"]

    assert markets["keyset_closed"] is True
    assert registry["keyset_closed"] is True
    assert event_catalog["keyset_closed"] is True
    assert markets["keyset_volume_min"] == 0.0
    assert registry["keyset_volume_min"] == 0.0
    assert event_catalog["keyset_volume_min"] == 0.0
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
    assert AssetKey(["polymarket", "wc2026", "raw", "event_catalog"]) in selected
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


def test_match_order_book_job_is_isolated_and_unscheduled():
    config = polymarket_wc2026_match_order_book_run_config()["ops"]
    raw = config["polymarket_wc2026_raw_match_order_book_snapshots"]["config"]
    dbt = config["oddsfox_dbt"]["config"]

    assert raw["requests_per_minute"] == 50
    assert raw["monthly_credit_budget"] == 20_000
    assert raw["force"] is False
    assert dbt["dbt_select"] == "+tag:pmxt_order_book"
    assert dbt["dbt_exclude"] == "tag:polygon_settlement"
    assert "polymarket_wc2026_raw_match_trades" not in config

    portrait = polymarket_wc2026_market_portrait_run_config(
        manifest_path="/private/target.yml"
    )["ops"]
    assert (
        portrait["polymarket_wc2026_raw_match_trades"]["config"]["manifest_path"]
        == "/private/target.yml"
    )

    selected = defs.get_job_def(
        "polymarket_wc2026_match_order_book_backfill"
    ).asset_layer.selected_asset_keys
    assert (
        AssetKey(["polymarket", "wc2026", "raw", "match_order_book_snapshots"])
        in selected
    )
    assert AssetKey(["polymarket", "wc2026", "marts", "match_order_book"]) in selected
    assert all(
        schedule.job_name != "polymarket_wc2026_match_order_book_backfill"
        for schedule in defs.schedules
    )

    graph = defs.resolve_asset_graph()
    selected_assets = POLYMARKET_WC2026_MATCH_ORDER_BOOK_DBT_SELECTION.resolve(graph)
    selected_checks = POLYMARKET_WC2026_MATCH_ORDER_BOOK_DBT_SELECTION.resolve_checks(
        graph
    )
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
    assert cfg["history_backfill_days"] == 0
    assert cfg["min_volume"] is None
    assert cfg["max_volume"] is None
    assert cfg["ended_market_grace_days"] is None


def test_hourly_schedule_enabled_by_env(monkeypatch):
    schedules_mod = _reload_schedules_module(monkeypatch, hourly=True)

    assert schedules_mod.polymarket_wc2026_hourly_odds_schedule.default_status == (
        DefaultScheduleStatus.RUNNING
    )


def test_kalshi_hourly_schedule_enabled_by_env(monkeypatch):
    schedules_mod = _reload_schedules_module(monkeypatch, kalshi_hourly=True)

    assert schedules_mod.kalshi_wc2026_hourly_odds_schedule.default_status == (
        DefaultScheduleStatus.RUNNING
    )


def test_wc2026_market_scope_registry_refresh_includes_event_catalog_dependency():
    selected = {
        tuple(key.path)
        for key in defs.resolve_job_def(
            "polymarket_wc2026_market_scope_registry_refresh"
        ).asset_layer.selected_asset_keys
    }

    assert ("polymarket", "wc2026", "raw", "markets") in selected
    assert ("polymarket", "wc2026", "raw", "event_catalog") in selected
    assert ("polymarket", "wc2026", "raw", "event_snapshots") in selected
    assert ("polymarket", "wc2026", "raw", "event_market_memberships") in selected
    assert ("openfootball", "wc2026", "raw", "schedule_fixtures") in selected
    assert ("polymarket", "wc2026", "ops", "market_scope_registry") in selected
    assert ("polymarket", "wc2026", "raw", "reviewed_event_membership") not in selected
    assert (
        "polymarket_wc2026_raw_event_catalog"
        in polymarket_wc2026_full_refresh_events_run_config()["ops"]
    )


def test_logical_atlas_job_has_explicit_producer_assets_and_no_odds_dependency():
    selected = {
        tuple(key.path)
        for key in defs.resolve_job_def(
            "polymarket_wc2026_logical_atlas"
        ).asset_layer.selected_asset_keys
    }
    required = {
        ("polymarket", "wc2026", "raw", "event_snapshots"),
        ("polymarket", "wc2026", "raw", "event_market_memberships"),
        ("polymarket", "wc2026", "intermediate", "event_membership"),
        ("polymarket", "wc2026", "release", "logical_bundle"),
    }
    assert required <= selected
    assert not any("odds" in part for key in selected for part in key)
    assert (
        "polymarket",
        "wc2026",
        "raw",
        "market_metadata_enrichment",
    ) not in selected
    assert not any("order_book" in part for key in selected for part in key)
    assert not any("polygon_settlement" in part for key in selected for part in key)

    config = polymarket_wc2026_logical_atlas_run_config()["ops"]
    assert "polymarket_wc2026_raw_token_odds_history_hourly" not in config
    assert "polymarket_wc2026_raw_market_metadata_enrichment" not in config
    assert config["oddsfox_dbt"]["config"]["dbt_select"] == (
        "+tag:wc2026_logical_atlas"
    )
    assert config["polymarket_wc2026_release_logical_bundle"]["config"] == {
        "output_dir": None
    }


def test_wc2026_full_pipeline_includes_logical_bundle_cutover_path():
    selected = {
        tuple(key.path)
        for key in defs.resolve_job_def(
            "polymarket_wc2026_full_pipeline"
        ).asset_layer.selected_asset_keys
    }
    assert ("polymarket", "wc2026", "release", "logical_bundle") in selected
    assert ("polymarket", "wc2026", "raw", "event_snapshots") in selected
    assert (
        "polymarket",
        "wc2026",
        "ops",
        "market_scope_registry",
    ) in selected
    assert (
        "polymarket",
        "wc2026",
        "raw",
        "market_metadata_enrichment",
    ) in selected
    assert (
        "polymarket",
        "wc2026",
        "raw",
        "token_odds_history_hourly",
    ) in selected

    config = polymarket_wc2026_full_refresh_events_run_config()["ops"]
    assert (
        config["polymarket_wc2026_raw_market_metadata_enrichment"]["config"]["force"]
        is True
    )


def test_scoped_dbt_jobs_select_only_their_expected_scope_assets():
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

    assert kalshi
    assert any(key[:2] == ("international_results", "wc2026") for key in kalshi)
    assert any(key[:2] == ("kalshi", "wc2026") for key in kalshi)
    assert not any(key[:2] == ("polymarket", "wc2026") for key in kalshi)

    assert wc2026
    assert any(key[:2] == ("international_results", "wc2026") for key in wc2026)
    assert any(key[:2] == ("polymarket", "wc2026") for key in wc2026)
    assert not any(key[:2] == ("kalshi", "wc2026") for key in wc2026)
