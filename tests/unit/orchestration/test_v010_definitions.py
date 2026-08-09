import importlib
from pathlib import Path

import yaml
from dagster import AssetKey, DefaultScheduleStatus

from oddsfox_pipeline.orchestration.config import (
    polymarket_wc2026_event_catalog_recall_audit_run_config,
    polymarket_wc2026_full_pipeline_run_config,
    polymarket_wc2026_full_refresh_events_run_config,
    polymarket_wc2026_hourly_odds_run_config,
    polymarket_wc2026_market_portrait_run_config,
    polymarket_wc2026_match_minute_odds_run_config,
    polymarket_wc2026_match_order_book_run_config,
    polymarket_wc2026_minute_odds_run_config,
    polymarket_wc2026_polygon_settlement_backfill_run_config,
)
from oddsfox_pipeline.orchestration.definitions import defs
from oddsfox_pipeline.orchestration.jobs import (
    POLYMARKET_WC2026_GOLDEN_MART_DBT_SELECTION,
    POLYMARKET_WC2026_MATCH_MINUTE_DBT_SELECTION,
    POLYMARKET_WC2026_MATCH_ORDER_BOOK_DBT_SELECTION,
    POLYMARKET_WC2026_MINUTE_ODDS_DBT_SELECTION,
    POLYMARKET_WC2026_POLYGON_SETTLEMENT_DBT_SELECTION,
    _merge_dbt_build_config,
    _merge_op_config,
    _merge_run_configs,
)
from oddsfox_pipeline.orchestration.shipped_scopes import (
    POLYMARKET_WC2026_GOLDEN_MART_DBT_SELECT,
)


def _polymarket_sources_paths() -> list[Path]:
    sources_dir = Path(__file__).resolve().parents[3] / "dbt" / "models" / "sources"
    return [
        sources_dir / "polymarket_wc2026_sources.yml",
        sources_dir / "international_results_wc2026_sources.yml",
        sources_dir / "kalshi_wc2026_sources.yml",
        sources_dir / "openfootball_wc2026_sources.yml",
    ]


def _reload_schedules_module(
    monkeypatch,
    *,
    kalshi_hourly: bool = False,
):
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
        "polymarket_wc2026_market_scope_registry_refresh",
        "polymarket_wc2026_event_catalog_recall_audit",
        "polymarket_wc2026_match_minute_odds_backfill",
        "polymarket_wc2026_minute_odds_backfill",
        "polymarket_wc2026_minute_odds_live_smoke",
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
        ("polymarket", "wc2026", "staging", "event_snapshots"),
        ("polymarket", "wc2026", "staging", "event_market_snapshots"),
        ("polymarket", "wc2026", "intermediate", "event_latest"),
        ("polymarket", "wc2026", "intermediate", "primary_market_token"),
        ("polymarket", "wc2026", "staging", "odds"),
        ("polymarket", "wc2026", "staging", "odds_daily"),
        ("polymarket", "wc2026", "staging", "ingestion_run_events"),
        ("polymarket", "wc2026", "staging", "sync_ledger"),
        ("polymarket", "wc2026", "staging", "token_sync_skips"),
        ("polymarket", "wc2026", "intermediate", "markets"),
        ("polymarket", "wc2026", "staging", "market_tokens"),
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
        ("polymarket", "wc2026", "marts", "market_hourly_odds"),
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


def test_merge_dbt_build_config_deduplicates_selects_and_excludes():
    merged = _merge_dbt_build_config(
        {
            "dbt_select": "+first shared",
            "dbt_exclude": "tag:one shared",
            "full_refresh": False,
        },
        {
            "dbt_select": "shared +second",
            "dbt_exclude": "shared tag:two",
            "full_refresh": True,
        },
    )

    assert merged == {
        "dbt_select": "+first shared +second",
        "dbt_exclude": "tag:one shared tag:two",
        "full_refresh": True,
    }
    assert _merge_dbt_build_config({"x": 1}, {"y": 2}) == {"x": 1, "y": 2}


def test_merge_op_config_handles_empty_dbt_and_plain_configs():
    incoming = {"config": {"dbt_select": "+model", "enabled": True}}
    assert _merge_op_config(None, incoming) == incoming
    assert _merge_op_config(
        {"config": {"dbt_exclude": "tag:skip", "enabled": False}},
        incoming,
    ) == {
        "config": {
            "dbt_select": "+model",
            "dbt_exclude": "tag:skip",
            "enabled": True,
        }
    }
    assert _merge_op_config(
        {"config": "old", "tags": {"a": "1"}},
        {"config": "new"},
    ) == {"config": "new", "tags": {"a": "1"}}
    assert _merge_op_config(
        {"config": {"enabled": False}},
        {"config": {"enabled": True}},
    ) == {"config": {"enabled": True}}


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
    assert event_catalog["include_slug_prefix_recall"] is False
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


def test_minute_odds_run_config_includes_catalog_when_refresh_enabled(monkeypatch):
    import oddsfox_pipeline.orchestration.config as orch_config

    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG", True
    )
    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH", True
    )
    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES", True
    )
    ops = orch_config.polymarket_wc2026_minute_odds_run_config()["ops"]
    markets = ops["polymarket_wc2026_raw_markets"]["config"]
    registry = ops["polymarket_wc2026_ops_market_scope_registry"]["config"]
    assert markets["keyset_closed"] is None
    assert markets["force_full_discovery"] is True
    assert registry["force_refresh"] is True
    assert registry["apply_event_volume_eligibility_gate"] is True
    assert "polymarket_wc2026_raw_event_catalog" in ops
    assert "polymarket_wc2026_raw_match_token_odds_history_minute" in ops
    assert "polymarket_wc2026_raw_futures_token_odds_history_minute" in ops


def test_minute_odds_run_config_skips_catalog_when_refresh_disabled(monkeypatch):
    import oddsfox_pipeline.orchestration.config as orch_config

    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG", False
    )
    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH", True
    )
    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES", True
    )
    ops = orch_config.polymarket_wc2026_minute_odds_run_config()["ops"]
    assert "polymarket_wc2026_raw_markets" not in ops
    assert "polymarket_wc2026_raw_event_catalog" not in ops
    assert "polymarket_wc2026_ops_market_scope_registry" not in ops
    assert "polymarket_wc2026_raw_market_metadata_enrichment" not in ops
    assert "polymarket_wc2026_raw_match_token_odds_history_minute" in ops
    assert "polymarket_wc2026_raw_futures_token_odds_history_minute" in ops
    assert ops["oddsfox_dbt"]["config"]["dbt_select"] == (
        "+polymarket_wc2026_market_minute_odds_data_quality"
    )


def test_minute_odds_run_config_skips_match_when_refresh_disabled(monkeypatch):
    import oddsfox_pipeline.orchestration.config as orch_config

    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG", False
    )
    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH", False
    )
    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES", True
    )
    ops = orch_config.polymarket_wc2026_minute_odds_run_config()["ops"]
    assert "polymarket_wc2026_raw_match_token_odds_history_minute" not in ops
    assert "polymarket_wc2026_raw_futures_token_odds_history_minute" in ops
    assert "oddsfox_dbt" in ops


def test_minute_odds_run_config_skips_futures_when_refresh_disabled(monkeypatch):
    import oddsfox_pipeline.orchestration.config as orch_config

    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG", False
    )
    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH", True
    )
    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES", False
    )
    ops = orch_config.polymarket_wc2026_minute_odds_run_config()["ops"]
    assert "polymarket_wc2026_raw_match_token_odds_history_minute" in ops
    assert "polymarket_wc2026_raw_futures_token_odds_history_minute" not in ops
    assert "oddsfox_dbt" in ops


def test_minute_odds_run_config_dbt_only_when_both_raw_legs_disabled(monkeypatch):
    import oddsfox_pipeline.orchestration.config as orch_config

    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG", False
    )
    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH", False
    )
    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES", False
    )
    ops = orch_config.polymarket_wc2026_minute_odds_run_config()["ops"]
    assert "polymarket_wc2026_raw_match_token_odds_history_minute" not in ops
    assert "polymarket_wc2026_raw_futures_token_odds_history_minute" not in ops
    assert set(ops) == {"oddsfox_dbt"}
    assert ops["oddsfox_dbt"]["config"]["dbt_select"] == (
        "+polymarket_wc2026_market_minute_odds_data_quality"
    )


def test_minute_odds_smoke_run_config_samples_both_legs_and_caps_futures(monkeypatch):
    import oddsfox_pipeline.orchestration.config as orch_config

    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG", True
    )
    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH", True
    )
    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES", True
    )
    production = orch_config.polymarket_wc2026_minute_odds_run_config()["ops"]
    smoke = orch_config.polymarket_wc2026_minute_odds_smoke_run_config()["ops"]
    match_key = "polymarket_wc2026_raw_match_token_odds_history_minute"
    futures_key = "polymarket_wc2026_raw_futures_token_odds_history_minute"

    assert production[match_key]["config"].get("market_sample_fraction") is None
    assert production[futures_key]["config"].get("market_sample_fraction") is None
    assert production[futures_key]["config"].get("sample_window_hours") is None

    assert smoke[match_key]["config"]["market_sample_fraction"] == 0.05
    assert smoke[match_key]["config"]["market_sample_seed"] == (
        "wc2026-minute-smoke-v1"
    )
    assert smoke[futures_key]["config"]["market_sample_fraction"] == 0.05
    assert smoke[futures_key]["config"]["market_sample_seed"] == (
        "wc2026-minute-smoke-v1"
    )
    assert smoke[futures_key]["config"]["sample_window_hours"] == 24
    assert smoke["oddsfox_dbt"]["config"]["dbt_select"] == (
        "+polymarket_wc2026_market_minute_odds_data_quality"
    )
    catalog = smoke["polymarket_wc2026_raw_event_catalog"]["config"]
    registry = smoke["polymarket_wc2026_ops_market_scope_registry"]["config"]
    assert catalog["include_slug_prefix_recall"] is False
    assert registry["include_slug_prefix_recall"] is False
    # Production match/unified minute backfills also skip exhaustive recall;
    # only the dedicated recall-audit job enables it.
    assert (
        production["polymarket_wc2026_raw_event_catalog"]["config"][
            "include_slug_prefix_recall"
        ]
        is False
    )
    assert (
        polymarket_wc2026_match_minute_odds_run_config()["ops"][
            "polymarket_wc2026_raw_event_catalog"
        ]["config"]["include_slug_prefix_recall"]
        is False
    )
    assert "polymarket_wc2026_minute_odds_live_smoke" in {
        job.name for job in defs.resolve_all_job_defs()
    }


def test_minute_odds_selection_skips_match_inputs_when_refresh_disabled(monkeypatch):
    import oddsfox_pipeline.orchestration.jobs as jobs

    monkeypatch.setattr(
        jobs, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG", False
    )
    monkeypatch.setattr(jobs, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH", False)
    monkeypatch.setattr(jobs, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES", True)
    graph = defs.resolve_asset_graph()
    selected = jobs.build_polymarket_wc2026_minute_odds_selection().resolve(graph)
    assert (
        AssetKey(["polymarket", "wc2026", "raw", "match_token_odds_history_minute"])
        not in selected
    )
    assert (
        AssetKey(["international_results", "wc2026", "raw", "match_results"])
        not in selected
    )
    assert (
        AssetKey(["openfootball", "wc2026", "raw", "schedule_fixtures"])
        not in selected
    )
    assert (
        AssetKey(["polymarket", "wc2026", "raw", "futures_token_odds_history_minute"])
        in selected
    )
    assert (
        AssetKey(
            ["polymarket", "wc2026", "observability", "market_minute_odds_data_quality"]
        )
        in selected
    )


def test_minute_odds_selection_skips_futures_when_refresh_disabled(monkeypatch):
    import oddsfox_pipeline.orchestration.jobs as jobs

    monkeypatch.setattr(
        jobs, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG", False
    )
    monkeypatch.setattr(jobs, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH", True)
    monkeypatch.setattr(jobs, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES", False)
    graph = defs.resolve_asset_graph()
    selected = jobs.build_polymarket_wc2026_minute_odds_selection().resolve(graph)
    assert (
        AssetKey(["polymarket", "wc2026", "raw", "match_token_odds_history_minute"])
        in selected
    )
    assert (
        AssetKey(["polymarket", "wc2026", "raw", "futures_token_odds_history_minute"])
        not in selected
    )


def test_minute_odds_selection_dbt_only_when_both_raw_legs_disabled(monkeypatch):
    import oddsfox_pipeline.orchestration.jobs as jobs

    monkeypatch.setattr(
        jobs, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG", False
    )
    monkeypatch.setattr(jobs, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH", False)
    monkeypatch.setattr(jobs, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES", False)
    graph = defs.resolve_asset_graph()
    selected = jobs.build_polymarket_wc2026_minute_odds_selection().resolve(graph)
    assert (
        AssetKey(["polymarket", "wc2026", "raw", "match_token_odds_history_minute"])
        not in selected
    )
    assert (
        AssetKey(["polymarket", "wc2026", "raw", "futures_token_odds_history_minute"])
        not in selected
    )
    assert AssetKey(["polymarket", "wc2026", "marts", "market_minute_odds"]) in selected
    assert (
        AssetKey(
            ["polymarket", "wc2026", "observability", "market_minute_odds_data_quality"]
        )
        in selected
    )


def test_minute_odds_job_reuses_match_minute_and_adds_futures_leg():
    from oddsfox_pipeline.config.settings import (
        POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG,
        POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES,
        POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH,
    )

    config = polymarket_wc2026_minute_odds_run_config()["ops"]
    dbt = config["oddsfox_dbt"]["config"]

    assert dbt["dbt_select"] == "+polymarket_wc2026_market_minute_odds_data_quality"
    assert dbt["dbt_exclude"] is None
    if POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH:
        assert "polymarket_wc2026_raw_match_token_odds_history_minute" in config
    else:
        assert "polymarket_wc2026_raw_match_token_odds_history_minute" not in config
    if POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES:
        futures = config["polymarket_wc2026_raw_futures_token_odds_history_minute"][
            "config"
        ]
        assert futures["requests_per_second"] == 40
        assert futures["batch_group_size"] == 20
        assert futures["window_hours"] == 24
        assert futures["auto_tune_rps"] is True
        assert futures["auto_tune_max_rps"] == 90
    else:
        assert "polymarket_wc2026_raw_futures_token_odds_history_minute" not in config
    selected = defs.resolve_job_def(
        "polymarket_wc2026_minute_odds_backfill"
    ).asset_layer.selected_asset_keys
    match_key = AssetKey(
        ["polymarket", "wc2026", "raw", "match_token_odds_history_minute"]
    )
    futures_key = AssetKey(
        ["polymarket", "wc2026", "raw", "futures_token_odds_history_minute"]
    )
    assert (match_key in selected) is POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH
    assert (futures_key in selected) is POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES
    assert (
        AssetKey(
            ["polymarket", "wc2026", "observability", "market_minute_odds_data_quality"]
        )
        in selected
    )
    assert AssetKey(["polymarket", "wc2026", "marts", "market_minute_odds"]) in selected
    catalog_key = AssetKey(["polymarket", "wc2026", "raw", "event_catalog"])
    if POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG:
        markets = config["polymarket_wc2026_raw_markets"]["config"]
        registry = config["polymarket_wc2026_ops_market_scope_registry"]["config"]
        assert markets["keyset_closed"] is None
        assert registry["keyset_closed"] is None
        assert registry["apply_event_volume_eligibility_gate"] is True
        assert catalog_key in selected
        assert "polymarket_wc2026_raw_event_catalog" in config
    else:
        assert catalog_key not in selected
        assert "polymarket_wc2026_raw_event_catalog" not in config
    assert all(
        schedule.job_name != "polymarket_wc2026_minute_odds_backfill"
        for schedule in defs.schedules
    )


def test_minute_odds_dbt_selection_does_not_leak_sibling_model_checks():
    graph = defs.resolve_asset_graph()
    selected_assets = POLYMARKET_WC2026_MINUTE_ODDS_DBT_SELECTION.resolve(graph)
    selected_checks = POLYMARKET_WC2026_MINUTE_ODDS_DBT_SELECTION.resolve_checks(graph)

    assert selected_checks
    assert {check.asset_key for check in selected_checks} <= selected_assets
    assert (
        AssetKey(
            ["polymarket", "wc2026", "observability", "market_minute_odds_data_quality"]
        )
        in selected_assets
    )


def test_match_minute_dbt_selection_does_not_leak_sibling_model_checks():
    graph = defs.resolve_asset_graph()
    selected_assets = POLYMARKET_WC2026_MATCH_MINUTE_DBT_SELECTION.resolve(graph)
    selected_checks = POLYMARKET_WC2026_MATCH_MINUTE_DBT_SELECTION.resolve_checks(graph)

    assert selected_checks
    assert {check.asset_key for check in selected_checks} <= selected_assets


def test_golden_mart_dbt_selection_does_not_leak_sibling_model_checks():
    graph = defs.resolve_asset_graph()
    selected_assets = POLYMARKET_WC2026_GOLDEN_MART_DBT_SELECTION.resolve(graph)
    selected_checks = POLYMARKET_WC2026_GOLDEN_MART_DBT_SELECTION.resolve_checks(graph)

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
    assert dbt["dbt_exclude"] == "tag:polygon_settlement tag:match_minute"
    assert "polymarket_wc2026_raw_match_trades" not in config

    portrait = polymarket_wc2026_market_portrait_run_config(
        manifest_path="/private/target.yml"
    )["ops"]
    assert (
        portrait["polymarket_wc2026_raw_match_trades"]["config"]["manifest_path"]
        == "/private/target.yml"
    )
    assert portrait["oddsfox_dbt"]["config"]["dbt_select"] == (
        "+tag:pmxt_order_book +tag:market_portrait"
    )

    selected = defs.get_job_def(
        "polymarket_wc2026_match_order_book_backfill"
    ).asset_layer.selected_asset_keys
    assert (
        AssetKey(["polymarket", "wc2026", "raw", "match_order_book_snapshots"])
        in selected
    )
    assert AssetKey(["polymarket", "wc2026", "marts", "match_order_book"]) in selected
    assert AssetKey(["polymarket", "wc2026", "marts", "match_trades"]) not in selected
    assert (
        AssetKey(
            ["polymarket", "wc2026", "intermediate", "match_trade_publication_gate"]
        )
        not in selected
    )
    assert all(
        schedule.job_name != "polymarket_wc2026_match_order_book_backfill"
        for schedule in defs.schedules
    )


def test_event_catalog_recall_audit_job_is_isolated_and_unscheduled():
    config = polymarket_wc2026_event_catalog_recall_audit_run_config()["ops"]
    catalog_cfg = config["polymarket_wc2026_raw_event_catalog"]["config"]
    assert catalog_cfg["include_slug_prefix_recall"] is True
    assert catalog_cfg["slug_prefix_recall_max_pages_without_progress"] is None

    routine = polymarket_wc2026_full_refresh_events_run_config()["ops"]
    routine_catalog = routine["polymarket_wc2026_raw_event_catalog"]["config"]
    assert routine_catalog["include_slug_prefix_recall"] is False

    match_catalog = polymarket_wc2026_match_minute_odds_run_config()["ops"][
        "polymarket_wc2026_raw_event_catalog"
    ]["config"]
    assert match_catalog["include_slug_prefix_recall"] is False

    selected = defs.resolve_job_def(
        "polymarket_wc2026_event_catalog_recall_audit"
    ).asset_layer.selected_asset_keys
    assert AssetKey(["polymarket", "wc2026", "raw", "event_catalog"]) in selected
    assert (
        AssetKey(["polymarket", "wc2026", "ops", "market_scope_registry"]) in selected
    )
    assert all(
        schedule.job_name != "polymarket_wc2026_event_catalog_recall_audit"
        for schedule in defs.schedules
    )


def test_minute_odds_run_config_skips_slug_prefix_recall(monkeypatch):
    import oddsfox_pipeline.orchestration.config as orch_config

    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG", True
    )
    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH", True
    )
    monkeypatch.setattr(
        orch_config, "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES", True
    )
    ops = orch_config.polymarket_wc2026_minute_odds_run_config()["ops"]
    catalog = ops["polymarket_wc2026_raw_event_catalog"]["config"]
    registry = ops["polymarket_wc2026_ops_market_scope_registry"]["config"]
    assert catalog["include_slug_prefix_recall"] is False
    assert registry["include_slug_prefix_recall"] is False


def test_match_order_book_dbt_selection_does_not_leak_sibling_model_checks():
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


def test_kalshi_events_source_asset_key_points_at_raw_events():
    sources = Path(__file__).resolve().parents[3] / "dbt" / "models" / "sources"
    data = yaml.safe_load((sources / "kalshi_wc2026_sources.yml").read_text())
    tables = {
        table["name"]: tuple(table["meta"]["dagster"]["asset_key"])
        for table in data["sources"][0]["tables"]
    }
    assert tables["events"] == ("kalshi", "wc2026", "raw", "events")
    assert tables["markets"] == ("kalshi", "wc2026", "raw", "markets")

    parents = {
        key.to_user_string()
        for key in defs.resolve_asset_graph()
        .get(AssetKey(["kalshi", "wc2026", "staging", "events"]))
        .parent_keys
    }
    assert "kalshi/wc2026/raw/events" in parents
    assert "kalshi/wc2026/raw/markets" not in parents


def test_hourly_odds_run_config_defaults():
    cfg = polymarket_wc2026_hourly_odds_run_config()["ops"][
        "polymarket_wc2026_raw_token_odds_history_hourly"
    ]["config"]
    assert cfg["fidelity"] == 60
    assert cfg["overlap_minutes"] == 60
    assert cfg["window_hours"] == 720
    assert cfg["history_backfill_days"] == 0
    assert cfg["min_volume"] is None
    assert cfg["max_volume"] is None
    assert cfg["ended_market_grace_days"] is None


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
    assert (
        "polymarket_wc2026_raw_event_catalog"
        in polymarket_wc2026_full_refresh_events_run_config()["ops"]
    )


def test_wc2026_full_pipeline_includes_registry_and_hourly_odds():
    selected = {
        tuple(key.path)
        for key in defs.resolve_job_def(
            "polymarket_wc2026_full_pipeline"
        ).asset_layer.selected_asset_keys
    }
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
    assert ("polymarket", "wc2026", "marts", "market_hourly_odds") in selected
    assert ("international_results", "wc2026", "raw", "match_results") not in selected
    assert ("polymarket", "wc2026", "raw", "match_trades") not in selected
    assert ("polymarket", "wc2026", "marts", "match_trades") not in selected
    assert ("polymarket", "wc2026", "marts", "match_minute_odds") not in selected

    config = polymarket_wc2026_full_refresh_events_run_config()["ops"]
    assert (
        config["polymarket_wc2026_raw_market_metadata_enrichment"]["config"]["force"]
        is True
    )
    full_config = polymarket_wc2026_full_pipeline_run_config()["ops"]["oddsfox_dbt"][
        "config"
    ]
    assert full_config["dbt_select"] == POLYMARKET_WC2026_GOLDEN_MART_DBT_SELECT


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
    assert not any(key[:2] == ("international_results", "wc2026") for key in wc2026)
    assert any(key[:2] == ("polymarket", "wc2026") for key in wc2026)
    assert ("polymarket", "wc2026", "marts", "market_hourly_odds") in wc2026
    assert not any(key[:2] == ("kalshi", "wc2026") for key in wc2026)
