from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from oddsfox_pipeline.config.settings_polymarket import (
    POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES,
)
from oddsfox_pipeline.ingestion.polymarket.dlt_source import (
    polymarket_wc2026_markets_source,
)
from oddsfox_pipeline.naming import (
    SCOPE_SOCCER,
    SCOPE_WC2026,
    SOURCE_INTERNATIONAL_RESULTS,
    SOURCE_KALSHI,
    SOURCE_OPENFOOTBALL,
    SOURCE_POLYMARKET,
    flat_name,
    schema_name,
)
from oddsfox_pipeline.orchestration import assets
from oddsfox_pipeline.orchestration.config import (
    kalshi_wc2026_full_refresh_events_run_config,
    kalshi_wc2026_hourly_odds_run_config,
    polymarket_soccer_full_pipeline_run_config,
    polymarket_wc2026_dbt_build_run_config,
    polymarket_wc2026_full_refresh_events_run_config,
    polymarket_wc2026_hourly_odds_run_config,
    polymarket_wc2026_market_portrait_run_config,
    polymarket_wc2026_match_minute_odds_run_config,
    polymarket_wc2026_match_order_book_run_config,
    polymarket_wc2026_minute_odds_run_config,
    polymarket_wc2026_polygon_settlement_backfill_run_config,
    polymarket_wc2026_polygon_settlement_release_run_config,
)
from oddsfox_pipeline.orchestration.definitions import defs
from oddsfox_pipeline.orchestration.shipped_scopes import SHIPPED_SCOPE_SPECS
from oddsfox_pipeline.storage.duckdb.schemas import dbt_schemas
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    INTERNATIONAL_RESULTS_WC2026_RAW_SCHEMA,
    OPENFOOTBALL_WC2026_RAW_SCHEMA,
    POLYMARKET_SOCCER_OPS_SCHEMA,
    POLYMARKET_SOCCER_RAW_SCHEMA,
    POLYMARKET_WC2026_OPS_SCHEMA,
    POLYMARKET_WC2026_RAW_SCHEMA,
)
from tests.support.terminology_policy import load_policy

pytestmark = pytest.mark.repo_check

ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT = yaml.safe_load((ROOT / "dbt" / "dbt_project.yml").read_text())
DBT_MODEL_FAMILIES = DBT_PROJECT["models"]["oddsfox"]

# Flat op names are still enumerated: Dagster ops are not derivable from
# SHIPPED_SCOPE_SPECS the way jobs are.
EXPECTED_OP_NAMES = {
    "international_results_historical_raw_snapshot",
    "international_results_wc2026_raw_match_results",
    "openfootball_wc2026_raw_schedule_fixtures",
    "kalshi_wc2026_raw_markets",
    "kalshi_wc2026_raw_markets_snapshot",
    "kalshi_wc2026_ops_market_scope_registry",
    "kalshi_wc2026_raw_market_candlesticks_hourly",
    "polymarket_wc2026_raw_markets",
    "polymarket_wc2026_raw_markets_snapshot",
    "polymarket_wc2026_raw_event_catalog",
    "polymarket_wc2026_ops_market_scope_registry",
    "polymarket_wc2026_raw_market_metadata_enrichment",
    "polymarket_wc2026_raw_token_odds_history_hourly",
    "polymarket_wc2026_raw_match_token_odds_history_minute",
    "polymarket_wc2026_raw_futures_token_odds_history_minute",
    "polymarket_wc2026_raw_match_order_book_snapshots",
    "polymarket_wc2026_raw_match_trades",
    "polymarket_wc2026_raw_polygon_settlement_fills",
    "polymarket_wc2026_release_polygon_settlement_odds_bundle",
    "polymarket_soccer_raw_event_catalog",
    "polymarket_soccer_ops_pipeline_preflight",
    "polymarket_soccer_ops_match_result_registry",
    "polymarket_soccer_raw_match_result_token_odds_history_minute",
    "polymarket_soccer_monitoring_dbt",
    "oddsfox_dbt",
}

EXPECTED_SCRIPT_FILES = {
    "build_polymarket_wc2026_polygon_settlement_release.py",
    "cleanup_polymarket_wc2026_registry_hygiene.py",
    "count_polymarket_wc2026_gamma_tag_events.py",
    "export_polymarket_wc2026_market_hourly_odds.py",
    "repair_polymarket_wc2026_token_sync_ledger.py",
    "sync_polymarket_markets_catalog.py",
}

# Build inverted tokens without embedding the contiguous retired literal.
_INVERTED_NS = f"{SCOPE_WC2026}_{SOURCE_POLYMARKET}"
_INVERTED_NS_UPPER = _INVERTED_NS.upper()

OLD_SCRIPT_FILES = {
    "count_wc2026_gamma_tag_events.py",
    "export_wc2026_hourly_odds.py",
    "export_polymarket_wc2026_hourly_odds.py",
    "export_wc2026_knockout_markets.py",
    "export_polymarket_wc2026_knockout_markets.py",
    f"repair_{_INVERTED_NS}_token_sync_ledger.py",
}

_ALLOWED_ASSET_ROOTS = frozenset(
    {
        (SOURCE_POLYMARKET, SCOPE_WC2026),
        (SOURCE_POLYMARKET, SCOPE_SOCCER),
        (SOURCE_POLYMARKET, "catalog"),
        (SOURCE_INTERNATIONAL_RESULTS, "historical"),
        (SOURCE_INTERNATIONAL_RESULTS, SCOPE_WC2026),
        (SOURCE_OPENFOOTBALL, SCOPE_WC2026),
        (SOURCE_KALSHI, SCOPE_WC2026),
    }
)

_SOURCE_FIRST_OP_PREFIXES = (
    f"{SOURCE_INTERNATIONAL_RESULTS}_",
    f"{SOURCE_OPENFOOTBALL}_",
    f"{SOURCE_KALSHI}_",
    f"{SOURCE_POLYMARKET}_",
    "oddsfox_dbt",
)


def _expected_job_names() -> set[str]:
    shipped = {
        name
        for spec in SHIPPED_SCOPE_SPECS
        for name in (
            spec.registry_job_name,
            spec.odds_job_name,
            spec.dbt_job_name,
            spec.full_job_name,
        )
    }
    return shipped | set(load_policy().extension_jobs)


def _job_expected_source(job_name: str) -> str:
    if job_name.startswith(f"{SOURCE_INTERNATIONAL_RESULTS}_"):
        return SOURCE_INTERNATIONAL_RESULTS
    if job_name.startswith(f"{SOURCE_KALSHI}_"):
        return SOURCE_KALSHI
    if job_name.startswith(f"{SOURCE_POLYMARKET}_"):
        return SOURCE_POLYMARKET
    raise AssertionError(f"unexpected job name: {job_name}")


def _job_expected_scope(job_name: str) -> str:
    if job_name == "international_results_historical_ingest":
        return "historical"
    if job_name.startswith("polymarket_soccer_"):
        return SCOPE_SOCCER
    return SCOPE_WC2026


def _dbt_model_families() -> dict[str, dict]:
    return {
        name: cfg
        for name, cfg in DBT_MODEL_FAMILIES.items()
        if isinstance(cfg, dict) and not str(name).startswith("+")
    }


def _layer_filename_prefix(family: str, layer: str) -> str:
    if layer == "staging":
        return f"stg_{family}_"
    if layer == "intermediate":
        return f"int_{family}_"
    return f"{family}_"


def _dbt_layer_dirs() -> dict[str, str]:
    """Map relative model dir → expected filename prefix from dbt_project.yml."""
    layers = ("staging", "intermediate", "marts", "observability")
    mapping: dict[str, str] = {}
    for family, cfg in _dbt_model_families().items():
        for layer in layers:
            layer_cfg = cfg.get(layer)
            if not isinstance(layer_cfg, dict):
                continue
            if "+schema" not in layer_cfg:
                continue
            mapping[f"{family}/{layer}"] = _layer_filename_prefix(family, layer)
    return mapping


def test_public_jobs_are_source_first_and_tagged():
    jobs = [job for job in defs.resolve_all_job_defs() if job.name != "__ASSET_JOB"]

    assert {job.name for job in jobs} == _expected_job_names()
    for job in jobs:
        assert job.tags["source"] == _job_expected_source(job.name)
        assert job.tags["scope"] == _job_expected_scope(job.name)


def test_public_schedule_is_source_first_and_targets_source_first_job():
    assert {schedule.name for schedule in defs.schedules} == {
        "international_results_daily_schedule",
        "kalshi_wc2026_hourly_odds_schedule",
        "polymarket_soccer_daily_schedule",
    }
    assert {schedule.job_name for schedule in defs.schedules} == {
        "international_results_historical_ingest",
        "kalshi_wc2026_hourly_odds_ingest",
        "polymarket_soccer_full_pipeline",
    }


def test_dagster_op_names_and_run_config_keys_are_source_first():
    actual_op_names = {
        assets.international_results_historical_raw_snapshot.op.name,
        assets.international_results_wc2026_raw_match_results.op.name,
        assets.openfootball_wc2026_raw_schedule_fixtures.op.name,
        assets.kalshi_wc2026_raw_markets.op.name,
        assets.kalshi_wc2026_raw_markets_snapshot.op.name,
        assets.kalshi_wc2026_ops_market_scope_registry.op.name,
        assets.kalshi_wc2026_raw_market_candlesticks_hourly.op.name,
        assets.polymarket_wc2026_raw_markets.op.name,
        assets.polymarket_wc2026_raw_markets_snapshot.op.name,
        assets.polymarket_wc2026_raw_event_catalog.op.name,
        assets.polymarket_wc2026_ops_market_scope_registry.op.name,
        assets.polymarket_wc2026_raw_market_metadata_enrichment.op.name,
        assets.polymarket_wc2026_raw_token_odds_history_hourly.op.name,
        assets.polymarket_wc2026_raw_match_token_odds_history_minute.op.name,
        assets.polymarket_wc2026_raw_futures_token_odds_history_minute.op.name,
        assets.polymarket_wc2026_raw_match_order_book_snapshots.op.name,
        assets.polymarket_wc2026_raw_match_trades.op.name,
        assets.polymarket_wc2026_raw_polygon_settlement_fills.op.name,
        assets.polymarket_wc2026_release_polygon_settlement_odds_bundle.op.name,
        assets.polymarket_soccer_raw_event_catalog.op.name,
        assets.polymarket_soccer_ops_pipeline_preflight.op.name,
        assets.polymarket_soccer_ops_match_result_registry.op.name,
        assets.polymarket_soccer_raw_match_result_token_odds_history_minute.op.name,
        assets.polymarket_soccer_monitoring_dbt.op.name,
        assets.oddsfox_dbt.op.name,
    }
    run_config_ops = (
        set(polymarket_wc2026_full_refresh_events_run_config()["ops"])
        | set(polymarket_wc2026_hourly_odds_run_config()["ops"])
        | set(polymarket_wc2026_match_minute_odds_run_config()["ops"])
        | set(polymarket_wc2026_minute_odds_run_config()["ops"])
        | set(polymarket_wc2026_match_order_book_run_config()["ops"])
        | set(polymarket_wc2026_market_portrait_run_config()["ops"])
        | set(polymarket_wc2026_polygon_settlement_backfill_run_config()["ops"])
        | set(
            polymarket_wc2026_polygon_settlement_release_run_config(
                dataset_version="1.0.0",
            )["ops"]
        )
        | set(polymarket_wc2026_dbt_build_run_config()["ops"])
        | set(kalshi_wc2026_full_refresh_events_run_config()["ops"])
        | set(kalshi_wc2026_hourly_odds_run_config()["ops"])
        | set(polymarket_soccer_full_pipeline_run_config()["ops"])
    )

    assert actual_op_names == EXPECTED_OP_NAMES
    assert all(name.startswith(_SOURCE_FIRST_OP_PREFIXES) for name in actual_op_names)
    expected_run_config_ops = EXPECTED_OP_NAMES - {
        "international_results_historical_raw_snapshot",
        "international_results_wc2026_raw_match_results",
        "openfootball_wc2026_raw_schedule_fixtures",
        "kalshi_wc2026_raw_markets_snapshot",
        "polymarket_wc2026_raw_markets_snapshot",
        "polymarket_soccer_ops_pipeline_preflight",
        "polymarket_soccer_ops_match_result_registry",
    }
    if not POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES:
        expected_run_config_ops.remove(
            "polymarket_wc2026_raw_futures_token_odds_history_minute"
        )
    assert run_config_ops == expected_run_config_ops


def test_registered_asset_keys_are_hierarchical_source_scope_layer():
    asset_keys = {tuple(key.path) for key in defs.resolve_all_asset_keys()}
    policy = load_policy()

    assert policy.critical_asset_keys <= asset_keys
    shipped_asset_keys = {
        key for key in asset_keys if "us_midterms_2026" not in "/".join(key)
    }
    assert all(
        key[:2] in _ALLOWED_ASSET_ROOTS or key[0] == SCOPE_WC2026
        for key in shipped_asset_keys
    )
    assert all(len(key) >= 3 for key in shipped_asset_keys)
    assert not any(_INVERTED_NS in part for key in asset_keys for part in key)


def test_dlt_source_name_is_source_first():
    assert polymarket_wc2026_markets_source().name == flat_name(
        SOURCE_POLYMARKET, SCOPE_WC2026
    )


def test_dbt_project_uses_source_first_directory_and_schemas():
    families = _dbt_model_families()
    for family in families:
        assert (ROOT / "dbt" / "models" / family).is_dir(), family
    assert not (ROOT / "dbt" / "models" / _INVERTED_NS).exists()

    naming_families = {
        flat_name(SOURCE_POLYMARKET, SCOPE_WC2026): (
            SOURCE_POLYMARKET,
            SCOPE_WC2026,
        ),
        flat_name(SOURCE_KALSHI, SCOPE_WC2026): (SOURCE_KALSHI, SCOPE_WC2026),
        flat_name(SOURCE_INTERNATIONAL_RESULTS, SCOPE_WC2026): (
            SOURCE_INTERNATIONAL_RESULTS,
            SCOPE_WC2026,
        ),
    }
    for family, cfg in families.items():
        for layer, layer_cfg in cfg.items():
            if not isinstance(layer_cfg, dict) or "+schema" not in layer_cfg:
                continue
            if family in naming_families:
                source, scope = naming_families[family]
                expected = schema_name(source, scope, layer)
            else:
                expected = f"{family}_{layer}"
            assert layer_cfg["+schema"] == expected, f"{family}/{layer}"


def test_dbt_model_filenames_are_source_first_by_layer():
    for path, prefix in _dbt_layer_dirs().items():
        for model_path in (ROOT / "dbt" / "models" / path).glob("*.sql"):
            assert model_path.stem.startswith(prefix), model_path.name


def test_storage_schema_constants_are_source_first():
    assert POLYMARKET_WC2026_RAW_SCHEMA == schema_name(
        SOURCE_POLYMARKET, SCOPE_WC2026, "raw"
    )
    assert POLYMARKET_WC2026_OPS_SCHEMA == schema_name(
        SOURCE_POLYMARKET, SCOPE_WC2026, "ops"
    )
    assert POLYMARKET_SOCCER_RAW_SCHEMA == schema_name(
        SOURCE_POLYMARKET, SCOPE_SOCCER, "raw"
    )
    assert POLYMARKET_SOCCER_OPS_SCHEMA == schema_name(
        SOURCE_POLYMARKET, SCOPE_SOCCER, "ops"
    )
    assert INTERNATIONAL_RESULTS_WC2026_RAW_SCHEMA == schema_name(
        SOURCE_INTERNATIONAL_RESULTS, SCOPE_WC2026, "raw"
    )
    assert OPENFOOTBALL_WC2026_RAW_SCHEMA == schema_name(
        SOURCE_OPENFOOTBALL, SCOPE_WC2026, "raw"
    )
    project_schemas = {
        layer_cfg["+schema"]
        for cfg in _dbt_model_families().values()
        for layer_cfg in cfg.values()
        if isinstance(layer_cfg, dict) and "+schema" in layer_cfg
    }
    assert project_schemas <= set(dbt_schemas.DBT_MODELED_SCHEMAS)
    assert "wc2026_staging" in dbt_schemas.DBT_MODELED_SCHEMAS


def test_dbt_source_metadata_uses_hierarchical_asset_keys():
    sources = (
        yaml.safe_load(
            (
                ROOT / "dbt" / "models" / "sources" / "polymarket_wc2026_sources.yml"
            ).read_text()
        )["sources"]
        + yaml.safe_load(
            (
                ROOT
                / "dbt"
                / "models"
                / "sources"
                / "international_results_wc2026_sources.yml"
            ).read_text()
        )["sources"]
        + yaml.safe_load(
            (
                ROOT / "dbt" / "models" / "sources" / "kalshi_wc2026_sources.yml"
            ).read_text()
        )["sources"]
        + yaml.safe_load(
            (
                ROOT / "dbt" / "models" / "sources" / "openfootball_wc2026_sources.yml"
            ).read_text()
        )["sources"]
        + yaml.safe_load(
            (
                ROOT / "dbt" / "models" / "sources" / "polymarket_soccer_sources.yml"
            ).read_text()
        )["sources"]
    )
    source_asset_keys = {
        tuple(table["meta"]["dagster"]["asset_key"])
        for source in sources
        for table in source["tables"]
    }
    registered_asset_keys = {tuple(key.path) for key in defs.resolve_all_asset_keys()}

    assert source_asset_keys <= registered_asset_keys
    assert all(
        key[:2] in _ALLOWED_ASSET_ROOTS - {(SOURCE_POLYMARKET, "catalog")}
        for key in source_asset_keys
    )
    assert all(len(key) >= 4 for key in source_asset_keys)


def test_source_specific_script_filenames_are_source_first():
    script_names = {path.name for path in (ROOT / "scripts").glob("*.py")}

    assert EXPECTED_SCRIPT_FILES <= script_names
    assert OLD_SCRIPT_FILES.isdisjoint(script_names)


def test_changelog_old_namespace_reference_is_only_breaking_reset_note():
    text = (ROOT / "CHANGELOG.md").read_text()
    old_namespace_lines = [
        line.strip()
        for line in text.splitlines()
        if _INVERTED_NS in line or _INVERTED_NS_UPPER in line
    ]

    assert old_namespace_lines == [
        (
            f"`{flat_name(SOURCE_POLYMARKET, SCOPE_WC2026)}` instead of "
            f"`{_INVERTED_NS}`. Dagster asset keys are"
        )
    ]
