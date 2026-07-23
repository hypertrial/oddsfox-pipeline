from __future__ import annotations

import re
from pathlib import Path

import yaml

from oddsfox_pipeline.ingestion.polymarket.dlt_source import (
    polymarket_wc2026_markets_source,
)
from oddsfox_pipeline.orchestration import assets
from oddsfox_pipeline.orchestration.config import (
    kalshi_wc2026_full_refresh_events_run_config,
    kalshi_wc2026_hourly_odds_run_config,
    polymarket_wc2026_dbt_build_run_config,
    polymarket_wc2026_full_refresh_events_run_config,
    polymarket_wc2026_hourly_odds_run_config,
    polymarket_wc2026_match_minute_odds_run_config,
    polymarket_wc2026_polygon_settlement_backfill_run_config,
    polymarket_wc2026_polygon_settlement_release_run_config,
    wc2026_knockout_match_odds_full_pipeline_run_config,
)
from oddsfox_pipeline.orchestration.definitions import defs
from oddsfox_pipeline.storage.duckdb.schemas import dbt_schemas
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    INTERNATIONAL_RESULTS_WC2026_RAW_SCHEMA,
    OPENFOOTBALL_WC2026_RAW_SCHEMA,
    POLYMARKET_WC2026_OPS_SCHEMA,
    POLYMARKET_WC2026_RAW_SCHEMA,
)

ROOT = Path(__file__).resolve().parents[1]

EXPECTED_JOB_NAMES = {
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
    "polymarket_wc2026_market_registry_refresh",
    "polymarket_wc2026_match_minute_odds_backfill",
    "polymarket_wc2026_polygon_settlement_backfill",
    "polymarket_wc2026_polygon_settlement_release",
    "polymarket_wc2026_hourly_odds_ingest",
    "polymarket_wc2026_dbt_build",
    "polymarket_wc2026_full_pipeline",
    "wc2026_knockout_match_odds_full_pipeline",
}

EXPECTED_OP_NAMES = {
    "international_results_historical_raw_snapshot",
    "international_results_wc2026_raw_match_results",
    "openfootball_wc2026_raw_knockout_fixtures",
    "kalshi_wc2026_raw_markets",
    "kalshi_wc2026_raw_markets_snapshot",
    "kalshi_wc2026_ops_market_scope_registry",
    "kalshi_wc2026_raw_market_candlesticks_hourly",
    "polymarket_us_midterms_2026_raw_markets",
    "polymarket_us_midterms_2026_raw_markets_snapshot",
    "polymarket_us_midterms_2026_ops_market_scope_registry",
    "polymarket_us_midterms_2026_raw_market_metadata_backfill",
    "polymarket_us_midterms_2026_raw_token_odds_history_hourly",
    "polymarket_wc2026_raw_markets",
    "polymarket_wc2026_raw_markets_snapshot",
    "polymarket_wc2026_ops_market_scope_registry",
    "polymarket_wc2026_raw_market_metadata_backfill",
    "polymarket_wc2026_raw_token_odds_history_hourly",
    "polymarket_wc2026_raw_match_token_odds_history_minute",
    "polymarket_wc2026_raw_polygon_settlement_fills",
    "polymarket_wc2026_release_polygon_settlement_odds_bundle",
    "oddsfox_dbt",
}

EXPECTED_ASSET_KEYS = {
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
    ("polymarket", "us_midterms_2026", "raw", "markets"),
    ("polymarket", "us_midterms_2026", "raw", "markets_snapshot"),
    ("polymarket", "us_midterms_2026", "ops", "market_scope_registry"),
    ("polymarket", "us_midterms_2026", "raw", "market_metadata_backfill"),
    ("polymarket", "us_midterms_2026", "raw", "token_odds_history_hourly"),
    ("polymarket", "us_midterms_2026", "staging", "markets"),
    ("polymarket", "us_midterms_2026", "marts", "market_token_hourly_odds"),
    ("polymarket", "us_midterms_2026", "observability", "sync_run_observability"),
    ("polymarket", "wc2026", "raw", "markets"),
    ("polymarket", "wc2026", "raw", "markets_snapshot"),
    ("polymarket", "wc2026", "ops", "market_scope_registry"),
    ("polymarket", "wc2026", "raw", "market_metadata_backfill"),
    ("polymarket", "wc2026", "raw", "token_odds_history_hourly"),
    ("polymarket", "wc2026", "raw", "match_token_odds_history_minute"),
    ("polymarket", "wc2026", "raw", "polygon_settlement_fills"),
    ("polymarket", "wc2026", "release", "polygon_settlement_odds_bundle"),
    ("polymarket", "wc2026", "staging", "markets"),
    ("polymarket", "wc2026", "staging", "match_minute_odds_history"),
    ("polymarket", "wc2026", "intermediate", "token_universe"),
    ("polymarket", "wc2026", "intermediate", "match_advance_tokens"),
    ("polymarket", "wc2026", "intermediate", "match_hourly_odds"),
    ("polymarket", "wc2026", "intermediate", "match_market_universe"),
    ("polymarket", "wc2026", "intermediate", "match_token_minute_odds"),
    ("polymarket", "wc2026", "intermediate", "match_minute_odds_candidate"),
    ("polymarket", "wc2026", "intermediate", "match_minute_publication_gate"),
    ("polymarket", "wc2026", "marts", "knockout_token_hourly_odds"),
    ("polymarket", "wc2026", "marts", "match_minute_odds"),
    ("polymarket", "wc2026", "observability", "sync_run_observability"),
    ("polymarket", "wc2026", "observability", "match_minute_odds_data_quality"),
    ("kalshi", "wc2026", "raw", "events"),
    ("kalshi", "wc2026", "raw", "markets"),
    ("kalshi", "wc2026", "raw", "markets_snapshot"),
    ("kalshi", "wc2026", "ops", "market_scope_registry"),
    ("kalshi", "wc2026", "raw", "market_candlesticks_hourly"),
    ("kalshi", "wc2026", "staging", "markets"),
    ("kalshi", "wc2026", "intermediate", "match_advance_markets"),
    ("kalshi", "wc2026", "intermediate", "match_hourly_odds"),
    ("kalshi", "wc2026", "marts", "stage_markets"),
    ("kalshi", "wc2026", "marts", "group_winner_markets"),
    ("kalshi", "wc2026", "observability", "sync_run_observability"),
}

OLD_ACTIVE_PATTERNS = (
    re.compile(r"wc2026_polymarket"),
    re.compile(r"WC2026_POLYMARKET"),
    re.compile(r"(?<!polymarket_)(?<!kalshi_)wc2026_market_registry_refresh"),
    re.compile(r"(?<!polymarket_)(?<!kalshi_)wc2026_hourly_odds_ingest"),
    re.compile(r"(?<!polymarket_)(?<!kalshi_)wc2026_dbt_build"),
    re.compile(r"(?<!polymarket_)(?<!kalshi_)wc2026_full_pipeline"),
    re.compile(r"(?<!polymarket_)(?<!kalshi_)wc2026_hourly_odds_schedule"),
    re.compile(r"export_wc2026_"),
    re.compile(r"repair_wc2026_"),
    re.compile(r"count_wc2026_"),
    re.compile(r"rebuild_minutely"),
    re.compile(r"minutely_backfill"),
)

EXPECTED_SCRIPT_FILES = {
    "build_polymarket_wc2026_polygon_settlement_release.py",
    "count_polymarket_wc2026_gamma_tag_events.py",
    "export_polymarket_wc2026_knockout_hourly_odds.py",
    "repair_polymarket_wc2026_token_sync_ledger.py",
}

OLD_SCRIPT_FILES = {
    "count_wc2026_gamma_tag_events.py",
    "export_wc2026_hourly_odds.py",
    "export_polymarket_wc2026_hourly_odds.py",
    "export_wc2026_knockout_markets.py",
    "export_polymarket_wc2026_knockout_markets.py",
    "repair_wc2026_polymarket_token_sync_ledger.py",
}

ACTIVE_REFERENCE_PATHS = (
    ROOT / "src",
    ROOT / "dbt" / "dbt_project.yml",
    ROOT / "dbt" / "profiles" / "profiles.yml",
    ROOT / "dbt" / "README.md",
    ROOT / "dbt" / "models",
    ROOT / "dbt" / "tests",
    ROOT / "scripts",
    ROOT / ".env.example",
    ROOT / ".github" / "workflows" / "ci.yml",
    ROOT / "README.md",
    ROOT / "AGENTS.md",
    ROOT / "CONTRIBUTING.md",
    ROOT / "docs",
)


def _text_files(path: Path):
    if path.is_file():
        yield path
        return
    for candidate in path.rglob("*"):
        if "__pycache__" in candidate.parts:
            continue
        if candidate.is_file() and candidate.suffix not in {".png", ".ico", ".pyc"}:
            yield candidate


def test_public_jobs_are_source_first_and_tagged():
    jobs = [job for job in defs.resolve_all_job_defs() if job.name != "__ASSET_JOB"]

    assert {job.name for job in jobs} == EXPECTED_JOB_NAMES
    for job in jobs:
        if job.name.startswith("international_results_"):
            expected_source = "international_results"
        elif job.name.startswith("kalshi_"):
            expected_source = "kalshi"
        elif job.name.startswith("polymarket_"):
            expected_source = "polymarket"
        else:
            expected_source = "cross_domain"
        assert job.tags["source"] == expected_source
        if job.name.startswith("polymarket_us_midterms_2026_"):
            assert job.tags["scope"] == "us_midterms_2026"
        elif job.name == "international_results_historical_ingest":
            assert job.tags["scope"] == "historical"
        else:
            assert job.tags["scope"] == "wc2026"


def test_public_schedule_is_source_first_and_targets_source_first_job():
    assert {schedule.name for schedule in defs.schedules} == {
        "international_results_daily_schedule",
        "kalshi_wc2026_hourly_odds_schedule",
        "polymarket_us_midterms_2026_hourly_odds_schedule",
        "polymarket_wc2026_hourly_odds_schedule",
        "wc2026_knockout_match_odds_hourly_schedule",
    }
    assert {schedule.job_name for schedule in defs.schedules} == {
        "international_results_historical_ingest",
        "kalshi_wc2026_hourly_odds_ingest",
        "polymarket_us_midterms_2026_hourly_odds_ingest",
        "polymarket_wc2026_hourly_odds_ingest",
        "wc2026_knockout_match_odds_full_pipeline",
    }


def test_dagster_op_names_and_run_config_keys_are_source_first():
    actual_op_names = {
        assets.international_results_historical_raw_snapshot.op.name,
        assets.international_results_wc2026_raw_match_results.op.name,
        assets.openfootball_wc2026_raw_knockout_fixtures.op.name,
        assets.kalshi_wc2026_raw_markets.op.name,
        assets.kalshi_wc2026_raw_markets_snapshot.op.name,
        assets.kalshi_wc2026_ops_market_scope_registry.op.name,
        assets.kalshi_wc2026_raw_market_candlesticks_hourly.op.name,
        assets.polymarket_us_midterms_2026_raw_markets.op.name,
        assets.polymarket_us_midterms_2026_raw_markets_snapshot.op.name,
        assets.polymarket_us_midterms_2026_ops_market_scope_registry.op.name,
        assets.polymarket_us_midterms_2026_raw_market_metadata_backfill.op.name,
        assets.polymarket_us_midterms_2026_raw_token_odds_history_hourly.op.name,
        assets.polymarket_wc2026_raw_markets.op.name,
        assets.polymarket_wc2026_raw_markets_snapshot.op.name,
        assets.polymarket_wc2026_ops_market_scope_registry.op.name,
        assets.polymarket_wc2026_raw_market_metadata_backfill.op.name,
        assets.polymarket_wc2026_raw_token_odds_history_hourly.op.name,
        assets.polymarket_wc2026_raw_match_token_odds_history_minute.op.name,
        assets.polymarket_wc2026_raw_polygon_settlement_fills.op.name,
        assets.polymarket_wc2026_release_polygon_settlement_odds_bundle.op.name,
        assets.oddsfox_dbt.op.name,
    }
    run_config_ops = (
        set(polymarket_wc2026_full_refresh_events_run_config()["ops"])
        | set(polymarket_wc2026_hourly_odds_run_config()["ops"])
        | set(polymarket_wc2026_match_minute_odds_run_config()["ops"])
        | set(polymarket_wc2026_polygon_settlement_backfill_run_config()["ops"])
        | set(
            polymarket_wc2026_polygon_settlement_release_run_config(
                dataset_version="1.0.0",
                publisher_name="test",
            )["ops"]
        )
        | set(polymarket_wc2026_dbt_build_run_config()["ops"])
        | set(kalshi_wc2026_full_refresh_events_run_config()["ops"])
        | set(kalshi_wc2026_hourly_odds_run_config()["ops"])
        | set(wc2026_knockout_match_odds_full_pipeline_run_config()["ops"])
    )

    assert actual_op_names == EXPECTED_OP_NAMES
    assert run_config_ops == EXPECTED_OP_NAMES - {
        "international_results_historical_raw_snapshot",
        "international_results_wc2026_raw_match_results",
        "openfootball_wc2026_raw_knockout_fixtures",
        "kalshi_wc2026_raw_markets_snapshot",
        "polymarket_us_midterms_2026_raw_markets",
        "polymarket_us_midterms_2026_raw_markets_snapshot",
        "polymarket_us_midterms_2026_ops_market_scope_registry",
        "polymarket_us_midterms_2026_raw_market_metadata_backfill",
        "polymarket_us_midterms_2026_raw_token_odds_history_hourly",
        "polymarket_wc2026_raw_markets_snapshot",
    }


def test_registered_asset_keys_are_hierarchical_source_scope_layer():
    asset_keys = {tuple(key.path) for key in defs.resolve_all_asset_keys()}

    assert EXPECTED_ASSET_KEYS <= asset_keys
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
    assert all(len(key) >= 3 for key in asset_keys)
    assert not any("wc2026_polymarket" in part for key in asset_keys for part in key)


def test_dlt_source_name_is_source_first():
    assert polymarket_wc2026_markets_source().name == "polymarket_wc2026"


def test_dbt_project_uses_source_first_directory_and_schemas():
    assert (ROOT / "dbt" / "models" / "polymarket_wc2026").is_dir()
    assert (ROOT / "dbt" / "models" / "polymarket_us_midterms_2026").is_dir()
    assert (ROOT / "dbt" / "models" / "international_results_wc2026").is_dir()
    assert (ROOT / "dbt" / "models" / "kalshi_wc2026").is_dir()
    assert (ROOT / "dbt" / "models" / "openfootball_wc2026").is_dir()
    assert (ROOT / "dbt" / "models" / "wc2026").is_dir()
    assert not (ROOT / "dbt" / "models" / "wc2026_polymarket").exists()

    project = yaml.safe_load((ROOT / "dbt" / "dbt_project.yml").read_text())
    model_cfg = project["models"]["oddsfox"]["polymarket_wc2026"]
    kalshi_cfg = project["models"]["oddsfox"]["kalshi_wc2026"]
    midterms_cfg = project["models"]["oddsfox"]["polymarket_us_midterms_2026"]
    results_cfg = project["models"]["oddsfox"]["international_results_wc2026"]
    openfootball_cfg = project["models"]["oddsfox"]["openfootball_wc2026"]
    wc2026_cfg = project["models"]["oddsfox"]["wc2026"]

    assert model_cfg["staging"]["+schema"] == "polymarket_wc2026_staging"
    assert model_cfg["intermediate"]["+schema"] == "polymarket_wc2026_intermediate"
    assert model_cfg["marts"]["+schema"] == "polymarket_wc2026_marts"
    assert model_cfg["observability"]["+schema"] == "polymarket_wc2026_observability"
    assert kalshi_cfg["staging"]["+schema"] == "kalshi_wc2026_staging"
    assert kalshi_cfg["intermediate"]["+schema"] == "kalshi_wc2026_intermediate"
    assert kalshi_cfg["marts"]["+schema"] == "kalshi_wc2026_marts"
    assert kalshi_cfg["observability"]["+schema"] == "kalshi_wc2026_observability"
    assert midterms_cfg["staging"]["+schema"] == "polymarket_us_midterms_2026_staging"
    assert (
        midterms_cfg["intermediate"]["+schema"]
        == "polymarket_us_midterms_2026_intermediate"
    )
    assert midterms_cfg["marts"]["+schema"] == "polymarket_us_midterms_2026_marts"
    assert (
        midterms_cfg["observability"]["+schema"]
        == "polymarket_us_midterms_2026_observability"
    )
    assert results_cfg["staging"]["+schema"] == "international_results_wc2026_staging"
    assert (
        results_cfg["intermediate"]["+schema"]
        == "international_results_wc2026_intermediate"
    )
    assert results_cfg["marts"]["+schema"] == "international_results_wc2026_marts"
    assert (
        results_cfg["observability"]["+schema"]
        == "international_results_wc2026_observability"
    )
    assert openfootball_cfg["staging"]["+schema"] == "openfootball_wc2026_staging"
    assert wc2026_cfg["intermediate"]["+schema"] == "wc2026_intermediate"
    assert wc2026_cfg["marts"]["+schema"] == "wc2026_marts"
    assert wc2026_cfg["observability"]["+schema"] == "wc2026_observability"


def test_dbt_model_filenames_are_source_first_by_layer():
    layer_prefixes = {
        "polymarket_wc2026/staging": "stg_polymarket_wc2026_",
        "polymarket_wc2026/intermediate": "int_polymarket_wc2026_",
        "polymarket_wc2026/marts": "polymarket_wc2026_",
        "polymarket_wc2026/observability": "polymarket_wc2026_",
        "polymarket_us_midterms_2026/staging": "stg_polymarket_us_midterms_2026_",
        "polymarket_us_midterms_2026/intermediate": "int_polymarket_us_midterms_2026_",
        "polymarket_us_midterms_2026/marts": "polymarket_us_midterms_2026_",
        "polymarket_us_midterms_2026/observability": "polymarket_us_midterms_2026_",
        "international_results_wc2026/staging": "stg_international_results_wc2026_",
        "international_results_wc2026/intermediate": "int_international_results_wc2026_",
        "international_results_wc2026/marts": "international_results_wc2026_",
        "international_results_wc2026/observability": "international_results_wc2026_",
        "kalshi_wc2026/staging": "stg_kalshi_wc2026_",
        "kalshi_wc2026/intermediate": "int_kalshi_wc2026_",
        "kalshi_wc2026/marts": "kalshi_wc2026_",
        "kalshi_wc2026/observability": "kalshi_wc2026_",
        "openfootball_wc2026/staging": "stg_openfootball_wc2026_",
        "wc2026/intermediate": "int_wc2026_",
        "wc2026/marts": "wc2026_",
        "wc2026/observability": "wc2026_",
    }

    for path, prefix in layer_prefixes.items():
        for model_path in (ROOT / "dbt" / "models" / path).glob("*.sql"):
            assert model_path.stem.startswith(prefix)


def test_storage_schema_constants_are_source_first():
    from oddsfox_pipeline.storage.duckdb.schemas.constants import (
        POLYMARKET_US_MIDTERMS_2026_OPS_SCHEMA,
        POLYMARKET_US_MIDTERMS_2026_RAW_SCHEMA,
        polymarket_us_midterms_2026_ops_tbl,
        polymarket_us_midterms_2026_raw_tbl,
    )

    assert POLYMARKET_WC2026_RAW_SCHEMA == "polymarket_wc2026_raw"
    assert POLYMARKET_WC2026_OPS_SCHEMA == "polymarket_wc2026_ops"
    assert POLYMARKET_US_MIDTERMS_2026_RAW_SCHEMA == "polymarket_us_midterms_2026_raw"
    assert POLYMARKET_US_MIDTERMS_2026_OPS_SCHEMA == "polymarket_us_midterms_2026_ops"
    assert polymarket_us_midterms_2026_raw_tbl("markets").endswith(
        '"polymarket_us_midterms_2026_raw"."markets"'
    )
    assert polymarket_us_midterms_2026_ops_tbl("token_sync_ledger").endswith(
        '"polymarket_us_midterms_2026_ops"."token_sync_ledger"'
    )
    assert INTERNATIONAL_RESULTS_WC2026_RAW_SCHEMA == "international_results_wc2026_raw"
    assert OPENFOOTBALL_WC2026_RAW_SCHEMA == "openfootball_wc2026_raw"
    assert dbt_schemas.DBT_MODELED_SCHEMAS == (
        "international_results_wc2026_staging",
        "international_results_wc2026_intermediate",
        "international_results_wc2026_marts",
        "international_results_wc2026_observability",
        "openfootball_wc2026_staging",
        "wc2026_staging",
        "wc2026_intermediate",
        "wc2026_marts",
        "wc2026_observability",
        "polymarket_wc2026_staging",
        "polymarket_wc2026_intermediate",
        "polymarket_wc2026_marts",
        "polymarket_wc2026_observability",
        "kalshi_wc2026_staging",
        "kalshi_wc2026_intermediate",
        "kalshi_wc2026_marts",
        "kalshi_wc2026_observability",
        "polymarket_us_midterms_2026_staging",
        "polymarket_us_midterms_2026_intermediate",
        "polymarket_us_midterms_2026_marts",
        "polymarket_us_midterms_2026_observability",
    )


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
                / "polymarket_us_midterms_2026_sources.yml"
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
    )
    source_asset_keys = {
        tuple(table["meta"]["dagster"]["asset_key"])
        for source in sources
        for table in source["tables"]
    }
    registered_asset_keys = {tuple(key.path) for key in defs.resolve_all_asset_keys()}

    assert source_asset_keys <= registered_asset_keys
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
        for key in source_asset_keys
    )
    assert all(len(key) >= 4 for key in source_asset_keys)


def test_source_specific_script_filenames_are_source_first():
    script_names = {path.name for path in (ROOT / "scripts").glob("*.py")}

    assert EXPECTED_SCRIPT_FILES <= script_names
    assert OLD_SCRIPT_FILES.isdisjoint(script_names)


def test_active_surfaces_do_not_reference_old_namespace():
    offenders: list[str] = []
    for base_path in ACTIVE_REFERENCE_PATHS:
        for path in _text_files(base_path):
            text = path.read_text(errors="ignore")
            for pattern in OLD_ACTIVE_PATTERNS:
                if pattern.search(text):
                    offenders.append(f"{path.relative_to(ROOT)}: {pattern.pattern}")

    assert offenders == []


def test_changelog_old_namespace_reference_is_only_breaking_reset_note():
    text = (ROOT / "CHANGELOG.md").read_text()
    old_namespace_lines = [
        line.strip()
        for line in text.splitlines()
        if "wc2026_polymarket" in line or "WC2026_POLYMARKET" in line
    ]

    assert old_namespace_lines == [
        "`polymarket_wc2026` instead of `wc2026_polymarket`. Dagster asset keys are"
    ]
