from __future__ import annotations

import re
from pathlib import Path

import yaml

from oddsfox_pipeline.ingestion.polymarket.dlt_source import polymarket_markets_source
from oddsfox_pipeline.orchestration import assets
from oddsfox_pipeline.orchestration.config import (
    polymarket_wc2026_dbt_build_run_config,
    polymarket_wc2026_full_refresh_events_run_config,
    polymarket_wc2026_hourly_odds_run_config,
)
from oddsfox_pipeline.orchestration.definitions import defs
from oddsfox_pipeline.storage.duckdb.schemas import dbt_schemas
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    POLYMARKET_WC2026_OPS_SCHEMA,
    POLYMARKET_WC2026_RAW_SCHEMA,
)

ROOT = Path(__file__).resolve().parents[1]

EXPECTED_JOB_NAMES = {
    "polymarket_wc2026_market_registry_refresh",
    "polymarket_wc2026_hourly_odds_ingest",
    "polymarket_wc2026_dbt_build",
    "polymarket_wc2026_full_pipeline",
}

EXPECTED_OP_NAMES = {
    "polymarket_wc2026_raw_markets",
    "polymarket_wc2026_raw_markets_snapshot",
    "polymarket_wc2026_ops_market_scope_registry",
    "polymarket_wc2026_raw_market_metadata_backfill",
    "polymarket_wc2026_raw_token_odds_history_hourly",
    "polymarket_wc2026_dbt",
}

EXPECTED_ASSET_KEYS = {
    ("polymarket", "wc2026", "raw", "markets"),
    ("polymarket", "wc2026", "raw", "markets_snapshot"),
    ("polymarket", "wc2026", "ops", "market_scope_registry"),
    ("polymarket", "wc2026", "raw", "market_metadata_backfill"),
    ("polymarket", "wc2026", "raw", "token_odds_history_hourly"),
    ("polymarket", "wc2026", "staging", "markets"),
    ("polymarket", "wc2026", "intermediate", "token_universe"),
    ("polymarket", "wc2026", "marts", "token_hourly_odds"),
    ("polymarket", "wc2026", "observability", "sync_run_observability"),
}

OLD_ACTIVE_PATTERNS = (
    re.compile(r"wc2026_polymarket"),
    re.compile(r"WC2026_POLYMARKET"),
    re.compile(r"(?<!polymarket_)wc2026_market_registry_refresh"),
    re.compile(r"(?<!polymarket_)wc2026_hourly_odds_ingest"),
    re.compile(r"(?<!polymarket_)wc2026_dbt_build"),
    re.compile(r"(?<!polymarket_)wc2026_full_pipeline"),
    re.compile(r"(?<!polymarket_)wc2026_hourly_odds_schedule"),
    re.compile(r"export_wc2026_"),
    re.compile(r"repair_wc2026_"),
    re.compile(r"count_wc2026_"),
    re.compile(r"rebuild_minutely"),
    re.compile(r"minutely_backfill"),
)

EXPECTED_SCRIPT_FILES = {
    "count_polymarket_wc2026_gamma_tag_events.py",
    "export_polymarket_wc2026_hourly_odds.py",
    "export_polymarket_wc2026_knockout_markets.py",
    "repair_polymarket_wc2026_token_sync_ledger.py",
}

OLD_SCRIPT_FILES = {
    "count_wc2026_gamma_tag_events.py",
    "export_wc2026_hourly_odds.py",
    "export_wc2026_knockout_markets.py",
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
        assert job.tags["source"] == "polymarket"
        assert job.tags["scope"] == "wc2026"


def test_public_schedule_is_source_first_and_targets_source_first_job():
    assert {schedule.name for schedule in defs.schedules} == {
        "polymarket_wc2026_hourly_odds_schedule"
    }
    assert defs.schedules[0].job_name == "polymarket_wc2026_hourly_odds_ingest"


def test_dagster_op_names_and_run_config_keys_are_source_first():
    actual_op_names = {
        assets.polymarket_wc2026_raw_markets.op.name,
        assets.polymarket_wc2026_raw_markets_snapshot.op.name,
        assets.polymarket_wc2026_ops_market_scope_registry.op.name,
        assets.polymarket_wc2026_raw_market_metadata_backfill.op.name,
        assets.polymarket_wc2026_raw_token_odds_history_hourly.op.name,
        assets.polymarket_wc2026_dbt.op.name,
    }
    run_config_ops = (
        set(polymarket_wc2026_full_refresh_events_run_config()["ops"])
        | set(polymarket_wc2026_hourly_odds_run_config()["ops"])
        | set(polymarket_wc2026_dbt_build_run_config()["ops"])
    )

    assert actual_op_names == EXPECTED_OP_NAMES
    assert run_config_ops == EXPECTED_OP_NAMES - {"polymarket_wc2026_raw_markets"}


def test_registered_asset_keys_are_hierarchical_source_scope_layer():
    asset_keys = {tuple(key.path) for key in defs.resolve_all_asset_keys()}

    assert EXPECTED_ASSET_KEYS <= asset_keys
    assert all(key[:2] == ("polymarket", "wc2026") for key in asset_keys)
    assert all(len(key) >= 4 for key in asset_keys)
    assert not any("wc2026_polymarket" in part for key in asset_keys for part in key)


def test_dlt_source_name_is_source_first():
    assert polymarket_markets_source().name == "polymarket_wc2026"


def test_dbt_project_uses_source_first_directory_and_schemas():
    assert (ROOT / "dbt" / "models" / "polymarket_wc2026").is_dir()
    assert not (ROOT / "dbt" / "models" / "wc2026_polymarket").exists()

    project = yaml.safe_load((ROOT / "dbt" / "dbt_project.yml").read_text())
    model_cfg = project["models"]["oddsfox"]["polymarket_wc2026"]

    assert model_cfg["staging"]["+schema"] == "polymarket_wc2026_staging"
    assert model_cfg["intermediate"]["+schema"] == "polymarket_wc2026_intermediate"
    assert model_cfg["marts"]["+schema"] == "polymarket_wc2026_marts"
    assert model_cfg["observability"]["+schema"] == "polymarket_wc2026_observability"


def test_dbt_model_filenames_are_source_first_by_layer():
    layer_prefixes = {
        "staging": "stg_polymarket_wc2026_",
        "intermediate": "int_polymarket_wc2026_",
        "marts": "polymarket_wc2026_",
        "observability": "polymarket_wc2026_",
    }
    models_root = ROOT / "dbt" / "models" / "polymarket_wc2026"

    for layer, prefix in layer_prefixes.items():
        for model_path in (models_root / layer).glob("*.sql"):
            assert model_path.stem.startswith(prefix)


def test_storage_schema_constants_are_source_first():
    assert POLYMARKET_WC2026_RAW_SCHEMA == "polymarket_wc2026_raw"
    assert POLYMARKET_WC2026_OPS_SCHEMA == "polymarket_wc2026_ops"
    assert dbt_schemas.DBT_MODELED_SCHEMAS == (
        "polymarket_wc2026_staging",
        "polymarket_wc2026_intermediate",
        "polymarket_wc2026_marts",
        "polymarket_wc2026_observability",
    )


def test_dbt_source_metadata_uses_hierarchical_asset_keys():
    sources = yaml.safe_load(
        (
            ROOT / "dbt" / "models" / "sources" / "polymarket_wc2026_sources.yml"
        ).read_text()
    )["sources"]
    source_asset_keys = {
        tuple(table["meta"]["dagster"]["asset_key"])
        for source in sources
        for table in source["tables"]
    }
    registered_asset_keys = {tuple(key.path) for key in defs.resolve_all_asset_keys()}

    assert source_asset_keys <= registered_asset_keys
    assert all(key[:2] == ("polymarket", "wc2026") for key in source_asset_keys)
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
