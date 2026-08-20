import re
from pathlib import Path

import pytest
import yaml
from scripts.seed_dbt_source_freshness import FRESHNESS_SOURCE_TABLES

from oddsfox_pipeline.orchestration.shipped_scopes import SHIPPED_SCOPE_SPECS

pytestmark = pytest.mark.repo_check


def test_sqlfluff_dbt_templating_fails_closed():
    pyproject = (Path(__file__).resolve().parents[3] / "pyproject.toml").read_text()
    dbt_config = pyproject.split("[tool.sqlfluff.templater.dbt]", 1)[1].split("\n[", 1)[
        0
    ]

    assert "dbt_skip_compilation_error = false" in dbt_config


def test_dbt_project_contains_only_shipped_model_families():
    dbt_root = Path(__file__).resolve().parents[3] / "dbt"

    assert (dbt_root / "dbt_project.yml").exists()
    assert (dbt_root / "profiles" / "profiles.yml").exists()
    assert (dbt_root / "models" / "sources" / "polymarket_wc2026_sources.yml").exists()
    assert (dbt_root / "models" / "sources" / "oddsfox_reference_sources.yml").exists()
    assert (
        dbt_root / "models" / "polymarket_wc2026" / "staging" / "staging.yml"
    ).exists()
    assert (
        dbt_root / "models" / "polymarket_wc2026" / "marts" / "polymarket_wc2026.yml"
    ).exists()
    assert (
        dbt_root / "models" / "kalshi_wc2026" / "intermediate" / "intermediate.yml"
    ).exists()
    assert (
        dbt_root / "models" / "kalshi_wc2026" / "marts" / "kalshi_wc2026.yml"
    ).exists()
    assert (dbt_root / "models" / "reference").is_dir()
    assert (dbt_root / "seeds" / "polymarket_wc2026_pipeline_policy.csv").exists()
    assert (dbt_root / "seeds" / "schema.yml").exists()
    assert (
        dbt_root
        / "models"
        / "polymarket_wc2026"
        / "observability"
        / "observability.yml"
    ).exists()

    model_dirs = {p.name for p in (dbt_root / "models").iterdir() if p.is_dir()}
    assert model_dirs == {
        "kalshi_wc2026",
        "polymarket_catalog",
        "polymarket_soccer",
        "polymarket_wc2026",
        "reference",
        "sources",
        "wc2026",
    }


def test_dbt_project_version():
    text = (Path(__file__).resolve().parents[3] / "dbt" / "dbt_project.yml").read_text()

    assert "version: 0.1.8" in text
    assert "profile: oddsfox" in text


def test_shipped_scope_specs_have_matching_dbt_project_entries():
    dbt_root = Path(__file__).resolve().parents[3] / "dbt"
    project = yaml.safe_load((dbt_root / "dbt_project.yml").read_text())
    models = project["models"]["oddsfox"]
    seeds = project["seeds"]["oddsfox"]

    for spec in SHIPPED_SCOPE_SPECS:
        assert spec.namespace in models
        assert {spec.source, spec.scope} <= set(models[spec.namespace]["+tags"])
        if spec.source_seed is not None:
            assert f"{spec.namespace}_pipeline_policy" in seeds
        assert (dbt_root / "models" / spec.namespace).is_dir()
        assert (
            dbt_root / "models" / "sources" / f"{spec.namespace}_sources.yml"
        ).is_file()


def test_dbt_source_freshness_tables_are_seeded_for_ci():
    sources_root = Path(__file__).resolve().parents[3] / "dbt" / "models" / "sources"
    freshness_tables: set[tuple[str, str]] = set()

    for source_path in sources_root.glob("*_sources.yml"):
        data = yaml.safe_load(source_path.read_text(encoding="utf-8"))
        for source in data["sources"]:
            source_name = source["name"]
            for table in source["tables"]:
                if "freshness" not in table:
                    continue
                assert table.get("loaded_at_field"), (
                    f"{source_name}.{table['name']} has freshness without "
                    "loaded_at_field"
                )
                freshness_tables.add((source_name, table["name"]))

    assert freshness_tables == FRESHNESS_SOURCE_TABLES


def test_hourly_odds_materialization_shape():
    project = yaml.safe_load(
        (Path(__file__).resolve().parents[3] / "dbt" / "dbt_project.yml").read_text()
    )
    intermediate = project["models"]["oddsfox"]["polymarket_wc2026"]["intermediate"]
    marts = project["models"]["oddsfox"]["polymarket_wc2026"]["marts"]

    assert (
        intermediate["int_polymarket_wc2026_token_hourly_odds"]["+materialized"]
        == "incremental"
    )
    assert marts["polymarket_wc2026_market_hourly_odds"]["+materialized"] == "table"
    assert "polymarket_wc2026_graph_" + "token_hourly_odds" not in marts
    assert "polymarket_wc2026_token_hourly_odds" not in marts
    assert "polymarket_wc2026_token_daily_odds" not in marts


def test_wc2026_pipeline_policy_seed_is_configured_and_documented():
    dbt_root = Path(__file__).resolve().parents[3] / "dbt"
    project = yaml.safe_load((dbt_root / "dbt_project.yml").read_text())
    seeds = project["seeds"]["oddsfox"]
    seed_docs = yaml.safe_load((dbt_root / "seeds" / "schema.yml").read_text())
    documented = {seed["name"] for seed in seed_docs["seeds"]}

    assert (
        seeds["polymarket_wc2026_pipeline_policy"]["+schema"]
        == "polymarket_wc2026_staging"
    )
    assert "polymarket_wc2026_pipeline_policy" in documented


def test_event_catalog_intermediate_models_exist_and_are_documented():
    dbt_root = Path(__file__).resolve().parents[3] / "dbt"
    intermediate_root = dbt_root / "models" / "polymarket_wc2026" / "intermediate"
    docs = yaml.safe_load((intermediate_root / "intermediate.yml").read_text())
    documented = {model["name"] for model in docs["models"]}

    assert (intermediate_root / "int_polymarket_wc2026_event_latest.sql").exists()
    assert (
        intermediate_root / "int_polymarket_wc2026_primary_market_token.sql"
    ).exists()
    assert (intermediate_root / "int_polymarket_wc2026_token_hourly_odds.sql").exists()
    assert "int_polymarket_wc2026_token_hourly_odds" in documented


def test_market_hourly_odds_mart_columns_are_documented():
    dbt_root = Path(__file__).resolve().parents[3] / "dbt"
    docs = yaml.safe_load(
        (
            dbt_root
            / "models"
            / "polymarket_wc2026"
            / "marts"
            / "polymarket_wc2026.yml"
        ).read_text()
    )
    mart = next(
        model
        for model in docs["models"]
        if model["name"] == "polymarket_wc2026_market_hourly_odds"
    )
    columns = {column["name"] for column in mart["columns"]}

    assert {
        "market_id",
        "clob_token_id",
        "odds_hour_epoch",
        "close_odds",
        "event_volume_usd_lifetime_reported",
    } <= columns


def test_multi_parent_singular_tests_have_dagster_asset_metadata():
    test_root = Path(__file__).resolve().parents[3] / "dbt" / "tests"

    for path in test_root.glob("*.sql"):
        text = path.read_text()
        reference_count = len(re.findall(r"\{\{\s*(?:ref|source)\(", text))
        if reference_count < 2:
            continue

        assert "'asset_key':" in text, path.name
        assert "'ref': {'name':" in text, path.name


def test_market_models_read_reference_sources_without_bridge_models():
    dbt_root = Path(__file__).resolve().parents[3] / "dbt"
    reference_root = dbt_root / "models" / "reference"
    source_text = (
        dbt_root / "models" / "sources" / "oddsfox_reference_sources.yml"
    ).read_text()

    assert not list(reference_root.rglob("*.sql"))
    assert "oddsfox_reference" in source_text
    assert "raw.githubusercontent.com" not in source_text
    model_text = "\n".join(
        path.read_text(encoding="utf-8") for path in dbt_root.rglob("*.sql")
    )
    assert (
        "source('oddsfox_reference', 'international_results_wc2026_matches')"
        in model_text
    )
    assert (
        "source('oddsfox_reference', 'openfootball_wc2026_schedule_fixtures')"
        in model_text
    )


def test_kalshi_wc2026_models_are_documented():
    dbt_root = Path(__file__).resolve().parents[3] / "dbt"
    kalshi_root = dbt_root / "models" / "kalshi_wc2026"
    intermediate = yaml.safe_load(
        (kalshi_root / "intermediate" / "intermediate.yml").read_text()
    )
    marts = yaml.safe_load((kalshi_root / "marts" / "kalshi_wc2026.yml").read_text())
    documented_intermediate = {model["name"] for model in intermediate["models"]}
    documented_marts = {model["name"] for model in marts["models"]}

    assert "int_kalshi_wc2026_market_hourly_odds" in documented_intermediate
    assert "int_kalshi_wc2026_stage_classification" in documented_intermediate
    assert "int_kalshi_wc2026_group_winner_classification" in documented_intermediate
    assert "kalshi_wc2026_stage_market_hourly_odds" in documented_marts
    assert "kalshi_wc2026_group_winner_market_hourly_odds" in documented_marts
    assert "kalshi_wc2026_stage_markets" in documented_marts
    assert "kalshi_wc2026_group_winner_markets" in documented_marts


def test_wc2026_contract_fingerprint_covers_documented_strategy_relations():
    dbt_root = Path(__file__).resolve().parents[3] / "dbt"
    fingerprint_sql = (
        dbt_root / "models" / "wc2026" / "marts" / "wc2026_contract_metadata.sql"
    ).read_text(encoding="utf-8")
    # Exact pipe-delimited reconstruction so substring collisions
    # (team_ratings vs team_ratings_pre_match) cannot mask a missing token.
    md5_block = fingerprint_sql.split("md5(", 1)[1].split(
        ") as contract_fingerprint", 1
    )[0]
    tokens = "".join(re.findall(r"'([^']*)'", md5_block)).split("|")
    required = (
        "wc2026.v1",
        "fixtures",
        "results",
        "team_identities",
        "player_features",
        "squad_player_features",
        "team_ratings",
        "team_ratings_pre_match",
        "club_strength",
        "base_camp_venues",
        "travel_features",
        "venue_markets",
        "price_liquidity",
        "event_state_timing",
        "international_matches",
        "third_place_slot_assignments",
        "source_provenance",
    )
    assert tokens == list(required)


def test_match_order_book_inventory_requires_fifa_match_104():
    dbt_root = Path(__file__).resolve().parents[3] / "dbt"
    quality = (
        dbt_root
        / "models"
        / "polymarket_wc2026"
        / "observability"
        / "polymarket_wc2026_match_order_book_quality_issues.sql"
    ).read_text(encoding="utf-8")
    inventory = (
        dbt_root / "tests" / "assert_polymarket_wc2026_match_order_book_inventory.sql"
    ).read_text(encoding="utf-8")
    data_quality = (
        dbt_root
        / "models"
        / "polymarket_wc2026"
        / "observability"
        / "polymarket_wc2026_match_order_book_data_quality.sql"
    ).read_text(encoding="utf-8")

    assert "fifa_match_id != 104" in quality
    assert "fifa_match_id <= 72" not in quality
    assert "fifa_match_id != 104" in inventory
    assert "as fifa_match_id" in data_quality
