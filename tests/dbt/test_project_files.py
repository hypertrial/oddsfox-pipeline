import re
from pathlib import Path

import yaml


def test_dbt_project_sources_are_wc2026_only():
    dbt_root = Path(__file__).resolve().parents[2] / "dbt"

    assert (dbt_root / "dbt_project.yml").exists()
    assert (dbt_root / "profiles" / "profiles.yml").exists()
    assert (dbt_root / "models" / "sources" / "polymarket_wc2026_sources.yml").exists()
    assert (
        dbt_root / "models" / "sources" / "international_results_wc2026_sources.yml"
    ).exists()
    assert (
        dbt_root / "models" / "polymarket_wc2026" / "staging" / "staging.yml"
    ).exists()
    assert (
        dbt_root / "models" / "polymarket_wc2026" / "marts" / "polymarket_wc2026.yml"
    ).exists()
    assert (
        dbt_root
        / "models"
        / "international_results_wc2026"
        / "intermediate"
        / "intermediate.yml"
    ).exists()
    assert (
        dbt_root
        / "models"
        / "international_results_wc2026"
        / "marts"
        / "international_results_wc2026.yml"
    ).exists()
    assert (
        dbt_root
        / "models"
        / "international_results_wc2026"
        / "observability"
        / "observability.yml"
    ).exists()
    assert (
        dbt_root / "seeds" / "international_results_wc2026_team_aliases.csv"
    ).exists()
    assert (dbt_root / "seeds" / "polymarket_wc2026_contract.csv").exists()
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
        "international_results_wc2026",
        "kalshi_wc2026",
        "polymarket_us_midterms_2026",
        "polymarket_wc2026",
        "sources",
    }


def test_dbt_project_version():
    text = (Path(__file__).resolve().parents[2] / "dbt" / "dbt_project.yml").read_text()

    assert "version: 0.1.4" in text
    assert "profile: oddsfox" in text


def test_hourly_odds_materialization_shape():
    project = yaml.safe_load(
        (Path(__file__).resolve().parents[2] / "dbt" / "dbt_project.yml").read_text()
    )
    intermediate = project["models"]["oddsfox"]["polymarket_wc2026"]["intermediate"]
    marts = project["models"]["oddsfox"]["polymarket_wc2026"]["marts"]

    assert (
        intermediate["int_polymarket_wc2026_token_hourly_odds"]["+materialized"]
        == "incremental"
    )
    assert (
        marts["polymarket_wc2026_knockout_token_hourly_odds"]["+materialized"] == "view"
    )
    assert marts["polymarket_wc2026_graph_token_hourly_odds"]["+materialized"] == "view"
    assert "polymarket_wc2026_token_hourly_odds" not in marts
    assert "polymarket_wc2026_token_daily_odds" not in marts


def test_wc2026_contract_seed_is_configured_and_documented():
    dbt_root = Path(__file__).resolve().parents[2] / "dbt"
    project = yaml.safe_load((dbt_root / "dbt_project.yml").read_text())
    seeds = project["seeds"]["oddsfox"]
    seed_docs = yaml.safe_load((dbt_root / "seeds" / "schema.yml").read_text())
    documented = {seed["name"] for seed in seed_docs["seeds"]}

    assert seeds["polymarket_wc2026_contract"]["+schema"] == "polymarket_wc2026_staging"
    assert "polymarket_wc2026_contract" in documented
    assert (
        seeds["polymarket_us_midterms_2026_contract"]["+schema"]
        == "polymarket_us_midterms_2026_staging"
    )
    assert "polymarket_us_midterms_2026_contract" in documented


def test_us_midterms_2026_mart_materialization_shape():
    project = yaml.safe_load(
        (Path(__file__).resolve().parents[2] / "dbt" / "dbt_project.yml").read_text()
    )
    intermediate = project["models"]["oddsfox"]["polymarket_us_midterms_2026"][
        "intermediate"
    ]
    marts = project["models"]["oddsfox"]["polymarket_us_midterms_2026"]["marts"]

    assert (
        intermediate["int_polymarket_us_midterms_2026_token_hourly_odds"][
            "+materialized"
        ]
        == "incremental"
    )
    assert (
        marts["polymarket_us_midterms_2026_market_token_hourly_odds"]["+materialized"]
        == "view"
    )


def test_knockout_classifier_intermediate_exists_and_is_documented():
    dbt_root = Path(__file__).resolve().parents[2] / "dbt"
    intermediate_root = dbt_root / "models" / "polymarket_wc2026" / "intermediate"
    docs = yaml.safe_load((intermediate_root / "intermediate.yml").read_text())
    documented = {model["name"] for model in docs["models"]}

    assert (
        intermediate_root / "int_polymarket_wc2026_knockout_market_classification.sql"
    ).exists()
    assert (intermediate_root / "int_polymarket_wc2026_token_hourly_odds.sql").exists()
    assert "int_polymarket_wc2026_knockout_market_classification" in documented
    assert "int_polymarket_wc2026_token_hourly_odds" in documented


def test_knockout_observability_models_are_documented():
    dbt_root = Path(__file__).resolve().parents[2] / "dbt"
    observability_root = dbt_root / "models" / "polymarket_wc2026" / "observability"
    docs = yaml.safe_load((observability_root / "observability.yml").read_text())
    documented = {model["name"] for model in docs["models"]}
    dq_columns = {
        column["name"]
        for model in docs["models"]
        if model["name"] == "polymarket_wc2026_knockout_data_quality"
        for column in model["columns"]
    }
    coverage_columns = {
        column["name"]
        for model in docs["models"]
        if model["name"] == "polymarket_wc2026_knockout_stage_coverage"
        for column in model["columns"]
    }

    assert (
        observability_root / "polymarket_wc2026_knockout_stage_coverage.sql"
    ).exists()
    assert (observability_root / "polymarket_wc2026_knockout_data_quality.sql").exists()
    assert "polymarket_wc2026_knockout_stage_coverage" in documented
    assert "polymarket_wc2026_knockout_data_quality" in documented
    assert "issue_count" in dq_columns
    assert {
        "expected_hourly_rows",
        "avg_hourly_rows_per_token",
        "min_hourly_rows_per_token",
        "max_hourly_rows_per_token",
        "hourly_completeness_ratio",
    }.issubset(coverage_columns)


def test_knockout_mart_semantic_columns_are_documented():
    dbt_root = Path(__file__).resolve().parents[2] / "dbt"
    docs = yaml.safe_load(
        (
            dbt_root
            / "models"
            / "polymarket_wc2026"
            / "marts"
            / "polymarket_wc2026.yml"
        ).read_text()
    )
    expected_models = {
        "polymarket_wc2026_knockout_market_tokens",
        "polymarket_wc2026_knockout_markets",
        "polymarket_wc2026_knockout_token_hourly_odds",
        "polymarket_wc2026_graph_token_hourly_odds",
    }
    for model in docs["models"]:
        if model["name"] not in expected_models:
            continue
        columns = {column["name"] for column in model["columns"]}
        assert "progression_outcome_label" in columns
        if model["name"] == "polymarket_wc2026_graph_token_hourly_odds":
            assert "price_represents" not in columns
            assert "is_progression_token" in columns
            assert "opposite_clob_token_id" in columns
        else:
            assert "price_represents" in columns
            assert "is_active_team_live_market" in columns
        if model["name"] == "polymarket_wc2026_knockout_markets":
            assert "is_actionable_live_market" in columns
        else:
            assert "is_actionable_live_market" not in columns


def test_multi_parent_singular_tests_have_dagster_asset_metadata():
    test_root = Path(__file__).resolve().parents[2] / "dbt" / "tests"

    for path in test_root.glob("*.sql"):
        text = path.read_text()
        reference_count = len(re.findall(r"\{\{\s*(?:ref|source)\(", text))
        if reference_count < 2:
            continue

        assert "'asset_key':" in text, path.name
        assert "'ref': {'name':" in text, path.name


def test_international_results_models_are_documented():
    dbt_root = Path(__file__).resolve().parents[2] / "dbt"
    results_root = dbt_root / "models" / "international_results_wc2026"
    marts = yaml.safe_load(
        (results_root / "marts" / "international_results_wc2026.yml").read_text()
    )
    observability = yaml.safe_load(
        (results_root / "observability" / "observability.yml").read_text()
    )
    documented_marts = {model["name"] for model in marts["models"]}
    documented_observability = {model["name"] for model in observability["models"]}

    assert (
        results_root / "marts" / "international_results_wc2026_matches.sql"
    ).exists()
    assert (
        results_root
        / "intermediate"
        / "int_international_results_wc2026_match_teams.sql"
    ).exists()
    assert (
        results_root / "marts" / "international_results_wc2026_team_status.sql"
    ).exists()
    assert (
        results_root / "observability" / "international_results_wc2026_data_quality.sql"
    ).exists()
    assert "international_results_wc2026_matches" in documented_marts
    assert "international_results_wc2026_team_status" in documented_marts
    assert "international_results_wc2026_data_quality" in documented_observability
