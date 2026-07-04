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
        "polymarket_wc2026",
        "sources",
    }


def test_dbt_project_version():
    text = (Path(__file__).resolve().parents[2] / "dbt" / "dbt_project.yml").read_text()

    assert "version: 0.1.4" in text
    assert "profile: oddsfox" in text


def test_time_series_marts_are_materialized_tables():
    project = yaml.safe_load(
        (Path(__file__).resolve().parents[2] / "dbt" / "dbt_project.yml").read_text()
    )
    marts = project["models"]["oddsfox"]["polymarket_wc2026"]["marts"]

    assert (
        marts["polymarket_wc2026_knockout_token_hourly_odds"]["+materialized"]
        == "table"
    )
    assert "polymarket_wc2026_token_hourly_odds" not in marts
    assert "polymarket_wc2026_token_daily_odds" not in marts


def test_knockout_observability_models_are_documented():
    dbt_root = Path(__file__).resolve().parents[2] / "dbt"
    observability_root = dbt_root / "models" / "polymarket_wc2026" / "observability"
    docs = yaml.safe_load((observability_root / "observability.yml").read_text())
    documented = {model["name"] for model in docs["models"]}

    assert (
        observability_root / "polymarket_wc2026_knockout_stage_coverage.sql"
    ).exists()
    assert (observability_root / "polymarket_wc2026_knockout_data_quality.sql").exists()
    assert "polymarket_wc2026_knockout_stage_coverage" in documented
    assert "polymarket_wc2026_knockout_data_quality" in documented


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
