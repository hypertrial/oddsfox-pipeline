from pathlib import Path

import yaml


def test_dbt_project_is_polymarket_wc2026_only():
    dbt_root = Path(__file__).resolve().parents[2] / "dbt"

    assert (dbt_root / "dbt_project.yml").exists()
    assert (dbt_root / "profiles" / "profiles.yml").exists()
    assert (dbt_root / "models" / "sources" / "polymarket_wc2026_sources.yml").exists()
    assert (
        dbt_root / "models" / "polymarket_wc2026" / "staging" / "staging.yml"
    ).exists()
    assert (
        dbt_root / "models" / "polymarket_wc2026" / "marts" / "polymarket_wc2026.yml"
    ).exists()
    assert (
        dbt_root
        / "models"
        / "polymarket_wc2026"
        / "observability"
        / "observability.yml"
    ).exists()

    model_dirs = {p.name for p in (dbt_root / "models").iterdir() if p.is_dir()}
    assert model_dirs == {"polymarket_wc2026", "sources"}


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
