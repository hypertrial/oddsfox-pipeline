import re
from pathlib import Path

import yaml
from scripts.seed_dbt_source_freshness import FRESHNESS_SOURCE_TABLES

from oddsfox_pipeline.orchestration.scope_registry import SHIPPED_SCOPE_SPECS


def test_sqlfluff_dbt_templating_fails_closed():
    pyproject = (Path(__file__).resolve().parents[2] / "pyproject.toml").read_text()
    dbt_config = pyproject.split("[tool.sqlfluff.templater.dbt]", 1)[1].split("\n[", 1)[
        0
    ]

    assert "dbt_skip_compilation_error = false" in dbt_config


def test_dbt_project_sources_are_wc2026_only():
    dbt_root = Path(__file__).resolve().parents[2] / "dbt"

    assert (dbt_root / "dbt_project.yml").exists()
    assert (dbt_root / "profiles" / "profiles.yml").exists()
    assert (dbt_root / "models" / "sources" / "polymarket_wc2026_sources.yml").exists()
    assert (
        dbt_root / "models" / "sources" / "international_results_wc2026_sources.yml"
    ).exists()
    assert (
        dbt_root / "models" / "sources" / "openfootball_wc2026_sources.yml"
    ).exists()
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
        "openfootball_wc2026",
        "polymarket_us_midterms_2026",
        "polymarket_wc2026",
        "sources",
        "wc2026",
    }


def test_dbt_project_version():
    text = (Path(__file__).resolve().parents[2] / "dbt" / "dbt_project.yml").read_text()

    assert "version: 0.1.7" in text
    assert "profile: oddsfox" in text


def test_shipped_scope_specs_have_matching_dbt_project_entries():
    dbt_root = Path(__file__).resolve().parents[2] / "dbt"
    project = yaml.safe_load((dbt_root / "dbt_project.yml").read_text())
    models = project["models"]["oddsfox"]
    seeds = project["seeds"]["oddsfox"]

    for spec in SHIPPED_SCOPE_SPECS:
        assert spec.namespace in models
        assert {spec.source, spec.scope} <= set(models[spec.namespace]["+tags"])
        assert f"{spec.namespace}_contract" in seeds
        assert (dbt_root / "models" / spec.namespace).is_dir()
        assert (
            dbt_root / "models" / "sources" / f"{spec.namespace}_sources.yml"
        ).is_file()


def test_dbt_source_freshness_tables_are_seeded_for_ci():
    sources_root = Path(__file__).resolve().parents[2] / "dbt" / "models" / "sources"
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


def test_match_hourly_facts_are_incremental_cross_domain_models():
    project = yaml.safe_load(
        (Path(__file__).resolve().parents[2] / "dbt" / "dbt_project.yml").read_text()
    )
    models = project["models"]["oddsfox"]
    polymarket = models["polymarket_wc2026"]["intermediate"]
    kalshi = models["kalshi_wc2026"]["intermediate"]

    assert polymarket["int_polymarket_wc2026_match_hourly_odds"] == {
        "+materialized": "incremental",
        "+tags": ["cross_domain"],
    }
    assert polymarket["int_polymarket_wc2026_match_advance_tokens"] == {
        "+tags": ["cross_domain"]
    }
    assert kalshi["int_kalshi_wc2026_match_hourly_odds"] == {
        "+materialized": "incremental",
        "+tags": ["cross_domain"],
    }
    assert kalshi["int_kalshi_wc2026_match_advance_markets"] == {
        "+tags": ["cross_domain"]
    }


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


def test_kalshi_wc2026_models_are_documented():
    dbt_root = Path(__file__).resolve().parents[2] / "dbt"
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
