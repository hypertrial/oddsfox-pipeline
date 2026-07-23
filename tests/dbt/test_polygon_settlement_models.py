from pathlib import Path

import yaml

DBT_ROOT = Path(__file__).resolve().parents[2] / "dbt"
REPO_ROOT = DBT_ROOT.parent
MODEL_ROOT = DBT_ROOT / "models" / "polymarket_wc2026"

MODEL_FILES = {
    "staging/stg_polymarket_wc2026_polygon_settlement_markets.sql",
    "staging/stg_polymarket_wc2026_polygon_settlement_fills.sql",
    "staging/stg_polymarket_wc2026_polygon_settlement_scan_runs.sql",
    "staging/stg_polymarket_wc2026_polygon_settlement_scan_chunks.sql",
    "intermediate/int_polymarket_wc2026_polygon_settlement_market_universe.sql",
    "intermediate/int_polymarket_wc2026_polygon_settlement_token_minute_odds.sql",
    "intermediate/int_polymarket_wc2026_polygon_settlement_minute_odds_candidate.sql",
    "intermediate/int_polymarket_wc2026_polygon_settlement_publication_gate.sql",
    "observability/polymarket_wc2026_polygon_settlement_token_coverage.sql",
    "observability/polymarket_wc2026_polygon_settlement_quality_issues.sql",
    "observability/polymarket_wc2026_polygon_settlement_data_quality.sql",
    "marts/polymarket_wc2026_polygon_settlement_minute_odds.sql",
}


def test_polygon_settlement_graph_is_tagged_and_source_isolated():
    sql = []
    for relative_path in MODEL_FILES:
        text = (MODEL_ROOT / relative_path).read_text(encoding="utf-8").lower()
        assert "polygon_settlement" in text.partition("}}")[0]
        sql.append(text)

    graph_sql = "\n".join(sql)
    assert "stg_polymarket_wc2026_markets" not in graph_sql
    assert "match_minute_odds_history" not in graph_sql
    assert "clob_token" not in graph_sql
    assert "international_results" not in graph_sql
    assert "openfootball_wc2026" not in graph_sql


def test_polygon_settlement_contract_is_dense_half_open_and_fail_closed():
    candidate = (
        MODEL_ROOT
        / "intermediate"
        / "int_polymarket_wc2026_polygon_settlement_minute_odds_candidate.sql"
    ).read_text(encoding="utf-8")
    gate = (
        MODEL_ROOT
        / "observability"
        / "polymarket_wc2026_polygon_settlement_data_quality.sql"
    ).read_text(encoding="utf-8")
    mart = (
        MODEL_ROOT / "marts" / "polymarket_wc2026_polygon_settlement_minute_odds.sql"
    ).read_text(encoding="utf-8")

    assert "range(0, universe.window_minutes)" in candidate
    assert {"both_observed", "yes_only", "no_only", "no_fills"} <= set(
        candidate.split("'")[1::2]
    )
    assert "39120 as expected_minute_rows" in gate
    assert "case when raw_fill_rows = 0 then 'raw_empty' end" in gate
    assert "blocking_issue_keys is null as publication_ready" in gate
    assert "bd46a148289f9930da66c140d4d7d2325e95d387" in gate
    assert "stage = 'group_stage'" in gate
    assert "market_structure <> 'neg_risk'" in gate
    assert "stage <> 'group_stage'" in gate
    assert "market_structure <> 'standard'" in gate
    assert "gap_or_overlap_count" in gate
    for blocker in (
        "seed_inventory",
        "seed_stage_distribution",
        "seed_proposition_shape",
        "seed_unique_ids",
        "seed_windows",
        "seed_evidence",
        "scan_missing",
        "scan_manifest",
        "scan_integrity",
        "scan_chunks",
        "raw_empty",
        "raw_scan_mismatch",
        "raw_duplicates",
        "raw_normalization_pairs",
        "raw_mapping",
        "raw_values",
        "raw_chunk_coverage",
        "minute_inventory",
        "minute_axis",
        "minute_values",
        "aggregate_reconciliation",
        "quality_errors",
    ):
        assert f"'{blocker}'" in gate
    assert "candidate.yes_open_price as yes_open" in mart
    assert "candidate.no_close_price as no_close" in mart


def test_polygon_settlement_seed_and_sources_are_tagged():
    project = yaml.safe_load((DBT_ROOT / "dbt_project.yml").read_text())
    seed_config = project["seeds"]["oddsfox"][
        "polymarket_wc2026_polygon_settlement_markets"
    ]
    assert "polygon_settlement" in seed_config["+tags"]

    sources = yaml.safe_load(
        (DBT_ROOT / "models" / "sources" / "polymarket_wc2026_sources.yml").read_text()
    )["sources"]
    polygon_tables = {
        table["name"]: table
        for source in sources
        for table in source["tables"]
        if table["name"].startswith("polygon_settlement_")
    }
    assert set(polygon_tables) == {
        "polygon_settlement_fills",
        "polygon_settlement_scan_runs",
        "polygon_settlement_scan_chunks",
        "polygon_settlement_fill_stage",
    }
    assert all(
        "polygon_settlement" in table["config"]["tags"]
        for table in polygon_tables.values()
    )


def test_polygon_settlement_graph_has_an_isolated_release_gate():
    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    integration = (
        REPO_ROOT / "tests" / "integration" / "test_polygon_settlement_dbt.py"
    ).read_text(encoding="utf-8")

    ordinary_build = makefile.split("dbt-build dbt-test:", 1)[1].split("\n\n", 1)[0]
    ordinary_unit = makefile.split("dbt-unit:", 1)[1].split("\n\n", 1)[0]
    release_gate = makefile.split("release-gate-core:", 1)[1].split("\n\n", 1)[0]
    dedicated_build = makefile.split("dbt-polygon-settlement-ci:", 1)[1].split(
        "\n\n", 1
    )[0]

    assert "--exclude tag:polygon_settlement" in ordinary_build
    assert ordinary_unit.count("--exclude tag:polygon_settlement") == 3
    assert "$(MAKE) dbt-polygon-settlement-ci" in release_gate
    assert "tests/integration/test_polygon_settlement_dbt.py" in dedicated_build
    assert '["build", "--full-refresh", "--select", "tag:polygon_settlement"]' in (
        integration
    )
