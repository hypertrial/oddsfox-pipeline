from __future__ import annotations

from dagster import AssetKey

from oddsfox_pipeline.storage.duckdb.schemas import dbt_schemas


def test_dbt_schema_helpers_cover_fallback_and_polymarket_names():
    assert dbt_schemas.qualified_relation("schema", "model") == "schema.model"
    assert (
        dbt_schemas.resolve_source_slug({"name": "polymarket_wc2026_knockout_markets"})
        == dbt_schemas.DBT_SOURCE_POLYMARKET_WC2026
    )
    assert (
        dbt_schemas.resolve_source_slug(
            {"name": "international_results_wc2026_matches"}
        )
        == dbt_schemas.DBT_SOURCE_INTERNATIONAL_RESULTS_WC2026
    )
    assert (
        dbt_schemas.resolve_source_slug({"name": "other_model"})
        == dbt_schemas.DBT_FALLBACK_SCHEMA
    )
    assert (
        dbt_schemas.shorten_model_name(
            "polymarket_wc2026_sync_run_observability",
            dbt_schemas.DBT_SOURCE_POLYMARKET_WC2026,
        )
        == "sync_run_observability"
    )
    assert (
        dbt_schemas.shorten_model_name(
            "international_results_wc2026_team_status",
            dbt_schemas.DBT_SOURCE_INTERNATIONAL_RESULTS_WC2026,
        )
        == "team_status"
    )
    assert (
        dbt_schemas.shorten_model_name(
            "custom_model",
            dbt_schemas.DBT_SOURCE_POLYMARKET_WC2026,
        )
        == "custom_model"
    )
    assert dbt_schemas.dbt_model_asset_key_for_name(
        "stg_international_results_wc2026_match_results",
        dbt_schemas.DBT_SOURCE_INTERNATIONAL_RESULTS_WC2026,
    ) == AssetKey(["international_results", "wc2026", "staging", "match_results"])
    assert dbt_schemas.dbt_model_asset_key_for_name(
        "int_international_results_wc2026_match_teams",
        dbt_schemas.DBT_SOURCE_INTERNATIONAL_RESULTS_WC2026,
    ) == AssetKey(["international_results", "wc2026", "intermediate", "match_teams"])
    assert dbt_schemas.dbt_model_asset_key_for_name(
        "international_results_wc2026_team_status",
        dbt_schemas.DBT_SOURCE_INTERNATIONAL_RESULTS_WC2026,
    ) == AssetKey(["international_results", "wc2026", "marts", "team_status"])
    assert dbt_schemas.dbt_model_asset_key_for_name(
        "international_results_wc2026_data_quality",
        dbt_schemas.DBT_SOURCE_INTERNATIONAL_RESULTS_WC2026,
    ) == AssetKey(["international_results", "wc2026", "observability", "data_quality"])
    assert dbt_schemas.dbt_model_asset_key_for_name(
        "custom_model",
        dbt_schemas.DBT_SOURCE_INTERNATIONAL_RESULTS_WC2026,
    ) == AssetKey(["international_results", "wc2026", "marts", "custom_model"])
    assert dbt_schemas.dbt_model_asset_key_for_name(
        "stg_polymarket_wc2026_markets",
        dbt_schemas.DBT_SOURCE_POLYMARKET_WC2026,
    ) == AssetKey(["polymarket", "wc2026", "staging", "markets"])
    assert dbt_schemas.dbt_model_asset_key_for_name(
        "int_polymarket_wc2026_token_universe",
        dbt_schemas.DBT_SOURCE_POLYMARKET_WC2026,
    ) == AssetKey(["polymarket", "wc2026", "intermediate", "token_universe"])
    assert dbt_schemas.dbt_model_asset_key_for_name(
        "polymarket_wc2026_knockout_token_hourly_odds",
        dbt_schemas.DBT_SOURCE_POLYMARKET_WC2026,
    ) == AssetKey(["polymarket", "wc2026", "marts", "knockout_token_hourly_odds"])
    assert dbt_schemas.dbt_model_asset_key_for_name(
        "polymarket_wc2026_sync_run_observability",
        dbt_schemas.DBT_SOURCE_POLYMARKET_WC2026,
    ) == AssetKey(["polymarket", "wc2026", "observability", "sync_run_observability"])
    assert dbt_schemas.dbt_model_asset_key_for_name(
        "polymarket_wc2026_knockout_stage_coverage",
        dbt_schemas.DBT_SOURCE_POLYMARKET_WC2026,
    ) == AssetKey(["polymarket", "wc2026", "observability", "knockout_stage_coverage"])
    assert dbt_schemas.dbt_model_asset_key_for_name(
        "polymarket_wc2026_knockout_data_quality",
        dbt_schemas.DBT_SOURCE_POLYMARKET_WC2026,
    ) == AssetKey(["polymarket", "wc2026", "observability", "knockout_data_quality"])
    assert dbt_schemas.dbt_model_asset_key_for_name(
        "other_model",
        dbt_schemas.DBT_FALLBACK_SCHEMA,
    ) == AssetKey("dbt_other_model")
    assert dbt_schemas.dbt_model_asset_key({"name": "other_model"}) == AssetKey(
        "dbt_other_model"
    )
