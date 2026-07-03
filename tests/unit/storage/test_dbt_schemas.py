from __future__ import annotations

from dagster import AssetKey

from oddsfox_pipeline.storage.duckdb.schemas import dbt_schemas


def test_dbt_schema_helpers_cover_fallback_and_polymarket_names():
    assert dbt_schemas.qualified_relation("schema", "model") == "schema.model"
    assert (
        dbt_schemas.resolve_source_slug({"name": "wc2026_markets"})
        == dbt_schemas.DBT_SOURCE_WC2026_POLYMARKET
    )
    assert (
        dbt_schemas.resolve_source_slug({"name": "other_model"})
        == dbt_schemas.DBT_FALLBACK_SCHEMA
    )
    assert (
        dbt_schemas.shorten_model_name(
            "wc2026_sync_run_observability",
            dbt_schemas.DBT_SOURCE_WC2026_POLYMARKET,
        )
        == "sync_run_observability"
    )
    assert (
        dbt_schemas.shorten_model_name(
            "custom_model",
            dbt_schemas.DBT_SOURCE_WC2026_POLYMARKET,
        )
        == "custom_model"
    )
    assert dbt_schemas.dbt_model_asset_key_for_name(
        "stg_wc2026_polymarket_markets",
        dbt_schemas.DBT_SOURCE_WC2026_POLYMARKET,
    ) == AssetKey("wc2026_polymarket_stg_markets")
    assert dbt_schemas.dbt_model_asset_key_for_name(
        "wc2026_token_hourly_odds",
        dbt_schemas.DBT_SOURCE_WC2026_POLYMARKET,
    ) == AssetKey("wc2026_polymarket_token_hourly_odds")
    assert dbt_schemas.dbt_model_asset_key_for_name(
        "other_model",
        dbt_schemas.DBT_FALLBACK_SCHEMA,
    ) == AssetKey("dbt_other_model")
