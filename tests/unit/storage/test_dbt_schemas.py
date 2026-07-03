from __future__ import annotations

from dagster import AssetKey

from oddsfox_pipeline.storage.duckdb.schemas import dbt_schemas


def test_dbt_schema_helpers_cover_fallback_and_polymarket_names():
    assert dbt_schemas.qualified_relation("schema", "model") == "schema.model"
    assert (
        dbt_schemas.resolve_source_slug({"name": "selected_markets"})
        == dbt_schemas.DBT_SOURCE_POLYMARKET
    )
    assert (
        dbt_schemas.resolve_source_slug({"name": "other_model"})
        == dbt_schemas.DBT_FALLBACK_SCHEMA
    )
    assert (
        dbt_schemas.shorten_model_name(
            "polymarket_sync_run_observability",
            dbt_schemas.DBT_SOURCE_POLYMARKET,
        )
        == "sync_run_observability"
    )
    assert dbt_schemas.dbt_model_asset_key_for_name(
        "other_model",
        dbt_schemas.DBT_FALLBACK_SCHEMA,
    ) == AssetKey("dbt_other_model")
