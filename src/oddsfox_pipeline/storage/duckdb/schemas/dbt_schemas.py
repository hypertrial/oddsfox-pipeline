"""dbt-modeled DuckDB schema names and Dagster asset-key helpers."""

from __future__ import annotations

from typing import Final, Mapping, Sequence

from dagster import AssetKey

DBT_SOURCE_WC2026_POLYMARKET: Final = "wc2026_polymarket"

WC2026_POLYMARKET_STAGING_SCHEMA: Final = "wc2026_polymarket_staging"
WC2026_POLYMARKET_INTERMEDIATE_SCHEMA: Final = "wc2026_polymarket_intermediate"
WC2026_POLYMARKET_MARTS_SCHEMA: Final = "wc2026_polymarket_marts"
WC2026_POLYMARKET_OBSERVABILITY_SCHEMA: Final = "wc2026_polymarket_observability"
DBT_FALLBACK_SCHEMA: Final = "dbt"

DBT_MODELED_SCHEMAS: Final[tuple[str, ...]] = (
    WC2026_POLYMARKET_STAGING_SCHEMA,
    WC2026_POLYMARKET_INTERMEDIATE_SCHEMA,
    WC2026_POLYMARKET_MARTS_SCHEMA,
    WC2026_POLYMARKET_OBSERVABILITY_SCHEMA,
)


def qualified_relation(schema: str, model_name: str) -> str:
    return f"{schema}.{model_name}"


def resolve_source_slug(
    props: Mapping[str, object],
    *,
    fqn: Sequence[str] | None = None,
) -> str:
    tags = set(props.get("tags") or ())
    path_fqn = list(fqn or props.get("fqn") or ())
    if "polymarket" in tags or (
        len(path_fqn) >= 2 and path_fqn[1] == "wc2026_polymarket"
    ):
        return DBT_SOURCE_WC2026_POLYMARKET
    name = str(props.get("name") or "")
    if name.startswith(
        ("stg_wc2026_polymarket_", "int_wc2026_polymarket_", "wc2026_")
    ) or name in {
        "wc2026_market_coverage",
        "wc2026_token_coverage",
        "wc2026_token_hourly_odds",
        "wc2026_token_daily_odds",
        "wc2026_markets",
        "wc2026_sync_run_observability",
    }:
        return DBT_SOURCE_WC2026_POLYMARKET
    return DBT_FALLBACK_SCHEMA


def shorten_model_name(model_name: str, source_slug: str) -> str:
    if source_slug == DBT_SOURCE_WC2026_POLYMARKET:
        for layer, prefix in (
            ("stg", "stg_wc2026_polymarket_"),
            ("int", "int_wc2026_polymarket_"),
        ):
            if model_name.startswith(prefix):
                return f"{layer}_{model_name[len(prefix) :]}"
        if model_name.startswith("wc2026_"):
            return model_name[len("wc2026_") :]
    return model_name


def dbt_model_asset_key(
    props: Mapping[str, object],
    *,
    fqn: Sequence[str] | None = None,
) -> AssetKey:
    source = resolve_source_slug(props, fqn=fqn)
    name = str(props.get("name") or "")
    return AssetKey(f"{source}_{shorten_model_name(name, source)}")


def dbt_model_asset_key_for_name(model_name: str, source_slug: str) -> AssetKey:
    return AssetKey(f"{source_slug}_{shorten_model_name(model_name, source_slug)}")


__all__ = [
    "DBT_FALLBACK_SCHEMA",
    "DBT_MODELED_SCHEMAS",
    "DBT_SOURCE_WC2026_POLYMARKET",
    "WC2026_POLYMARKET_INTERMEDIATE_SCHEMA",
    "WC2026_POLYMARKET_MARTS_SCHEMA",
    "WC2026_POLYMARKET_OBSERVABILITY_SCHEMA",
    "WC2026_POLYMARKET_STAGING_SCHEMA",
    "dbt_model_asset_key",
    "dbt_model_asset_key_for_name",
    "qualified_relation",
    "resolve_source_slug",
    "shorten_model_name",
]
