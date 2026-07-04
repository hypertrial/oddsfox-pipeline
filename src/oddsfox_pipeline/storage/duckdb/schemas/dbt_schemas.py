"""dbt-modeled DuckDB schema names and Dagster asset-key helpers."""

from __future__ import annotations

from typing import Final, Mapping, Sequence

from dagster import AssetKey

from oddsfox_pipeline.naming import (
    SCOPE_WC2026,
    SOURCE_POLYMARKET,
    asset_key,
    schema_name,
)

DBT_SOURCE_POLYMARKET_WC2026: Final = "polymarket_wc2026"

POLYMARKET_WC2026_STAGING_SCHEMA: Final = schema_name(
    SOURCE_POLYMARKET, SCOPE_WC2026, "staging"
)
POLYMARKET_WC2026_INTERMEDIATE_SCHEMA: Final = schema_name(
    SOURCE_POLYMARKET, SCOPE_WC2026, "intermediate"
)
POLYMARKET_WC2026_MARTS_SCHEMA: Final = schema_name(
    SOURCE_POLYMARKET, SCOPE_WC2026, "marts"
)
POLYMARKET_WC2026_OBSERVABILITY_SCHEMA: Final = schema_name(
    SOURCE_POLYMARKET, SCOPE_WC2026, "observability"
)
DBT_FALLBACK_SCHEMA: Final = "dbt"
POLYMARKET_WC2026_OBSERVABILITY_MODELS: Final[tuple[str, ...]] = (
    "polymarket_wc2026_knockout_stage_coverage",
    "polymarket_wc2026_knockout_data_quality",
    "polymarket_wc2026_sync_run_observability",
)

DBT_MODELED_SCHEMAS: Final[tuple[str, ...]] = (
    POLYMARKET_WC2026_STAGING_SCHEMA,
    POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
    POLYMARKET_WC2026_MARTS_SCHEMA,
    POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
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
        len(path_fqn) >= 2 and path_fqn[1] == "polymarket_wc2026"
    ):
        return DBT_SOURCE_POLYMARKET_WC2026
    name = str(props.get("name") or "")
    if name.startswith(
        (
            "stg_polymarket_wc2026_",
            "int_polymarket_wc2026_",
            "polymarket_wc2026_",
        )
    ):
        return DBT_SOURCE_POLYMARKET_WC2026
    return DBT_FALLBACK_SCHEMA


def shorten_model_name(model_name: str, source_slug: str) -> str:
    if source_slug == DBT_SOURCE_POLYMARKET_WC2026:
        return _polymarket_wc2026_subject(model_name)
    return model_name


def _polymarket_wc2026_layer(
    model_name: str,
    props: Mapping[str, object] | None = None,
    *,
    fqn: Sequence[str] | None = None,
) -> str:
    path_fqn = list(fqn or (props or {}).get("fqn") or ())
    for segment in path_fqn:
        if segment in {"staging", "intermediate", "marts", "observability"}:
            return segment
    if model_name.startswith("stg_polymarket_wc2026_"):
        return "staging"
    if model_name.startswith("int_polymarket_wc2026_"):
        return "intermediate"
    if model_name in POLYMARKET_WC2026_OBSERVABILITY_MODELS:
        return "observability"
    return "marts"


def _polymarket_wc2026_subject(model_name: str) -> str:
    for prefix in (
        "stg_polymarket_wc2026_",
        "int_polymarket_wc2026_",
        "polymarket_wc2026_",
    ):
        if model_name.startswith(prefix):
            return model_name[len(prefix) :]
    return model_name


def dbt_model_asset_key(
    props: Mapping[str, object],
    *,
    fqn: Sequence[str] | None = None,
) -> AssetKey:
    source = resolve_source_slug(props, fqn=fqn)
    name = str(props.get("name") or "")
    if source == DBT_SOURCE_POLYMARKET_WC2026:
        return asset_key(
            SOURCE_POLYMARKET,
            SCOPE_WC2026,
            _polymarket_wc2026_layer(name, props, fqn=fqn),
            _polymarket_wc2026_subject(name),
        )
    return AssetKey(f"{source}_{shorten_model_name(name, source)}")


def dbt_model_asset_key_for_name(
    model_name: str,
    source_slug: str,
    *,
    layer: str | None = None,
) -> AssetKey:
    if source_slug == DBT_SOURCE_POLYMARKET_WC2026:
        return asset_key(
            SOURCE_POLYMARKET,
            SCOPE_WC2026,
            layer or _polymarket_wc2026_layer(model_name),
            _polymarket_wc2026_subject(model_name),
        )
    return AssetKey(f"{source_slug}_{shorten_model_name(model_name, source_slug)}")


__all__ = [
    "DBT_FALLBACK_SCHEMA",
    "DBT_MODELED_SCHEMAS",
    "DBT_SOURCE_POLYMARKET_WC2026",
    "POLYMARKET_WC2026_INTERMEDIATE_SCHEMA",
    "POLYMARKET_WC2026_MARTS_SCHEMA",
    "POLYMARKET_WC2026_OBSERVABILITY_SCHEMA",
    "POLYMARKET_WC2026_STAGING_SCHEMA",
    "dbt_model_asset_key",
    "dbt_model_asset_key_for_name",
    "qualified_relation",
    "resolve_source_slug",
    "shorten_model_name",
]
