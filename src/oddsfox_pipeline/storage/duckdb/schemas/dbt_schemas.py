"""dbt-modeled DuckDB schema names and Dagster asset-key helpers."""

from __future__ import annotations

from typing import Final, Mapping, Sequence

from dagster import AssetKey

from oddsfox_pipeline.naming import (
    SCOPE_US_MIDTERMS_2026,
    SCOPE_WC2026,
    SOURCE_INTERNATIONAL_RESULTS,
    SOURCE_KALSHI,
    SOURCE_POLYMARKET,
    asset_key,
    schema_name,
)

DBT_SOURCE_INTERNATIONAL_RESULTS_WC2026: Final = "international_results_wc2026"
DBT_SOURCE_OPENFOOTBALL_WC2026: Final = "openfootball_wc2026"
DBT_SOURCE_KALSHI_WC2026: Final = "kalshi_wc2026"
DBT_SOURCE_POLYMARKET_WC2026: Final = "polymarket_wc2026"
DBT_SOURCE_POLYMARKET_US_MIDTERMS_2026: Final = "polymarket_us_midterms_2026"

_POLYMARKET_SOURCE_SCOPES: dict[str, str] = {
    DBT_SOURCE_POLYMARKET_WC2026: SCOPE_WC2026,
    DBT_SOURCE_POLYMARKET_US_MIDTERMS_2026: SCOPE_US_MIDTERMS_2026,
}
_KALSHI_SOURCE_SCOPES: dict[str, str] = {
    DBT_SOURCE_KALSHI_WC2026: SCOPE_WC2026,
}

INTERNATIONAL_RESULTS_WC2026_STAGING_SCHEMA: Final = schema_name(
    SOURCE_INTERNATIONAL_RESULTS, SCOPE_WC2026, "staging"
)
INTERNATIONAL_RESULTS_WC2026_INTERMEDIATE_SCHEMA: Final = schema_name(
    SOURCE_INTERNATIONAL_RESULTS, SCOPE_WC2026, "intermediate"
)
INTERNATIONAL_RESULTS_WC2026_MARTS_SCHEMA: Final = schema_name(
    SOURCE_INTERNATIONAL_RESULTS, SCOPE_WC2026, "marts"
)
INTERNATIONAL_RESULTS_WC2026_OBSERVABILITY_SCHEMA: Final = schema_name(
    SOURCE_INTERNATIONAL_RESULTS, SCOPE_WC2026, "observability"
)
OPENFOOTBALL_WC2026_STAGING_SCHEMA: Final = "openfootball_wc2026_staging"
WC2026_INTERMEDIATE_SCHEMA: Final = "wc2026_intermediate"
WC2026_MARTS_SCHEMA: Final = "wc2026_marts"
WC2026_OBSERVABILITY_SCHEMA: Final = "wc2026_observability"
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
KALSHI_WC2026_STAGING_SCHEMA: Final = schema_name(
    SOURCE_KALSHI, SCOPE_WC2026, "staging"
)
KALSHI_WC2026_INTERMEDIATE_SCHEMA: Final = schema_name(
    SOURCE_KALSHI, SCOPE_WC2026, "intermediate"
)
KALSHI_WC2026_MARTS_SCHEMA: Final = schema_name(SOURCE_KALSHI, SCOPE_WC2026, "marts")
KALSHI_WC2026_OBSERVABILITY_SCHEMA: Final = schema_name(
    SOURCE_KALSHI, SCOPE_WC2026, "observability"
)
POLYMARKET_US_MIDTERMS_2026_STAGING_SCHEMA: Final = schema_name(
    SOURCE_POLYMARKET, SCOPE_US_MIDTERMS_2026, "staging"
)
POLYMARKET_US_MIDTERMS_2026_INTERMEDIATE_SCHEMA: Final = schema_name(
    SOURCE_POLYMARKET, SCOPE_US_MIDTERMS_2026, "intermediate"
)
POLYMARKET_US_MIDTERMS_2026_MARTS_SCHEMA: Final = schema_name(
    SOURCE_POLYMARKET, SCOPE_US_MIDTERMS_2026, "marts"
)
POLYMARKET_US_MIDTERMS_2026_OBSERVABILITY_SCHEMA: Final = schema_name(
    SOURCE_POLYMARKET, SCOPE_US_MIDTERMS_2026, "observability"
)
DBT_FALLBACK_SCHEMA: Final = "dbt"
POLYMARKET_WC2026_OBSERVABILITY_MODELS: Final[tuple[str, ...]] = (
    "polymarket_wc2026_knockout_stage_coverage",
    "polymarket_wc2026_knockout_data_quality",
    "polymarket_wc2026_sync_run_observability",
)
KALSHI_WC2026_OBSERVABILITY_MODELS: Final[tuple[str, ...]] = (
    "kalshi_wc2026_stage_coverage",
    "kalshi_wc2026_data_quality",
    "kalshi_wc2026_sync_run_observability",
)
POLYMARKET_US_MIDTERMS_2026_OBSERVABILITY_MODELS: Final[tuple[str, ...]] = (
    "polymarket_us_midterms_2026_sync_run_observability",
)
INTERNATIONAL_RESULTS_WC2026_OBSERVABILITY_MODELS: Final[tuple[str, ...]] = (
    "international_results_wc2026_data_quality",
)

DBT_MODELED_SCHEMAS: Final[tuple[str, ...]] = (
    INTERNATIONAL_RESULTS_WC2026_STAGING_SCHEMA,
    INTERNATIONAL_RESULTS_WC2026_INTERMEDIATE_SCHEMA,
    INTERNATIONAL_RESULTS_WC2026_MARTS_SCHEMA,
    INTERNATIONAL_RESULTS_WC2026_OBSERVABILITY_SCHEMA,
    OPENFOOTBALL_WC2026_STAGING_SCHEMA,
    WC2026_INTERMEDIATE_SCHEMA,
    WC2026_MARTS_SCHEMA,
    WC2026_OBSERVABILITY_SCHEMA,
    POLYMARKET_WC2026_STAGING_SCHEMA,
    POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
    POLYMARKET_WC2026_MARTS_SCHEMA,
    POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
    KALSHI_WC2026_STAGING_SCHEMA,
    KALSHI_WC2026_INTERMEDIATE_SCHEMA,
    KALSHI_WC2026_MARTS_SCHEMA,
    KALSHI_WC2026_OBSERVABILITY_SCHEMA,
    POLYMARKET_US_MIDTERMS_2026_STAGING_SCHEMA,
    POLYMARKET_US_MIDTERMS_2026_INTERMEDIATE_SCHEMA,
    POLYMARKET_US_MIDTERMS_2026_MARTS_SCHEMA,
    POLYMARKET_US_MIDTERMS_2026_OBSERVABILITY_SCHEMA,
)

DBT_EXPECTED_RELATIONS: Final[tuple[tuple[str, str], ...]] = (
    (
        OPENFOOTBALL_WC2026_STAGING_SCHEMA,
        "stg_openfootball_wc2026_knockout_fixtures",
    ),
    (WC2026_INTERMEDIATE_SCHEMA, "int_wc2026_knockout_fixtures"),
    (WC2026_MARTS_SCHEMA, "wc2026_knockout_match_hourly_odds"),
    (
        WC2026_OBSERVABILITY_SCHEMA,
        "wc2026_knockout_match_odds_coverage",
    ),
    (
        WC2026_OBSERVABILITY_SCHEMA,
        "wc2026_knockout_match_odds_data_quality",
    ),
    (
        INTERNATIONAL_RESULTS_WC2026_STAGING_SCHEMA,
        "stg_international_results_wc2026_match_results",
    ),
    (
        INTERNATIONAL_RESULTS_WC2026_STAGING_SCHEMA,
        "international_results_wc2026_team_aliases",
    ),
    (
        INTERNATIONAL_RESULTS_WC2026_INTERMEDIATE_SCHEMA,
        "int_international_results_wc2026_match_teams",
    ),
    (
        INTERNATIONAL_RESULTS_WC2026_MARTS_SCHEMA,
        "international_results_wc2026_matches",
    ),
    (
        INTERNATIONAL_RESULTS_WC2026_MARTS_SCHEMA,
        "international_results_wc2026_team_status",
    ),
    (
        INTERNATIONAL_RESULTS_WC2026_OBSERVABILITY_SCHEMA,
        "international_results_wc2026_data_quality",
    ),
    (POLYMARKET_WC2026_STAGING_SCHEMA, "stg_polymarket_wc2026_markets"),
    (POLYMARKET_WC2026_STAGING_SCHEMA, "stg_polymarket_wc2026_market_tokens"),
    (POLYMARKET_WC2026_STAGING_SCHEMA, "stg_polymarket_wc2026_odds"),
    (POLYMARKET_WC2026_STAGING_SCHEMA, "stg_polymarket_wc2026_odds_daily"),
    (POLYMARKET_WC2026_STAGING_SCHEMA, "stg_polymarket_wc2026_pipeline_run_events"),
    (POLYMARKET_WC2026_STAGING_SCHEMA, "stg_polymarket_wc2026_sync_ledger"),
    (POLYMARKET_WC2026_STAGING_SCHEMA, "stg_polymarket_wc2026_token_sync_skips"),
    (POLYMARKET_WC2026_STAGING_SCHEMA, "polymarket_wc2026_contract"),
    (POLYMARKET_WC2026_INTERMEDIATE_SCHEMA, "int_polymarket_wc2026_markets"),
    (POLYMARKET_WC2026_INTERMEDIATE_SCHEMA, "int_polymarket_wc2026_token_universe"),
    (POLYMARKET_WC2026_INTERMEDIATE_SCHEMA, "int_polymarket_wc2026_market_tokens"),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_token_hourly_odds",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_knockout_market_classification",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_match_advance_tokens",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_match_hourly_odds",
    ),
    (POLYMARKET_WC2026_MARTS_SCHEMA, "polymarket_wc2026_knockout_market_tokens"),
    (POLYMARKET_WC2026_MARTS_SCHEMA, "polymarket_wc2026_knockout_markets"),
    (
        POLYMARKET_WC2026_MARTS_SCHEMA,
        "polymarket_wc2026_knockout_token_hourly_odds",
    ),
    (POLYMARKET_WC2026_MARTS_SCHEMA, "polymarket_wc2026_graph_token_hourly_odds"),
    (
        POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
        "polymarket_wc2026_knockout_stage_coverage",
    ),
    (
        POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
        "polymarket_wc2026_knockout_data_quality",
    ),
    (
        POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
        "polymarket_wc2026_sync_run_observability",
    ),
    (
        POLYMARKET_US_MIDTERMS_2026_STAGING_SCHEMA,
        "stg_polymarket_us_midterms_2026_markets",
    ),
    (
        POLYMARKET_US_MIDTERMS_2026_STAGING_SCHEMA,
        "stg_polymarket_us_midterms_2026_market_tokens",
    ),
    (
        POLYMARKET_US_MIDTERMS_2026_STAGING_SCHEMA,
        "stg_polymarket_us_midterms_2026_odds",
    ),
    (
        POLYMARKET_US_MIDTERMS_2026_STAGING_SCHEMA,
        "stg_polymarket_us_midterms_2026_odds_daily",
    ),
    (
        POLYMARKET_US_MIDTERMS_2026_STAGING_SCHEMA,
        "stg_polymarket_us_midterms_2026_pipeline_run_events",
    ),
    (
        POLYMARKET_US_MIDTERMS_2026_STAGING_SCHEMA,
        "stg_polymarket_us_midterms_2026_sync_ledger",
    ),
    (
        POLYMARKET_US_MIDTERMS_2026_STAGING_SCHEMA,
        "stg_polymarket_us_midterms_2026_token_sync_skips",
    ),
    (
        POLYMARKET_US_MIDTERMS_2026_STAGING_SCHEMA,
        "polymarket_us_midterms_2026_contract",
    ),
    (
        POLYMARKET_US_MIDTERMS_2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_us_midterms_2026_markets",
    ),
    (
        POLYMARKET_US_MIDTERMS_2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_us_midterms_2026_token_universe",
    ),
    (
        POLYMARKET_US_MIDTERMS_2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_us_midterms_2026_market_tokens",
    ),
    (
        POLYMARKET_US_MIDTERMS_2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_us_midterms_2026_token_hourly_odds",
    ),
    (
        POLYMARKET_US_MIDTERMS_2026_MARTS_SCHEMA,
        "polymarket_us_midterms_2026_market_token_hourly_odds",
    ),
    (
        POLYMARKET_US_MIDTERMS_2026_OBSERVABILITY_SCHEMA,
        "polymarket_us_midterms_2026_sync_run_observability",
    ),
    (KALSHI_WC2026_STAGING_SCHEMA, "stg_kalshi_wc2026_events"),
    (KALSHI_WC2026_STAGING_SCHEMA, "stg_kalshi_wc2026_markets"),
    (
        KALSHI_WC2026_STAGING_SCHEMA,
        "stg_kalshi_wc2026_market_candlesticks_hourly",
    ),
    (KALSHI_WC2026_STAGING_SCHEMA, "kalshi_wc2026_contract"),
    (KALSHI_WC2026_INTERMEDIATE_SCHEMA, "int_kalshi_wc2026_markets"),
    (KALSHI_WC2026_INTERMEDIATE_SCHEMA, "int_kalshi_wc2026_market_hourly_odds"),
    (
        KALSHI_WC2026_INTERMEDIATE_SCHEMA,
        "int_kalshi_wc2026_stage_classification",
    ),
    (
        KALSHI_WC2026_INTERMEDIATE_SCHEMA,
        "int_kalshi_wc2026_group_winner_classification",
    ),
    (
        KALSHI_WC2026_INTERMEDIATE_SCHEMA,
        "int_kalshi_wc2026_match_advance_markets",
    ),
    (
        KALSHI_WC2026_INTERMEDIATE_SCHEMA,
        "int_kalshi_wc2026_match_hourly_odds",
    ),
    (KALSHI_WC2026_MARTS_SCHEMA, "kalshi_wc2026_stage_markets"),
    (KALSHI_WC2026_MARTS_SCHEMA, "kalshi_wc2026_stage_market_hourly_odds"),
    (KALSHI_WC2026_MARTS_SCHEMA, "kalshi_wc2026_group_winner_markets"),
    (
        KALSHI_WC2026_MARTS_SCHEMA,
        "kalshi_wc2026_group_winner_market_hourly_odds",
    ),
    (KALSHI_WC2026_OBSERVABILITY_SCHEMA, "kalshi_wc2026_stage_coverage"),
    (KALSHI_WC2026_OBSERVABILITY_SCHEMA, "kalshi_wc2026_data_quality"),
    (
        KALSHI_WC2026_OBSERVABILITY_SCHEMA,
        "kalshi_wc2026_sync_run_observability",
    ),
)


def qualified_relation(schema: str, model_name: str) -> str:
    return f"{schema}.{model_name}"


def _kalshi_source_slug(model_name: str) -> str | None:
    if model_name.startswith(
        (
            "stg_kalshi_wc2026_",
            "int_kalshi_wc2026_",
            "kalshi_wc2026_",
        )
    ):
        return DBT_SOURCE_KALSHI_WC2026
    return None


def _polymarket_source_slug(model_name: str) -> str | None:
    if model_name.startswith("stg_polymarket_us_midterms_2026_"):
        return DBT_SOURCE_POLYMARKET_US_MIDTERMS_2026
    if model_name.startswith(
        (
            "stg_polymarket_wc2026_",
            "int_polymarket_wc2026_",
            "polymarket_wc2026_",
        )
    ):
        return DBT_SOURCE_POLYMARKET_WC2026
    if model_name.startswith(
        (
            "int_polymarket_us_midterms_2026_",
            "polymarket_us_midterms_2026_",
        )
    ):
        return DBT_SOURCE_POLYMARKET_US_MIDTERMS_2026
    return None


def resolve_source_slug(
    props: Mapping[str, object],
    *,
    fqn: Sequence[str] | None = None,
) -> str:
    tags = set(props.get("tags") or ())
    path_fqn = list(fqn or props.get("fqn") or ())
    if len(path_fqn) >= 2 and path_fqn[1] in _POLYMARKET_SOURCE_SCOPES:
        return path_fqn[1]
    if len(path_fqn) >= 2 and path_fqn[1] in _KALSHI_SOURCE_SCOPES:
        return path_fqn[1]
    if len(path_fqn) >= 2 and path_fqn[1] == "international_results_wc2026":
        return DBT_SOURCE_INTERNATIONAL_RESULTS_WC2026
    name = str(props.get("name") or "")
    kalshi_slug = _kalshi_source_slug(name)
    if kalshi_slug is not None:
        return kalshi_slug
    polymarket_slug = _polymarket_source_slug(name)
    if polymarket_slug is not None:
        return polymarket_slug
    if "international_results" in tags or (
        len(path_fqn) >= 2 and path_fqn[1] == "international_results_wc2026"
    ):
        return DBT_SOURCE_INTERNATIONAL_RESULTS_WC2026
    if name.startswith(
        (
            "stg_international_results_wc2026_",
            "int_international_results_wc2026_",
            "international_results_wc2026_",
        )
    ):
        return DBT_SOURCE_INTERNATIONAL_RESULTS_WC2026
    return DBT_FALLBACK_SCHEMA


def _polymarket_layer(
    model_name: str,
    props: Mapping[str, object] | None = None,
    *,
    fqn: Sequence[str] | None = None,
    observability_models: tuple[str, ...],
    staging_prefix: str,
    intermediate_prefix: str,
) -> str:
    path_fqn = list(fqn or (props or {}).get("fqn") or ())
    for segment in path_fqn:
        if segment in {"staging", "intermediate", "marts", "observability"}:
            return segment
    if model_name.startswith(staging_prefix):
        return "staging"
    if model_name.startswith(intermediate_prefix):
        return "intermediate"
    if model_name in observability_models:
        return "observability"
    return "marts"


def _polymarket_subject(
    model_name: str,
    *,
    staging_prefix: str,
    intermediate_prefix: str,
    mart_prefix: str,
) -> str:
    for prefix in (staging_prefix, intermediate_prefix, mart_prefix):
        if model_name.startswith(prefix):
            return model_name[len(prefix) :]
    return model_name


def _kalshi_wc2026_layer(
    model_name: str,
    props: Mapping[str, object] | None = None,
    *,
    fqn: Sequence[str] | None = None,
) -> str:
    return _polymarket_layer(
        model_name,
        props,
        fqn=fqn,
        observability_models=KALSHI_WC2026_OBSERVABILITY_MODELS,
        staging_prefix="stg_kalshi_wc2026_",
        intermediate_prefix="int_kalshi_wc2026_",
    )


def _polymarket_wc2026_layer(
    model_name: str,
    props: Mapping[str, object] | None = None,
    *,
    fqn: Sequence[str] | None = None,
) -> str:
    return _polymarket_layer(
        model_name,
        props,
        fqn=fqn,
        observability_models=POLYMARKET_WC2026_OBSERVABILITY_MODELS,
        staging_prefix="stg_polymarket_wc2026_",
        intermediate_prefix="int_polymarket_wc2026_",
    )


def _polymarket_us_midterms_2026_layer(
    model_name: str,
    props: Mapping[str, object] | None = None,
    *,
    fqn: Sequence[str] | None = None,
) -> str:
    return _polymarket_layer(
        model_name,
        props,
        fqn=fqn,
        observability_models=POLYMARKET_US_MIDTERMS_2026_OBSERVABILITY_MODELS,
        staging_prefix="stg_polymarket_us_midterms_2026_",
        intermediate_prefix="int_polymarket_us_midterms_2026_",
    )


def _international_results_wc2026_layer(
    model_name: str,
    props: Mapping[str, object] | None = None,
    *,
    fqn: Sequence[str] | None = None,
) -> str:
    path_fqn = list(fqn or (props or {}).get("fqn") or ())
    for segment in path_fqn:
        if segment in {"staging", "intermediate", "marts", "observability"}:
            return segment
    if model_name.startswith("stg_international_results_wc2026_"):
        return "staging"
    if model_name.startswith("int_international_results_wc2026_"):
        return "intermediate"
    if model_name == "international_results_wc2026_team_aliases":
        return "staging"
    if model_name in INTERNATIONAL_RESULTS_WC2026_OBSERVABILITY_MODELS:
        return "observability"
    return "marts"


def _kalshi_wc2026_subject(model_name: str) -> str:
    return _polymarket_subject(
        model_name,
        staging_prefix="stg_kalshi_wc2026_",
        intermediate_prefix="int_kalshi_wc2026_",
        mart_prefix="kalshi_wc2026_",
    )


def _polymarket_wc2026_subject(model_name: str) -> str:
    return _polymarket_subject(
        model_name,
        staging_prefix="stg_polymarket_wc2026_",
        intermediate_prefix="int_polymarket_wc2026_",
        mart_prefix="polymarket_wc2026_",
    )


def _polymarket_us_midterms_2026_subject(model_name: str) -> str:
    return _polymarket_subject(
        model_name,
        staging_prefix="stg_polymarket_us_midterms_2026_",
        intermediate_prefix="int_polymarket_us_midterms_2026_",
        mart_prefix="polymarket_us_midterms_2026_",
    )


def _international_results_wc2026_subject(model_name: str) -> str:
    for prefix in (
        "stg_international_results_wc2026_",
        "int_international_results_wc2026_",
        "international_results_wc2026_",
    ):
        if model_name.startswith(prefix):
            return model_name[len(prefix) :]
    return model_name


def shorten_model_name(model_name: str, source_slug: str) -> str:
    if source_slug == DBT_SOURCE_INTERNATIONAL_RESULTS_WC2026:
        return _international_results_wc2026_subject(model_name)
    if source_slug == DBT_SOURCE_KALSHI_WC2026:
        return _kalshi_wc2026_subject(model_name)
    if source_slug == DBT_SOURCE_POLYMARKET_WC2026:
        return _polymarket_wc2026_subject(model_name)
    if source_slug == DBT_SOURCE_POLYMARKET_US_MIDTERMS_2026:
        return _polymarket_us_midterms_2026_subject(model_name)
    return model_name


def dbt_model_asset_key_for_name(
    model_name: str,
    source_slug: str,
    *,
    layer: str | None = None,
    props: Mapping[str, object] | None = None,
    fqn: Sequence[str] | None = None,
) -> AssetKey:
    if source_slug == DBT_SOURCE_INTERNATIONAL_RESULTS_WC2026:
        return asset_key(
            SOURCE_INTERNATIONAL_RESULTS,
            SCOPE_WC2026,
            layer or _international_results_wc2026_layer(model_name, props, fqn=fqn),
            _international_results_wc2026_subject(model_name),
        )
    if source_slug == DBT_SOURCE_KALSHI_WC2026:
        return asset_key(
            SOURCE_KALSHI,
            SCOPE_WC2026,
            layer or _kalshi_wc2026_layer(model_name, props, fqn=fqn),
            _kalshi_wc2026_subject(model_name),
        )
    if source_slug == DBT_SOURCE_POLYMARKET_WC2026:
        return asset_key(
            SOURCE_POLYMARKET,
            SCOPE_WC2026,
            layer or _polymarket_wc2026_layer(model_name, props, fqn=fqn),
            _polymarket_wc2026_subject(model_name),
        )
    if source_slug == DBT_SOURCE_POLYMARKET_US_MIDTERMS_2026:
        return asset_key(
            SOURCE_POLYMARKET,
            SCOPE_US_MIDTERMS_2026,
            layer or _polymarket_us_midterms_2026_layer(model_name, props, fqn=fqn),
            _polymarket_us_midterms_2026_subject(model_name),
        )
    return AssetKey(f"{source_slug}_{shorten_model_name(model_name, source_slug)}")


def dbt_model_asset_key(
    props: Mapping[str, object],
    *,
    fqn: Sequence[str] | None = None,
) -> AssetKey:
    source = resolve_source_slug(props, fqn=fqn)
    name = str(props.get("name") or "")
    return dbt_model_asset_key_for_name(name, source, props=props, fqn=fqn)


__all__ = [
    "DBT_FALLBACK_SCHEMA",
    "DBT_EXPECTED_RELATIONS",
    "DBT_MODELED_SCHEMAS",
    "DBT_SOURCE_INTERNATIONAL_RESULTS_WC2026",
    "DBT_SOURCE_KALSHI_WC2026",
    "DBT_SOURCE_OPENFOOTBALL_WC2026",
    "DBT_SOURCE_POLYMARKET_US_MIDTERMS_2026",
    "DBT_SOURCE_POLYMARKET_WC2026",
    "INTERNATIONAL_RESULTS_WC2026_INTERMEDIATE_SCHEMA",
    "INTERNATIONAL_RESULTS_WC2026_MARTS_SCHEMA",
    "INTERNATIONAL_RESULTS_WC2026_OBSERVABILITY_SCHEMA",
    "INTERNATIONAL_RESULTS_WC2026_STAGING_SCHEMA",
    "OPENFOOTBALL_WC2026_STAGING_SCHEMA",
    "KALSHI_WC2026_INTERMEDIATE_SCHEMA",
    "KALSHI_WC2026_MARTS_SCHEMA",
    "KALSHI_WC2026_OBSERVABILITY_SCHEMA",
    "KALSHI_WC2026_STAGING_SCHEMA",
    "POLYMARKET_US_MIDTERMS_2026_INTERMEDIATE_SCHEMA",
    "POLYMARKET_US_MIDTERMS_2026_MARTS_SCHEMA",
    "POLYMARKET_US_MIDTERMS_2026_OBSERVABILITY_SCHEMA",
    "POLYMARKET_US_MIDTERMS_2026_STAGING_SCHEMA",
    "POLYMARKET_WC2026_INTERMEDIATE_SCHEMA",
    "POLYMARKET_WC2026_MARTS_SCHEMA",
    "POLYMARKET_WC2026_OBSERVABILITY_SCHEMA",
    "POLYMARKET_WC2026_STAGING_SCHEMA",
    "WC2026_INTERMEDIATE_SCHEMA",
    "WC2026_MARTS_SCHEMA",
    "WC2026_OBSERVABILITY_SCHEMA",
    "dbt_model_asset_key",
    "dbt_model_asset_key_for_name",
    "qualified_relation",
    "resolve_source_slug",
    "shorten_model_name",
]
