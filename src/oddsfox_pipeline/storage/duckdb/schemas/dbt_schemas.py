"""dbt-modeled DuckDB schema names and Dagster asset-key helpers."""

from __future__ import annotations

from typing import Final, Mapping, Sequence

from dagster import AssetKey

from oddsfox_pipeline.naming import (
    SCOPE_SOCCER,
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
DBT_SOURCE_POLYMARKET_SOCCER: Final = "polymarket_soccer"
DBT_SOURCE_POLYMARKET_CATALOG: Final = "polymarket_catalog"
DBT_SOURCE_WC2026: Final = "wc2026"

_POLYMARKET_SOURCE_SCOPES: dict[str, str] = {
    DBT_SOURCE_POLYMARKET_WC2026: SCOPE_WC2026,
    DBT_SOURCE_POLYMARKET_SOCCER: SCOPE_SOCCER,
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
WC2026_STAGING_SCHEMA: Final = "wc2026_staging"
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
POLYMARKET_SOCCER_STAGING_SCHEMA: Final = schema_name(
    SOURCE_POLYMARKET, SCOPE_SOCCER, "staging"
)
POLYMARKET_SOCCER_INTERMEDIATE_SCHEMA: Final = schema_name(
    SOURCE_POLYMARKET, SCOPE_SOCCER, "intermediate"
)
POLYMARKET_SOCCER_MARTS_SCHEMA: Final = schema_name(
    SOURCE_POLYMARKET, SCOPE_SOCCER, "marts"
)
POLYMARKET_SOCCER_OBSERVABILITY_SCHEMA: Final = schema_name(
    SOURCE_POLYMARKET, SCOPE_SOCCER, "observability"
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
POLYMARKET_CATALOG_STAGING_SCHEMA: Final = "polymarket_catalog_staging"
DBT_FALLBACK_SCHEMA: Final = "dbt"
POLYMARKET_WC2026_OBSERVABILITY_MODELS: Final[tuple[str, ...]] = (
    "polymarket_wc2026_match_order_book_data_quality",
    "polymarket_wc2026_match_order_book_quality_issues",
    "polymarket_wc2026_match_minute_odds_data_quality",
    "polymarket_wc2026_market_minute_odds_data_quality",
    "polymarket_wc2026_polygon_settlement_data_quality",
    "polymarket_wc2026_polygon_settlement_quality_issues",
    "polymarket_wc2026_polygon_settlement_token_coverage",
    "polymarket_wc2026_ingestion_run_observability",
)
KALSHI_WC2026_OBSERVABILITY_MODELS: Final[tuple[str, ...]] = (
    "kalshi_wc2026_stage_coverage",
    "kalshi_wc2026_data_quality",
    "kalshi_wc2026_ingestion_run_observability",
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
    WC2026_STAGING_SCHEMA,
    WC2026_INTERMEDIATE_SCHEMA,
    WC2026_MARTS_SCHEMA,
    WC2026_OBSERVABILITY_SCHEMA,
    POLYMARKET_CATALOG_STAGING_SCHEMA,
    POLYMARKET_WC2026_STAGING_SCHEMA,
    POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
    POLYMARKET_WC2026_MARTS_SCHEMA,
    POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
    POLYMARKET_SOCCER_STAGING_SCHEMA,
    POLYMARKET_SOCCER_INTERMEDIATE_SCHEMA,
    POLYMARKET_SOCCER_MARTS_SCHEMA,
    POLYMARKET_SOCCER_OBSERVABILITY_SCHEMA,
    KALSHI_WC2026_STAGING_SCHEMA,
    KALSHI_WC2026_INTERMEDIATE_SCHEMA,
    KALSHI_WC2026_MARTS_SCHEMA,
    KALSHI_WC2026_OBSERVABILITY_SCHEMA,
)

DBT_EXPECTED_RELATIONS: Final[tuple[tuple[str, str], ...]] = (
    (
        OPENFOOTBALL_WC2026_STAGING_SCHEMA,
        "stg_openfootball_wc2026_schedule_fixtures",
    ),
    (WC2026_STAGING_SCHEMA, "wc2026_schedule_matches"),
    (WC2026_STAGING_SCHEMA, "wc2026_third_place_options"),
    (WC2026_STAGING_SCHEMA, "wc2026_base_camps_teams"),
    (WC2026_STAGING_SCHEMA, "wc2026_venues"),
    (WC2026_STAGING_SCHEMA, "wc2026_team_canonical_aliases"),
    (WC2026_STAGING_SCHEMA, "wc2026_tournament_classification"),
    (WC2026_MARTS_SCHEMA, "wc2026_base_camp_venues"),
    (WC2026_MARTS_SCHEMA, "wc2026_club_strength_current"),
    (WC2026_MARTS_SCHEMA, "wc2026_club_strength_history"),
    (WC2026_MARTS_SCHEMA, "wc2026_club_strength_snapshot"),
    (WC2026_MARTS_SCHEMA, "wc2026_contract_metadata"),
    (WC2026_MARTS_SCHEMA, "wc2026_event_state_timing"),
    (WC2026_MARTS_SCHEMA, "wc2026_fixtures"),
    (WC2026_MARTS_SCHEMA, "wc2026_international_matches"),
    (WC2026_MARTS_SCHEMA, "wc2026_player_features"),
    (WC2026_MARTS_SCHEMA, "wc2026_price_liquidity_current"),
    (WC2026_MARTS_SCHEMA, "wc2026_price_liquidity_history"),
    (WC2026_MARTS_SCHEMA, "wc2026_results"),
    (WC2026_MARTS_SCHEMA, "wc2026_source_provenance"),
    (WC2026_MARTS_SCHEMA, "wc2026_squad_player_features"),
    (WC2026_MARTS_SCHEMA, "wc2026_team_identities"),
    (WC2026_MARTS_SCHEMA, "wc2026_team_ratings_current"),
    (WC2026_MARTS_SCHEMA, "wc2026_team_ratings_history"),
    (WC2026_MARTS_SCHEMA, "wc2026_team_ratings_pre_match"),
    (WC2026_MARTS_SCHEMA, "wc2026_third_place_lookup"),
    (WC2026_MARTS_SCHEMA, "wc2026_third_place_slot_assignments"),
    (WC2026_MARTS_SCHEMA, "wc2026_travel_features"),
    (WC2026_MARTS_SCHEMA, "wc2026_venue_markets"),
    (WC2026_OBSERVABILITY_SCHEMA, "wc2026_source_availability"),
    (WC2026_OBSERVABILITY_SCHEMA, "wc2026_strategy_input_readiness"),
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
    (POLYMARKET_CATALOG_STAGING_SCHEMA, "stg_polymarket_catalog_markets"),
    (POLYMARKET_WC2026_STAGING_SCHEMA, "stg_polymarket_wc2026_markets"),
    (POLYMARKET_WC2026_STAGING_SCHEMA, "stg_polymarket_wc2026_event_snapshots"),
    (
        POLYMARKET_WC2026_STAGING_SCHEMA,
        "stg_polymarket_wc2026_event_market_snapshots",
    ),
    (
        POLYMARKET_WC2026_STAGING_SCHEMA,
        "stg_polymarket_wc2026_match_minute_odds_history",
    ),
    (
        POLYMARKET_WC2026_STAGING_SCHEMA,
        "stg_polymarket_wc2026_match_minute_fetch_audit",
    ),
    (
        POLYMARKET_WC2026_STAGING_SCHEMA,
        "stg_polymarket_wc2026_futures_minute_odds_history",
    ),
    (
        POLYMARKET_WC2026_STAGING_SCHEMA,
        "stg_polymarket_wc2026_futures_minute_fetch_audit",
    ),
    (
        POLYMARKET_WC2026_STAGING_SCHEMA,
        "stg_polymarket_wc2026_match_order_book_snapshots",
    ),
    (POLYMARKET_WC2026_STAGING_SCHEMA, "stg_polymarket_wc2026_market_tokens"),
    (POLYMARKET_WC2026_STAGING_SCHEMA, "stg_polymarket_wc2026_odds"),
    (POLYMARKET_WC2026_STAGING_SCHEMA, "stg_polymarket_wc2026_odds_daily"),
    (POLYMARKET_WC2026_STAGING_SCHEMA, "stg_polymarket_wc2026_ingestion_run_events"),
    (POLYMARKET_WC2026_STAGING_SCHEMA, "stg_polymarket_wc2026_sync_ledger"),
    (POLYMARKET_WC2026_STAGING_SCHEMA, "stg_polymarket_wc2026_token_sync_skips"),
    (
        POLYMARKET_WC2026_STAGING_SCHEMA,
        "stg_polymarket_wc2026_polygon_settlement_markets",
    ),
    (
        POLYMARKET_WC2026_STAGING_SCHEMA,
        "stg_polymarket_wc2026_polygon_settlement_fills",
    ),
    (
        POLYMARKET_WC2026_STAGING_SCHEMA,
        "stg_polymarket_wc2026_polygon_settlement_scan_runs",
    ),
    (
        POLYMARKET_WC2026_STAGING_SCHEMA,
        "stg_polymarket_wc2026_polygon_settlement_scan_chunks",
    ),
    (
        POLYMARKET_WC2026_STAGING_SCHEMA,
        "polymarket_wc2026_polygon_settlement_markets",
    ),
    (POLYMARKET_WC2026_STAGING_SCHEMA, "polymarket_wc2026_pipeline_policy"),
    (POLYMARKET_WC2026_INTERMEDIATE_SCHEMA, "int_polymarket_wc2026_markets"),
    (POLYMARKET_WC2026_INTERMEDIATE_SCHEMA, "int_polymarket_wc2026_event_latest"),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_primary_market_token",
    ),
    (POLYMARKET_WC2026_INTERMEDIATE_SCHEMA, "int_polymarket_wc2026_token_working_set"),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_token_hourly_odds",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_match_working_set",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_match_token_minute_odds",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_match_minute_odds_candidate",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_match_minute_publication_gate",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_futures_token_minute_odds",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_token_minute_odds",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_match_order_book_levels",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_match_order_book_publication_gate",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_match_trade_publication_gate",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_polygon_settlement_working_set",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_polygon_settlement_token_minute_odds",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_polygon_settlement_minute_odds_candidate",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_polygon_settlement_publication_gate",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_polygon_settlement_seed_quality_summary",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_polygon_settlement_latest_published_scan",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_polygon_settlement_scan_quality_summary",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_polygon_settlement_raw_quality_summary",
    ),
    (
        POLYMARKET_WC2026_INTERMEDIATE_SCHEMA,
        "int_polymarket_wc2026_polygon_settlement_minute_quality_summary",
    ),
    (
        POLYMARKET_WC2026_MARTS_SCHEMA,
        "polymarket_wc2026_match_minute_odds",
    ),
    (
        POLYMARKET_WC2026_MARTS_SCHEMA,
        "polymarket_wc2026_market_minute_odds",
    ),
    (
        POLYMARKET_WC2026_MARTS_SCHEMA,
        "polymarket_wc2026_match_order_book",
    ),
    (
        POLYMARKET_WC2026_MARTS_SCHEMA,
        "polymarket_wc2026_match_order_book_states",
    ),
    (
        POLYMARKET_WC2026_MARTS_SCHEMA,
        "polymarket_wc2026_match_trades",
    ),
    (
        POLYMARKET_WC2026_MARTS_SCHEMA,
        "polymarket_wc2026_polygon_settlement_minute_odds",
    ),
    (
        POLYMARKET_WC2026_MARTS_SCHEMA,
        "polymarket_wc2026_market_hourly_odds",
    ),
    (
        POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
        "polymarket_wc2026_match_minute_odds_data_quality",
    ),
    (
        POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
        "polymarket_wc2026_market_minute_odds_data_quality",
    ),
    (
        POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
        "polymarket_wc2026_match_order_book_data_quality",
    ),
    (
        POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
        "polymarket_wc2026_match_order_book_quality_issues",
    ),
    (
        POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
        "polymarket_wc2026_match_minute_token_coverage",
    ),
    (
        POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
        "polymarket_wc2026_match_minute_odds_quality_issues",
    ),
    (
        POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
        "polymarket_wc2026_polygon_settlement_data_quality",
    ),
    (
        POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
        "polymarket_wc2026_polygon_settlement_quality_issues",
    ),
    (
        POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
        "polymarket_wc2026_polygon_settlement_token_coverage",
    ),
    (
        POLYMARKET_WC2026_OBSERVABILITY_SCHEMA,
        "polymarket_wc2026_ingestion_run_observability",
    ),
    (POLYMARKET_SOCCER_STAGING_SCHEMA, "stg_polymarket_soccer_event_latest"),
    (
        POLYMARKET_SOCCER_STAGING_SCHEMA,
        "stg_polymarket_soccer_match_minute_audit_latest",
    ),
    (
        POLYMARKET_SOCCER_STAGING_SCHEMA,
        "stg_polymarket_soccer_match_minute_audit_latest_published_success",
    ),
    (
        POLYMARKET_SOCCER_STAGING_SCHEMA,
        "stg_polymarket_soccer_match_primary_minute_ohlc",
    ),
    (
        POLYMARKET_SOCCER_STAGING_SCHEMA,
        "stg_polymarket_soccer_match_result_registry",
    ),
    (
        POLYMARKET_SOCCER_INTERMEDIATE_SCHEMA,
        "int_polymarket_soccer_match_result_market_state",
    ),
    (
        POLYMARKET_SOCCER_INTERMEDIATE_SCHEMA,
        "int_polymarket_soccer_match_result_observed",
    ),
    (
        POLYMARKET_SOCCER_INTERMEDIATE_SCHEMA,
        "int_polymarket_soccer_match_result_minute_odds",
    ),
    (POLYMARKET_SOCCER_MARTS_SCHEMA, "polymarket_soccer_matches"),
    (
        POLYMARKET_SOCCER_MARTS_SCHEMA,
        "polymarket_soccer_match_result_minute_odds_observed",
    ),
    (
        POLYMARKET_SOCCER_MARTS_SCHEMA,
        "polymarket_soccer_match_result_minute_odds",
    ),
    (
        POLYMARKET_SOCCER_OBSERVABILITY_SCHEMA,
        "polymarket_soccer_match_result_data_quality",
    ),
    (
        POLYMARKET_SOCCER_OBSERVABILITY_SCHEMA,
        "polymarket_soccer_match_result_exclusions",
    ),
    (
        POLYMARKET_SOCCER_OBSERVABILITY_SCHEMA,
        "polymarket_soccer_match_result_token_fetch_status",
    ),
    (
        POLYMARKET_SOCCER_OBSERVABILITY_SCHEMA,
        "polymarket_soccer_pipeline_trends",
    ),
    (
        POLYMARKET_SOCCER_OBSERVABILITY_SCHEMA,
        "polymarket_soccer_pipeline_alerts",
    ),
    (
        POLYMARKET_SOCCER_OBSERVABILITY_SCHEMA,
        "polymarket_soccer_pipeline_health",
    ),
    (KALSHI_WC2026_STAGING_SCHEMA, "stg_kalshi_wc2026_events"),
    (KALSHI_WC2026_STAGING_SCHEMA, "stg_kalshi_wc2026_markets"),
    (
        KALSHI_WC2026_STAGING_SCHEMA,
        "stg_kalshi_wc2026_market_candlesticks_hourly",
    ),
    (KALSHI_WC2026_STAGING_SCHEMA, "kalshi_wc2026_pipeline_policy"),
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
        "kalshi_wc2026_ingestion_run_observability",
    ),
)


# Strategy marts set config(alias=...); DuckDB physical names omit the wc2026_ prefix.
WC2026_MART_RELATION_ALIASES: Final[dict[str, str]] = {
    "wc2026_base_camp_venues": "base_camp_venues",
    "wc2026_club_strength_current": "club_strength_current",
    "wc2026_club_strength_history": "club_strength_history",
    "wc2026_club_strength_snapshot": "club_strength_snapshot",
    "wc2026_contract_metadata": "contract_metadata",
    "wc2026_event_state_timing": "event_state_timing",
    "wc2026_fixtures": "fixtures",
    "wc2026_international_matches": "international_matches",
    "wc2026_player_features": "player_features",
    "wc2026_price_liquidity_current": "price_liquidity_current",
    "wc2026_price_liquidity_history": "price_liquidity_history",
    "wc2026_results": "results",
    "wc2026_source_provenance": "source_provenance",
    "wc2026_squad_player_features": "squad_player_features",
    "wc2026_team_identities": "team_identities",
    "wc2026_team_ratings_current": "team_ratings_current",
    "wc2026_team_ratings_history": "team_ratings_history",
    "wc2026_team_ratings_pre_match": "team_ratings_pre_match",
    "wc2026_third_place_lookup": "third_place_lookup",
    "wc2026_third_place_slot_assignments": "third_place_slot_assignments",
    "wc2026_travel_features": "travel_features",
    "wc2026_venue_markets": "venue_markets",
}


def dbt_physical_relation_name(schema: str, model_name: str) -> str:
    """Return the DuckDB table/view name for a dbt model inventory entry."""
    if schema == WC2026_MARTS_SCHEMA:
        return WC2026_MART_RELATION_ALIASES.get(model_name, model_name)
    return model_name


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
    if model_name.startswith("stg_polymarket_catalog_"):
        return DBT_SOURCE_POLYMARKET_CATALOG
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
            "stg_polymarket_soccer_",
            "int_polymarket_soccer_",
            "polymarket_soccer_",
        )
    ):
        return DBT_SOURCE_POLYMARKET_SOCCER
    return None


def resolve_source_slug(
    props: Mapping[str, object],
    *,
    fqn: Sequence[str] | None = None,
) -> str:
    tags = set(props.get("tags") or ())
    path_fqn = list(fqn or props.get("fqn") or ())
    if len(path_fqn) >= 2 and path_fqn[1] == DBT_SOURCE_POLYMARKET_CATALOG:
        return DBT_SOURCE_POLYMARKET_CATALOG
    if len(path_fqn) >= 2 and path_fqn[1] in _POLYMARKET_SOURCE_SCOPES:
        return path_fqn[1]
    if len(path_fqn) >= 2 and path_fqn[1] in _KALSHI_SOURCE_SCOPES:
        return path_fqn[1]
    if len(path_fqn) >= 2 and path_fqn[1] == "international_results_wc2026":
        return DBT_SOURCE_INTERNATIONAL_RESULTS_WC2026
    if len(path_fqn) >= 2 and path_fqn[1] == "wc2026":
        return DBT_SOURCE_WC2026
    name = str(props.get("name") or "")
    kalshi_slug = _kalshi_source_slug(name)
    if kalshi_slug is not None:
        return kalshi_slug
    polymarket_slug = _polymarket_source_slug(name)
    if polymarket_slug is not None:
        return polymarket_slug
    if name.startswith(("int_wc2026_", "wc2026_")):
        return DBT_SOURCE_WC2026
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


def _polymarket_soccer_layer(
    model_name: str,
    props: Mapping[str, object] | None = None,
    *,
    fqn: Sequence[str] | None = None,
) -> str:
    return _polymarket_layer(
        model_name,
        props,
        fqn=fqn,
        observability_models=(
            "polymarket_soccer_match_result_data_quality",
            "polymarket_soccer_pipeline_alerts",
            "polymarket_soccer_pipeline_health",
            "polymarket_soccer_pipeline_trends",
        ),
        staging_prefix="stg_polymarket_soccer_",
        intermediate_prefix="int_polymarket_soccer_",
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


def _polymarket_soccer_subject(model_name: str) -> str:
    return _polymarket_subject(
        model_name,
        staging_prefix="stg_polymarket_soccer_",
        intermediate_prefix="int_polymarket_soccer_",
        mart_prefix="polymarket_soccer_",
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
    if source_slug == DBT_SOURCE_WC2026:
        return _wc2026_subject(model_name)
    if source_slug == DBT_SOURCE_INTERNATIONAL_RESULTS_WC2026:
        return _international_results_wc2026_subject(model_name)
    if source_slug == DBT_SOURCE_KALSHI_WC2026:
        return _kalshi_wc2026_subject(model_name)
    if source_slug == DBT_SOURCE_POLYMARKET_WC2026:
        return _polymarket_wc2026_subject(model_name)
    if source_slug == DBT_SOURCE_POLYMARKET_SOCCER:
        return _polymarket_soccer_subject(model_name)
    if source_slug == DBT_SOURCE_POLYMARKET_CATALOG:
        prefix = "stg_polymarket_catalog_"
        if model_name.startswith(prefix):
            return model_name[len(prefix) :]
    return model_name


def _wc2026_subject(model_name: str) -> str:
    for prefix in ("int_wc2026_", "wc2026_"):
        if model_name.startswith(prefix):
            return model_name[len(prefix) :]
    return model_name


def dbt_model_asset_key_for_name(
    model_name: str,
    source_slug: str,
    *,
    layer: str | None = None,
    props: Mapping[str, object] | None = None,
    fqn: Sequence[str] | None = None,
) -> AssetKey:
    if source_slug == DBT_SOURCE_WC2026:
        path_fqn = list(fqn or (props or {}).get("fqn") or ())
        resolved_layer = layer
        if resolved_layer is None:
            resolved_layer = next(
                (
                    segment
                    for segment in path_fqn
                    if segment in {"staging", "intermediate", "marts", "observability"}
                ),
                "marts",
            )
        return AssetKey(["wc2026", resolved_layer, _wc2026_subject(model_name)])
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
    if source_slug == DBT_SOURCE_POLYMARKET_SOCCER:
        return asset_key(
            SOURCE_POLYMARKET,
            SCOPE_SOCCER,
            layer or _polymarket_soccer_layer(model_name, props, fqn=fqn),
            _polymarket_soccer_subject(model_name),
        )
    if source_slug == DBT_SOURCE_POLYMARKET_CATALOG:
        path_fqn = list(fqn or (props or {}).get("fqn") or ())
        resolved_layer = layer or next(
            (
                segment
                for segment in path_fqn
                if segment in {"staging", "intermediate", "marts", "observability"}
            ),
            "staging",
        )
        return AssetKey(
            [
                SOURCE_POLYMARKET,
                "catalog",
                resolved_layer,
                shorten_model_name(model_name, source_slug),
            ]
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
    "WC2026_MART_RELATION_ALIASES",
    "dbt_physical_relation_name",
    "DBT_SOURCE_INTERNATIONAL_RESULTS_WC2026",
    "DBT_SOURCE_KALSHI_WC2026",
    "DBT_SOURCE_OPENFOOTBALL_WC2026",
    "DBT_SOURCE_POLYMARKET_CATALOG",
    "DBT_SOURCE_POLYMARKET_WC2026",
    "DBT_SOURCE_POLYMARKET_SOCCER",
    "DBT_SOURCE_WC2026",
    "INTERNATIONAL_RESULTS_WC2026_INTERMEDIATE_SCHEMA",
    "INTERNATIONAL_RESULTS_WC2026_MARTS_SCHEMA",
    "INTERNATIONAL_RESULTS_WC2026_OBSERVABILITY_SCHEMA",
    "INTERNATIONAL_RESULTS_WC2026_STAGING_SCHEMA",
    "OPENFOOTBALL_WC2026_STAGING_SCHEMA",
    "KALSHI_WC2026_INTERMEDIATE_SCHEMA",
    "KALSHI_WC2026_MARTS_SCHEMA",
    "KALSHI_WC2026_OBSERVABILITY_SCHEMA",
    "KALSHI_WC2026_STAGING_SCHEMA",
    "POLYMARKET_WC2026_INTERMEDIATE_SCHEMA",
    "POLYMARKET_WC2026_MARTS_SCHEMA",
    "POLYMARKET_WC2026_OBSERVABILITY_SCHEMA",
    "POLYMARKET_CATALOG_STAGING_SCHEMA",
    "POLYMARKET_WC2026_STAGING_SCHEMA",
    "POLYMARKET_SOCCER_INTERMEDIATE_SCHEMA",
    "POLYMARKET_SOCCER_MARTS_SCHEMA",
    "POLYMARKET_SOCCER_OBSERVABILITY_SCHEMA",
    "POLYMARKET_SOCCER_STAGING_SCHEMA",
    "WC2026_INTERMEDIATE_SCHEMA",
    "WC2026_MARTS_SCHEMA",
    "WC2026_OBSERVABILITY_SCHEMA",
    "WC2026_STAGING_SCHEMA",
    "dbt_model_asset_key",
    "dbt_model_asset_key_for_name",
    "qualified_relation",
    "resolve_source_slug",
    "shorten_model_name",
]
