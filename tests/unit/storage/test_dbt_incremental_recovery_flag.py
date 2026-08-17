from __future__ import annotations

from oddsfox_pipeline.storage.duckdb.metadata import (
    POLYMARKET_SOCCER_INCREMENTAL_MODELS,
    clear_dbt_incremental_in_progress,
    clear_polymarket_token_hourly_odds_incremental_in_progress,
    dbt_incremental_recovery_needed,
    mark_dbt_incremental_in_progress,
    mark_polymarket_token_hourly_odds_incremental_in_progress,
    polymarket_token_hourly_odds_incremental_recovery_needed,
)


def test_incremental_recovery_flag_round_trip(
    reset_connection_globals,
    duck,
) -> None:
    assert polymarket_token_hourly_odds_incremental_recovery_needed() is False
    mark_polymarket_token_hourly_odds_incremental_in_progress()
    assert polymarket_token_hourly_odds_incremental_recovery_needed() is True
    clear_polymarket_token_hourly_odds_incremental_in_progress()
    assert polymarket_token_hourly_odds_incremental_recovery_needed() is False


def test_soccer_incremental_flags_are_model_scoped(reset_connection_globals, duck):
    observed, dense = POLYMARKET_SOCCER_INCREMENTAL_MODELS
    mark_dbt_incremental_in_progress(observed)
    assert dbt_incremental_recovery_needed(observed) is True
    assert dbt_incremental_recovery_needed(dense) is False
    assert polymarket_token_hourly_odds_incremental_recovery_needed() is False
    clear_dbt_incremental_in_progress(observed)
    assert dbt_incremental_recovery_needed(observed) is False
