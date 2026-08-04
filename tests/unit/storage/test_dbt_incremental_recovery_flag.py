from __future__ import annotations

from oddsfox_pipeline.storage.duckdb.metadata import (
    clear_polymarket_token_hourly_odds_incremental_in_progress,
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
