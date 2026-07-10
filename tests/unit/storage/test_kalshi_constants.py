"""Unit tests for Kalshi schema naming helpers."""

from __future__ import annotations

from oddsfox_pipeline.naming import SOURCE_KALSHI, schema_name
from oddsfox_pipeline.storage.duckdb.schemas import constants


def test_kalshi_schema_helpers_use_wc2026_defaults_and_custom_scopes():
    assert constants.polymarket_wc2026_q("polymarket_wc2026_raw", "markets") == (
        '"polymarket_wc2026_raw"."markets"'
    )
    assert constants.kalshi_q("kalshi_wc2026_raw", "markets") == (
        '"kalshi_wc2026_raw"."markets"'
    )
    assert constants.kalshi_raw_schema("wc2026") == "kalshi_wc2026_raw"
    assert constants.kalshi_ops_schema("wc2026") == "kalshi_wc2026_ops"
    assert constants.kalshi_wc2026_raw_tbl("events") == ('"kalshi_wc2026_raw"."events"')
    assert constants.kalshi_wc2026_ops_tbl("sync_run_metrics") == (
        '"kalshi_wc2026_ops"."sync_run_metrics"'
    )

    custom_scope = "custom_scope"
    assert constants.kalshi_raw_schema(custom_scope) == schema_name(
        SOURCE_KALSHI, custom_scope, "raw"
    )
    assert constants.kalshi_ops_schema(custom_scope) == schema_name(
        SOURCE_KALSHI, custom_scope, "ops"
    )
