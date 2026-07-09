"""Stable import surface for Kalshi Dagster assets and tests."""

from __future__ import annotations

from oddsfox_pipeline.ingestion.kalshi.candlesticks.sync import sync_hourly_candlesticks
from oddsfox_pipeline.ingestion.kalshi.markets.sync import sync_markets
from oddsfox_pipeline.ingestion.kalshi.series_scope.registry import (
    refresh_registry_and_collect,
)
from oddsfox_pipeline.orchestration import pipeline_ops as _ops

ProgressGuardrail = _ops.ProgressGuardrail
Thread = _ops.Thread
delta_dbt_models = _ops.delta_dbt_models
delta_raw_layer = _ops.delta_raw_layer
format_dbt_snapshot_log = _ops.format_dbt_snapshot_log
format_raw_snapshot_log = _ops.format_raw_snapshot_log
snapshot_dbt_models = _ops.snapshot_dbt_models
snapshot_raw_layer = _ops.snapshot_raw_layer
stream_dbt_build = _ops.stream_dbt_build


def sync_kalshi_markets(**kwargs):
    return sync_markets(**kwargs)


def sync_kalshi_market_scope_registry(**kwargs):
    from oddsfox_pipeline.ingestion.kalshi.client import build_client

    client = build_client()
    result = refresh_registry_and_collect(client, **kwargs)
    return result.summary


def sync_kalshi_candlesticks(**kwargs):
    return sync_hourly_candlesticks(**kwargs)


__all__ = [
    "ProgressGuardrail",
    "Thread",
    "delta_dbt_models",
    "delta_raw_layer",
    "format_dbt_snapshot_log",
    "format_raw_snapshot_log",
    "snapshot_dbt_models",
    "snapshot_raw_layer",
    "stream_dbt_build",
    "sync_kalshi_candlesticks",
    "sync_kalshi_market_scope_registry",
    "sync_kalshi_markets",
]
