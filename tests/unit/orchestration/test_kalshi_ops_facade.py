"""Kalshi ops facade re-exports match pipeline_ops where intended."""

from __future__ import annotations

import pytest

from oddsfox_pipeline.ingestion.kalshi.candlesticks import sync as candlesticks_sync
from oddsfox_pipeline.ingestion.kalshi.markets import sync as markets_sync
from oddsfox_pipeline.ingestion.kalshi.series_scope import registry as series_registry
from oddsfox_pipeline.orchestration import kalshi_ops, pipeline_ops

pytestmark = pytest.mark.facade

_PIPELINE_OPS_REEXPORTS = (
    "ProgressGuardrail",
    "Thread",
    "delta_dbt_models",
    "delta_raw_layer",
    "format_dbt_snapshot_log",
    "format_raw_snapshot_log",
    "snapshot_dbt_models",
    "snapshot_raw_layer",
    "stream_dbt_build",
)


def test_kalshi_ops_facade_matches_pipeline_ops_helpers() -> None:
    for name in _PIPELINE_OPS_REEXPORTS:
        assert getattr(kalshi_ops, name) is getattr(pipeline_ops, name)


def test_kalshi_sync_wrappers_delegate_to_ingestion_modules(monkeypatch) -> None:
    monkeypatch.setattr(
        kalshi_ops,
        "sync_markets",
        lambda **kwargs: {"task": "sync_markets", **kwargs},
    )
    fake_result = type(
        "Result",
        (),
        {"summary": {"registry_rows_upserted": 2}},
    )()
    monkeypatch.setattr(
        kalshi_ops,
        "refresh_registry_and_collect",
        lambda *_args, **_kwargs: fake_result,
    )
    monkeypatch.setattr(
        kalshi_ops,
        "sync_hourly_candlesticks",
        lambda **kwargs: {"task": "sync_hourly_candlesticks", **kwargs},
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.kalshi.client.build_client",
        lambda: object(),
    )

    assert kalshi_ops.sync_kalshi_markets(scope_name="wc2026")["task"] == "sync_markets"
    assert kalshi_ops.sync_kalshi_market_scope_registry(
        config=object(),
    ) == {"registry_rows_upserted": 2}
    assert (
        kalshi_ops.sync_kalshi_candlesticks(scope_name="wc2026")["task"]
        == "sync_hourly_candlesticks"
    )
    assert markets_sync.sync_markets is not kalshi_ops.sync_kalshi_markets
    assert (
        series_registry.refresh_registry_and_collect
        is not kalshi_ops.sync_kalshi_market_scope_registry
    )
    assert (
        candlesticks_sync.sync_hourly_candlesticks
        is not kalshi_ops.sync_kalshi_candlesticks
    )
