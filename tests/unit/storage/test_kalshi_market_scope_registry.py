"""Storage tests for kalshi_wc2026_ops.market_scope_registry."""

from __future__ import annotations

import pytest

from oddsfox_pipeline.storage.duckdb.kalshi_market_scope_registry import (
    KalshiRegistryRow,
    get_registry_market_tickers,
    registry_market_count,
    upsert_registry_rows,
)


def test_upsert_empty_rows_is_noop(duck):
    assert upsert_registry_rows([]) == 0


def test_upsert_and_query_registry(duck):
    n = upsert_registry_rows(
        [
            KalshiRegistryRow(
                "KXMENWORLDCUP-WINNER-USA",
                "KXMENWORLDCUP-WINNER",
                "KXMENWORLDCUP",
                "events_api",
            ),
            KalshiRegistryRow(
                "KXMENWORLDCUP-WINNER-BRA",
                "KXMENWORLDCUP-WINNER",
                "KXMENWORLDCUP",
                "events_api",
            ),
        ]
    )
    assert n == 2
    assert registry_market_count() == 2
    assert get_registry_market_tickers() == [
        "KXMENWORLDCUP-WINNER-BRA",
        "KXMENWORLDCUP-WINNER-USA",
    ]


def test_registry_helpers_reject_blank_scope(duck):
    upsert_registry_rows(
        [
            KalshiRegistryRow(
                "KXWC-MKT1",
                "KXWC-EVT1",
                "KXWC",
                "seed",
                scope_name="wc2026",
            )
        ]
    )
    with pytest.raises(ValueError, match="scope_name"):
        get_registry_market_tickers("")
