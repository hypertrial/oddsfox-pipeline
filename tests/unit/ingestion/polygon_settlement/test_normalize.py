from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from tests.unit.ingestion.polygon_settlement.conftest import (
    build_manifest as _manifest,
)
from tests.unit.ingestion.polygon_settlement.conftest import (
    event as _event,
)
from tests.unit.ingestion.polygon_settlement.conftest import (
    normalize as _normalize,
)
from tests.unit.ingestion.polygon_settlement.conftest import (
    polygon_settlement_module,
)
from tests.unit.ingestion.test_polygon_seed import complete_seed_rows

from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (
    PolygonMarketManifest,
    parse_polygon_market,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_settlement import (
    NORMALIZER_VERSION,
    normalize_v2_segment,
)


def test_normalize_complementary_uses_passive_leg_only() -> None:
    market = _manifest().markets[0]
    passive = _event("order_filled", "SELL", market.yes_token_id, 1_000_000, 600_000, 1)
    active = _event("order_filled", "BUY", market.yes_token_id, 600_000, 1_000_000, 2)
    matched = _event(
        "orders_matched", "BUY", market.yes_token_id, 600_000, 1_000_000, 3
    )

    rows = _normalize((passive,), active, matched)

    assert len(rows) == 1
    assert rows[0]["normalization_kind"] == "complementary"
    assert rows[0]["is_derived"] is False
    assert rows[0]["share_volume"] == Decimal("1.000000")
    assert rows[0]["gross_collateral_volume"] == Decimal("0.600000")
    assert rows[0]["price"] == Decimal("0.600000000000000000")
    assert rows[0]["decoder_version"] == NORMALIZER_VERSION


@pytest.mark.parametrize(
    ("side", "kind", "active_amounts", "passive_amounts"),
    [
        (
            "BUY",
            "mint",
            (1_800_000, 3_000_000),
            ((400_000, 1_000_000), (800_000, 2_000_000)),
        ),
        (
            "SELL",
            "merge",
            (3_000_000, 1_800_000),
            ((1_000_000, 400_000), (2_000_000, 800_000)),
        ),
    ],
)
def test_normalize_multimaker_mint_and_merge_derives_counterparts(
    side, kind, active_amounts, passive_amounts
) -> None:
    market = _manifest().markets[0]
    passive = tuple(
        _event("order_filled", side, market.no_token_id, maker, taker, index)
        for index, (maker, taker) in enumerate(passive_amounts, 1)
    )
    active = _event("order_filled", side, market.yes_token_id, *active_amounts, 3)
    matched = _event("orders_matched", side, market.yes_token_id, *active_amounts, 4)

    rows = _normalize(passive, active, matched)

    assert len(rows) == 4
    assert {row["normalization_kind"] for row in rows} == {kind}
    assert sum(row["is_derived"] for row in rows) == 2
    assert {row["token_id"] for row in rows} == {
        market.yes_token_id,
        market.no_token_id,
    }
    derived = [row for row in rows if row["is_derived"]]
    assert {row["price"] for row in derived} == {Decimal("0.600000000000000000")}


def test_normalizer_accepts_only_the_active_maker_asset_refund_dimension() -> None:
    market = _manifest().markets[0]

    # Finalized Polygon block 89,316,793 exposed the one-micro-USDC case: each
    # maker's integer ratio rounds independently, while V2 later refunds the
    # unused active BUY collateral.  The passive economic legs remain exact.
    buy_passive = (
        _event("order_filled", "BUY", market.no_token_id, 3_836_160, 3_840_000, 1),
        _event("order_filled", "BUY", market.no_token_id, 9_930_000, 10_000_000, 2),
        _event(
            "order_filled",
            "BUY",
            market.no_token_id,
            101_980_506,
            102_906_665,
            3,
        ),
    )
    buy_active = _event(
        "order_filled", "BUY", market.yes_token_id, 1_000_000, 116_746_665, 4
    )
    buy_matched = _event(
        "orders_matched", "BUY", market.yes_token_id, 1_000_000, 116_746_665, 5
    )

    buy_rows = _normalize(buy_passive, buy_active, buy_matched)

    assert len(buy_rows) == 6
    assert [row["passive_log_index"] for row in buy_rows] == [1, 1, 2, 2, 3, 3]
    assert [row["normalized_leg_ordinal"] for row in buy_rows] == [0, 1, 0, 1, 0, 1]
    assert (
        sum(
            int(row["gross_collateral_volume"] * 1_000_000)
            for row in buy_rows
            if row["is_derived"]
        )
        == 999_999
    )
    assert (
        len(
            _normalize(
                buy_passive,
                replace(buy_active, maker_amount=1_000_100),
                replace(buy_matched, maker_amount=1_000_100),
            )
        )
        == 6
    )
    with pytest.raises(ValueError, match="conserve"):
        _normalize(
            buy_passive,
            replace(buy_active, taker_amount=116_746_666),
            replace(buy_matched, taker_amount=116_746_666),
        )
    with pytest.raises(ValueError, match="conserve"):
        _normalize(
            buy_passive,
            replace(buy_active, maker_amount=999_998),
            replace(buy_matched, maker_amount=999_998),
        )

    sell_passive = (
        _event("order_filled", "SELL", market.no_token_id, 4_000_000, 1_000_000, 1),
        _event("order_filled", "SELL", market.no_token_id, 6_000_000, 3_000_000, 2),
    )
    sell_active = _event(
        "order_filled", "SELL", market.yes_token_id, 10_000_001, 6_000_000, 3
    )
    sell_matched = _event(
        "orders_matched", "SELL", market.yes_token_id, 10_000_001, 6_000_000, 4
    )

    assert len(_normalize(sell_passive, sell_active, sell_matched)) == 4
    with pytest.raises(ValueError, match="conserve"):
        _normalize(
            sell_passive,
            replace(sell_active, maker_amount=9_999_999),
            replace(sell_matched, maker_amount=9_999_999),
        )
    with pytest.raises(ValueError, match="conserve"):
        _normalize(
            sell_passive,
            replace(sell_active, taker_amount=6_000_001),
            replace(sell_matched, taker_amount=6_000_001),
        )


def test_normalizer_rejects_bad_aggregate_shape_and_strictly_filters_window() -> None:
    market = _manifest().markets[0]
    passive = _event("order_filled", "SELL", market.yes_token_id, 1_000_000, 600_000, 1)
    active = _event("order_filled", "BUY", market.yes_token_id, 700_000, 1_000_000, 2)
    matched = _event(
        "orders_matched", "BUY", market.yes_token_id, 700_000, 1_000_000, 3
    )
    with pytest.raises(ValueError, match="conserve"):
        _normalize((passive,), active, matched)

    active = _event("order_filled", "BUY", market.yes_token_id, 600_000, 1_000_000, 2)
    matched = _event(
        "orders_matched", "BUY", market.yes_token_id, 600_000, 1_000_000, 3
    )
    assert _normalize((passive,), active, matched, offset_minutes=150) == []
    assert _normalize((passive,), active, matched, offset_minutes=-1) == []


@pytest.mark.parametrize(
    ("side", "paired_kind", "passive", "active_amounts"),
    [
        (
            "BUY",
            "mint",
            (
                ("SELL", "yes", 1_000_000, 600_000),
                ("BUY", "no", 400_000, 1_000_000),
            ),
            (1_200_000, 2_000_000),
        ),
        (
            "SELL",
            "merge",
            (
                ("BUY", "yes", 600_000, 1_000_000),
                ("SELL", "no", 1_000_000, 400_000),
            ),
            (2_000_000, 1_200_000),
        ),
    ],
)
def test_normalizer_allows_per_maker_complementary_and_paired_shapes(
    side, paired_kind, passive, active_amounts
) -> None:
    market = _manifest().markets[0]
    token = {"yes": market.yes_token_id, "no": market.no_token_id}
    passive_events = tuple(
        _event("order_filled", maker_side, token[outcome], maker, taker, index)
        for index, (maker_side, outcome, maker, taker) in enumerate(passive, 1)
    )
    active = _event("order_filled", side, market.yes_token_id, *active_amounts, 3)
    matched = _event("orders_matched", side, market.yes_token_id, *active_amounts, 4)

    rows = _normalize(passive_events, active, matched)

    assert len(rows) == 3
    assert {row["normalization_kind"] for row in rows} == {
        "complementary",
        paired_kind,
    }
    assert sum(row["is_derived"] for row in rows) == 1
    assert [row["passive_log_index"] for row in rows] == [1, 2, 2]
    assert [row["normalized_leg_ordinal"] for row in rows] == [0, 0, 1]

    # The general V2 path can refund favorable-crossing surplus larger than
    # integer-rounding dust, including mixed complementary + paired segments.
    surplus_active = replace(active, maker_amount=active.maker_amount + 123)
    surplus_matched = replace(matched, maker_amount=matched.maker_amount + 123)
    assert len(_normalize(passive_events, surplus_active, surplus_matched)) == 3


def test_normalizer_rejects_malformed_leg_inside_mixed_segment() -> None:
    market = _manifest().markets[0]
    passive = (
        _event("order_filled", "SELL", market.yes_token_id, 1_000_000, 600_000, 1),
        _event("order_filled", "BUY", market.no_token_id, 400_000, 1_000_000, 2),
        _event("order_filled", "SELL", market.no_token_id, 1_000_000, 300_000, 3),
    )
    active = _event("order_filled", "BUY", market.yes_token_id, 1, 3_000_000, 4)
    matched = _event("orders_matched", "BUY", market.yes_token_id, 1, 3_000_000, 5)

    with pytest.raises(ValueError, match="Unsupported V2 target-market match shape"):
        _normalize(passive, active, matched)


def test_normalizer_rejects_every_malformed_segment_shape() -> None:
    manifest = _manifest()
    market = manifest.markets[0]
    passive = _event("order_filled", "SELL", market.yes_token_id, 1_000_000, 600_000, 1)
    active = _event("order_filled", "BUY", market.yes_token_id, 600_000, 1_000_000, 2)
    matched = _event(
        "orders_matched", "BUY", market.yes_token_id, 600_000, 1_000_000, 3
    )
    cases = [
        ((), active, matched, "preceded by passive"),
        ((passive,), replace(active, kind="orders_matched"), matched, "boundary"),
        ((passive,), active, replace(matched, kind="order_filled"), "boundary"),
        (
            (replace(passive, transaction_index=1),),
            active,
            matched,
            "transaction boundaries",
        ),
        (
            (passive,),
            active,
            replace(matched, maker_amount=500_000),
            "exactly match",
        ),
    ]
    for passive_rows, active_row, matched_row, message in cases:
        with pytest.raises(ValueError, match=message):
            _normalize(passive_rows, active_row, matched_row)

    unrelated_passive = _event("order_filled", "SELL", "99999", 1_000_000, 600_000, 1)
    unrelated_active = _event("order_filled", "BUY", "99999", 600_000, 1_000_000, 2)
    unrelated_matched = _event("orders_matched", "BUY", "99999", 600_000, 1_000_000, 3)
    assert _normalize((unrelated_passive,), unrelated_active, unrelated_matched) == []

    with pytest.raises(ValueError, match="unregistered active"):
        _normalize((passive,), unrelated_active, unrelated_matched)
    with pytest.raises(ValueError, match="unregistered passive"):
        _normalize((unrelated_passive,), active, matched)

    wrong_exchange = replace(active, exchange_address="0x" + "9" * 40)
    with pytest.raises(ValueError, match="wrong V2 exchange"):
        _normalize(
            (replace(passive, exchange_address=wrong_exchange.exchange_address),),
            wrong_exchange,
            replace(matched, exchange_address=wrong_exchange.exchange_address),
        )

    second_market = replace(
        parse_polygon_market(complete_seed_rows()[1]),
        exchange_address=market.exchange_address,
    )
    two_market_manifest = PolygonMarketManifest(
        markets=(market, second_market), sha256="1" * 64, version="1.0.0"
    )
    cross_passive = _event(
        "order_filled", "SELL", second_market.yes_token_id, 1_000_000, 600_000, 1
    )
    with pytest.raises(ValueError, match="independent market conditions"):
        normalize_v2_segment(
            (cross_passive,),
            active,
            matched,
            manifest=two_market_manifest,
            scan_id="scan",
            from_block=99,
            to_block=101,
            block_timestamp=market.window_start_at_utc,
            ingested_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
        )

    same_side = replace(
        passive, side="BUY", maker_amount=600_000, taker_amount=1_000_000
    )
    with pytest.raises(ValueError, match="Unsupported"):
        _normalize((same_side,), active, matched)


def test_normalizer_handles_no_side_mint_and_exact_numeric_guards() -> None:
    market = _manifest().markets[0]
    passive = _event("order_filled", "BUY", market.yes_token_id, 400_000, 1_000_000, 1)
    active = _event("order_filled", "BUY", market.no_token_id, 600_000, 1_000_000, 2)
    matched = _event("orders_matched", "BUY", market.no_token_id, 600_000, 1_000_000, 3)
    assert len(_normalize((passive,), active, matched)) == 2

    for value in (-1, 0, 340_282_366_920_938_463_375, 10**38):
        with pytest.raises(ValueError, match="volume"):
            polygon_settlement_module._decimal_volume(value)
    assert polygon_settlement_module._decimal_volume(
        340_282_366_920_938_463_374
    ) == Decimal("340282366920938.463374")
    for collateral, shares in ((0, 0), (0, 1), (-1, 1), (2, 1)):
        with pytest.raises(ValueError, match="price inputs"):
            polygon_settlement_module._decimal_price(collateral, shares)

    assert polygon_settlement_module._decimal_price(1, 7) == Decimal(
        "0.142857142857142857"
    )
    assert polygon_settlement_module._decimal_price(4, 7) == Decimal(
        "0.571428571428571429"
    )
    assert polygon_settlement_module._decimal_price(1, 524_288) == Decimal(
        "0.000001907348632812"
    )
    assert polygon_settlement_module._decimal_price(3, 524_288) == Decimal(
        "0.000005722045898438"
    )

    zero_derived = _event(
        "order_filled", "BUY", market.yes_token_id, 1_000_000, 1_000_000, 1
    )
    active_zero = _event("order_filled", "BUY", market.no_token_id, 0, 1_000_000, 2)
    matched_zero = _event("orders_matched", "BUY", market.no_token_id, 0, 1_000_000, 3)
    with pytest.raises(ValueError, match="derived collateral"):
        _normalize((zero_derived,), active_zero, matched_zero)
    for bad in (
        replace(passive, taker_amount=0),
        replace(passive, maker_amount=-1),
        replace(passive, maker_amount=2, taker_amount=1),
    ):
        with pytest.raises(ValueError, match="invalid share/collateral"):
            polygon_settlement_module._amounts(bad)
