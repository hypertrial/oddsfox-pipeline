from __future__ import annotations

import json
from dataclasses import fields, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from threading import Event, Lock, get_ident
from time import sleep

import pytest
import requests
from tests.unit.ingestion.test_polygon_seed import complete_seed_rows

import oddsfox_pipeline.ingestion.polymarket.polygon_rpc as polygon_rpc_module
import oddsfox_pipeline.ingestion.polymarket.polygon_settlement as polygon_settlement_module
from oddsfox_pipeline.ingestion.polymarket.polygon_rpc import (
    EVENT_TOPICS,
    ORDER_FILLED_TOPIC,
    ORDERS_MATCHED_TOPIC,
    DecodedSettlementEvent,
    PolygonBlock,
    PolygonReceipt,
    PolygonRPC,
    PolygonRPCError,
    PolygonRPCMetrics,
    PolygonRPCSizeLimitError,
    adaptive_log_leaves,
    decode_settlement_log,
    sanitize_rpc_origin,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (
    NEG_RISK_V2_EXCHANGE,
    STANDARD_V2_EXCHANGE,
    PolygonMarketManifest,
    parse_polygon_market,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_settlement import (
    NORMALIZER_VERSION,
    PolygonSettlementSyncConfig,
    build_polygon_scan_plan,
    build_polygon_target_ranges,
    decode_and_normalize_leaf,
    normalize_v2_segment,
    sync_polygon_settlement_fills,
    verify_polygon_settlement_scan,
)
from oddsfox_pipeline.resources.http import APIClient, RateLimiter


def _manifest() -> PolygonMarketManifest:
    rows = complete_seed_rows()
    standard = replace(
        parse_polygon_market(rows[0]),
        market_structure="standard",
        exchange_address=STANDARD_V2_EXCHANGE,
    )
    neg_risk = parse_polygon_market(rows[1])
    return PolygonMarketManifest(
        markets=(standard, neg_risk),
        sha256=standard.manifest_sha256,
        version=standard.manifest_version,
    )


def _event(
    kind: str,
    side: str,
    token_id: str,
    maker: int,
    taker: int,
    log_index: int,
    *,
    transaction_index: int = 0,
) -> DecodedSettlementEvent:
    return DecodedSettlementEvent(
        kind=kind,
        exchange_address=STANDARD_V2_EXCHANGE.casefold(),
        block_number=100,
        block_hash="0x" + "1" * 64,
        transaction_hash="0x" + "2" * 64,
        transaction_index=transaction_index,
        log_index=log_index,
        side=side,
        token_id=token_id,
        maker_amount=maker,
        taker_amount=taker,
    )


def _normalize(passive, active, matched, *, offset_minutes: int = 1):
    manifest = _manifest()
    market = manifest.markets[0]
    return normalize_v2_segment(
        passive,
        active,
        matched,
        manifest=manifest,
        scan_id="scan",
        from_block=99,
        to_block=101,
        block_timestamp=market.window_start_at_utc + timedelta(minutes=offset_minutes),
        ingested_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
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


def test_analysis_windows_and_safety_block_ranges_are_merged() -> None:
    market = _manifest().markets[0]
    adjacent = replace(
        market,
        proposition_id="adjacent",
        window_start_at_utc=market.window_end_at_utc,
        window_end_at_utc=market.window_end_at_utc + timedelta(minutes=150),
    )
    disjoint = replace(
        market,
        proposition_id="disjoint",
        window_start_at_utc=adjacent.window_end_at_utc + timedelta(minutes=1),
        window_end_at_utc=adjacent.window_end_at_utc + timedelta(minutes=151),
    )
    manifest = PolygonMarketManifest(
        markets=(market, adjacent, disjoint), sha256="1" * 64, version="1.0.0"
    )

    class RangeRPC:
        def __init__(self):
            self.boundaries = iter((10, 11, 11, 12, 13, 14))

        def first_block_at_or_after(self, *_args, **_kwargs):
            return next(self.boundaries)

        def block(self, number):
            return PolygonBlock(
                number,
                f"0x{number:064x}",
                market.window_start_at_utc + timedelta(seconds=number),
            )

    ranges = build_polygon_target_ranges(
        RangeRPC(),
        manifest,
        PolygonBlock(20, "0x" + "f" * 64, disjoint.window_end_at_utc),
    )
    assert [(item.from_block, item.to_block) for item in ranges] == [(9, 14)]
    assert {item.exchange_address for item in ranges} == {
        STANDARD_V2_EXCHANGE.casefold()
    }

    class BatchRangeRPC(RangeRPC):
        def first_blocks_at_or_after(self, _timestamps, *, finalized_head):
            assert finalized_head.number == 20
            return (10, 11, 11, 12, 13, 14)

    assert [
        (item.from_block, item.to_block)
        for item in build_polygon_target_ranges(
            BatchRangeRPC(),
            manifest,
            PolygonBlock(20, "0x" + "f" * 64, disjoint.window_end_at_utc),
        )
    ] == [(9, 14)]


def test_scan_plan_separates_exchanges_and_keeps_exact_token_bounds() -> None:
    standard = _manifest().markets[0]
    neg_risk = replace(
        standard,
        proposition_id="neg-risk",
        condition_id="0x" + "9" * 64,
        yes_token_id="901",
        no_token_id="902",
        market_structure="neg_risk",
        exchange_address=NEG_RISK_V2_EXCHANGE,
        window_start_at_utc=standard.window_end_at_utc + timedelta(minutes=1),
        window_end_at_utc=standard.window_end_at_utc + timedelta(minutes=151),
    )
    manifest = PolygonMarketManifest(
        markets=(standard, neg_risk), sha256="1" * 64, version="1.0.0"
    )

    class PlanRPC:
        def first_blocks_at_or_after(self, _timestamps, *, finalized_head):
            assert finalized_head.number == 300
            return (100, 110, 200, 210)

        def blocks(self, numbers):
            return {
                number: PolygonBlock(
                    number,
                    f"0x{number:064x}",
                    standard.window_start_at_utc + timedelta(seconds=number),
                )
                for number in dict.fromkeys(numbers)
            }

    plan = build_polygon_scan_plan(
        PlanRPC(),
        manifest,
        PolygonBlock(300, "0x" + "f" * 64, neg_risk.window_end_at_utc),
    )

    assert [
        (item.exchange_address, item.from_block, item.to_block)
        for item in plan.target_ranges
    ] == [
        (STANDARD_V2_EXCHANGE.casefold(), 99, 110),
        (NEG_RISK_V2_EXCHANGE.casefold(), 199, 210),
    ]
    assert (
        plan.token_targets[standard.yes_token_id].first_valid_block,
        plan.token_targets[standard.yes_token_id].first_invalid_block,
    ) == (100, 110)
    assert (
        plan.token_targets[neg_risk.no_token_id].first_valid_block,
        plan.token_targets[neg_risk.no_token_id].first_invalid_block,
    ) == (200, 210)
    # The old cross-product planner would have scanned both ranges twice.
    assert sum(item.to_block - item.from_block + 1 for item in plan.target_ranges) == 24


def test_scan_plan_fails_closed_on_boundary_and_inventory_errors() -> None:
    manifest = _manifest()
    finalized = PolygonBlock(
        300,
        "0x" + "f" * 64,
        manifest.markets[0].window_end_at_utc + timedelta(days=1),
    )

    class BoundaryRPC:
        def __init__(self, boundaries):
            self.boundaries = boundaries

        def first_blocks_at_or_after(self, _timestamps, *, finalized_head):
            assert finalized_head == finalized
            return self.boundaries

        def blocks(self, numbers):
            return {
                number: PolygonBlock(
                    number,
                    f"0x{number:064x}",
                    manifest.markets[0].window_start_at_utc,
                )
                for number in dict.fromkeys(numbers)
            }

    with pytest.raises(RuntimeError, match="wrong count"):
        build_polygon_scan_plan(BoundaryRPC((100,)), manifest, finalized)
    with pytest.raises(RuntimeError, match="invalid finalized bounds"):
        build_polygon_scan_plan(BoundaryRPC((101, 101)), manifest, finalized)

    empty = PolygonMarketManifest(markets=(), sha256="1" * 64, version="1.0.0")
    with pytest.raises(RuntimeError, match="no target block ranges"):
        build_polygon_scan_plan(BoundaryRPC(()), empty, finalized)


def _word(value: int) -> str:
    return f"{value:064x}"


def _raw_log(kind: str, side: int, token: int, maker: int, taker: int, index: int):
    order_filled = kind == "order_filled"
    words = [side, token, maker, taker]
    if order_filled:
        words.extend([0, 0, 0])
    return {
        "address": STANDARD_V2_EXCHANGE,
        "blockNumber": "0x64",
        "blockHash": "0x" + "1" * 64,
        "transactionHash": "0x" + "2" * 64,
        "transactionIndex": "0x0",
        "logIndex": hex(index),
        "removed": False,
        "topics": [
            ORDER_FILLED_TOPIC if order_filled else ORDERS_MATCHED_TOPIC,
            "0x" + "3" * 64,
            "0x" + "4" * 64,
            *(["0x" + "5" * 64] if order_filled else []),
        ],
        "data": "0x" + "".join(_word(value) for value in words),
    }


def test_decoder_drops_identifying_topics_and_leaf_orders_logs() -> None:
    manifest = _manifest()
    market = manifest.markets[0]
    raw_logs = [
        _raw_log("orders_matched", 0, int(market.yes_token_id), 600_000, 1_000_000, 3),
        _raw_log("order_filled", 0, int(market.yes_token_id), 600_000, 1_000_000, 2),
        _raw_log("order_filled", 1, int(market.yes_token_id), 1_000_000, 600_000, 1),
    ]

    class RPC:
        def block(self, number):
            return PolygonBlock(number, "0x" + "1" * 64, market.window_start_at_utc)

    rows, scoped_count, scoped_hash = decode_and_normalize_leaf(
        raw_logs,
        rpc=RPC(),
        manifest=manifest,
        scan_id="scan",
        exchange_address=STANDARD_V2_EXCHANGE,
        from_block=99,
        to_block=101,
        ingested_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
    )

    decoded = decode_settlement_log(raw_logs[1])
    assert scoped_count == 3
    assert len(scoped_hash) == 64
    assert len(rows) == 1
    assert not (
        {"maker", "taker", "order_hash", "topics", "data"}
        & {f.name for f in fields(decoded)}
    )
    assert not ({"maker", "taker", "order_hash", "topics", "data"} & set(rows[0]))


def test_leaf_only_canonicalizes_and_segments_target_transactions() -> None:
    manifest = _manifest()
    market = manifest.markets[0]
    target_logs = [
        _raw_log("order_filled", 1, int(market.yes_token_id), 1_000_000, 600_000, 1),
        _raw_log("order_filled", 0, int(market.yes_token_id), 600_000, 1_000_000, 2),
        _raw_log("orders_matched", 0, int(market.yes_token_id), 600_000, 1_000_000, 3),
    ]
    # Structurally valid, but deliberately unmatched and stale. It is unrelated
    # V2 traffic and therefore cannot affect the target-market scan.
    unrelated = {
        **_raw_log("order_filled", 0, 999, 1, 2, 1),
        "blockNumber": "0x63",
        "blockHash": "0x" + "8" * 64,
        "transactionHash": "0x" + "9" * 64,
    }

    class RPC:
        def __init__(self):
            self.calls = []

        def block(self, number):
            self.calls.append(number)
            return PolygonBlock(number, "0x" + "1" * 64, market.window_start_at_utc)

    rpc = RPC()
    ingested_at = datetime(2026, 8, 1, tzinfo=timezone.utc)
    actual = decode_and_normalize_leaf(
        [unrelated, *target_logs],
        rpc=rpc,
        manifest=manifest,
        scan_id="scan",
        exchange_address=STANDARD_V2_EXCHANGE,
        from_block=99,
        to_block=101,
        ingested_at=ingested_at,
    )
    baseline_rpc = RPC()
    baseline = decode_and_normalize_leaf(
        target_logs,
        rpc=baseline_rpc,
        manifest=manifest,
        scan_id="scan",
        exchange_address=STANDARD_V2_EXCHANGE,
        from_block=99,
        to_block=101,
        ingested_at=ingested_at,
    )

    assert actual == baseline
    assert rpc.calls == [100]
    assert baseline_rpc.calls == [100]


def test_leaf_with_no_target_events_makes_no_block_calls_but_still_decodes() -> None:
    manifest = _manifest()
    unrelated = {
        **_raw_log("order_filled", 0, 999, 1, 2, 1),
        "blockHash": "0x" + "8" * 64,
        "transactionHash": "0x" + "9" * 64,
    }

    class RPC:
        def block(self, _number):
            raise AssertionError("unrelated event block must not be fetched")

    rows, scoped_count, scoped_hash = decode_and_normalize_leaf(
        [unrelated],
        rpc=RPC(),
        manifest=manifest,
        scan_id="scan",
        exchange_address=STANDARD_V2_EXCHANGE,
        from_block=99,
        to_block=101,
    )

    assert rows == []
    assert scoped_count == 0
    assert scoped_hash == polygon_settlement_module._sha256_json([])
    with pytest.raises(PolygonRPCError, match="Removed"):
        decode_and_normalize_leaf(
            [{**unrelated, "removed": True}],
            rpc=RPC(),
            manifest=manifest,
            scan_id="scan",
            exchange_address=STANDARD_V2_EXCHANGE,
            from_block=99,
            to_block=101,
        )


def test_leaf_canonicalizes_every_event_in_selected_target_transaction() -> None:
    manifest = _manifest()
    market = manifest.markets[0]
    unregistered_passive = _raw_log("order_filled", 1, 999, 1_000_000, 600_000, 1)
    target_tail = [
        _raw_log("order_filled", 0, int(market.yes_token_id), 600_000, 1_000_000, 2),
        _raw_log("orders_matched", 0, int(market.yes_token_id), 600_000, 1_000_000, 3),
    ]

    class RPC:
        def block(self, number):
            return PolygonBlock(number, "0x" + "1" * 64, market.window_start_at_utc)

    kwargs = {
        "rpc": RPC(),
        "manifest": manifest,
        "scan_id": "scan",
        "exchange_address": STANDARD_V2_EXCHANGE,
        "from_block": 99,
        "to_block": 101,
    }
    with pytest.raises(ValueError, match="disagrees with finalized header"):
        decode_and_normalize_leaf(
            [
                {**unregistered_passive, "blockHash": "0x" + "8" * 64},
                *target_tail,
            ],
            **kwargs,
        )
    with pytest.raises(ValueError, match="unregistered passive"):
        decode_and_normalize_leaf(
            [unregistered_passive, *target_tail],
            **kwargs,
        )


def test_leaf_rejects_scope_duplicates_headers_and_incomplete_transactions() -> None:
    manifest = _manifest()
    market = manifest.markets[0]
    valid = _raw_log("order_filled", 1, int(market.yes_token_id), 1_000_000, 600_000, 1)

    class RPC:
        def __init__(self, block_hash="0x" + "1" * 64):
            self.block_hash = block_hash

        def block(self, number):
            return PolygonBlock(number, self.block_hash, market.window_start_at_utc)

    kwargs = {
        "rpc": RPC(),
        "manifest": manifest,
        "scan_id": "scan",
        "exchange_address": STANDARD_V2_EXCHANGE,
        "from_block": 99,
        "to_block": 101,
    }
    with pytest.raises(ValueError, match="outside the requested scope"):
        decode_and_normalize_leaf([{**valid, "address": "0x" + "9" * 40}], **kwargs)
    with pytest.raises(ValueError, match="outside the requested scope"):
        decode_and_normalize_leaf([{**valid, "blockNumber": "0x66"}], **kwargs)
    with pytest.raises(ValueError, match="duplicate"):
        decode_and_normalize_leaf([valid, valid], **kwargs)
    with pytest.raises(ValueError, match="disagrees"):
        decode_and_normalize_leaf([valid], **{**kwargs, "rpc": RPC("0x" + "8" * 64)})
    with pytest.raises(ValueError, match="no passive and active"):
        decode_and_normalize_leaf(
            [
                _raw_log(
                    "orders_matched",
                    0,
                    int(market.yes_token_id),
                    600_000,
                    1_000_000,
                    1,
                )
            ],
            **kwargs,
        )
    with pytest.raises(ValueError, match="unmatched OrderFilled"):
        decode_and_normalize_leaf([valid], **kwargs)


def test_decoder_rejects_removed_malformed_and_zero_values() -> None:
    raw = _raw_log("order_filled", 0, 1, 2, 3, 1)
    with pytest.raises(PolygonRPCError, match="Removed"):
        decode_settlement_log({**raw, "removed": True})
    with pytest.raises(PolygonRPCError, match="7 words"):
        decode_settlement_log({**raw, "data": "0x00"})
    with pytest.raises(PolygonRPCError, match="positive"):
        decode_settlement_log(_raw_log("order_filled", 0, 0, 2, 3, 1))


def test_pinned_v2_abi_decodes_side_token_and_seven_data_words() -> None:
    decoded = decode_settlement_log(
        _raw_log("order_filled", 1, 987654321, 2_000_000, 750_000, 9)
    )
    assert decoded.side == "SELL"
    assert decoded.token_id == "987654321"
    assert decoded.maker_amount == 2_000_000
    assert decoded.taker_amount == 750_000
    assert ORDER_FILLED_TOPIC == (
        "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
    )


class _FakeAPI:
    def __init__(self, responses):
        self.responses = iter(responses)

    def post(self, *_args, **_kwargs):
        value = next(self.responses)
        if isinstance(value, BaseException):
            raise value
        return value


class _CaptureAPI(_FakeAPI):
    def __init__(self, responses):
        super().__init__(responses)
        self.payloads = []

    def post(self, *_args, **kwargs):
        self.payloads.append(kwargs["json"])
        return super().post(*_args, **kwargs)


def test_rpc_batch_reorders_rejects_malformed_and_adaptively_splits() -> None:
    api = _CaptureAPI(
        [
            [
                {"jsonrpc": "2.0", "id": 2, "result": "second"},
                {"jsonrpc": "2.0", "id": 1, "result": "first"},
            ]
        ]
    )
    rpc = PolygonRPC("https://example.com/key", api_client=api)
    assert rpc.batch_call([("one", [1]), ("two", [2])]) == ["first", "second"]
    assert [row["method"] for row in api.payloads[0]] == ["one", "two"]
    assert rpc.batch_call([]) == []

    valid_one = {"jsonrpc": "2.0", "id": 1, "result": "one"}
    valid_two = {"jsonrpc": "2.0", "id": 2, "result": "two"}
    malformed = [
        ({}, "malformed batch envelope"),
        ([valid_one], "malformed batch envelope"),
        ([1, valid_two], "malformed batch envelope"),
        ([{**valid_one, "jsonrpc": "1.0"}, valid_two], "malformed batch"),
        ([{**valid_one, "id": True}, valid_two], "malformed batch"),
        ([{**valid_one, "id": 99}, valid_two], "malformed batch"),
        ([valid_one, valid_one], "malformed batch"),
        (
            [
                {**valid_one, "error": {"code": -1}},
                valid_two,
            ],
            "malformed batch",
        ),
        (
            [{"jsonrpc": "2.0", "id": 1, "error": "bad"}, valid_two],
            "malformed batch error",
        ),
        (
            [
                {"jsonrpc": "2.0", "id": 1, "error": {"code": "bad"}},
                valid_two,
            ],
            "malformed batch error",
        ),
        (
            [{"jsonrpc": "2.0", "id": 1, "error": {"code": -7}}, valid_two],
            "batch error code -7",
        ),
        ([{"jsonrpc": "2.0", "id": 1}, valid_two], "omitted result"),
    ]
    for response, message in malformed:
        failing = PolygonRPC("https://example.com/key", api_client=_FakeAPI([response]))
        with pytest.raises(PolygonRPCError, match=message):
            failing.batch_call([("one", []), ("two", [])])

    transport = PolygonRPC(
        "https://example.com/key",
        api_client=_FakeAPI([requests.ConnectionError("secret endpoint")]),
    )
    with pytest.raises(PolygonRPCError, match="transport failed"):
        transport.batch_call([("one", [])])

    adaptive = PolygonRPC("https://example.com/key", api_client=_FakeAPI([]))

    def size_limited(calls):
        if len(calls) > 1:
            raise PolygonRPCSizeLimitError("provider batch limit")
        return [calls[0][0]]

    adaptive.batch_call = size_limited
    assert adaptive._adaptive_batch_call([("one", []), ("two", []), ("three", [])]) == [
        "one",
        "two",
        "three",
    ]
    adaptive.batch_call = lambda _calls: (_ for _ in ()).throw(
        PolygonRPCError("single failure")
    )
    with pytest.raises(PolygonRPCError, match="single failure"):
        adaptive._adaptive_batch_call([("single", [])])


def test_rpc_classifies_size_transport_and_metric_aware_posts() -> None:
    activity = []

    class MetricAPI:
        def post_with_metrics(self, *_args, **_kwargs):
            return {"jsonrpc": "2.0", "id": 1, "result": "ok"}, 3, 2

    metric_rpc = PolygonRPC(
        "https://example.com/key",
        api_client=MetricAPI(),
        activity_callback=activity.append,
    )
    assert metric_rpc.call("test", []) == "ok"
    assert metric_rpc.metrics.http_request_count == 3
    assert metric_rpc.metrics.retry_count == 2
    assert activity == ["test"]

    for response, message in (
        (
            {
                "jsonrpc": "2.0",
                "id": 1,
                "error": {"code": -32005, "message": "response size exceeded"},
            },
            "range size limit",
        ),
        (
            [
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -32000, "message": "too many results"},
                }
            ],
            "batch size limit",
        ),
    ):
        rpc = PolygonRPC("https://example.com/key", api_client=_FakeAPI([response]))
        with pytest.raises(PolygonRPCSizeLimitError, match=message):
            if isinstance(response, list):
                rpc.batch_call([("test", [])])
            else:
                rpc.call("test", [])

    for status, expected in ((413, PolygonRPCSizeLimitError), (500, PolygonRPCError)):
        response = requests.Response()
        response.status_code = status
        error = requests.HTTPError(response=response)
        rpc = PolygonRPC("https://example.com/key", api_client=_FakeAPI([error]))
        with pytest.raises(expected):
            rpc.call("test", [])

    single = PolygonRPC("https://example.com/key", api_client=_FakeAPI([]))
    single.batch_call = lambda _calls: (_ for _ in ()).throw(
        PolygonRPCSizeLimitError("single provider limit")
    )
    with pytest.raises(PolygonRPCSizeLimitError, match="single provider limit"):
        single._adaptive_batch_call([("test", [])])


def _block_result(number: int) -> dict[str, str]:
    return {
        "number": hex(number),
        "hash": f"0x{number:064x}",
        "timestamp": hex(number),
    }


def test_rpc_batched_blocks_cache_and_vectorized_timestamp_search() -> None:
    api = _CaptureAPI(
        [
            [
                {"jsonrpc": "2.0", "id": 2, "result": _block_result(1)},
                {"jsonrpc": "2.0", "id": 1, "result": _block_result(2)},
            ]
        ]
    )
    rpc = PolygonRPC("https://example.com/key", api_client=api)
    assert list(rpc.blocks([2, 1, 2])) == [2, 1]
    assert rpc.blocks([1, 2])[2].number == 2
    assert len(api.payloads) == 1
    for invalid in (-1, True, "1", [1]):
        with pytest.raises(ValueError, match="non-negative integers"):
            rpc.blocks([invalid])
    for topics in ((), ("not-a-topic",)):
        with pytest.raises(ValueError, match="event topics"):
            rpc.logs(STANDARD_V2_EXCHANGE, 1, 2, event_topics=topics)

    unavailable = PolygonRPC(
        "https://example.com/key",
        api_client=_FakeAPI([[{"jsonrpc": "2.0", "id": 1, "result": None}]]),
    )
    with pytest.raises(PolygonRPCError, match="unavailable"):
        unavailable.blocks([1])
    wrong = PolygonRPC(
        "https://example.com/key",
        api_client=_FakeAPI(
            [[{"jsonrpc": "2.0", "id": 1, "result": _block_result(2)}]]
        ),
    )
    with pytest.raises(PolygonRPCError, match="wrong block"):
        wrong.blocks([1])

    search = PolygonRPC("https://example.com/key", api_client=_FakeAPI([]))
    origin = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def deterministic_blocks(numbers):
        return {
            number: PolygonBlock(
                number,
                f"0x{number:064x}",
                origin + timedelta(seconds=number),
            )
            for number in numbers
        }

    search.blocks = deterministic_blocks
    head = deterministic_blocks([10])[10]
    assert search.first_blocks_at_or_after(
        [origin, origin + timedelta(seconds=3), origin + timedelta(seconds=7)],
        finalized_head=head,
    ) == (0, 3, 7)
    short_head = deterministic_blocks([2])[2]
    assert search.first_blocks_at_or_after(
        [origin, origin + timedelta(seconds=2)],
        finalized_head=short_head,
    ) == (0, 2)
    assert search.first_blocks_at_or_after([], finalized_head=head) == ()
    with pytest.raises(PolygonRPCError, match="extends past"):
        search.first_blocks_at_or_after(
            [origin + timedelta(seconds=11)], finalized_head=head
        )


def _receipt_result(*, logs=None, **overrides):
    value = {
        "transactionHash": "0x" + "2" * 64,
        "blockNumber": "0x64",
        "blockHash": "0x" + "1" * 64,
        "transactionIndex": "0x0",
        "status": "0x1",
        "logs": [_raw_log("orders_matched", 0, 1, 2, 3, 1)] if logs is None else logs,
    }
    value.update(overrides)
    return value


def test_rpc_batched_receipts_validate_every_locator() -> None:
    transaction_hash = "0x" + "2" * 64
    rpc = PolygonRPC(
        "https://example.com/key",
        api_client=_FakeAPI(
            [[{"jsonrpc": "2.0", "id": 1, "result": _receipt_result()}]]
        ),
    )
    receipts = rpc.transaction_receipts([transaction_hash, transaction_hash])
    assert list(receipts) == [transaction_hash]
    assert receipts[transaction_hash].block_number == 100
    assert len(receipts[transaction_hash].logs) == 1
    assert rpc.metrics.receipt_rpc_call_count == 1
    assert rpc.metrics.http_request_count == 1
    assert rpc.transaction_receipts([]) == {}
    with pytest.raises(ValueError, match="32-byte hex"):
        rpc.transaction_receipts(["secret"])

    base_log = _raw_log("orders_matched", 0, 1, 2, 3, 1)
    invalid = [
        (None, "unavailable"),
        (_receipt_result(transactionHash="0x" + "9" * 64), "wrong receipt"),
        (_receipt_result(status="0x0"), "not successful"),
        ({**_receipt_result(), "logs": None}, "receipt logs"),
        (_receipt_result(logs=[{**base_log, "removed": True}]), "inconsistent"),
        (
            _receipt_result(logs=[{**base_log, "transactionHash": "0x" + "9" * 64}]),
            "inconsistent",
        ),
        (
            _receipt_result(logs=[{**base_log, "blockNumber": "0x65"}]),
            "inconsistent",
        ),
        (
            _receipt_result(logs=[{**base_log, "blockHash": "0x" + "9" * 64}]),
            "inconsistent",
        ),
        (
            _receipt_result(logs=[{**base_log, "transactionIndex": "0x1"}]),
            "inconsistent",
        ),
    ]
    for result, message in invalid:
        failing = PolygonRPC(
            "https://example.com/key",
            api_client=_FakeAPI([[{"jsonrpc": "2.0", "id": 1, "result": result}]]),
        )
        with pytest.raises(PolygonRPCError, match=message):
            failing.transaction_receipts([transaction_hash])


def test_rpc_caller_sized_receipt_batch_handles_empty_and_complete() -> None:
    transaction_hash = "0x" + "2" * 64
    rpc = PolygonRPC(
        "https://example.com/key",
        api_client=_FakeAPI(
            [[{"jsonrpc": "2.0", "id": 1, "result": _receipt_result()}]]
        ),
    )
    assert rpc.transaction_receipt_batch([]) == {}
    assert list(rpc.transaction_receipt_batch([transaction_hash])) == [transaction_hash]


def test_rpc_envelope_origin_binary_search_and_errors() -> None:
    rpc = PolygonRPC(
        "https://user:secret@example.com/private/key?token=hidden",
        api_client=_FakeAPI([{"jsonrpc": "2.0", "id": 1, "result": "0x89"}]),
    )
    assert rpc.origin == "https://example.com"
    assert rpc.chain_id() == 137
    assert (
        sanitize_rpc_origin("https://example.com:8443/path?q=secret")
        == "https://example.com:8443"
    )

    failing = PolygonRPC(
        "https://example.com/key",
        api_client=_FakeAPI(
            [
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -1, "message": "bare-secret-value"},
                }
            ]
        ),
    )
    with pytest.raises(PolygonRPCError, match="error code -1") as rpc_error:
        failing.call("eth_getLogs", [])
    assert "bare-secret-value" not in str(rpc_error.value)
    transport = PolygonRPC(
        "https://example.com/key", api_client=_FakeAPI([requests.Timeout("secret")])
    )
    with pytest.raises(PolygonRPCError, match="transport failed") as raised:
        transport.call("eth_chainId", [])
    assert "example.com" not in str(raised.value)

    class Blocks:
        def block(self, number):
            return PolygonBlock(
                number,
                f"0x{number:064x}",
                datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=number),
            )

    head = Blocks().block(10)
    result = PolygonRPC.first_block_at_or_after(
        Blocks(),
        datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=4),
        finalized_head=head,
    )
    assert result == 4


def test_rpc_origin_and_decoder_reject_all_malformed_fields() -> None:
    assert (
        sanitize_rpc_origin("https://[2001:4860:4860::8888]:443/key")
        == "https://[2001:4860:4860::8888]"
    )
    for url in (
        "http://example.com",
        "https:///missing-host",
        "https://example.com:bad",
    ):
        with pytest.raises(ValueError):
            sanitize_rpc_origin(url)

    base = _raw_log("order_filled", 0, 1, 2, 3, 1)
    malformed = [
        ({**base, "address": "0x1234"}, "exchange address"),
        ({**base, "topics": None}, "topics are missing"),
        ({**base, "topics": ["0x1234"]}, "32-byte hex"),
        ({**base, "blockNumber": "0x01"}, "hex quantity"),
        ({**base, "topics": base["topics"][:-1]}, "four topics"),
        (
            {
                **_raw_log("orders_matched", 0, 1, 2, 3, 1),
                "topics": [ORDERS_MATCHED_TOPIC],
            },
            "three topics",
        ),
        (
            {**base, "topics": ["0x" + "f" * 64, *base["topics"][1:]]},
            "Unexpected",
        ),
        (
            {**base, "data": "0x" + "".join(_word(v) for v in [2, 1, 2, 3, 0, 0, 0])},
            "side",
        ),
    ]
    for raw, message in malformed:
        with pytest.raises(PolygonRPCError, match=message):
            decode_settlement_log(raw)


def test_rpc_protocol_envelopes_block_cache_finality_and_logs(monkeypatch) -> None:
    created = {}

    class CreatedAPI:
        def __init__(self, base_url, **kwargs):
            created.update(base_url=base_url, **kwargs)

    monkeypatch.setattr(
        polygon_rpc_module, "validate_outbound_https_url", lambda url: url
    )
    monkeypatch.setattr(polygon_rpc_module, "APIClient", CreatedAPI)
    constructed = PolygonRPC(
        "https://rpc.example/key",
        retries=2,
        backoff_factor=0.25,
        requests_per_second=7,
    )
    assert created == {
        "base_url": "https://rpc.example/key",
        "retries": 2,
        "backoff_factor": 0.25,
        "requests_per_second": 7,
        "rate_limiter": None,
    }
    assert constructed.origin == "https://rpc.example"

    malformed_responses = [
        ([], "malformed response envelope"),
        ({"id": 1, "result": 1}, "malformed response envelope"),
        ({"jsonrpc": "1.0", "id": 1, "result": 1}, "malformed response envelope"),
        ({"jsonrpc": "2.0", "id": 99, "result": 1}, "malformed response envelope"),
        (
            {"jsonrpc": "2.0", "id": 1, "result": 1, "error": None},
            "malformed response envelope",
        ),
        ({"jsonrpc": "2.0", "id": 1, "error": "bad"}, "malformed error"),
        (
            {"jsonrpc": "2.0", "id": 1, "error": {"code": "-1"}},
            "malformed error",
        ),
        ({"jsonrpc": "2.0", "id": 1}, "omitted result"),
    ]
    for response, message in malformed_responses:
        rpc = PolygonRPC("https://example.com/key", api_client=_FakeAPI([response]))
        with pytest.raises(PolygonRPCError, match=message):
            rpc.call("test", [])

    rpc = PolygonRPC("https://example.com/key", api_client=_FakeAPI([]))
    calls = []

    def block_call(method, params):
        calls.append((method, params))
        return {
            "number": "0x2",
            "hash": "0x" + "A" * 64,
            "timestamp": "0x1",
        }

    rpc.call = block_call
    assert rpc.block(2).number == 2
    assert rpc.block(2).hash == "0x" + "a" * 64
    assert len(calls) == 1

    finalized = PolygonRPC("https://example.com/key", api_client=_FakeAPI([]))
    finalized.call = lambda _method, _params: {
        "number": "0x3",
        "hash": "0x" + "3" * 64,
        "timestamp": "0x2",
    }
    assert finalized.finalized_head().number == 3

    unavailable = PolygonRPC("https://example.com/key", api_client=_FakeAPI([]))
    unavailable.call = lambda _method, _params: None
    with pytest.raises(PolygonRPCError, match="unavailable"):
        unavailable.block(2)
    wrong = PolygonRPC("https://example.com/key", api_client=_FakeAPI([]))
    wrong.call = lambda _method, _params: {
        "number": "0x3",
        "hash": "0x" + "3" * 64,
        "timestamp": "0x2",
    }
    with pytest.raises(PolygonRPCError, match="wrong block"):
        wrong.block(2)

    logs_rpc = PolygonRPC("https://example.com/key", api_client=_FakeAPI([]))
    observed_params = []

    def logs_call(_method, params):
        observed_params.extend(params)
        return [{}]

    logs_rpc.call = logs_call
    assert logs_rpc.logs(STANDARD_V2_EXCHANGE, 1, 2) == [{}]
    assert observed_params[0]["fromBlock"] == "0x1"
    for start, end in ((-1, 2), (2, 1)):
        with pytest.raises(ValueError, match="Invalid inclusive"):
            logs_rpc.logs(STANDARD_V2_EXCHANGE, start, end)
    for invalid in (None, [1]):
        logs_rpc.call = lambda _method, _params, value=invalid: value
        with pytest.raises(PolygonRPCError, match="list of objects"):
            logs_rpc.logs(STANDARD_V2_EXCHANGE, 1, 2)

    class FutureBlocks:
        def block(self, number):
            return PolygonBlock(
                number,
                f"0x{number:064x}",
                datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=number),
            )

    future = FutureBlocks().block(10)
    with pytest.raises(PolygonRPCError, match="extends past"):
        PolygonRPC.first_block_at_or_after(
            FutureBlocks(),
            future.timestamp + timedelta(seconds=1),
            finalized_head=future,
        )


def test_adaptive_log_scan_splits_errors_and_config_validates() -> None:
    class SplitRPC:
        def logs(self, _address, start, end):
            if start != end:
                raise PolygonRPCSizeLimitError("provider range limit")
            return [{"block": start}]

    leaves = list(adaptive_log_leaves(SplitRPC(), "0xexchange", 1, 4))
    assert [(start, end) for start, end, _ in leaves] == [
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 4),
    ]

    class AlwaysFailRPC:
        def logs(self, _address, _start, _end):
            raise PolygonRPCSizeLimitError("provider range limit")

    with pytest.raises(PolygonRPCError, match="provider range limit"):
        list(adaptive_log_leaves(AlwaysFailRPC(), "0xexchange", 1, 1))

    invalid_configs = (
        ({"requests_per_second": 0}, "requests_per_second"),
        ({"workers": 0}, "workers"),
        ({"initial_block_chunk_size": 249}, "initial_block_chunk_size"),
        ({"initial_block_chunk_size": 20_001}, "initial_block_chunk_size"),
        ({"initial_receipt_batch_size": 4}, "initial_receipt_batch_size"),
        ({"initial_receipt_batch_size": 51}, "initial_receipt_batch_size"),
        ({"transient_retries": -1}, "transient_retries"),
        ({"transient_backoff_seconds": -1}, "backoff"),
    )
    for values, message in invalid_configs:
        with pytest.raises(ValueError, match=message):
            PolygonSettlementSyncConfig(**values)


def test_gap_and_range_work_helpers_cover_resume_boundaries() -> None:
    assert polygon_settlement_module._gaps((10, 20), [(1, 2), (12, 14), (30, 40)]) == [
        (10, 11),
        (15, 20),
    ]
    with pytest.raises(RuntimeError, match="overlap"):
        polygon_settlement_module._gaps((10, 20), [(10, 15), (14, 18)])
    assert polygon_settlement_module._gaps((10, 20), [(10, 12), (13, 15)]) == [(16, 20)]
    assert polygon_settlement_module._gaps((10, 20), [(10, 20)]) == []
    with pytest.raises(RuntimeError, match="cross target bounds"):
        polygon_settlement_module._gaps((10, 20), [(9, 12)])
    state = polygon_settlement_module._RangeWork(
        target=polygon_settlement_module.PolygonTargetRange(
            exchange_address=STANDARD_V2_EXCHANGE.casefold(),
            from_block=1,
            to_block=5,
            from_block_hash="0x" + "1" * 64,
            to_block_hash="0x" + "2" * 64,
        ),
        gaps=polygon_settlement_module.deque([(1, 5)]),
        chunk_size=2,
    )
    assert [state.next_chunk(), state.next_chunk(), state.next_chunk()] == [
        (1, 2),
        (3, 4),
        (5, 5),
    ]


def test_stored_target_range_parser_rejects_bad_json_and_typed_values() -> None:
    with pytest.raises(RuntimeError, match="malformed"):
        polygon_settlement_module._parse_target_ranges("{")

    valid = {
        "exchange_address": STANDARD_V2_EXCHANGE,
        "from_block": 10,
        "to_block": 12,
        "from_block_hash": "0x" + "1" * 64,
        "to_block_hash": "0x" + "2" * 64,
    }
    malformed = (
        [valid],
        [{**valid, "exchange_address": "0x" + "9" * 40}],
        [{**valid, "from_block": True}],
        [{**valid, "to_block": 9}],
        [{**valid, "from_block_hash": "0x" + "z" * 64}],
        [valid, {**valid, "from_block": 12, "to_block": 13}],
    )
    for payload in malformed:
        with pytest.raises(RuntimeError, match="malformed"):
            polygon_settlement_module._parse_target_ranges(json.dumps(payload))


def test_incremental_hash_matches_canonical_json_without_materializing_payloads() -> (
    None
):
    market = _manifest().markets[0]
    transactions = [
        [
            _event("order_filled", "SELL", market.yes_token_id, 1, 1, 1),
            _event("orders_matched", "BUY", market.yes_token_id, 1, 1, 2),
        ]
    ]
    count, digest = polygon_settlement_module._incremental_scoped_hash(
        transactions, {market.yes_token_id}
    )
    expected = polygon_settlement_module._sha256_json(
        [
            polygon_settlement_module._event_payload(event)
            for transaction in transactions
            for event in transaction
        ]
    )
    assert (count, digest) == (2, expected)

    count, digest = polygon_settlement_module._incremental_scoped_hash(
        transactions, {"unrelated-token"}
    )
    assert (count, digest) == (0, polygon_settlement_module._sha256_json([]))


def test_receipt_batch_adaptation_grows_and_splits_only_within_safe_bounds() -> None:
    hashes = [f"0x{value:064x}" for value in range(1, 61)]

    class FastRPC:
        def __init__(self):
            self.metrics = PolygonRPCMetrics()
            self.sizes = []

        def transaction_receipt_batch(self, batch):
            self.sizes.append(len(batch))
            return {value: object() for value in batch}

    fast = FastRPC()
    fetched, splits = polygon_settlement_module._fetch_receipts_adaptively(
        fast, hashes, initial_batch_size=5
    )
    assert list(fetched) == hashes
    assert fast.sizes == [5, 10, 20, 25]
    assert splits == 0

    class LimitedRPC(FastRPC):
        def transaction_receipt_batch(self, batch):
            self.sizes.append(len(batch))
            if len(batch) > 5:
                raise PolygonRPCSizeLimitError("recognized provider batch limit")
            return {value: object() for value in batch}

    limited = LimitedRPC()
    fetched, splits = polygon_settlement_module._fetch_receipts_adaptively(
        limited, hashes[:11], initial_batch_size=20
    )
    assert list(fetched) == hashes[:11]
    assert max(limited.sizes) <= 20
    assert all(size >= 5 for size in limited.sizes[:-1])
    assert limited.sizes[-1] == 1
    assert limited.sizes[:3] == [11, 10, 5]
    assert splits >= 2


def test_receipt_batch_adaptation_fails_closed_and_shrinks_after_retry(
    monkeypatch,
) -> None:
    hashes = [f"0x{value:064x}" for value in range(1, 11)]

    class MinimumLimitedRPC:
        metrics = PolygonRPCMetrics()

        def transaction_receipt_batch(self, _batch):
            raise PolygonRPCSizeLimitError("recognized provider batch limit")

    with pytest.raises(RuntimeError, match="safe minimum"):
        polygon_settlement_module._fetch_receipts_adaptively(
            MinimumLimitedRPC(), hashes[:5], initial_batch_size=5
        )

    class IncompleteRPC:
        metrics = PolygonRPCMetrics()

        def transaction_receipt_batch(self, batch):
            return {batch[0]: object()}

    with pytest.raises(ValueError, match="did not return every"):
        polygon_settlement_module._fetch_receipts_adaptively(
            IncompleteRPC(), hashes[:5], initial_batch_size=5
        )

    class RetriedRPC:
        def __init__(self):
            self.metrics = PolygonRPCMetrics()
            self.sizes = []

        def transaction_receipt_batch(self, batch):
            self.sizes.append(len(batch))
            self.metrics.retry_count += 1
            return {value: object() for value in batch}

    retried = RetriedRPC()
    polygon_settlement_module._fetch_receipts_adaptively(
        retried, [*hashes, "0x" + "f" * 64], initial_batch_size=10
    )
    assert retried.sizes == [10, 1]

    timestamps = iter((0.0, 21.0))
    monkeypatch.setattr(
        polygon_settlement_module, "monotonic", lambda: next(timestamps)
    )

    class SlowRPC:
        metrics = PolygonRPCMetrics()

        def transaction_receipt_batch(self, batch):
            return {value: object() for value in batch}

    assert (
        len(
            polygon_settlement_module._fetch_receipts_adaptively(
                SlowRPC(), hashes, initial_batch_size=10
            )[0]
        )
        == 10
    )

    timestamps = iter((0.0, 10.0, 10.0, 20.0))
    monkeypatch.setattr(
        polygon_settlement_module, "monotonic", lambda: next(timestamps)
    )
    assert (
        len(
            polygon_settlement_module._fetch_receipts_adaptively(
                SlowRPC(), hashes[:6], initial_batch_size=5
            )[0]
        )
        == 6
    )


def test_complete_leaf_filters_wrong_window_before_receipts_and_reports_metrics() -> (
    None
):
    manifest = _manifest()
    market = manifest.markets[0]

    class FilterRPC:
        def __init__(self):
            self.metrics = PolygonRPCMetrics()
            self.receipt_calls = 0

        def logs(self, _address, _start, _end, *, event_topics):
            assert tuple(event_topics) == (ORDERS_MATCHED_TOPIC,)
            self.metrics.log_rpc_call_count += 1
            return [
                _raw_log(
                    "orders_matched",
                    0,
                    int(market.yes_token_id),
                    600_000,
                    1_000_000,
                    3,
                )
            ]

        def transaction_receipt_batch(self, _batch):
            self.receipt_calls += 1
            raise AssertionError("wrong-window discovery reached receipt fetch")

        def blocks(self, numbers):
            return {
                number: PolygonBlock(
                    number,
                    f"0x{number:064x}",
                    market.window_start_at_utc + timedelta(seconds=number - 100),
                )
                for number in dict.fromkeys(numbers)
            }

    rpc = FilterRPC()
    token_targets = {
        token_id: polygon_settlement_module.PolygonTokenTarget(
            market=mapped_market,
            outcome_side=outcome,
            exchange_address=STANDARD_V2_EXCHANGE.casefold(),
            first_valid_block=101,
            first_invalid_block=102,
        )
        for token_id, (mapped_market, outcome) in manifest.by_token.items()
    }
    result = polygon_settlement_module._collect_and_normalize_leaf(
        rpc=rpc,
        manifest=manifest,
        token_targets=token_targets,
        token_index=manifest.by_token,
        scan_id="scan",
        exchange_address=STANDARD_V2_EXCHANGE.casefold(),
        from_block=99,
        to_block=101,
        log_chunk_size=250,
        receipt_batch_size=5,
    )
    assert rpc.receipt_calls == 0
    assert result.rows == ()
    assert result.metrics.discovery_count == 1
    assert result.metrics.eligible_discovery_count == 0
    assert result.metrics.filtered_discovery_count == 1
    assert result.metrics.receipt_transaction_count == 0


def test_complete_leaf_rejects_provider_and_receipt_inconsistencies() -> None:
    manifest = _manifest()
    market = manifest.markets[0]
    token_targets = {
        token_id: polygon_settlement_module.PolygonTokenTarget(
            market=mapped_market,
            outcome_side=outcome,
            exchange_address=STANDARD_V2_EXCHANGE.casefold(),
            first_valid_block=100,
            first_invalid_block=101,
        )
        for token_id, (mapped_market, outcome) in manifest.by_token.items()
    }
    kwargs = {
        "manifest": manifest,
        "token_targets": token_targets,
        "token_index": manifest.by_token,
        "scan_id": "scan",
        "exchange_address": STANDARD_V2_EXCHANGE.casefold(),
        "from_block": 99,
        "to_block": 101,
        "log_chunk_size": 250,
        "receipt_batch_size": 5,
    }

    class InvalidDiscoveryRPC(_SyncRPC):
        def logs(self, *_args, **_kwargs):
            return [self._settlement_rows()[0]]

    with pytest.raises(ValueError, match="invalid discovery"):
        polygon_settlement_module._collect_and_normalize_leaf(
            rpc=InvalidDiscoveryRPC(manifest), **kwargs
        )

    class DuplicateDiscoveryRPC(_SyncRPC):
        def logs(self, *_args, **_kwargs):
            matched = self._settlement_rows()[-1]
            return [matched, matched]

    with pytest.raises(ValueError, match="duplicate discovery"):
        polygon_settlement_module._collect_and_normalize_leaf(
            rpc=DuplicateDiscoveryRPC(manifest), **kwargs
        )

    class OutsideReceiptRPC(_SyncRPC):
        def transaction_receipts(self, hashes):
            return {
                key: replace(receipt, block_number=102)
                for key, receipt in super().transaction_receipts(hashes).items()
            }

    with pytest.raises(ValueError, match="no in-range receipt"):
        polygon_settlement_module._collect_and_normalize_leaf(
            rpc=OutsideReceiptRPC(manifest), **kwargs
        )

    class DuplicateReceiptRPC(_SyncRPC):
        def transaction_receipts(self, hashes):
            return {
                key: replace(receipt, logs=(*receipt.logs, receipt.logs[0]))
                for key, receipt in super().transaction_receipts(hashes).items()
            }

    with pytest.raises(ValueError, match="duplicate settlement logs"):
        polygon_settlement_module._collect_and_normalize_leaf(
            rpc=DuplicateReceiptRPC(manifest), **kwargs
        )

    class ConflictingReceiptRPC(_SyncRPC):
        def transaction_receipts(self, hashes):
            conflicting = _raw_log(
                "orders_matched",
                0,
                int(market.no_token_id),
                600_000,
                1_000_000,
                3,
            )
            return {
                key: replace(receipt, logs=(*receipt.logs[:-1], conflicting))
                for key, receipt in super().transaction_receipts(hashes).items()
            }

    with pytest.raises(ValueError, match="discovery and receipt logs disagree"):
        polygon_settlement_module._collect_and_normalize_leaf(
            rpc=ConflictingReceiptRPC(manifest), **kwargs
        )

    class HeaderMismatchRPC(_SyncRPC):
        def block(self, number):
            result = super().block(number)
            return replace(result, hash="0x" + "9" * 64) if number == 100 else result

    with pytest.raises(ValueError, match="block hash disagrees"):
        polygon_settlement_module._collect_and_normalize_leaf(
            rpc=HeaderMismatchRPC(manifest), **kwargs
        )

    class FilteringReceiptRPC(_SyncRPC):
        def transaction_receipts(self, hashes):
            unrelated = {**self._settlement_rows()[0], "address": "0x" + "9" * 40}
            return {
                key: replace(receipt, logs=(unrelated, *receipt.logs))
                for key, receipt in super().transaction_receipts(hashes).items()
            }

    assert polygon_settlement_module._collect_and_normalize_leaf(
        rpc=FilteringReceiptRPC(manifest), **kwargs
    ).rows


@pytest.mark.parametrize(
    ("timestamps", "expected_size"),
    [((0.0, 0.0, 31.0, 31.0), 250), ((0.0, 0.0, 10.0, 10.0), 500)],
)
def test_complete_leaf_adapts_or_retains_log_chunk_size(
    monkeypatch, timestamps, expected_size
) -> None:
    manifest = _manifest()
    rpc = _SyncRPC(manifest)
    rpc.logs = lambda *_args, **_kwargs: []
    clock = iter(timestamps)
    monkeypatch.setattr(polygon_settlement_module, "monotonic", lambda: next(clock))
    result = polygon_settlement_module._collect_and_normalize_leaf(
        rpc=rpc,
        manifest=manifest,
        token_targets={},
        token_index={},
        scan_id="scan",
        exchange_address=STANDARD_V2_EXCHANGE.casefold(),
        from_block=99,
        to_block=101,
        log_chunk_size=500,
        receipt_batch_size=5,
    )
    assert result.next_log_chunk_size == expected_size


def test_status_json_is_atomic_allowlisted_and_rejects_endpoint_fields(
    tmp_path,
) -> None:
    path = tmp_path / "status.json"
    polygon_settlement_module._write_status(
        path,
        {"scan_id": "scan", "version": NORMALIZER_VERSION, "status": "running"},
    )
    assert json.loads(path.read_text(encoding="utf-8")) == {
        "scan_id": "scan",
        "status": "running",
        "version": NORMALIZER_VERSION,
    }
    assert not list(tmp_path.glob("*.tmp"))
    with pytest.raises(ValueError, match="prohibited field"):
        polygon_settlement_module._write_status(
            path, {"scan_id": "scan", "rpc_url": "https://secret.invalid/key"}
        )


@pytest.mark.parametrize("already_missing", [False, True])
def test_status_json_removes_temporary_file_after_atomic_replace_failure(
    tmp_path, monkeypatch, already_missing
) -> None:
    path = tmp_path / "status.json"
    monkeypatch.setattr(
        polygon_settlement_module.os,
        "replace",
        lambda *_args: (_ for _ in ()).throw(OSError("synthetic replace failure")),
    )
    if already_missing:
        monkeypatch.setattr(
            polygon_settlement_module.os,
            "unlink",
            lambda *_args: (_ for _ in ()).throw(FileNotFoundError()),
        )
    with pytest.raises(OSError, match="synthetic replace failure"):
        polygon_settlement_module._write_status(path, {"scan_id": "scan"})
    if not already_missing:
        assert not list(tmp_path.iterdir())


def test_concurrent_leaf_collection_is_bounded_thread_local_without_head_of_line() -> (
    None
):
    manifest = _manifest()
    market = manifest.markets[0]
    main_thread = get_ident()
    fast_finished = Event()
    release_slow = Event()
    lock = Lock()
    active = 0
    max_active = 0
    completed: list[int] = []
    rpc_owners: list[tuple[int, int]] = []

    class WorkerRPC(_SyncRPC):
        def __init__(self, manifest) -> None:
            super().__init__(manifest)
            self.owner: int | None = None

        def logs(self, address, start, end, *, event_topics=EVENT_TOPICS):
            assert tuple(event_topics) == (ORDERS_MATCHED_TOPIC,)
            nonlocal active, max_active
            thread_id = get_ident()
            assert thread_id != main_thread
            if self.owner is None:
                self.owner = thread_id
                rpc_owners.append((id(self), thread_id))
            assert self.owner == thread_id
            with lock:
                active += 1
                max_active = max(max_active, active)
            try:
                if start == 99:
                    assert fast_finished.wait(timeout=2)
                    assert release_slow.wait(timeout=2)
                elif start == 100:
                    fast_finished.set()
                with lock:
                    completed.append(start)
                return super().logs(address, start, end, event_topics=event_topics)
            finally:
                with lock:
                    active -= 1

    work = [
        polygon_settlement_module._RangeWork(
            target=polygon_settlement_module.PolygonTargetRange(
                exchange_address=STANDARD_V2_EXCHANGE.casefold(),
                from_block=value,
                to_block=value,
                from_block_hash=f"0x{value:064x}",
                to_block_hash=f"0x{value:064x}",
            ),
            gaps=polygon_settlement_module.deque([(value, value)]),
            chunk_size=250,
        )
        for value in range(99, 106)
    ]
    token_targets = {
        token_id: polygon_settlement_module.PolygonTokenTarget(
            market=market,
            outcome_side=outcome,
            exchange_address=STANDARD_V2_EXCHANGE.casefold(),
            first_valid_block=100,
            first_invalid_block=101,
        )
        for token_id, outcome in (
            (market.yes_token_id, "yes"),
            (market.no_token_id, "no"),
        )
    }
    iterator = polygon_settlement_module._concurrent_leaf_results(
        work,
        rpc_factory=lambda: WorkerRPC(manifest),
        manifest=manifest,
        token_targets=token_targets,
        token_index=manifest.by_token,
        scan_id="scan",
        receipt_batch_size=5,
        workers=2,
    )
    first = next(iterator)
    assert first[1] == 100
    release_slow.set()
    fetched = [first, *iterator]

    assert sorted(start for _, start, *_rest in fetched) == list(range(99, 106))
    assert completed[0] == 100
    assert max_active == 2
    assert len(rpc_owners) == 2
    assert len({owner for _, owner in rpc_owners}) == 2
    with pytest.raises(ValueError, match="workers must be positive"):
        list(
            polygon_settlement_module._concurrent_leaf_results(
                [],
                rpc_factory=lambda: WorkerRPC(manifest),
                manifest=manifest,
                token_targets=token_targets,
                token_index=manifest.by_token,
                scan_id="scan",
                receipt_batch_size=5,
                workers=0,
            )
        )


def test_concurrent_leaf_fetch_preserves_leaves_before_terminal_split_error() -> None:
    manifest = _manifest()

    class WorkerRPC(_SyncRPC):
        def logs(self, _address, start, end, *, event_topics=EVENT_TOPICS):
            assert tuple(event_topics) == (ORDERS_MATCHED_TOPIC,)
            if (start, end) in {(0, 999), (500, 999), (500, 749)}:
                raise PolygonRPCSizeLimitError("provider range failure")
            return []

    rpc = WorkerRPC(manifest)
    leaves, error = polygon_settlement_module._collect_parent_range(
        rpc=rpc,
        manifest=manifest,
        token_targets={},
        token_index={},
        scan_id="scan",
        exchange_address=STANDARD_V2_EXCHANGE.casefold(),
        from_block=0,
        to_block=999,
        log_chunk_size=1_000,
        receipt_batch_size=5,
    )
    assert [(leaf.from_block, leaf.to_block) for leaf in leaves] == [(0, 499)]
    assert isinstance(error, PolygonRPCError)

    def broken_factory():
        raise RuntimeError("worker setup failed")

    setup_failure = list(
        polygon_settlement_module._concurrent_leaf_results(
            [
                polygon_settlement_module._RangeWork(
                    target=polygon_settlement_module.PolygonTargetRange(
                        exchange_address=STANDARD_V2_EXCHANGE.casefold(),
                        from_block=4,
                        to_block=5,
                        from_block_hash="0x" + "4" * 64,
                        to_block_hash="0x" + "5" * 64,
                    ),
                    gaps=polygon_settlement_module.deque([(4, 5)]),
                    chunk_size=250,
                )
            ],
            rpc_factory=broken_factory,
            manifest=manifest,
            token_targets={},
            token_index={},
            scan_id="scan",
            receipt_batch_size=5,
            workers=1,
        )
    )
    assert setup_failure[0][1:3] == (4, 5)
    assert isinstance(setup_failure[0][4], RuntimeError)


def test_concurrent_leaf_generator_cancels_pending_work_when_closed() -> None:
    manifest = _manifest()

    class SlowRPC(_SyncRPC):
        def logs(self, address, start, end, *, event_topics=EVENT_TOPICS):
            if start == 2:
                sleep(0.02)
            return super().logs(address, start, end, event_topics=event_topics)

    work = [
        polygon_settlement_module._RangeWork(
            target=polygon_settlement_module.PolygonTargetRange(
                exchange_address=STANDARD_V2_EXCHANGE.casefold(),
                from_block=value,
                to_block=value,
                from_block_hash=f"0x{value:064x}",
                to_block_hash=f"0x{value:064x}",
            ),
            gaps=polygon_settlement_module.deque([(value, value)]),
            chunk_size=250,
        )
        for value in (1, 2)
    ]
    iterator = polygon_settlement_module._concurrent_leaf_results(
        work,
        rpc_factory=lambda: SlowRPC(manifest),
        manifest=manifest,
        token_targets={},
        token_index={},
        scan_id="scan",
        receipt_batch_size=5,
        workers=2,
    )
    next(iterator)
    iterator.close()


def test_concurrent_leaf_requeues_range_after_empty_worker_result(monkeypatch) -> None:
    manifest = _manifest()
    state = polygon_settlement_module._RangeWork(
        target=polygon_settlement_module.PolygonTargetRange(
            exchange_address=STANDARD_V2_EXCHANGE.casefold(),
            from_block=1,
            to_block=1,
            from_block_hash="0x" + "1" * 64,
            to_block_hash="0x" + "1" * 64,
        ),
        gaps=polygon_settlement_module.deque([(1, 1)]),
        chunk_size=250,
    )
    monkeypatch.setattr(
        polygon_settlement_module,
        "_collect_parent_range",
        lambda **_kwargs: ([], None),
    )
    results = list(
        polygon_settlement_module._concurrent_leaf_results(
            [state],
            rpc_factory=lambda: _SyncRPC(manifest),
            manifest=manifest,
            token_targets={},
            token_index={},
            scan_id="scan",
            receipt_batch_size=5,
            workers=1,
        )
    )
    assert len(results) == 1
    assert results[0][3:] == ([], None)


def test_api_client_post_uses_shared_timeout_and_rate_control(monkeypatch) -> None:
    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"ok": True}

    observed = {}

    def post(url, **kwargs):
        observed.update(url=url, **kwargs)
        return Response()

    client = APIClient("https://example.com", request_timeout=7)
    monkeypatch.setattr(client.session, "post", post)
    assert client.post("/rpc", json={"id": 1}) == {"ok": True}
    assert observed["url"] == "https://example.com/rpc"
    assert observed["timeout"] == 7


class _SyncRPC:
    def __init__(
        self,
        manifest,
        *,
        fail_neg_risk=False,
        collateral=600_000,
        origin="https://rpc.example",
        hash_overrides=None,
    ):
        self.manifest = manifest
        self.fail_neg_risk = fail_neg_risk
        self.collateral = collateral
        self.origin = origin
        self.hash_overrides = hash_overrides or {}

    def chain_id(self):
        return 137

    def finalized_head(self):
        return PolygonBlock(
            200,
            f"0x{200:064x}",
            self.manifest.markets[0].window_end_at_utc + timedelta(days=1),
        )

    def first_block_at_or_after(self, timestamp, *, finalized_head, low=0):
        del finalized_head, low
        return 100 if timestamp == self.manifest.markets[0].window_start_at_utc else 101

    def block(self, number):
        if number in self.hash_overrides:
            block_hash = self.hash_overrides[number]
            timestamp = self.manifest.markets[0].window_start_at_utc + timedelta(
                seconds=number - 100
            )
        elif number == 100:
            block_hash = "0x" + "1" * 64
            timestamp = self.manifest.markets[0].window_start_at_utc
        else:
            block_hash = f"0x{number:064x}"
            timestamp = self.manifest.markets[0].window_start_at_utc + timedelta(
                seconds=number - 100
            )
        return PolygonBlock(number, block_hash, timestamp)

    def blocks(self, numbers):
        return {number: self.block(number) for number in dict.fromkeys(numbers)}

    def _settlement_rows(self):
        market = self.manifest.markets[0]
        return [
            _raw_log(
                "order_filled",
                1,
                int(market.yes_token_id),
                1_000_000,
                self.collateral,
                1,
            ),
            _raw_log(
                "order_filled",
                0,
                int(market.yes_token_id),
                self.collateral,
                1_000_000,
                2,
            ),
            _raw_log(
                "orders_matched",
                0,
                int(market.yes_token_id),
                self.collateral,
                1_000_000,
                3,
            ),
        ]

    def logs(self, address, start, end, *, event_topics=EVENT_TOPICS):
        if self.fail_neg_risk and address.casefold() != STANDARD_V2_EXCHANGE.casefold():
            raise PolygonRPCError("secondary provider range failure")
        if (
            address.casefold() != STANDARD_V2_EXCHANGE.casefold()
            or not start <= 100 <= end
        ):
            return []
        allowed = set(event_topics)
        return [row for row in self._settlement_rows() if row["topics"][0] in allowed]

    def transaction_receipts(self, transaction_hashes):
        rows = self._settlement_rows()
        return {
            transaction_hash: PolygonReceipt(
                transaction_hash=transaction_hash,
                block_number=100,
                block_hash="0x" + "1" * 64,
                transaction_index=0,
                logs=tuple(rows),
            )
            for transaction_hash in dict.fromkeys(transaction_hashes)
        }


def test_orders_matched_discovery_reconstructs_identical_normalized_rows() -> None:
    manifest = _manifest()
    rpc = _SyncRPC(manifest)
    ingested_at = datetime(2026, 8, 1, tzinfo=timezone.utc)
    full_logs = rpc._settlement_rows()
    expected = decode_and_normalize_leaf(
        full_logs,
        rpc=rpc,
        manifest=manifest,
        scan_id="scan",
        exchange_address=STANDARD_V2_EXCHANGE,
        from_block=99,
        to_block=101,
        ingested_at=ingested_at,
    )

    class FilteringRPC(_SyncRPC):
        def transaction_receipts(self, hashes):
            receipts = super().transaction_receipts(hashes)
            unrelated_address = {
                **self._settlement_rows()[0],
                "address": "0x" + "9" * 40,
            }
            unrelated_topic = {**self._settlement_rows()[0], "topics": []}
            return {
                key: replace(
                    receipt,
                    logs=(unrelated_address, unrelated_topic, *receipt.logs),
                )
                for key, receipt in receipts.items()
            }

    discovered = polygon_settlement_module.discover_and_normalize_leaf(
        [full_logs[-1]],
        rpc=FilteringRPC(manifest),
        manifest=manifest,
        scan_id="scan",
        exchange_address=STANDARD_V2_EXCHANGE,
        from_block=99,
        to_block=101,
        ingested_at=ingested_at,
    )
    assert discovered[:3] == expected
    assert discovered[3] == 4  # one discovery plus three receipt log objects


def test_orders_matched_discovery_rejects_incomplete_or_conflicting_receipts() -> None:
    manifest = _manifest()
    market = manifest.markets[0]
    matched = _raw_log(
        "orders_matched",
        0,
        int(market.yes_token_id),
        600_000,
        1_000_000,
        3,
    )
    kwargs = {
        "manifest": manifest,
        "scan_id": "scan",
        "exchange_address": STANDARD_V2_EXCHANGE,
        "from_block": 99,
        "to_block": 101,
    }

    class NoReceiptRPC(_SyncRPC):
        def transaction_receipts(self, _hashes):
            return {}

    with pytest.raises(ValueError, match="no in-range receipt"):
        polygon_settlement_module.discover_and_normalize_leaf(
            [matched], rpc=NoReceiptRPC(manifest), **kwargs
        )

    class OutsideReceiptRPC(_SyncRPC):
        def transaction_receipts(self, hashes):
            receipts = super().transaction_receipts(hashes)
            return {
                key: replace(receipt, block_number=102)
                for key, receipt in receipts.items()
            }

    with pytest.raises(ValueError, match="no in-range receipt"):
        polygon_settlement_module.discover_and_normalize_leaf(
            [matched], rpc=OutsideReceiptRPC(manifest), **kwargs
        )

    class ConflictingReceiptRPC(_SyncRPC):
        def transaction_receipts(self, hashes):
            receipts = super().transaction_receipts(hashes)
            conflicting = _raw_log(
                "orders_matched",
                0,
                int(market.no_token_id),
                600_000,
                1_000_000,
                3,
            )
            return {
                key: replace(receipt, logs=(*receipt.logs[:-1], conflicting))
                for key, receipt in receipts.items()
            }

    with pytest.raises(ValueError, match="discovery and receipt logs disagree"):
        polygon_settlement_module.discover_and_normalize_leaf(
            [matched], rpc=ConflictingReceiptRPC(manifest), **kwargs
        )


def test_orders_matched_discovery_validates_scope_duplicates_and_empty_target() -> None:
    manifest = _manifest()
    market = manifest.markets[0]
    matched = _raw_log(
        "orders_matched",
        0,
        int(market.yes_token_id),
        600_000,
        1_000_000,
        3,
    )
    rpc = _SyncRPC(manifest)
    kwargs = {
        "rpc": rpc,
        "manifest": manifest,
        "scan_id": "scan",
        "exchange_address": STANDARD_V2_EXCHANGE,
        "from_block": 99,
        "to_block": 101,
    }
    invalid = (
        _raw_log(
            "order_filled",
            0,
            int(market.yes_token_id),
            600_000,
            1_000_000,
            3,
        ),
        {**matched, "address": "0x" + "9" * 40},
        {**matched, "blockNumber": "0x66"},
    )
    for raw in invalid:
        with pytest.raises(ValueError, match="invalid discovery"):
            polygon_settlement_module.discover_and_normalize_leaf([raw], **kwargs)
    with pytest.raises(ValueError, match="duplicate discovery"):
        polygon_settlement_module.discover_and_normalize_leaf(
            [matched, matched], **kwargs
        )

    unrelated = _raw_log("orders_matched", 0, 999, 2, 3, 3)
    assert polygon_settlement_module.discover_and_normalize_leaf(
        [unrelated], **kwargs
    ) == ([], 0, polygon_settlement_module._sha256_json([]), 1)


def _create_interrupted_scan(conn, manifest, tmp_path) -> str:
    class InterruptedRPC(_SyncRPC):
        def logs(self, *_args, **_kwargs):
            raise PolygonRPCError("synthetic interruption")

    with pytest.raises(PolygonRPCError):
        sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
            client=InterruptedRPC(manifest),
        )
    return str(
        conn.execute(
            f"SELECT scan_id FROM {polygon_settlement_module.RUNS_TABLE}"
        ).fetchone()[0]
    )


@pytest.mark.parametrize(
    "payload",
    [
        [],
        [{"unexpected": 1}],
        [
            {
                "from_block": -1,
                "to_block": 101,
                "from_block_hash": "0x" + "1" * 64,
                "to_block_hash": "0x" + "2" * 64,
            }
        ],
        [
            {
                "from_block": 99,
                "to_block": 101,
                "from_block_hash": "short",
                "to_block_hash": "0x" + "2" * 64,
            }
        ],
        [
            {
                "from_block": 99,
                "to_block": 101,
                "from_block_hash": "0x" + "z" * 64,
                "to_block_hash": "0x" + "2" * 64,
            }
        ],
    ],
)
def test_cached_target_ranges_reject_malformed_storage(
    duck, monkeypatch, tmp_path, payload
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_module, "load_polygon_market_seed", lambda _path: manifest
    )
    rpc = _SyncRPC(manifest)
    with duck.get_connection() as conn:
        _create_interrupted_scan(conn, manifest, tmp_path)
        conn.execute(
            f"UPDATE {polygon_settlement_module.RUNS_TABLE} "
            "SET target_ranges_json = ?::JSON",
            [json.dumps(payload)],
        )
        with pytest.raises(RuntimeError, match="target ranges are malformed"):
            polygon_settlement_module._load_compatible_target_ranges(
                conn,
                rpc,
                manifest,
                provider_label="primary",
                provider_origin=rpc.origin,
                finalized_head=rpc.finalized_head(),
            )


def test_cached_target_ranges_handle_missing_and_compatible_runs(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_module, "load_polygon_market_seed", lambda _path: manifest
    )
    rpc = _SyncRPC(manifest)
    kwargs = {
        "provider_label": "primary",
        "provider_origin": rpc.origin,
        "finalized_head": rpc.finalized_head(),
    }
    with duck.get_connection() as conn:
        assert (
            polygon_settlement_module._load_compatible_target_ranges(
                conn, rpc, manifest, **kwargs
            )
            is None
        )
        _create_interrupted_scan(conn, manifest, tmp_path)
        cached = polygon_settlement_module._load_compatible_target_ranges(
            conn, rpc, manifest, **kwargs
        )
        assert cached is not None
        assert [
            (item.exchange_address, item.from_block, item.to_block) for item in cached
        ] == [
            (STANDARD_V2_EXCHANGE.casefold(), 99, 101),
            (NEG_RISK_V2_EXCHANGE.casefold(), 99, 101),
        ]


def test_cached_target_ranges_reject_inconsistent_provenance(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_module, "load_polygon_market_seed", lambda _path: manifest
    )
    rpc = _SyncRPC(manifest)
    with duck.get_connection() as conn:
        _create_interrupted_scan(conn, manifest, tmp_path)
        conn.execute(
            f"UPDATE {polygon_settlement_module.RUNS_TABLE} "
            "SET boundary_blocks_sha256 = ?",
            ["0" * 64],
        )
        with pytest.raises(RuntimeError, match="provenance is inconsistent"):
            polygon_settlement_module._load_compatible_target_ranges(
                conn,
                rpc,
                manifest,
                provider_label="primary",
                provider_origin=rpc.origin,
                finalized_head=rpc.finalized_head(),
            )


@pytest.mark.parametrize("stale_block", [99, 101])
def test_cached_target_ranges_fall_back_for_stale_boundaries(
    duck, monkeypatch, tmp_path, stale_block
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_module, "load_polygon_market_seed", lambda _path: manifest
    )
    rpc = _SyncRPC(manifest, hash_overrides={stale_block: "0x" + "8" * 64})
    with duck.get_connection() as conn:
        _create_interrupted_scan(conn, manifest, tmp_path)
        assert (
            polygon_settlement_module._load_compatible_target_ranges(
                conn,
                rpc,
                manifest,
                provider_label="primary",
                provider_origin=rpc.origin,
                finalized_head=rpc.finalized_head(),
            )
            is None
        )


@pytest.mark.parametrize("stale_head", ["number", "hash"])
def test_cached_target_ranges_fall_back_for_stale_finalized_head(
    duck, monkeypatch, tmp_path, stale_head
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_module, "load_polygon_market_seed", lambda _path: manifest
    )
    rpc = _SyncRPC(manifest)
    with duck.get_connection() as conn:
        _create_interrupted_scan(conn, manifest, tmp_path)
        if stale_head == "number":
            conn.execute(
                f"UPDATE {polygon_settlement_module.RUNS_TABLE} "
                "SET finalized_head_number = 201"
            )
        else:
            conn.execute(
                f"UPDATE {polygon_settlement_module.RUNS_TABLE} "
                "SET finalized_head_hash = ?",
                ["0x" + "8" * 64],
            )
        assert (
            polygon_settlement_module._load_compatible_target_ranges(
                conn,
                rpc,
                manifest,
                provider_label="primary",
                provider_origin=rpc.origin,
                finalized_head=rpc.finalized_head(),
            )
            is None
        )


class _MainThreadConnection:
    """DuckDB proxy that fails if a worker thread reaches persistence."""

    def __init__(self, conn, owner: int) -> None:
        self._conn = conn
        self._owner = owner

    def execute(self, *args, **kwargs):
        assert get_ident() == self._owner
        return self._conn.execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        assert get_ident() == self._owner
        return self._conn.executemany(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._conn, name)


class _FailingExecuteConnection:
    def __init__(self, conn, marker: str) -> None:
        self._conn = conn
        self._marker = marker

    def execute(self, query, *args, **kwargs):
        if self._marker in query:
            raise RuntimeError("synthetic persistence failure")
        return self._conn.execute(query, *args, **kwargs)


def test_concurrent_sync_shares_one_limiter_and_keeps_duckdb_on_main(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    main_thread = get_ident()
    creations: list[tuple[int, object, int]] = []
    lock = Lock()
    monkeypatch.setattr(
        polygon_settlement_module, "load_polygon_market_seed", lambda _path: manifest
    )

    def rpc_factory(_url, **kwargs):
        result = _SyncRPC(manifest)
        with lock:
            creations.append((get_ident(), kwargs.get("rate_limiter"), id(result)))
        return result

    monkeypatch.setattr(polygon_settlement_module, "PolygonRPC", rpc_factory)
    with duck.get_connection() as conn:
        summary = sync_polygon_settlement_fills(
            _MainThreadConnection(conn, main_thread),
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
            config=PolygonSettlementSyncConfig(initial_block_chunk_size=250),
        )

    assert summary["published"] is True
    primary = [row for row in creations if row[0] == main_thread]
    workers = [row for row in creations if row[0] != main_thread]
    assert len(primary) == 1
    assert workers
    limiter = primary[0][1]
    assert isinstance(limiter, RateLimiter)
    assert all(row[1] is limiter for row in workers)
    assert len({row[2] for row in creations}) == len(creations)


def test_concurrent_sync_adaptively_splits_worker_rpc_error(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    main_thread = get_ident()
    monkeypatch.setattr(
        polygon_settlement_module, "load_polygon_market_seed", lambda _path: manifest
    )

    class PrimaryRPC(_SyncRPC):
        def finalized_head(self):
            return PolygonBlock(
                1_000,
                f"0x{1_000:064x}",
                manifest.markets[0].window_end_at_utc + timedelta(days=1),
            )

        def first_block_at_or_after(self, timestamp, **_kwargs):
            return 100 if timestamp == manifest.markets[0].window_start_at_utc else 600

        def logs(self, address, start, end, *, event_topics=EVENT_TOPICS):
            if address.casefold() == STANDARD_V2_EXCHANGE.casefold() and (
                start,
                end,
            ) == (99, 600):
                raise PolygonRPCSizeLimitError("provider range limit")
            return super().logs(address, start, end, event_topics=event_topics)

    class WorkerRPC(_SyncRPC):
        def logs(self, address, start, end, *, event_topics=EVENT_TOPICS):
            if address.casefold() == STANDARD_V2_EXCHANGE.casefold() and (
                start,
                end,
            ) == (99, 600):
                raise PolygonRPCSizeLimitError("provider range limit")
            return super().logs(address, start, end, event_topics=event_topics)

    def rpc_factory(_url, **_kwargs):
        cls = PrimaryRPC if get_ident() == main_thread else WorkerRPC
        return cls(manifest)

    monkeypatch.setattr(polygon_settlement_module, "PolygonRPC", rpc_factory)
    with duck.get_connection() as conn:
        summary = sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
            config=PolygonSettlementSyncConfig(initial_block_chunk_size=600),
        )
        standard_chunks = conn.execute(
            f"""
            SELECT from_block, to_block, event_count
            FROM {polygon_settlement_module.CHUNKS_TABLE}
            WHERE exchange_address = ? AND status = 'success'
            ORDER BY from_block
            """,
            [STANDARD_V2_EXCHANGE.casefold()],
        ).fetchall()

    assert summary["fill_count"] == 1
    assert standard_chunks == [(99, 349, 4), (350, 600, 0)]


def test_concurrent_sync_cancels_unsubmitted_work_and_attributes_failure(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    main_thread = get_ident()
    started: list[tuple[str, int, int]] = []
    lock = Lock()
    monkeypatch.setattr(
        polygon_settlement_module, "load_polygon_market_seed", lambda _path: manifest
    )

    class WorkerRPC(_SyncRPC):
        def logs(self, address, start, end, *, event_topics=EVENT_TOPICS):
            with lock:
                started.append((address.casefold(), start, end))
            if address.casefold() == STANDARD_V2_EXCHANGE.casefold() and (
                start,
                end,
            ) == (99, 101):
                raise RuntimeError("synthetic worker failure")
            sleep(0.02)
            return super().logs(address, start, end, event_topics=event_topics)

    def rpc_factory(_url, **_kwargs):
        if get_ident() == main_thread:
            return _SyncRPC(manifest)
        return WorkerRPC(manifest)

    monkeypatch.setattr(polygon_settlement_module, "PolygonRPC", rpc_factory)
    with duck.get_connection() as conn:
        with pytest.raises(RuntimeError, match="synthetic worker failure"):
            sync_polygon_settlement_fills(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://rpc.example/key",
                provider_label="primary",
                config=PolygonSettlementSyncConfig(initial_block_chunk_size=250),
            )
        failed = conn.execute(
            f"""
            SELECT exchange_address, from_block, to_block, error_type
            FROM {polygon_settlement_module.CHUNKS_TABLE}
            WHERE status = 'failed'
            """
        ).fetchall()

    assert {address for address, _start, _end in started} == {
        STANDARD_V2_EXCHANGE.casefold()
    }
    assert failed == [(STANDARD_V2_EXCHANGE.casefold(), 99, 101, "RuntimeError")]


def test_sync_preflight_requires_credentials_chain_ranges_and_constructs_client(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_module, "load_polygon_market_seed", lambda _path: manifest
    )
    with duck.get_connection() as conn:
        for rpc_url, label in (("", "provider"), ("https://rpc.example", "")):
            with pytest.raises(ValueError, match="are required"):
                sync_polygon_settlement_fills(
                    conn,
                    seed_path=tmp_path / "unused.csv",
                    rpc_url=rpc_url,
                    provider_label=label,
                    client=_SyncRPC(manifest),
                )

        with pytest.raises(ValueError, match="safe 1-64 character"):
            sync_polygon_settlement_fills(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://rpc.example",
                provider_label="https://rpc.example/api_key=secret",
                client=_SyncRPC(manifest),
            )

        wrong_chain = _SyncRPC(manifest)
        wrong_chain.chain_id = lambda: 1
        with pytest.raises(PolygonRPCError, match="chain ID 137"):
            sync_polygon_settlement_fills(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://rpc.example",
                provider_label="provider",
                client=wrong_chain,
            )

        monkeypatch.setattr(
            polygon_settlement_module,
            "build_polygon_scan_plan",
            lambda *_args: polygon_settlement_module.PolygonScanPlan((), {}),
        )
        with pytest.raises(RuntimeError, match="no target block ranges"):
            sync_polygon_settlement_fills(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://rpc.example",
                provider_label="provider",
                client=_SyncRPC(manifest),
            )

        created = {}

        def rpc_factory(url, **kwargs):
            created.update(url=url, **kwargs)
            result = _SyncRPC(manifest)
            result.chain_id = lambda: 1
            return result

        monkeypatch.setattr(polygon_settlement_module, "PolygonRPC", rpc_factory)
        with pytest.raises(PolygonRPCError, match="chain ID 137"):
            sync_polygon_settlement_fills(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://rpc.example/key",
                provider_label="provider",
            )
        assert created["url"] == "https://rpc.example/key"
        assert created["retries"] == 4


@pytest.mark.parametrize("disappears", [False, True])
def test_sync_rechecks_scan_that_storage_reports_as_published(
    duck, monkeypatch, tmp_path, disappears
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_module, "load_polygon_market_seed", lambda _path: manifest
    )
    expected = None if disappears else {"scan_id": "published", "published": True}
    summaries = iter((None, expected))
    monkeypatch.setattr(
        polygon_settlement_module,
        "_offline_published_summary",
        lambda *_args: next(summaries),
    )
    monkeypatch.setattr(
        polygon_settlement_module,
        "start_polygon_settlement_scan",
        lambda *_args, **_kwargs: True,
    )
    with duck.get_connection() as conn:
        if disappears:
            with pytest.raises(RuntimeError, match="disappeared during startup"):
                sync_polygon_settlement_fills(
                    conn,
                    seed_path=tmp_path / "unused.csv",
                    rpc_url="https://rpc.example/key",
                    provider_label="primary",
                    client=_SyncRPC(manifest),
                )
        else:
            assert (
                sync_polygon_settlement_fills(
                    conn,
                    seed_path=tmp_path / "unused.csv",
                    rpc_url="https://rpc.example/key",
                    provider_label="primary",
                    client=_SyncRPC(manifest),
                )
                == expected
            )


def test_sync_worker_rpc_reports_activity_and_requires_shared_limiter(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_module, "load_polygon_market_seed", lambda _path: manifest
    )
    activity = []

    class CallbackRPC(_SyncRPC):
        def __init__(self, callback=None):
            super().__init__(manifest)
            self.callback = callback

        def logs(self, *args, **kwargs):
            if self.callback is not None:
                self.callback("eth_getLogs")
                activity.append("eth_getLogs")
            return super().logs(*args, **kwargs)

    monkeypatch.setattr(
        polygon_settlement_module,
        "PolygonRPC",
        lambda _url, **kwargs: CallbackRPC(kwargs.get("activity_callback")),
    )
    with duck.get_connection() as conn:
        summary = sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
        )
        for table in (
            polygon_settlement_module.FILLS_TABLE,
            polygon_settlement_module.STAGE_TABLE,
            polygon_settlement_module.CHUNKS_TABLE,
            polygon_settlement_module.RUNS_TABLE,
        ):
            conn.execute(f"DELETE FROM {table}")
    assert summary["published"] is True
    assert activity

    monkeypatch.setattr(polygon_settlement_module, "RateLimiter", lambda _rps: None)
    with duck.get_connection() as conn:
        with pytest.raises(RuntimeError, match="shared limiter"):
            sync_polygon_settlement_fills(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://rpc.example/key",
                provider_label="primary",
            )


def test_sync_resumes_successful_leaves_publishes_and_short_circuits(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.polygon_settlement.load_polygon_market_seed",
        lambda _path: manifest,
    )
    config = PolygonSettlementSyncConfig(initial_block_chunk_size=250)

    class WideRPC(_SyncRPC):
        def finalized_head(self):
            return PolygonBlock(
                1_000,
                f"0x{1_000:064x}",
                manifest.markets[0].window_end_at_utc + timedelta(days=1),
            )

        def first_block_at_or_after(self, timestamp, **_kwargs):
            return 100 if timestamp == manifest.markets[0].window_start_at_utc else 600

    class InterruptedRPC(WideRPC):
        def logs(self, address, start, end, *, event_topics=EVENT_TOPICS):
            if start >= 349:
                raise PolygonRPCError("synthetic interruption")
            return super().logs(address, start, end, event_topics=event_topics)

    with duck.get_connection() as conn:
        with pytest.raises(PolygonRPCError):
            sync_polygon_settlement_fills(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://rpc.example/key",
                provider_label="primary",
                config=config,
                client=InterruptedRPC(manifest),
            )
        summary = sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
            config=config,
            client=WideRPC(manifest),
        )
        assert summary["published"] is True
        assert summary["resumed_chunk_count"] == 2
        assert summary["fill_count"] == 1

        repeated = sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="",
            provider_label="",
            config=config,
        )
        assert repeated["short_circuited"] is True
        assert repeated["offline"] is True
        assert repeated["fill_count"] == 1


@pytest.mark.parametrize(
    ("corruption", "message"),
    [
        ("provenance", "provenance is inconsistent"),
        ("missing_exchange", "incomplete exchange coverage"),
        ("gap", "gap or overlap"),
        ("incomplete", "incomplete coverage"),
        ("outside", "extends outside"),
        ("canonical", "canonical fills are inconsistent"),
    ],
)
def test_offline_published_scan_revalidates_all_local_invariants(
    duck, monkeypatch, tmp_path, corruption, message
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_module, "load_polygon_market_seed", lambda _path: manifest
    )
    with duck.get_connection() as conn:
        summary = sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
            client=_SyncRPC(manifest),
        )
        scan_id = summary["scan_id"]
        if corruption == "provenance":
            conn.execute(
                f"UPDATE {polygon_settlement_module.RUNS_TABLE} "
                "SET boundary_blocks_sha256 = ? WHERE scan_id = ?",
                ["0" * 64, scan_id],
            )
        elif corruption == "missing_exchange":
            conn.execute(
                f"DELETE FROM {polygon_settlement_module.CHUNKS_TABLE} "
                "WHERE scan_id = ?",
                [scan_id],
            )
        elif corruption == "gap":
            conn.execute(
                f"UPDATE {polygon_settlement_module.CHUNKS_TABLE} "
                "SET from_block = 100 WHERE scan_id = ?",
                [scan_id],
            )
        elif corruption == "incomplete":
            conn.execute(
                f"UPDATE {polygon_settlement_module.CHUNKS_TABLE} "
                "SET to_block = 100 WHERE scan_id = ?",
                [scan_id],
            )
        elif corruption == "outside":
            conn.execute(
                f"""
                INSERT INTO {polygon_settlement_module.CHUNKS_TABLE} (
                    scan_id, exchange_address, from_block, to_block,
                    from_block_hash, to_block_hash, status, event_count,
                    scoped_event_count, normalized_fill_count, scoped_event_sha256
                ) VALUES (?, ?, 102, 102, ?, ?, 'success', 0, 0, 0, ?)
                """,
                [
                    scan_id,
                    STANDARD_V2_EXCHANGE.casefold(),
                    "0x" + "1" * 64,
                    "0x" + "1" * 64,
                    "1" * 64,
                ],
            )
        else:
            conn.execute(f"DELETE FROM {polygon_settlement_module.FILLS_TABLE}")

        with pytest.raises(RuntimeError, match=message):
            polygon_settlement_module._offline_published_summary(conn, manifest)


def test_sync_rejects_and_discards_stale_resumed_leaf_boundary_hash(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_module, "load_polygon_market_seed", lambda _path: manifest
    )
    config = PolygonSettlementSyncConfig(initial_block_chunk_size=250)

    class WideRPC(_SyncRPC):
        def finalized_head(self):
            return PolygonBlock(
                1_000,
                f"0x{1_000:064x}",
                manifest.markets[0].window_end_at_utc + timedelta(days=1),
            )

        def first_block_at_or_after(self, timestamp, **_kwargs):
            return 100 if timestamp == manifest.markets[0].window_start_at_utc else 600

    class InterruptedRPC(WideRPC):
        def logs(self, address, start, end, *, event_topics=EVENT_TOPICS):
            if start >= 349:
                raise PolygonRPCError("synthetic interruption")
            return super().logs(address, start, end, event_topics=event_topics)

    with duck.get_connection() as conn:
        with pytest.raises(PolygonRPCError):
            sync_polygon_settlement_fills(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://rpc.example/key",
                provider_label="primary",
                config=config,
                client=InterruptedRPC(manifest),
            )
        scan_id = conn.execute(
            f"SELECT scan_id FROM {polygon_settlement_module.RUNS_TABLE}"
        ).fetchone()[0]

        with pytest.raises(RuntimeError, match="leaf boundary hash changed"):
            sync_polygon_settlement_fills(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://rpc.example/key",
                provider_label="primary",
                config=config,
                client=WideRPC(
                    manifest,
                    hash_overrides={348: "0x" + "8" * 64},
                ),
            )

        assert (
            conn.execute(
                f"""
            SELECT count(*) FROM {polygon_settlement_module.CHUNKS_TABLE}
            WHERE scan_id = ? AND exchange_address = ?
              AND from_block = 99 AND to_block = 100 AND status = 'success'
            """,
                [scan_id, STANDARD_V2_EXCHANGE.casefold()],
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                f"""
            SELECT count(*) FROM {polygon_settlement_module.STAGE_TABLE}
            WHERE scan_id = ? AND exchange_address = ?
              AND chunk_from_block = 99 AND chunk_to_block = 100
            """,
                [scan_id, STANDARD_V2_EXCHANGE.casefold()],
            ).fetchone()[0]
            == 0
        )

        recovered = sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
            config=config,
            client=_SyncRPC(manifest),
        )
        assert recovered["published"] is True
        assert recovered["fill_count"] == 1


def test_published_scan_ignores_stale_header_cleanup(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_module, "load_polygon_market_seed", lambda _path: manifest
    )
    with duck.get_connection() as conn:
        summary = sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
            client=_SyncRPC(manifest),
        )

        completed = polygon_settlement_module._revalidate_resumed_chunk_headers(
            conn,
            _SyncRPC(manifest, hash_overrides={99: "0x" + "8" * 64}),
            summary["scan_id"],
        )

        assert completed == {
            STANDARD_V2_EXCHANGE.casefold(): [(99, 101)],
            NEG_RISK_V2_EXCHANGE.casefold(): [(99, 101)],
        }
        assert (
            conn.execute(
                f"SELECT count(*) FROM {polygon_settlement_module.CHUNKS_TABLE} "
                "WHERE scan_id = ? AND status = 'success'",
                [summary["scan_id"]],
            ).fetchone()[0]
            == 2
        )


def test_stale_header_cleanup_rolls_back_on_persistence_failure(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_module, "load_polygon_market_seed", lambda _path: manifest
    )
    with duck.get_connection() as conn:
        scan_id = _create_interrupted_scan(conn, manifest, tmp_path)
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {polygon_settlement_module.CHUNKS_TABLE} (
                scan_id, exchange_address, from_block, to_block,
                from_block_hash, to_block_hash, status, event_count,
                scoped_event_count, normalized_fill_count, scoped_event_sha256
            ) VALUES (?, ?, 99, 101, ?, ?, 'success', 0, 0, 0, ?)
            """,
            [
                scan_id,
                STANDARD_V2_EXCHANGE.casefold(),
                "0x" + "9" * 64,
                f"0x{101:064x}",
                "7" * 64,
            ],
        )
        proxy = _FailingExecuteConnection(
            conn,
            f"DELETE FROM {polygon_settlement_module.CHUNKS_TABLE}",
        )

        with pytest.raises(RuntimeError, match="synthetic persistence failure"):
            polygon_settlement_module._revalidate_resumed_chunk_headers(
                proxy,
                _SyncRPC(manifest, hash_overrides={99: "0x" + "8" * 64}),
                scan_id,
            )

        assert (
            conn.execute(
                f"SELECT count(*) FROM {polygon_settlement_module.CHUNKS_TABLE} "
                "WHERE scan_id = ? AND status = 'success'",
                [scan_id],
            ).fetchone()[0]
            == 1
        )
        assert (
            conn.execute(
                f"SELECT count(*) FROM {polygon_settlement_module.STAGE_TABLE} "
                "WHERE scan_id = ?",
                [scan_id],
            ).fetchone()[0]
            == 0
        )


def test_sync_accepts_a_parent_chunk_iterator_without_close(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    primary = _SyncRPC(manifest)
    monkeypatch.setattr(
        polygon_settlement_module, "load_polygon_market_seed", lambda _path: manifest
    )
    monkeypatch.setattr(
        polygon_settlement_module,
        "PolygonRPC",
        lambda _url, **_kwargs: primary,
    )

    concurrent_results = polygon_settlement_module._concurrent_leaf_results

    def results_without_close(*args, **kwargs):
        return iter(list(concurrent_results(*args, **kwargs)))

    monkeypatch.setattr(
        polygon_settlement_module,
        "_concurrent_leaf_results",
        results_without_close,
    )
    with duck.get_connection() as conn:
        summary = sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
            config=PolygonSettlementSyncConfig(initial_block_chunk_size=250),
        )

    assert summary["published"] is True
    assert summary["fill_count"] == 1


def test_secondary_verification_reports_match_mismatch_and_error(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.polygon_settlement.load_polygon_market_seed",
        lambda _path: manifest,
    )
    with duck.get_connection() as conn:
        sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
            client=_SyncRPC(manifest),
        )
        with pytest.raises(ValueError, match="safe 1-64 character"):
            verify_polygon_settlement_scan(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://verify.example/key",
                provider_label="verify?api_key=secret",
                client=_SyncRPC(manifest, origin="https://verify.example"),
            )
        matched = verify_polygon_settlement_scan(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://verify.example/key",
            provider_label="verify",
            client=_SyncRPC(manifest, origin="https://verify.example"),
        )
        assert matched["verification_status"] == "matched"

        mismatched = verify_polygon_settlement_scan(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://verify.example/key",
            provider_label="verify",
            client=_SyncRPC(
                manifest,
                collateral=500_000,
                origin="https://verify.example",
            ),
        )
        assert mismatched["verification_status"] == "mismatched"
        assert mismatched["mismatched_chunks"]

        class ErrorRPC(_SyncRPC):
            def logs(self, *_args, **_kwargs):
                raise PolygonRPCError("synthetic verification failure")

        errored = verify_polygon_settlement_scan(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://verify.example/key",
            provider_label="verify",
            client=ErrorRPC(manifest, origin="https://verify.example"),
        )
        assert errored["verification_status"] == "error"

        non_independent = verify_polygon_settlement_scan(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/other-key",
            provider_label="verify",
            client=_SyncRPC(manifest),
        )
        assert non_independent == {
            "scan_id": non_independent["scan_id"],
            "verification_status": "error",
            "error_type": "NonIndependentVerificationProvider",
        }
        assert conn.execute(
            f"""
            select verification_status, verification_provider_label,
                   verification_provider_origin
            from {polygon_settlement_module.RUNS_TABLE}
            where scan_id = ?
            """,
            [non_independent["scan_id"]],
        ).fetchone() == ("error", "verify", "https://rpc.example")

        same_label = verify_polygon_settlement_scan(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://verify.example/key",
            provider_label="primary",
            client=_SyncRPC(manifest, origin="https://verify.example"),
        )
        assert same_label["error_type"] == "NonIndependentVerificationProvider"


def test_secondary_verification_requires_canonical_and_handles_optional_and_chain(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_module, "load_polygon_market_seed", lambda _path: manifest
    )
    with duck.get_connection() as conn:
        with pytest.raises(RuntimeError, match="one canonical"):
            verify_polygon_settlement_scan(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://verify.example",
                provider_label="verify",
                client=_SyncRPC(manifest),
            )
        sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example",
            provider_label="primary",
            client=_SyncRPC(manifest),
        )
        not_requested = verify_polygon_settlement_scan(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="",
            provider_label="",
            client=_SyncRPC(manifest),
        )
        assert not_requested["verification_status"] == "not_requested"

        for rpc_url, provider_label in (
            ("https://verify.example", ""),
            ("", "verify"),
        ):
            misconfigured = verify_polygon_settlement_scan(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url=rpc_url,
                provider_label=provider_label,
                client=_SyncRPC(manifest, origin="https://verify.example"),
            )
            assert misconfigured == {
                "scan_id": misconfigured["scan_id"],
                "verification_status": "error",
                "error_type": "VerificationConfigurationError",
            }
            assert conn.execute(
                f"""
                select verification_status, verification_provider_label,
                       verification_provider_origin
                from {polygon_settlement_module.RUNS_TABLE}
                where scan_id = ?
                """,
                [misconfigured["scan_id"]],
            ).fetchone() == ("error", None, None)

        wrong_chain = _SyncRPC(manifest, origin="https://verify.example")
        wrong_chain.chain_id = lambda: 1
        errored = verify_polygon_settlement_scan(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://verify.example",
            provider_label="verify",
            client=wrong_chain,
        )
        assert errored == {
            "scan_id": errored["scan_id"],
            "verification_status": "error",
            "error_type": "PolygonRPCError",
        }

        monkeypatch.setattr(
            polygon_settlement_module,
            "PolygonRPC",
            lambda _url: wrong_chain,
        )
        constructed = verify_polygon_settlement_scan(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://verify.example",
            provider_label="verify",
        )
        assert constructed["error_type"] == "PolygonRPCError"

        working_verifier = _SyncRPC(manifest, origin="https://verify.example")
        monkeypatch.setattr(
            polygon_settlement_module,
            "PolygonRPC",
            lambda _url: working_verifier,
        )
        constructed_match = verify_polygon_settlement_scan(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://verify.example",
            provider_label="verify",
        )
        assert constructed_match["verification_status"] == "matched"

        conn.execute(f"delete from {polygon_settlement_module.RUNS_TABLE}")
        with pytest.raises(RuntimeError, match="not published"):
            verify_polygon_settlement_scan(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://verify.example",
                provider_label="verify",
                client=wrong_chain,
            )
