from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from oddsfox_pipeline.storage.duckdb import dlt_batch as dlt_batch_mod
from oddsfox_pipeline.storage.duckdb.dlt_batch import (
    EVENT_SNAPSHOT_COLUMNS,
    load_market_tokens_stage,
    load_odds_history_stage,
    load_stage_rows,
    merge_match_order_book_snapshots,
)
from oddsfox_pipeline.storage.duckdb.dlt_batch_event_catalog import (
    merge_event_catalog_batch,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import polymarket_wc2026_raw_tbl
from oddsfox_pipeline.storage.duckdb.schemas.polymarket_raw_columns import (
    EVENT_CATALOG_MARKET_COLUMNS,
)


def _order_book_row(**overrides):
    row = {
        "scan_id": "scan-1",
        "manifest_sha256": "a" * 64,
        "fifa_match_id": 95,
        "stage": "round_of_16",
        "home_team": "Argentina",
        "away_team": "Egypt",
        "event_id": "665733",
        "event_slug": "fifwc-arg-egy-2026-07-07-more-markets",
        "market_id": "2793969",
        "market_slug": "fifwc-arg-egy-2026-07-07-team-to-advance",
        "market_type": "soccer_team_to_advance",
        "condition_id": "0x" + ("1" * 64),
        "outcome_label": "Argentina",
        "clob_token_id": "123",
        "window_start_ms": 1_000,
        "window_end_ms": 2_000,
        "snapshot_timestamp_ms": 1_500,
        "snapshot_at": "1970-01-01T00:00:01.500000+00:00",
        "snapshot_sha256": "b" * 64,
        "bids_json": '[{"price":"0.4","size":"10"}]',
        "asks_json": '[{"price":"0.6","size":"5"}]',
        "is_neg_risk": False,
        "last_trade_price": "0.5",
        "source_endpoint": "pmxt",
        "ingested_at": "2026-07-28T00:00:00+00:00",
    }
    row.update(overrides)
    return row


def test_dlt_batch_loads_stage_and_finalizes_market_tokens(duck):
    with duck.get_connection() as conn:
        conn.execute(
            f"""
            CREATE TABLE {polymarket_wc2026_raw_tbl("stage_market_tokens")} (
                market_id TEXT,
                clob_token_ids TEXT,
                updated_at TIMESTAMP
            )
            """
        )
        load_market_tokens_stage(
            [
                {
                    "market_id": "m1",
                    "clobTokenIds": '["tok-a"]',
                    "updated_at": "2026-01-01T00:00:00",
                }
            ],
            conn,
        )
        canonical = conn.execute(
            f"""
            SELECT market_id, clobTokenIds
            FROM {polymarket_wc2026_raw_tbl("market_tokens")}
            """
        ).fetchall()
        staged = conn.execute(
            f"""
            SELECT market_id, clob_token_ids
            FROM {polymarket_wc2026_raw_tbl("stage_market_tokens_v1")}
            """
        ).fetchall()

    assert canonical == [("m1", '["tok-a"]')]
    assert staged == canonical


def test_odds_history_is_append_only_for_existing_source_points(duck):
    first = {
        "clobTokenId": "token-1",
        "timestamp": 1_700_000_000,
        "price": 0.4,
        "ingested_at": "2026-06-11T00:00:00",
    }
    conflicting_replay = {
        **first,
        "price": 0.9,
        "ingested_at": "2026-06-12T00:00:00",
    }
    next_point = {
        **first,
        "timestamp": 1_700_000_060,
        "price": 0.5,
        "ingested_at": "2026-06-12T00:01:00",
    }

    with duck.get_connection() as conn:
        load_odds_history_stage([first], conn)
        load_odds_history_stage([conflicting_replay, next_point], conn)
        rows = conn.execute(
            f"""
            select timestamp, price, ingested_at
            from {polymarket_wc2026_raw_tbl("odds_history")}
            where clobTokenId = 'token-1'
            order by timestamp
            """
        ).fetchall()

    assert [(row[0], row[1]) for row in rows] == [
        (1_700_000_000, 0.4),
        (1_700_000_060, 0.5),
    ]
    assert str(rows[0][2]) == "2026-06-11 00:00:00"


def test_load_stage_rows_rejects_empty_rows():
    with pytest.raises(ValueError, match="rows must not be empty"):
        load_stage_rows(
            schema="polymarket_wc2026_raw", stage_table="stage", rows=[], columns={}
        )


def test_match_order_book_dlt_stage_merges_canonical_rows_idempotently(duck):
    row = _order_book_row()
    away_row = _order_book_row(
        outcome_label="Egypt",
        clob_token_id="456",
        snapshot_timestamp_ms=1_600,
        snapshot_at="1970-01-01T00:00:01.600000+00:00",
        snapshot_sha256="c" * 64,
    )
    with duck.get_connection() as conn:
        merge_match_order_book_snapshots([row], conn)
        merge_match_order_book_snapshots([row], conn)
        merge_match_order_book_snapshots([away_row], conn)
        canonical_count = conn.execute(
            """
            SELECT count(*)
            FROM polymarket_wc2026_raw.match_order_book_snapshots
            """
        ).fetchone()[0]
        staged_count = conn.execute(
            """
            SELECT count(*)
            FROM polymarket_wc2026_raw.stage_match_order_book_snapshots_v1
            """
        ).fetchone()[0]

        roles = conn.execute(
            """
            SELECT landscape_role
            FROM polymarket_wc2026_raw.match_order_book_snapshots
            ORDER BY clob_token_id
            """
        ).fetchall()

    assert canonical_count == 2
    assert staged_count == 1
    assert roles == [("home",), ("away",)]


def test_match_order_book_dlt_stage_rejects_unknown_role(duck):
    with duck.get_connection() as conn:
        with pytest.raises(ValueError, match="explicit landscape_role"):
            merge_match_order_book_snapshots(
                [_order_book_row(outcome_label="Draw")],
                conn,
            )


def test_load_stage_rows_drops_pending_packages(monkeypatch):
    class Pipe:
        has_pending_data = True

        def __init__(self):
            self.dropped = False
            self.runs = []

        def drop_pending_packages(self):
            self.dropped = True

        def run(self, rows, **kwargs):
            self.runs.append((rows, kwargs))

    pipe = Pipe()
    monkeypatch.setattr(dlt_batch_mod, "_pipeline", lambda _schema: pipe)

    stage = load_stage_rows(
        schema="polymarket_wc2026_raw",
        stage_table="stage_probe",
        rows=[{"id": "1"}],
        columns={"id": {"data_type": "text"}},
    )

    assert pipe.dropped is True
    assert pipe.runs
    assert stage == '"polymarket_wc2026_raw"."stage_probe"'


def test_match_order_book_merge_ignores_empty_batch(duck):
    with duck.get_connection() as conn:
        merge_match_order_book_snapshots([], conn)
        count = conn.execute(
            """
            select count(*)
            from polymarket_wc2026_raw.match_order_book_snapshots
            """
        ).fetchone()[0]

    assert count == 0


def test_dlt_pipeline_uses_public_active_duckdb_path(monkeypatch):
    created = {}

    class FakeDlt:
        class destinations:
            @staticmethod
            def duckdb(*, credentials):
                return {"credentials": credentials}

        @staticmethod
        def pipeline(**kwargs):
            created.update(kwargs)
            return object()

    dlt_batch_mod._PIPELINES.clear()
    monkeypatch.setattr(dlt_batch_mod, "dlt", FakeDlt)
    monkeypatch.setattr(dlt_batch_mod.duckdb_connection, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        dlt_batch_mod.duckdb_connection,
        "active_duckdb_path",
        lambda: "/tmp/public.duckdb",
    )

    dlt_batch_mod._pipeline("polymarket_wc2026_raw")

    assert created["destination"] == {"credentials": "/tmp/public.duckdb"}
    dlt_batch_mod._PIPELINES.clear()


def test_event_catalog_persists_market_payload_snapshot_and_records_removal(
    duck,
):
    observed_1 = "2026-08-01T00:00:00"
    observed_2 = "2026-08-02T00:00:00"

    def event_row(observed_at: str, source_market_count: int) -> dict:
        row = {
            column: None for column in EVENT_SNAPSHOT_COLUMNS if column != "row_order"
        }
        row.update(
            event_id="event-1",
            event_slug="event-1",
            event_title="World Cup event",
            event_volume_usd_lifetime_reported=120_000.0,
            tags_json='["2026-fifa-world-cup"]',
            series_slugs_json="[]",
            candidate_sources_json='["exact_2026_tag"]',
            source_market_count=source_market_count,
            observed_at=observed_at,
            source_endpoint="/events/keyset",
        )
        return row

    market = {
        column: None for column in EVENT_CATALOG_MARKET_COLUMNS if column != "row_order"
    }
    market.update(
        id="market-1",
        question="Will A win?",
        outcomes='["Yes","No"]',
        volume=1.0,
        scraped_at=observed_1,
    )

    with duck.get_connection() as conn:
        markets_before = conn.execute(
            "select count(*) from polymarket_wc2026_raw.markets"
        ).fetchone()[0]
        merge_event_catalog_batch(
            event_rows=[event_row(observed_1, 1)],
            tag_rows=[],
            event_market_rows=[
                {
                    "event_id": "event-1",
                    "market_id": "market-1",
                    "source_ordinal": 0,
                    "is_enclosing_event": True,
                    "observed_at": observed_1,
                }
            ],
            market_rows=[market],
            conn=conn,
        )
        payload = conn.execute(
            """
            select market_id, question, observed_at
            from polymarket_wc2026_raw.event_market_payload_snapshots
            """
        ).fetchone()
        markets_after = conn.execute(
            "select count(*) from polymarket_wc2026_raw.markets"
        ).fetchone()[0]

        merge_event_catalog_batch(
            event_rows=[event_row(observed_2, 0)],
            tag_rows=[],
            event_market_rows=[],
            market_rows=[],
            conn=conn,
        )
        latest_links = conn.execute(
            """
            with latest as (
                select event_id, max(observed_at) as observed_at
                from polymarket_wc2026_raw.event_snapshots
                group by event_id
            )
            select count(*)
            from polymarket_wc2026_raw.event_market_snapshots
            inner join latest using (event_id, observed_at)
            """
        ).fetchone()[0]

    assert payload == ("market-1", "Will A win?", datetime(2026, 8, 1))
    assert markets_after == markets_before
    assert latest_links == 0


def test_event_catalog_leaves_dlt_owned_market_target_untouched(duck):
    observed_at = "2026-08-02T00:00:00"
    event = {column: None for column in EVENT_SNAPSHOT_COLUMNS if column != "row_order"}
    event.update(
        event_id="event-upgrade",
        event_slug="event-upgrade",
        event_title="World Cup upgrade event",
        event_volume_usd_lifetime_reported=120_000.0,
        tags_json='["2026-fifa-world-cup"]',
        series_slugs_json="[]",
        candidate_sources_json='["exact_2026_tag"]',
        source_market_count=1,
        observed_at=observed_at,
        source_endpoint="/events/keyset",
    )
    market = {
        column: None for column in EVENT_CATALOG_MARKET_COLUMNS if column != "row_order"
    }
    market.update(
        id="market-upgrade",
        question="Total goals O/U 2.5?",
        market_resolution_source="Official FIFA results",
        outcomes='["Over","Under"]',
        volume=0.0,
        scraped_at=observed_at,
        group_item_threshold="2.5",
        line=2.5,
        neg_risk_market_id="neg-risk-set-1",
        neg_risk_request_id="neg-risk-request-1",
        neg_risk_other=False,
    )

    with duck.get_connection() as conn:
        conn.execute("drop table polymarket_wc2026_raw.markets")
        conn.execute(
            """
            create table polymarket_wc2026_raw.markets (
                id text,
                _dlt_load_id text not null,
                _dlt_id text not null
            )
            """
        )
        conn.execute(
            """
            insert into polymarket_wc2026_raw.markets values
                ('existing-market', 'existing-load', 'existing-row')
            """
        )
        merge_event_catalog_batch(
            event_rows=[event],
            tag_rows=[],
            event_market_rows=[],
            market_rows=[market],
            conn=conn,
        )
        dlt_rows = conn.execute(
            "select * from polymarket_wc2026_raw.markets"
        ).fetchall()
        snapshot = conn.execute(
            """
            select market_resolution_source, group_item_threshold, line,
                neg_risk_market_id, neg_risk_request_id, neg_risk_other
            from polymarket_wc2026_raw.event_market_payload_snapshots
            where market_id = 'market-upgrade'
            """
        ).fetchone()

    assert dlt_rows == [("existing-market", "existing-load", "existing-row")]
    assert snapshot == (
        "Official FIFA results",
        "2.5",
        2.5,
        "neg-risk-set-1",
        "neg-risk-request-1",
        False,
    )


def test_event_catalog_payload_snapshot_is_idempotent_at_observation_grain(duck):
    observed_at = "2026-08-02T00:00:00"
    event = {column: None for column in EVENT_SNAPSHOT_COLUMNS if column != "row_order"}
    event.update(
        event_id="event-no-key",
        event_slug="event-no-key",
        event_title="World Cup no-key event",
        event_volume_usd_lifetime_reported=120_000.0,
        tags_json='["2026-fifa-world-cup"]',
        series_slugs_json="[]",
        candidate_sources_json='["exact_2026_tag"]',
        source_market_count=1,
        observed_at=observed_at,
        source_endpoint="/events/keyset",
    )
    market = {
        column: None for column in EVENT_CATALOG_MARKET_COLUMNS if column != "row_order"
    }
    market.update(
        id="market-no-key",
        question="Stable observation",
        outcomes='["Yes","No"]',
        volume=20.0,
        scraped_at=observed_at,
    )

    with duck.get_connection() as conn:
        for _ in range(2):
            merge_event_catalog_batch(
                event_rows=[event],
                tag_rows=[],
                event_market_rows=[],
                market_rows=[market, market],
                conn=conn,
            )

        rows = conn.execute(
            """
            select market_id, question, volume
            from polymarket_wc2026_raw.event_market_payload_snapshots
            where market_id = 'market-no-key'
            """
        ).fetchall()

    assert rows == [("market-no-key", "Stable observation", 20.0)]


def test_event_catalog_rejects_divergent_rows_within_one_observation(duck):
    observed_at = "2026-08-02T00:00:00"
    event = {column: None for column in EVENT_SNAPSHOT_COLUMNS if column != "row_order"}
    event.update(
        event_id="event-stage-divergence",
        event_slug="event-stage-divergence",
        event_title="World Cup event",
        event_volume_usd_lifetime_reported=120_000.0,
        tags_json='["2026-fifa-world-cup"]',
        series_slugs_json="[]",
        candidate_sources_json='["exact_2026_tag"]',
        source_market_count=0,
        observed_at=observed_at,
        source_endpoint="/events/keyset",
    )
    changed = {**event, "event_title": "Changed title"}

    with duck.get_connection() as conn:
        with pytest.raises(RuntimeError, match="share one snapshot key"):
            merge_event_catalog_batch(
                event_rows=[event, changed],
                tag_rows=[],
                event_market_rows=[],
                market_rows=[],
                conn=conn,
            )
        assert (
            conn.execute(
                """
            select count(*)
            from polymarket_wc2026_raw.event_snapshots
            where event_id = 'event-stage-divergence'
            """
            ).fetchone()[0]
            == 0
        )


@pytest.mark.parametrize(
    ("divergent_relation", "message"),
    [
        ("event", "event snapshots"),
        ("tag", "event tag snapshots"),
        ("bridge", "event market snapshots"),
        ("payload", "event market payload snapshots"),
    ],
)
def test_event_catalog_replay_rejects_divergence_and_preserves_history_and_metrics(
    duck, divergent_relation, message
):
    observed_at = "2026-08-02T00:00:00"
    event = {column: None for column in EVENT_SNAPSHOT_COLUMNS if column != "row_order"}
    event.update(
        event_id="event-append-only",
        event_slug="event-append-only",
        event_title="World Cup append-only event",
        event_volume_usd_lifetime_reported=120_000.0,
        tags_json='["2026-fifa-world-cup"]',
        series_slugs_json="[]",
        candidate_sources_json='["exact_2026_tag"]',
        source_market_count=1,
        observed_at=observed_at,
        source_endpoint="/events/keyset",
    )
    tag = {
        "event_id": "event-append-only",
        "tag_key": "tag-wc2026",
        "tag_id": "tag-wc2026",
        "tag_slug": "2026-fifa-world-cup",
        "tag_label": "2026 FIFA World Cup",
        "observed_at": observed_at,
    }
    bridge = {
        "event_id": "event-append-only",
        "market_id": "market-append-only",
        "source_ordinal": 0,
        "is_enclosing_event": True,
        "observed_at": observed_at,
    }
    market = {
        column: None for column in EVENT_CATALOG_MARKET_COLUMNS if column != "row_order"
    }
    market.update(
        id="market-append-only",
        question="Will the append-only market resolve Yes?",
        outcomes='["Yes","No"]',
        volume=10.0,
        scraped_at=observed_at,
    )

    def merge(
        event_value: dict, tag_value: dict, bridge_value: dict, market_value: dict
    ) -> None:
        merge_event_catalog_batch(
            event_rows=[event_value],
            tag_rows=[tag_value],
            event_market_rows=[bridge_value],
            market_rows=[market_value],
            conn=conn,
        )

    tables = (
        "polymarket_wc2026_raw.event_snapshots",
        "polymarket_wc2026_raw.event_tag_snapshots",
        "polymarket_wc2026_raw.event_market_snapshots",
        "polymarket_wc2026_raw.event_market_payload_snapshots",
        "polymarket_wc2026_ops.sync_run_metrics",
    )
    with duck.get_connection() as conn:
        conn.execute(
            """
            insert or replace into polymarket_wc2026_ops.sync_run_metrics
            values (
                'event_catalog', timestamp '2026-08-01',
                '{"sentinel":true}', '[{"sentinel":true}]'
            )
            """
        )
        merge(event, tag, bridge, market)
        merge(event, tag, bridge, market)
        before = {
            table: conn.execute(f"select * from {table} order by all").fetchall()
            for table in tables
        }
        assert [len(before[table]) for table in tables[:4]] == [1, 1, 1, 1]

        divergent_event = dict(event)
        divergent_tag = dict(tag)
        divergent_bridge = dict(bridge)
        divergent_market = dict(market)
        if divergent_relation == "event":
            divergent_event["event_volume_usd_lifetime_reported"] = 130_000.0
        elif divergent_relation == "tag":
            divergent_tag["tag_label"] = "Changed label"
        elif divergent_relation == "bridge":
            divergent_bridge["source_ordinal"] = 9
        else:
            divergent_market["question"] = "Changed question"

        with pytest.raises(RuntimeError, match=message):
            merge(
                divergent_event,
                divergent_tag,
                divergent_bridge,
                divergent_market,
            )

        after = {
            table: conn.execute(f"select * from {table} order by all").fetchall()
            for table in tables
        }

    assert after == before


@pytest.mark.parametrize(
    ("omitted_relation", "message"),
    [
        ("event", "complete event snapshots relation"),
        ("tag", "complete event tag snapshots relation"),
        ("bridge", "complete event market snapshots relation"),
        ("payload", "complete event market payload snapshots relation"),
    ],
)
def test_event_catalog_replay_rejects_omissions_and_preserves_history_and_metrics(
    duck, omitted_relation, message
):
    observed_at = "2026-08-02T00:00:00"

    def event(event_id: str, source_market_count: int) -> dict:
        row = {
            column: None for column in EVENT_SNAPSHOT_COLUMNS if column != "row_order"
        }
        row.update(
            event_id=event_id,
            event_slug=event_id,
            event_title="World Cup append-only event",
            event_volume_usd_lifetime_reported=120_000.0,
            tags_json='["2026-fifa-world-cup"]',
            series_slugs_json="[]",
            candidate_sources_json='["exact_2026_tag"]',
            source_market_count=source_market_count,
            observed_at=observed_at,
            source_endpoint="/events/keyset",
        )
        return row

    events = [event("event-append-only", 1), event("event-second", 0)]
    tags = [
        {
            "event_id": "event-append-only",
            "tag_key": "tag-wc2026",
            "tag_id": "tag-wc2026",
            "tag_slug": "2026-fifa-world-cup",
            "tag_label": "2026 FIFA World Cup",
            "observed_at": observed_at,
        }
    ]
    bridges = [
        {
            "event_id": "event-append-only",
            "market_id": "market-append-only",
            "source_ordinal": 0,
            "is_enclosing_event": True,
            "observed_at": observed_at,
        }
    ]
    market = {
        column: None for column in EVENT_CATALOG_MARKET_COLUMNS if column != "row_order"
    }
    market.update(
        id="market-append-only",
        question="Will the append-only market resolve Yes?",
        outcomes='["Yes","No"]',
        volume=10.0,
        scraped_at=observed_at,
    )
    tables = (
        "polymarket_wc2026_raw.event_snapshots",
        "polymarket_wc2026_raw.event_tag_snapshots",
        "polymarket_wc2026_raw.event_market_snapshots",
        "polymarket_wc2026_raw.event_market_payload_snapshots",
        "polymarket_wc2026_ops.sync_run_metrics",
    )

    with duck.get_connection() as conn:
        conn.execute(
            """
            insert or replace into polymarket_wc2026_ops.sync_run_metrics
            values (
                'event_catalog', timestamp '2026-08-01',
                '{"sentinel":true}', '[{"sentinel":true}]'
            )
            """
        )
        merge_event_catalog_batch(
            event_rows=events,
            tag_rows=tags,
            event_market_rows=bridges,
            market_rows=[market],
            conn=conn,
        )
        before = {
            table: conn.execute(f"select * from {table} order by all").fetchall()
            for table in tables
        }

        replay_events = events[1:] if omitted_relation == "event" else events
        replay_tags = [] if omitted_relation == "tag" else tags
        replay_bridges = [] if omitted_relation == "bridge" else bridges
        replay_markets = [] if omitted_relation == "payload" else [market]
        with pytest.raises(RuntimeError, match=message):
            merge_event_catalog_batch(
                event_rows=replay_events,
                tag_rows=replay_tags,
                event_market_rows=replay_bridges,
                market_rows=replay_markets,
                conn=conn,
            )

        after = {
            table: conn.execute(f"select * from {table} order by all").fetchall()
            for table in tables
        }

    assert after == before


def test_event_catalog_replay_rejects_addition_to_originally_empty_relation(duck):
    observed_at = "2026-08-02T00:00:00"
    event = {column: None for column in EVENT_SNAPSHOT_COLUMNS if column != "row_order"}
    event.update(
        event_id="event-empty-tags",
        event_slug="event-empty-tags",
        event_title="World Cup event",
        event_volume_usd_lifetime_reported=120_000.0,
        tags_json="[]",
        series_slugs_json="[]",
        candidate_sources_json='["title_match"]',
        source_market_count=0,
        observed_at=observed_at,
        source_endpoint="/events/keyset",
    )
    added_tag = {
        "event_id": "event-empty-tags",
        "tag_key": "late-tag",
        "tag_id": "late-tag",
        "tag_slug": "late-tag",
        "tag_label": "Late tag",
        "observed_at": observed_at,
    }

    with duck.get_connection() as conn:
        merge_event_catalog_batch(
            event_rows=[event],
            tag_rows=[],
            event_market_rows=[],
            market_rows=[],
            conn=conn,
        )
        with pytest.raises(RuntimeError, match="complete event tag snapshots relation"):
            merge_event_catalog_batch(
                event_rows=[event],
                tag_rows=[added_tag],
                event_market_rows=[],
                market_rows=[],
                conn=conn,
            )
        tags = conn.execute(
            """
            select * from polymarket_wc2026_raw.event_tag_snapshots
            where event_id = 'event-empty-tags'
            """
        ).fetchall()

    assert tags == []


def test_event_catalog_rejects_mixed_observation_times_before_writes(duck):
    observed_at = "2026-08-02T00:00:00"
    event = {column: None for column in EVENT_SNAPSHOT_COLUMNS if column != "row_order"}
    event.update(
        event_id="event-rollback",
        event_slug="event-rollback",
        event_title="World Cup rollback event",
        event_volume_usd_lifetime_reported=120_000.0,
        tags_json='["2026-fifa-world-cup"]',
        series_slugs_json="[]",
        candidate_sources_json='["exact_2026_tag"]',
        source_market_count=1,
        observed_at=observed_at,
        source_endpoint="/events/keyset",
    )
    other_event = {**event, "event_id": "event-other", "observed_at": "2026-08-03"}

    with duck.get_connection() as conn:
        with pytest.raises(ValueError, match="share one non-null observed_at"):
            merge_event_catalog_batch(
                event_rows=[event, other_event],
                tag_rows=[],
                event_market_rows=[],
                market_rows=[],
                conn=conn,
            )
        rows = conn.execute(
            """
            select event_id
            from polymarket_wc2026_raw.event_snapshots
            where event_id in ('event-rollback', 'event-other')
            """
        ).fetchall()

    assert rows == []


def test_event_catalog_rejects_empty_and_cross_observation_inputs():
    conn = MagicMock()
    observed_at = "2026-08-02T00:00:00"

    with pytest.raises(ValueError, match="event_rows must not be empty"):
        merge_event_catalog_batch(
            event_rows=[],
            tag_rows=[],
            event_market_rows=[],
            market_rows=[],
            conn=conn,
        )

    with pytest.raises(ValueError, match="tag_rows must share"):
        merge_event_catalog_batch(
            event_rows=[{"observed_at": observed_at}],
            tag_rows=[{"observed_at": "2026-08-03T00:00:00"}],
            event_market_rows=[],
            market_rows=[],
            conn=conn,
        )

    with pytest.raises(ValueError, match="non-empty id"):
        merge_event_catalog_batch(
            event_rows=[{"observed_at": observed_at}],
            tag_rows=[],
            event_market_rows=[],
            market_rows=[{"id": " "}],
            conn=conn,
        )

    conn.assert_not_called()
