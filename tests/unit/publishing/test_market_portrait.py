from __future__ import annotations

import gzip
import json
from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from oddsfox_pipeline.publishing import market_portrait as subject
from oddsfox_pipeline.publishing.market_portrait import (
    FootballEvent,
    MatchFacts,
    RenderProfile,
    build_market_portrait_bundle,
    build_story,
)

UTC = timezone.utc


def _facts(**changes):
    start = datetime(2026, 6, 1, 18, tzinfo=UTC)
    values = {
        "fifa_match_id": 95,
        "stage": "round_of_16",
        "home_team": "Azure",
        "away_team": "Coral",
        "first_half_started_at": start,
        "first_half_ended_at": start + timedelta(minutes=48),
        "second_half_started_at": start + timedelta(minutes=63),
        "second_half_ended_at": start + timedelta(minutes=112),
        "game_ended_at": start + timedelta(minutes=112),
        "source_provenance_sha256": "a" * 64,
    }
    values.update(changes)
    return MatchFacts(**values)


def _published_connection(
    *,
    roles=("home", "away"),
    scan_status="published",
    raw_published=True,
    trade_count=1,
    include_trade=True,
):
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE SCHEMA polymarket_wc2026_ops")
    connection.execute("CREATE SCHEMA polymarket_wc2026_marts")
    connection.execute(
        """
        CREATE TABLE polymarket_wc2026_ops.match_order_book_scan_runs (
            scan_id VARCHAR, manifest_sha256 VARCHAR, status VARCHAR,
            raw_published BOOLEAN, aggregate_sha256 VARCHAR,
            finished_at TIMESTAMP
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE polymarket_wc2026_marts.polymarket_wc2026_match_order_book_states (
            scan_id VARCHAR, manifest_sha256 VARCHAR,
            fifa_match_id BIGINT, market_id VARCHAR,
            clob_token_id VARCHAR, landscape_role VARCHAR,
            snapshot_timestamp_ms BIGINT, provider_sequence BIGINT,
            snapshot_sha256 VARCHAR, bids_json VARCHAR, asks_json VARCHAR,
            last_trade_price_raw VARCHAR, ingested_at TIMESTAMP
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE polymarket_wc2026_marts.polymarket_wc2026_match_trades (
            scan_id VARCHAR, fifa_match_id BIGINT, trade_id VARCHAR,
            market_id VARCHAR, clob_token_id VARCHAR, landscape_role VARCHAR,
            trade_timestamp_ms BIGINT, event_sequence BIGINT,
            price VARCHAR, amount VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE polymarket_wc2026_ops.match_trade_scan_runs (
            scan_id VARCHAR, manifest_sha256 VARCHAR, status VARCHAR,
            trade_count BIGINT, aggregate_sha256 VARCHAR
        )
        """
    )
    connection.execute(
        """
        INSERT INTO polymarket_wc2026_ops.match_order_book_scan_runs
        VALUES ('scan', ?, ?, ?, ?, '2026-06-02')
        """,
        ["b" * 64, scan_status, raw_published, "c" * 64],
    )
    connection.execute(
        """
        INSERT INTO polymarket_wc2026_ops.match_trade_scan_runs
        VALUES ('scan', ?, 'published', ?, ?)
        """,
        ["b" * 64, trade_count, "d" * 64],
    )
    started = int(_facts().first_half_started_at.timestamp() * 1000)
    for role in roles:
        connection.execute(
            """
            INSERT INTO polymarket_wc2026_marts.polymarket_wc2026_match_order_book_states
            VALUES (
                'scan', ?, 95, 'market', ?, ?, ?, 0, ?,
                '[{"price":"0.4","size":"10","order_count":2}]',
                '[{"price":"0.6","size":"8","order_count":1}]',
                '0.5', '2026-06-02'
            )
            """,
            ["b" * 64, f"token-{role}", role, started, role[0] * 64],
        )
    if include_trade:
        connection.execute(
            """
            INSERT INTO polymarket_wc2026_marts.polymarket_wc2026_match_trades
            VALUES ('scan', 95, 'trade', 'market', 'token-home', 'home',
                    ?, 0, '0.6', '3')
            """,
            [started],
        )
    return connection


def test_story_uses_actual_periods_and_penalties_have_no_reaction():
    facts = _facts(
        first_extra_half_started_at=datetime(2026, 6, 1, 20, 8, tzinfo=UTC),
        first_extra_half_ended_at=datetime(2026, 6, 1, 20, 25, tzinfo=UTC),
        second_extra_half_started_at=datetime(2026, 6, 1, 20, 30, tzinfo=UTC),
        second_extra_half_ended_at=datetime(2026, 6, 1, 20, 47, tzinfo=UTC),
        game_ended_at=datetime(2026, 6, 1, 20, 55, tzinfo=UTC),
    )
    events = [
        FootballEvent(
            event_id="goal",
            event_order=1,
            event_type="Goal",
            period="2H",
            match_minute=90,
            stoppage_minute=4,
            minute_label="90+4",
        ),
        FootballEvent(
            event_id="penalty",
            event_order=2,
            event_type="Penalty scored",
            period="PENS",
            match_minute=None,
            stoppage_minute=None,
            minute_label="PENS",
            is_penalty_shootout=True,
            shootout_sequence=1,
        ),
    ]
    story = build_story(facts, events, [], [], RenderProfile())

    assert story["duration_seconds"] == 90
    assert story["alignment"] == "minute-aligned"
    assert [item["event_id"] for item in story["reactions"]] == []
    assert (
        next(item for item in story["annotations"] if item["event_id"] == "penalty")[
            "minute_label"
        ]
        == "PENS"
    )


def test_story_rejects_missing_actual_period_boundary():
    with pytest.raises(ValueError, match="requires both actual period boundaries"):
        build_story(
            _facts(second_half_ended_at=None),
            [],
            [],
            [],
            RenderProfile(),
        )


def test_landscape_roles_are_canonical_for_both_portrait_layouts():
    assert subject._landscape_roles(
        [{"role": "away_win"}, {"role": "home_win"}, {"role": "draw"}]
    ) == ["home_win", "draw", "away_win"]
    assert subject._landscape_roles([{"role": "away"}, {"role": "home"}]) == [
        "home",
        "away",
    ]
    with pytest.raises(ValueError, match="invalid portrait role inventory"):
        subject._landscape_roles([{"role": "draw"}])
    assert (
        build_story(_facts(game_ended_at=None), [], [], [], RenderProfile())[
            "duration_seconds"
        ]
        == 75
    )


@pytest.mark.parametrize(
    ("facts", "events", "match"),
    [
        (
            _facts(sanitization="none"),
            [],
            "deterministic-micro-epsilon-v1",
        ),
        (
            _facts(source_provenance_sha256="invalid"),
            [],
            "lowercase SHA-256",
        ),
        (
            _facts(first_half_started_at=datetime(2026, 6, 1, 18)),
            [],
            "timezone-aware",
        ),
        (
            _facts(first_half_ended_at=datetime(2026, 6, 1, 18, tzinfo=UTC)),
            [],
            "boundaries are inconsistent",
        ),
        (
            _facts(second_half_started_at=datetime(2026, 6, 1, 18, 30, tzinfo=UTC)),
            [],
            "overlap or are out of order",
        ),
        (
            _facts(game_ended_at=datetime(2026, 6, 1, 19, 30, tzinfo=UTC)),
            [],
            "precedes the final period boundary",
        ),
        (
            _facts(),
            [
                FootballEvent(
                    "penalty",
                    1,
                    "Penalty scored",
                    "PENS",
                    None,
                    None,
                    "PENS",
                    is_penalty_shootout=True,
                )
            ],
            "penalties require an actual game_ended_at after the final period",
        ),
        (
            _facts(),
            [
                FootballEvent("a", 2, "Other", "1H", 1, None, "1"),
                FootballEvent("b", 1, "Other", "1H", 2, None, "2"),
            ],
            "unique and monotonic",
        ),
        (
            _facts(),
            [
                FootballEvent(
                    "a",
                    1,
                    "Other",
                    "1H",
                    1,
                    None,
                    "1",
                    time_precision="second",
                )
            ],
            "minute-precision",
        ),
        (
            _facts(),
            [FootballEvent("a", 1, "Other", "PENS", 120, None, "PENS")],
            "without an invented minute",
        ),
        (
            _facts(),
            [FootballEvent("a", 1, "Other", "1H", None, None, "1")],
            "require a football minute",
        ),
        (
            _facts(),
            [FootballEvent("a", 1, "Other", "1H", 99, None, "99")],
            "falls outside its period",
        ),
    ],
)
def test_story_rejects_invalid_sanitized_facts(facts, events, match):
    with pytest.raises(ValueError, match=match):
        build_story(facts, events, [], [], RenderProfile())


def test_story_assigns_all_annotation_priorities_and_null_market_metrics():
    events = [
        FootballEvent("var", 1, "Review", "1H", 1, None, "1", var_decision="check"),
        FootballEvent("red", 2, "Dismissal", "1H", 2, None, "2", card_type="Red"),
        FootballEvent("card", 3, "Card", "1H", 3, None, "3"),
        FootballEvent("sub", 4, "Substitution", "1H", 4, None, "4"),
        FootballEvent("half", 5, "Half time", "1H", 5, None, "5"),
        FootballEvent("added", 6, "Added time", "1H", 6, None, "6"),
        FootballEvent("other", 7, "Injury", "1H", 7, None, "7"),
    ]
    start = int(_facts().first_half_started_at.timestamp() * 1000)
    story = build_story(
        _facts(),
        events,
        [
            {
                "event_sequence": 0,
                "role": "home",
                "timestamp_ms": start,
                "bids": [],
                "asks": [],
            }
        ],
        [],
        RenderProfile(),
    )

    assert [item["priority"] for item in story["annotations"]] == [
        "P0",
        "P0",
        "P1",
        "P1",
        "P1",
        "P1",
        "P2",
    ]
    metric = story["market_metrics"][0]
    assert metric["midpoint"] is None
    assert metric["spread"] is None
    assert metric["imbalance"] is None
    assert story["reactions"][0]["primary"]["before"] is None


def test_reactions_use_minute_open_close_and_following_minute_boundaries():
    facts = _facts()
    start = int(facts.first_half_started_at.timestamp() * 1000)

    def state(sequence, timestamp, bid, ask):
        return {
            "event_sequence": sequence,
            "role": "home",
            "timestamp_ms": timestamp,
            "bids": [{"price": bid, "size": "2"}],
            "asks": [{"price": ask, "size": "3"}],
        }

    story = build_story(
        facts,
        [
            FootballEvent(
                event_id="goal",
                event_order=1,
                event_type="Disallowed goal",
                period="1H",
                match_minute=1,
                stoppage_minute=None,
                minute_label="1",
                is_revoked=True,
            )
        ],
        [
            state(0, start - 1, "0.2", "0.4"),
            state(1, start + 60_000, "0.5", "0.7"),
            state(2, start + 120_000, "0.6", "0.8"),
        ],
        [],
        RenderProfile(),
    )

    reaction = story["reactions"][0]
    assert reaction["label"] == "minute-aligned market move"
    assert reaction["primary"]["before"]["midpoint"] == pytest.approx(0.3)
    assert reaction["primary"]["after"]["midpoint"] == pytest.approx(0.6)
    assert reaction["extended"]["after"]["midpoint"] == pytest.approx(0.7)
    assert story["football_minute_bands"][0]["weight"] == 1.5


def test_bundle_is_content_addressed_byte_stable_and_infers_aggressor(tmp_path):
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE SCHEMA polymarket_wc2026_ops")
    connection.execute("CREATE SCHEMA polymarket_wc2026_marts")
    connection.execute(
        """
        CREATE TABLE polymarket_wc2026_ops.match_order_book_scan_runs (
            scan_id VARCHAR, manifest_sha256 VARCHAR, status VARCHAR,
            raw_published BOOLEAN, aggregate_sha256 VARCHAR,
            finished_at TIMESTAMP
        )
        """
    )
    connection.execute(
        """
            CREATE TABLE polymarket_wc2026_marts.polymarket_wc2026_match_order_book_states (
                scan_id VARCHAR, manifest_sha256 VARCHAR,
                fifa_match_id BIGINT, market_id VARCHAR,
            clob_token_id VARCHAR, landscape_role VARCHAR,
            snapshot_timestamp_ms BIGINT, provider_sequence BIGINT,
            snapshot_sha256 VARCHAR, bids_json VARCHAR, asks_json VARCHAR,
            last_trade_price_raw VARCHAR, ingested_at TIMESTAMP
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE polymarket_wc2026_marts.polymarket_wc2026_match_trades (
            scan_id VARCHAR, fifa_match_id BIGINT, trade_id VARCHAR,
            market_id VARCHAR, clob_token_id VARCHAR, landscape_role VARCHAR,
            trade_timestamp_ms BIGINT, event_sequence BIGINT,
            price VARCHAR, amount VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE polymarket_wc2026_ops.match_trade_scan_runs (
            scan_id VARCHAR, manifest_sha256 VARCHAR, status VARCHAR,
            trade_count BIGINT, aggregate_sha256 VARCHAR
        )
        """
    )
    connection.execute(
        """
        INSERT INTO polymarket_wc2026_ops.match_order_book_scan_runs
        VALUES ('scan', ?, 'published', true, ?, '2026-06-02')
        """,
        ["b" * 64, "c" * 64],
    )
    connection.execute(
        """
        INSERT INTO polymarket_wc2026_ops.match_trade_scan_runs
        VALUES ('scan', ?, 'published', 1, ?)
        """,
        ["b" * 64, "d" * 64],
    )
    started = int(_facts().first_half_started_at.timestamp() * 1000)
    for role in ("home", "away"):
        connection.execute(
            """
                INSERT INTO polymarket_wc2026_marts.polymarket_wc2026_match_order_book_states
                VALUES (
                    'scan', ?, 95, 'market', ?, ?, ?, 0, ?,
                '[{"price":"0.4","size":"10","order_count":2}]',
                '[{"price":"0.6","size":"8","order_count":1}]',
                '0.5', '2026-06-02'
                )
                """,
            ["b" * 64, f"token-{role}", role, started, role[0] * 64],
        )
    connection.execute(
        """
        INSERT INTO polymarket_wc2026_marts.polymarket_wc2026_match_trades
        VALUES ('scan', 95, 'trade', 'market', 'token-home', 'home',
                ?, 0, '0.6', '3')
        """,
        [started],
    )

    first = build_market_portrait_bundle(
        connection,
        fifa_match_id=95,
        match_facts=_facts(),
        football_events=[],
        output_root=tmp_path,
        pipeline_revision="pipeline",
        scraper_revision="scraper",
    )
    second = build_market_portrait_bundle(
        connection,
        fifa_match_id=95,
        match_facts=_facts(),
        football_events=[],
        output_root=tmp_path,
        pipeline_revision="pipeline",
        scraper_revision="scraper",
    )

    assert first["bundle_id"] == second["bundle_id"]
    assert second["noop"] is True
    bundle = tmp_path / "95" / first["bundle_id"]
    manifest = json.loads((bundle / "manifest.json").read_text())
    trades = [
        json.loads(line)
        for line in gzip.decompress(
            (bundle / "trades.ndjson.gz").read_bytes()
        ).splitlines()
    ]
    assert manifest["contract_version"] == "oddsfox.market-portrait.v1"
    assert manifest["landscape_roles"] == ["home", "away"]
    assert manifest["source_facts"]["sanitization"].endswith("micro-epsilon-v1")
    assert manifest["pmxt"]["order_book_aggregate_sha256"] == "c" * 64
    assert manifest["pmxt"]["trade_aggregate_sha256"] == "d" * 64
    assert trades[0]["aggressor_side"] == "buy"

    (bundle / "story.json").write_text("changed")
    with pytest.raises(RuntimeError, match="different bytes"):
        build_market_portrait_bundle(
            connection,
            fifa_match_id=95,
            match_facts=_facts(),
            football_events=[],
            output_root=tmp_path,
            pipeline_revision="pipeline",
            scraper_revision="scraper",
        )


def test_bundle_rejects_mismatched_match_id_and_nonfinite_decimals(tmp_path):
    connection = _published_connection()
    with pytest.raises(ValueError, match="does not match MatchFacts"):
        build_market_portrait_bundle(
            connection,
            fifa_match_id=94,
            match_facts=_facts(),
            football_events=[],
            output_root=tmp_path,
        )
    with pytest.raises(ValueError, match="must be finite"):
        subject._decimal("NaN", "value")


def test_fetch_rows_requires_complete_published_contract():
    with pytest.raises(ValueError, match="portrait marts are required"):
        subject._fetch_rows(duckdb.connect(":memory:"), 95)

    invalid = _published_connection(scan_status="failed")
    with pytest.raises(ValueError, match="no published PMXT scan"):
        subject._fetch_rows(invalid, 95)

    invalid_roles = _published_connection(roles=("home",))
    with pytest.raises(ValueError, match="role inventory"):
        subject._fetch_rows(invalid_roles, 95)

    no_trade_run = _published_connection()
    no_trade_run.execute("DELETE FROM polymarket_wc2026_ops.match_trade_scan_runs")
    with pytest.raises(ValueError, match="non-empty published PMXT trade scan"):
        subject._fetch_rows(no_trade_run, 95)

    count_mismatch = _published_connection(trade_count=2)
    with pytest.raises(ValueError, match="trade count does not match"):
        subject._fetch_rows(count_mismatch, 95)

    unknown_role = _published_connection()
    unknown_role.execute(
        """
        UPDATE polymarket_wc2026_marts.polymarket_wc2026_match_trades
        SET landscape_role='draw'
        """
    )
    with pytest.raises(ValueError, match="unknown landscape role"):
        subject._fetch_rows(unknown_role, 95)


def test_fetch_rows_deduplicates_states_and_infers_sell_and_unknown():
    connection = _published_connection(trade_count=3)
    started = int(_facts().first_half_started_at.timestamp() * 1000)
    connection.execute(
        """
        INSERT INTO polymarket_wc2026_marts.polymarket_wc2026_match_order_book_states
        SELECT * FROM polymarket_wc2026_marts.polymarket_wc2026_match_order_book_states
        WHERE landscape_role='home'
        """
    )
    connection.execute(
        """
        UPDATE polymarket_wc2026_marts.polymarket_wc2026_match_trades
        SET trade_id='before', trade_timestamp_ms=?, price='0.5'
        """,
        [started - 1],
    )
    connection.execute(
        """
        INSERT INTO polymarket_wc2026_marts.polymarket_wc2026_match_trades
        VALUES
            ('scan', 95, 'sell', 'market', 'token-home', 'home',
             ?, 1, '0.4', '1'),
            ('scan', 95, 'inside', 'market', 'token-home', 'home',
             ?, 2, '0.5', '1')
        """,
        [started, started],
    )

    _, states, trades, _ = subject._fetch_rows(connection, 95)

    assert len(states) == 2
    assert [trade["aggressor_side"] for trade in trades] == [
        "unknown",
        "sell",
        "unknown",
    ]
