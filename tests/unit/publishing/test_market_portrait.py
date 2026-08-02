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
        "kickoff_at_utc": start - timedelta(minutes=1),
        "first_half_started_at": start,
        "first_half_ended_at": start + timedelta(minutes=48),
        "second_half_started_at": start + timedelta(minutes=63),
        "second_half_ended_at": start + timedelta(minutes=112),
        "match_ended_at": start + timedelta(minutes=112),
        "source_provenance_sha256": "a" * 64,
    }
    values.update(changes)
    return MatchFacts(**values)


def _add_alignment_contract(connection, roles=("home", "away")):
    connection.execute("CREATE SCHEMA polymarket_wc2026_intermediate")
    connection.execute(
        """
        CREATE TABLE polymarket_wc2026_intermediate
            .int_polymarket_wc2026_match_working_set (
            fifa_match_id BIGINT,
            scheduled_kickoff_at_utc TIMESTAMPTZ,
            match_started_at_utc TIMESTAMPTZ,
            fixture_mapping_count BIGINT,
            primary_mapping_count BIGINT
        )
        """
    )
    connection.execute(
        """
        INSERT INTO polymarket_wc2026_intermediate
            .int_polymarket_wc2026_match_working_set
        VALUES (95, ?, ?, 1, 1)
        """,
        [_facts().kickoff_at_utc, _facts().kickoff_at_utc],
    )
    connection.execute(
        """
        CREATE TABLE polymarket_wc2026_ops.match_order_book_scan_windows (
            scan_id VARCHAR, fifa_match_id BIGINT, clob_token_id VARCHAR,
            window_start_ms BIGINT, window_end_ms BIGINT, depth INTEGER
        )
        """
    )
    start = int((_facts().kickoff_at_utc - timedelta(days=1)).timestamp() * 1_000)
    end = int((_facts().match_ended_at + timedelta(days=1)).timestamp() * 1_000)
    for role in roles:
        connection.execute(
            """
            INSERT INTO polymarket_wc2026_ops.match_order_book_scan_windows
            VALUES ('scan', 95, ?, ?, ?, 0)
            """,
            [f"token-{role}", start, end],
        )


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
    _add_alignment_contract(connection, roles)
    return connection


def test_story_uses_actual_periods_and_penalties_have_no_reaction():
    facts = _facts(
        first_extra_half_started_at=datetime(2026, 6, 1, 20, 8, tzinfo=UTC),
        first_extra_half_ended_at=datetime(2026, 6, 1, 20, 25, tzinfo=UTC),
        second_extra_half_started_at=datetime(2026, 6, 1, 20, 30, tzinfo=UTC),
        second_extra_half_ended_at=datetime(2026, 6, 1, 20, 47, tzinfo=UTC),
        match_ended_at=datetime(2026, 6, 1, 20, 55, tzinfo=UTC),
        home_score=1,
        away_score=0,
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
            home_score=1,
            away_score=0,
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

    assert story["duration_seconds"] == 65
    assert story["alignment"] == "minute-aligned"
    assert [item["event_id"] for item in story["reactions"]] == []
    assert (
        next(item for item in story["annotations"] if item["event_id"] == "penalty")[
            "minute_label"
        ]
        == "PENS"
    )


def test_story_orders_backdated_period_marker_by_timeline_and_deduplicates_score():
    events = [
        FootballEvent(
            "away-1",
            1,
            "Goal",
            "1H",
            15,
            None,
            "15",
            away_score=1,
            home_score=0,
        ),
        FootballEvent(
            "away-2",
            2,
            "Goal",
            "2H",
            67,
            None,
            "67",
            away_score=2,
            home_score=0,
        ),
        FootballEvent(
            "home-1",
            3,
            "Goal",
            "2H",
            79,
            None,
            "79",
            away_score=2,
            home_score=1,
        ),
        FootballEvent(
            "home-2",
            4,
            "Goal",
            "2H",
            83,
            None,
            "83",
            away_score=2,
            home_score=2,
        ),
        FootballEvent(
            event_id="late-goal",
            event_order=5,
            event_type="Goal",
            period="2H",
            match_minute=90,
            stoppage_minute=2,
            minute_label="90+2",
            home_score=3,
            away_score=2,
        ),
        FootballEvent(
            event_id="final-half-marker",
            event_order=6,
            event_type="Half",
            period="2H",
            match_minute=90,
            stoppage_minute=None,
            minute_label="90",
            home_score=3,
            away_score=2,
        ),
    ]

    story = build_story(
        _facts(home_score=3, away_score=2), events, [], [], RenderProfile()
    )

    annotations = story["annotations"]
    assert [row["event_id"] for row in annotations[-2:]] == [
        "final-half-marker",
        "late-goal",
    ]
    marker, goal = annotations[-2:]
    assert (marker["home_score"], marker["away_score"]) == (2, 2)
    assert (goal["home_score"], goal["away_score"]) == (3, 2)
    assert story["score_checkpoints"][-1] == {
        "event_order": 5,
        "video_start_seconds": goal["video_end_seconds"],
        "home_score": 3,
        "away_score": 2,
    }


def test_story_uses_exact_sixty_second_bands_and_infers_both_stoppages():
    start = _facts().first_half_started_at
    first_end = start + timedelta(minutes=52)
    second_start = first_end + timedelta(minutes=15)
    second_end = second_start + timedelta(minutes=57)
    story = build_story(
        _facts(
            first_half_ended_at=first_end,
            second_half_started_at=second_start,
            second_half_ended_at=second_end,
            match_ended_at=second_end,
        ),
        [],
        [],
        [],
        RenderProfile(),
    )

    first = [row for row in story["football_minute_bands"] if row["period"] == "1H"]
    second = [row for row in story["football_minute_bands"] if row["period"] == "2H"]
    assert len(first) == 52
    assert len(second) == 57
    assert first[-1]["minute_label"] == "45+7"
    assert second[-1]["minute_label"] == "90+12"
    assert len(story["football_minute_bands"]) == 109
    for period in (first, second):
        for current, following in zip(period, period[1:]):
            current_end = datetime.fromisoformat(
                current["source_end"].replace("Z", "+00:00")
            )
            current_start = datetime.fromisoformat(
                current["source_start"].replace("Z", "+00:00")
            )
            assert current_end - current_start == timedelta(minutes=1)
            assert current["source_end"] == following["source_start"]
    video_widths = {
        round(row["video_end_seconds"] - row["video_start_seconds"], 8)
        for row in story["football_minute_bands"]
    }
    assert video_widths == {round(45 / 109, 8)}


@pytest.mark.parametrize(
    ("excess", "expected_count"),
    [
        (timedelta(milliseconds=1), 45),
        (timedelta(microseconds=1_001), 46),
    ],
)
def test_story_stoppage_inference_has_one_millisecond_tolerance(excess, expected_count):
    start = _facts().first_half_started_at
    first_end = start + timedelta(minutes=45) + excess
    second_start = first_end + timedelta(minutes=15)
    second_end = second_start + timedelta(minutes=45)

    story = build_story(
        _facts(
            first_half_ended_at=first_end,
            second_half_started_at=second_start,
            second_half_ended_at=second_end,
            match_ended_at=second_end,
        ),
        [],
        [],
        [],
        RenderProfile(),
    )

    first = [row for row in story["football_minute_bands"] if row["period"] == "1H"]
    assert len(first) == expected_count
    assert first[-1]["source_end"] == first_end.isoformat().replace("+00:00", "Z")


def test_story_clamps_only_the_final_band_and_rejects_empty_event_band():
    start = _facts().first_half_started_at
    first_end = start + timedelta(minutes=47, seconds=13)
    second_start = first_end + timedelta(minutes=15)
    second_end = second_start + timedelta(minutes=45)
    facts = _facts(
        first_half_ended_at=first_end,
        second_half_started_at=second_start,
        second_half_ended_at=second_end,
        match_ended_at=second_end,
    )

    story = build_story(facts, [], [], [], RenderProfile())
    first = [row for row in story["football_minute_bands"] if row["period"] == "1H"]
    penultimate_start = datetime.fromisoformat(
        first[-2]["source_start"].replace("Z", "+00:00")
    )
    penultimate_end = datetime.fromisoformat(
        first[-2]["source_end"].replace("Z", "+00:00")
    )
    final_start = datetime.fromisoformat(
        first[-1]["source_start"].replace("Z", "+00:00")
    )
    final_end = datetime.fromisoformat(first[-1]["source_end"].replace("Z", "+00:00"))
    assert penultimate_end - penultimate_start == timedelta(minutes=1)
    assert final_end - final_start == timedelta(seconds=13)

    with pytest.raises(ValueError, match="45\\+4 has no source duration"):
        build_story(
            _facts(),
            [
                FootballEvent(
                    "impossible",
                    1,
                    "Added time",
                    "1H",
                    45,
                    4,
                    "45+4",
                )
            ],
            [],
            [],
            RenderProfile(),
        )


def test_story_normalizes_nonscoring_and_same_minute_goal_scores_at_band_end():
    events = [
        FootballEvent(
            "revoked",
            1,
            "Disallowed goal",
            "1H",
            10,
            None,
            "10",
            home_score=1,
            away_score=0,
            is_revoked=True,
        ),
        FootballEvent(
            "disallowed",
            2,
            "Disallowed goal",
            "1H",
            11,
            None,
            "11",
            home_score=1,
            away_score=0,
        ),
        FootballEvent(
            "home",
            3,
            "Goal",
            "1H",
            15,
            None,
            "15",
            home_score=1,
            away_score=0,
        ),
        FootballEvent(
            "away",
            4,
            "Goal",
            "1H",
            15,
            None,
            "15",
            home_score=1,
            away_score=1,
        ),
    ]

    story = build_story(
        _facts(home_score=1, away_score=1), events, [], [], RenderProfile()
    )

    revoked, disallowed, home, away = story["annotations"]
    assert (revoked["home_score"], revoked["away_score"]) == (0, 0)
    assert (disallowed["home_score"], disallowed["away_score"]) == (0, 0)
    assert (home["home_score"], home["away_score"]) == (1, 0)
    assert (away["home_score"], away["away_score"]) == (1, 1)
    assert [
        checkpoint["video_start_seconds"]
        for checkpoint in story["score_checkpoints"][1:]
    ] == [home["video_end_seconds"], away["video_end_seconds"]]


@pytest.mark.parametrize(
    ("facts", "events", "message"),
    [
        (
            _facts(),
            [
                FootballEvent(
                    "missing",
                    1,
                    "Goal",
                    "1H",
                    1,
                    None,
                    "1",
                )
            ],
            "requires a post-event score",
        ),
        (
            _facts(),
            [
                FootballEvent(
                    "incomplete",
                    1,
                    "Goal",
                    "1H",
                    1,
                    None,
                    "1",
                    home_score=1,
                )
            ],
            "score must provide both teams or neither",
        ),
        (
            _facts(),
            [
                FootballEvent(
                    "jump",
                    1,
                    "Goal",
                    "1H",
                    1,
                    None,
                    "1",
                    home_score=2,
                    away_score=0,
                )
            ],
            "increment exactly one team by one",
        ),
        (
            _facts(),
            [
                FootballEvent(
                    "first",
                    1,
                    "Goal",
                    "1H",
                    1,
                    None,
                    "1",
                    home_score=1,
                    away_score=0,
                ),
                FootballEvent(
                    "regression",
                    2,
                    "Goal",
                    "1H",
                    2,
                    None,
                    "2",
                    home_score=0,
                    away_score=0,
                ),
            ],
            "increment exactly one team by one",
        ),
        (
            _facts(home_score=2, away_score=0),
            [
                FootballEvent(
                    "only-goal",
                    1,
                    "Goal",
                    "1H",
                    1,
                    None,
                    "1",
                    home_score=1,
                    away_score=0,
                )
            ],
            "does not match the final MatchFacts score",
        ),
    ],
)
def test_story_rejects_invalid_post_event_score_sequences(facts, events, message):
    with pytest.raises(ValueError, match=message):
        build_story(facts, events, [], [], RenderProfile())


def test_story_rejects_missing_actual_period_boundary():
    with pytest.raises(ValueError, match="requires both actual period boundaries"):
        build_story(
            _facts(second_half_ended_at=None),
            [],
            [],
            [],
            RenderProfile(),
        )


@pytest.mark.parametrize("inversion_microseconds", [1, 2])
def test_story_allows_declared_game_end_micro_epsilon_inversion(
    inversion_microseconds,
):
    facts = _facts(
        match_ended_at=_facts().second_half_ended_at
        - timedelta(microseconds=inversion_microseconds)
    )

    story = build_story(facts, [], [], [], RenderProfile())

    assert story["football_minute_bands"][-1][
        "source_end"
    ] == facts.second_half_ended_at.isoformat().replace("+00:00", "Z")


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
        build_story(_facts(match_ended_at=None), [], [], [], RenderProfile())[
            "duration_seconds"
        ]
        == 45
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
            _facts(kickoff_at_utc=datetime(2026, 6, 1, 17, tzinfo=UTC)),
            [],
            "between two minutes before and 30 minutes after",
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
            _facts(first_half_ended_at=datetime(2026, 6, 1, 18, 30, tzinfo=UTC)),
            [],
            "first_half duration is implausible",
        ),
        (
            _facts(
                second_half_started_at=datetime(2026, 6, 1, 19, 20, tzinfo=UTC),
                second_half_ended_at=datetime(2026, 6, 1, 20, 10, tzinfo=UTC),
                match_ended_at=datetime(2026, 6, 1, 20, 10, tzinfo=UTC),
            ),
            [],
            "halftime duration is implausible",
        ),
        (
            _facts(
                first_extra_half_started_at=datetime(2026, 6, 1, 20, 30, tzinfo=UTC),
                first_extra_half_ended_at=datetime(2026, 6, 1, 20, 45, tzinfo=UTC),
                second_extra_half_started_at=datetime(2026, 6, 1, 20, 48, tzinfo=UTC),
                second_extra_half_ended_at=datetime(2026, 6, 1, 21, 3, tzinfo=UTC),
                match_ended_at=datetime(2026, 6, 1, 21, 3, tzinfo=UTC),
            ),
            [],
            "full-time to extra-time break is implausible",
        ),
        (
            _facts(
                first_extra_half_started_at=datetime(2026, 6, 1, 20, tzinfo=UTC),
                first_extra_half_ended_at=datetime(2026, 6, 1, 20, 15, tzinfo=UTC),
                second_extra_half_started_at=datetime(2026, 6, 1, 20, 35, tzinfo=UTC),
                second_extra_half_ended_at=datetime(2026, 6, 1, 20, 50, tzinfo=UTC),
                match_ended_at=datetime(2026, 6, 1, 20, 50, tzinfo=UTC),
            ),
            [],
            "extra-time break is implausible",
        ),
        (
            _facts(match_ended_at=datetime(2026, 6, 1, 19, 30, tzinfo=UTC)),
            [],
            "precedes the final period boundary",
        ),
        (
            _facts(
                match_ended_at=_facts().second_half_ended_at - timedelta(microseconds=3)
            ),
            [],
            "precedes the final period boundary",
        ),
        (
            _facts(match_ended_at=datetime(2026, 6, 1, 20, 38, tzinfo=UTC)),
            [],
            "match_ended_at is implausibly late",
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
            "penalties require an actual match_ended_at after the final period",
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
                FootballEvent("same", 1, "Other", "1H", 1, None, "1"),
                FootballEvent("same", 2, "Other", "1H", 2, None, "2"),
            ],
            "event IDs must be unique",
        ),
        (
            _facts(home_score=1),
            [],
            "final match score must provide both teams or neither",
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
            state(2, start + 120_000, "0.6", "0.8"),
            state(0, start - 1, "0.2", "0.4"),
            state(1, start + 60_000, "0.5", "0.7"),
        ],
        [],
        RenderProfile(),
    )

    reaction = story["reactions"][0]
    assert reaction["label"] == "minute-aligned market move"
    assert reaction["primary"]["before"]["midpoint"] == pytest.approx(0.3)
    assert reaction["primary"]["after"]["midpoint"] == pytest.approx(0.6)
    assert reaction["extended"]["after"]["midpoint"] == pytest.approx(0.7)
    assert story["football_minute_bands"][0]["weight"] == 1.0


def test_reactions_do_not_reuse_observations_across_period_breaks():
    facts = _facts()
    first_end = int(facts.first_half_ended_at.timestamp() * 1_000)
    second_start = int(facts.second_half_started_at.timestamp() * 1_000)

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
                "first-half-end",
                1,
                "Other",
                "1H",
                45,
                3,
                "45+3",
            ),
            FootballEvent("second-half-start", 2, "Other", "2H", 46, None, "46"),
        ],
        [
            state(0, first_end - 60_001, "0.2", "0.4"),
            state(1, second_start, "0.5", "0.7"),
            state(2, second_start + 60_000, "0.6", "0.8"),
        ],
        [],
        RenderProfile(),
    )

    first, second = story["reactions"]
    assert first["primary"]["before"]["midpoint"] == pytest.approx(0.3)
    assert first["primary"]["after"] is None
    assert first["extended"]["source_end_ms"] is None
    assert first["extended"]["after"] is None
    assert second["primary"]["before"] is None
    assert second["primary"]["after"]["midpoint"] == pytest.approx(0.7)


def test_reactions_round_micro_epsilon_boundaries_by_predicate_direction():
    first_start = _facts().first_half_started_at - timedelta(microseconds=1)
    first_end = first_start + timedelta(minutes=48)
    second_start = first_end + timedelta(minutes=15)
    second_end = second_start + timedelta(minutes=49)
    facts = _facts(
        first_half_started_at=first_start,
        first_half_ended_at=first_end,
        second_half_started_at=second_start,
        second_half_ended_at=second_end,
        match_ended_at=second_end,
    )
    epoch = datetime(1970, 1, 1, tzinfo=UTC)

    def bounds(value):
        microseconds = (value - epoch) // timedelta(microseconds=1)
        return microseconds // 1_000, (microseconds + 999) // 1_000

    def state(sequence, timestamp, midpoint):
        return {
            "event_sequence": sequence,
            "role": "home",
            "timestamp_ms": timestamp,
            "bids": [{"price": str(midpoint - 0.1), "size": "2"}],
            "asks": [{"price": str(midpoint + 0.1), "size": "3"}],
        }

    first_floor, first_ceil = bounds(first_start)
    minute_end_floor, minute_end_ceil = bounds(first_start + timedelta(minutes=1))
    following_end_floor, following_end_ceil = bounds(first_start + timedelta(minutes=2))
    final_start_floor, _ = bounds(first_end - timedelta(minutes=1))
    final_end_floor, final_end_ceil = bounds(first_end)
    assert first_ceil == first_floor + 1
    assert minute_end_ceil == minute_end_floor + 1
    assert following_end_ceil == following_end_floor + 1
    assert final_end_ceil == final_end_floor + 1

    story = build_story(
        facts,
        [
            FootballEvent("opening", 1, "Other", "1H", 1, None, "1"),
            FootballEvent("closing", 2, "Other", "1H", 45, 3, "45+3"),
        ],
        [
            state(0, first_floor, 0.2),
            state(1, first_ceil, 0.3),
            state(2, minute_end_floor, 0.4),
            state(3, minute_end_ceil, 0.5),
            state(4, following_end_ceil, 0.6),
            state(5, final_start_floor, 0.65),
            state(6, final_end_floor, 0.7),
            state(7, final_end_ceil, 0.8),
        ],
        [],
        RenderProfile(),
    )

    opening, closing = story["reactions"]
    assert opening["primary"]["source_start_ms"] == first_ceil
    assert opening["primary"]["source_end_ms"] == minute_end_ceil
    assert opening["primary"]["before"]["timestamp_ms"] == first_floor
    assert opening["primary"]["after"]["timestamp_ms"] == minute_end_ceil
    assert opening["extended"]["source_end_ms"] == following_end_ceil
    assert opening["extended"]["after"]["timestamp_ms"] == following_end_ceil
    assert closing["primary"]["source_end_ms"] == final_end_ceil
    assert closing["primary"]["before"]["timestamp_ms"] == final_start_floor
    assert closing["primary"]["after"] is None


def test_story_starts_at_kickoff_and_flows_continuously_for_45_seconds():
    facts = _facts(home_score=1, away_score=0)
    story = build_story(
        facts,
        [
            FootballEvent(
                event_id="goal",
                event_order=1,
                event_type="Goal",
                period="1H",
                match_minute=15,
                stoppage_minute=None,
                minute_label="15",
                home_score=1,
                away_score=0,
            )
        ],
        [],
        [],
        RenderProfile(),
    )

    assert story["duration_seconds"] == 45
    assert story["segments"][0]["kind"] == "football_minute"
    assert story["segments"][0]["video_start_seconds"] == 0
    assert story["segments"][0][
        "source_start"
    ] == facts.first_half_started_at.isoformat().replace("+00:00", "Z")
    assert story["segments"][-1]["video_end_seconds"] == 45
    assert {row["kind"] for row in story["segments"]} == {"football_minute"}
    assert {row["weight"] for row in story["football_minute_bands"]} == {1.0}

    second_half = next(row for row in story["segments"] if row["period"] == "2H")
    first_half = story["segments"][story["segments"].index(second_half) - 1]
    assert first_half["video_end_seconds"] == second_half["video_start_seconds"]
    assert first_half["source_end"] == facts.first_half_ended_at.isoformat().replace(
        "+00:00", "Z"
    )
    assert second_half[
        "source_start"
    ] == facts.second_half_started_at.isoformat().replace("+00:00", "Z")


def test_story_validator_fails_closed_on_derived_invariant_violation():
    facts = _facts()
    story = build_story(facts, [], [], [], RenderProfile())
    story["football_minute_bands"][0]["weight"] = 2.0

    with pytest.raises(ValueError, match="football bands must have equal weight"):
        subject._validate_story(facts, story)


def test_explicit_nonzero_chapter_overrides_remain_supported():
    story = build_story(
        _facts(),
        [],
        [],
        [],
        RenderProfile(
            pre_match_seconds=2,
            halftime_seconds=1,
            post_match_seconds=2,
        ),
    )

    chapters = [row for row in story["segments"] if row["kind"] != "football_minute"]
    assert [row["kind"] for row in chapters] == [
        "pre_match",
        "halftime",
        "post_match",
    ]
    assert chapters[0]["video_start_seconds"] == 0
    assert chapters[0]["video_end_seconds"] == 2
    assert chapters[1]["video_end_seconds"] - chapters[1]["video_start_seconds"] == 1
    assert chapters[2]["video_start_seconds"] == 43
    assert chapters[2]["video_end_seconds"] == 45


def test_extra_time_adds_15_seconds_without_a_shootout():
    facts = _facts(
        first_extra_half_started_at=datetime(2026, 6, 1, 20, 8, tzinfo=UTC),
        first_extra_half_ended_at=datetime(2026, 6, 1, 20, 25, tzinfo=UTC),
        second_extra_half_started_at=datetime(2026, 6, 1, 20, 30, tzinfo=UTC),
        second_extra_half_ended_at=datetime(2026, 6, 1, 20, 47, tzinfo=UTC),
        match_ended_at=datetime(2026, 6, 1, 20, 47, tzinfo=UTC),
    )

    story = build_story(facts, [], [], [], RenderProfile())

    assert story["duration_seconds"] == 60
    assert story["segments"][-1]["period"] == "ET2"
    assert story["segments"][-1]["video_end_seconds"] == 60


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
    _add_alignment_contract(connection)
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
    assert manifest["render_defaults"] == {
        "duration_seconds": 45.0,
        "fps": 60,
        "halftime_seconds": 0.0,
        "height": 1080,
        "penalty_seconds": 5.0,
        "post_match_seconds": 0.0,
        "pre_match_seconds": 0.0,
        "regulation_seconds": 45.0,
        "width": 1920,
    }
    assert manifest["source_bounds"] == {
        "start": _facts().first_half_started_at.isoformat().replace("+00:00", "Z"),
        "end": _facts().match_ended_at.isoformat().replace("+00:00", "Z"),
    }
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


def test_bundle_rejects_shifted_timeline_and_market_window_undercoverage(tmp_path):
    shifted = _published_connection()
    two_hours = timedelta(hours=2)
    facts = _facts(
        kickoff_at_utc=_facts().kickoff_at_utc + two_hours,
        first_half_started_at=_facts().first_half_started_at + two_hours,
        first_half_ended_at=_facts().first_half_ended_at + two_hours,
        second_half_started_at=_facts().second_half_started_at + two_hours,
        second_half_ended_at=_facts().second_half_ended_at + two_hours,
        match_ended_at=_facts().match_ended_at + two_hours,
    )
    with pytest.raises(ValueError, match="validated match working set"):
        build_market_portrait_bundle(
            shifted,
            fifa_match_id=95,
            match_facts=facts,
            football_events=[],
            output_root=tmp_path,
        )

    undercovered = _published_connection()
    undercovered.execute(
        """
        UPDATE polymarket_wc2026_ops.match_order_book_scan_windows
        SET window_end_ms=?
        """,
        [int(_facts().match_ended_at.timestamp() * 1_000)],
    )
    with pytest.raises(ValueError, match="does not cover the football timeline"):
        build_market_portrait_bundle(
            undercovered,
            fifa_match_id=95,
            match_facts=_facts(),
            football_events=[],
            output_root=tmp_path,
        )


def test_bundle_requires_complete_timing_and_root_window_contracts(tmp_path):
    missing_contract = _published_connection()
    missing_contract.execute(
        """
        DROP TABLE polymarket_wc2026_intermediate
            .int_polymarket_wc2026_match_working_set
        """
    )
    with pytest.raises(ValueError, match="validated match timing"):
        build_market_portrait_bundle(
            missing_contract,
            fifa_match_id=95,
            match_facts=_facts(),
            football_events=[],
            output_root=tmp_path,
        )

    missing_timing = _published_connection()
    missing_timing.execute(
        """
        DELETE FROM polymarket_wc2026_intermediate
            .int_polymarket_wc2026_match_working_set
        """
    )
    with pytest.raises(ValueError, match="one consistent match timing"):
        build_market_portrait_bundle(
            missing_timing,
            fifa_match_id=95,
            match_facts=_facts(),
            football_events=[],
            output_root=tmp_path,
        )

    mismatched_start = _published_connection()
    mismatched_start.execute(
        """
        UPDATE polymarket_wc2026_intermediate
            .int_polymarket_wc2026_match_working_set
        SET match_started_at_utc=match_started_at_utc + INTERVAL 2 HOUR
        """
    )
    with pytest.raises(ValueError, match="validated market start"):
        build_market_portrait_bundle(
            mismatched_start,
            fifa_match_id=95,
            match_facts=_facts(),
            football_events=[],
            output_root=tmp_path,
        )

    missing_root = _published_connection()
    missing_root.execute(
        """
        DELETE FROM polymarket_wc2026_ops.match_order_book_scan_windows
        WHERE clob_token_id='token-away'
        """
    )
    with pytest.raises(ValueError, match="do not cover every portrait role"):
        build_market_portrait_bundle(
            missing_root,
            fifa_match_id=95,
            match_facts=_facts(),
            football_events=[],
            output_root=tmp_path,
        )


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
