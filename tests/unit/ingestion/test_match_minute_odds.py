from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from oddsfox_pipeline.ingestion.polymarket import match_minute


def _inventory_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute("create schema polymarket_wc2026_raw")
    conn.execute(
        """
        create table polymarket_wc2026_raw.markets (
            id varchar,
            event_id varchar,
            event_slug varchar,
            event_title varchar,
            event_start_time timestamp,
            event_finished_time timestamp,
            event_ended boolean,
            sports_market_type varchar,
            group_item_title varchar,
            outcomes varchar,
            clob_token_ids varchar,
            closed boolean
        )
        """
    )
    base = datetime(2026, 6, 1, tzinfo=timezone.utc)
    rows = []
    for game in range(1, 105):
        started = base + timedelta(hours=game * 4)
        finished = started + timedelta(minutes=97, seconds=30)
        teams = (f"Team {game} A", f"Team {game} B")
        titles = (teams[0], f"Draw ({teams[0]} vs. {teams[1]})", teams[1])
        for proposition, title in enumerate(titles):
            market_id = f"money-{game}-{proposition}"
            rows.append(
                (
                    market_id,
                    f"primary-{game}",
                    f"primary-{game}",
                    f"{teams[0]} vs. {teams[1]}",
                    started,
                    finished,
                    True,
                    "moneyline",
                    title,
                    json.dumps(["Yes", "No"]),
                    json.dumps([f"{market_id}-yes", f"{market_id}-no"]),
                    True,
                )
            )
        if game >= 73:
            market_id = f"advance-{game}"
            rows.append(
                (
                    market_id,
                    f"more-{game}",
                    f"more-{game}",
                    f"{teams[0]} vs. {teams[1]} - More Markets",
                    started,
                    None,
                    None,
                    "soccer_team_to_advance",
                    "Team to Advance",
                    json.dumps(list(teams)),
                    json.dumps([f"{market_id}-a", f"{market_id}-b"]),
                    True,
                )
            )
    conn.executemany(
        "insert into polymarket_wc2026_raw.markets values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    return conn


def test_match_minute_selection_uses_primary_timing_and_exact_inventory():
    conn = _inventory_connection()
    try:
        plans = match_minute.select_match_minute_token_plans(conn)
    finally:
        conn.close()

    assert len(plans) == 496
    final = [plan for plan in plans if plan.market_id == "advance-104"]
    assert len(final) == 2
    assert all(
        plan.finished_at - plan.started_at == timedelta(minutes=97, seconds=30)
        for plan in final
    )


def test_match_minute_selection_rejects_partial_inventory():
    conn = _inventory_connection()
    conn.execute("delete from polymarket_wc2026_raw.markets where id = 'money-1-2'")
    try:
        with pytest.raises(ValueError, match="three moneyline markets"):
            match_minute.select_match_minute_token_plans(conn)
    finally:
        conn.close()


def test_match_minute_helpers_cover_json_time_and_team_pair_edges():
    assert match_minute._json_list([" Team Á "]) == ["Team Á"]  # noqa: SLF001
    assert match_minute._json_list(None) == []  # noqa: SLF001
    assert match_minute._team_key("México U.S.A.") == "mexicousa"  # noqa: SLF001
    assert match_minute._utc(datetime(2026, 1, 1)).tzinfo == timezone.utc  # noqa: SLF001

    for teams in ([], ["", "Team"], ["Same", "Same"]):
        with pytest.raises(ValueError, match="Invalid WC2026 team pair"):
            match_minute._pair_key(teams, datetime(2026, 1, 1))  # noqa: SLF001


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            "delete from polymarket_wc2026_raw.markets where event_id = 'primary-1'",
            "Expected 104 primary moneyline events",
        ),
        (
            "delete from polymarket_wc2026_raw.markets where id = 'advance-73'",
            "Expected 32 advance markets",
        ),
        (
            "update polymarket_wc2026_raw.markets set event_start_time = null "
            "where event_id = 'primary-1'",
            "has no valid timing window",
        ),
        (
            "update polymarket_wc2026_raw.markets set event_finished_time = null "
            "where event_id = 'primary-1'",
            "has no valid timing window",
        ),
        (
            "update polymarket_wc2026_raw.markets set event_ended = false "
            "where event_id = 'primary-1'",
            "has no valid timing window",
        ),
        (
            "update polymarket_wc2026_raw.markets set event_start_time = null "
            "where id = 'advance-73'",
            "has no start time",
        ),
        (
            'update polymarket_wc2026_raw.markets set outcomes = \'["Other A", "Other B"]\' '
            "where id = 'advance-73'",
            "matched 0 primary events",
        ),
        (
            "update polymarket_wc2026_raw.markets set clob_token_ids = '[\"one\"]' "
            "where id = 'money-1-0'",
            "must map exactly two outcome tokens",
        ),
        (
            "update polymarket_wc2026_raw.markets set outcomes = '[\"Yes\"]' "
            "where id = 'money-1-0'",
            "must map exactly two outcome tokens",
        ),
        (
            "update polymarket_wc2026_raw.markets "
            "set outcomes = '[\"Up\", \"Down\"]' where id = 'money-1-0'",
            "outcomes must be literal Yes and No",
        ),
        (
            "update polymarket_wc2026_raw.markets "
            'set clob_token_ids = \'["same", "same"]\' '
            "where id = 'money-1-0'",
            "must map exactly two outcome tokens",
        ),
        (
            "update polymarket_wc2026_raw.markets "
            "set event_finished_time = event_start_time where event_id = 'primary-1'",
            "has an invalid primary window",
        ),
        (
            "update polymarket_wc2026_raw.markets "
            'set clob_token_ids = \'["money-1-0-yes", "duplicate-other"]\' '
            "where id = 'money-1-1'",
            "maps to more than one market",
        ),
    ],
)
def test_match_minute_selection_rejects_malformed_inventory(mutation, message):
    conn = _inventory_connection()
    conn.execute(mutation)
    try:
        with pytest.raises(ValueError, match=message):
            match_minute.select_match_minute_token_plans(conn)
    finally:
        conn.close()


def test_match_minute_selection_rejects_ambiguous_primary_events():
    conn = _inventory_connection()
    primary_103 = conn.execute(
        "select event_start_time, event_finished_time "
        "from polymarket_wc2026_raw.markets where event_id = 'primary-103' limit 1"
    ).fetchone()
    conn.execute(
        "update polymarket_wc2026_raw.markets "
        "set event_start_time = ?, event_finished_time = ?, "
        "group_item_title = replace(group_item_title, '104', '103') "
        "where event_id = 'primary-104'",
        primary_103,
    )
    try:
        with pytest.raises(ValueError, match="Duplicate or ambiguous primary"):
            match_minute.select_match_minute_token_plans(conn)
    finally:
        conn.close()


def test_match_minute_selection_rejects_duplicate_advance_mapping():
    conn = _inventory_connection()
    start_103 = conn.execute(
        "select event_start_time from polymarket_wc2026_raw.markets "
        "where event_id = 'primary-103' limit 1"
    ).fetchone()[0]
    conn.execute(
        "update polymarket_wc2026_raw.markets "
        'set event_start_time = ?, outcomes = \'["Team 103 A", "Team 103 B"]\' '
        "where id = 'advance-104'",
        [start_103],
    )
    try:
        with pytest.raises(ValueError, match="duplicate advance markets"):
            match_minute.select_match_minute_token_plans(conn)
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("constant", "value", "message"),
    [
        ("EXPECTED_GROUP_GAMES", 71, "Expected 71 group events"),
        ("EXPECTED_MARKETS", 247, "Expected 247 selected markets"),
        ("EXPECTED_TOKENS", 495, "Expected 495 tokens"),
    ],
)
def test_match_minute_selection_defensive_acceptance_counts(
    monkeypatch, constant, value, message
):
    conn = _inventory_connection()
    monkeypatch.setattr(match_minute, constant, value)
    try:
        with pytest.raises(ValueError, match=message):
            match_minute.select_match_minute_token_plans(conn)
    finally:
        conn.close()


def test_fetch_plan_pads_request_but_filters_exact_window():
    plan = match_minute.MatchMinuteTokenPlan(
        market_id="market",
        token_id="token",
        started_at=datetime(2026, 7, 1, 12, 0, 30, tzinfo=timezone.utc),
        finished_at=datetime(2026, 7, 1, 12, 2, 35, 100000, tzinfo=timezone.utc),
    )
    calls = []

    def fetch(*args):
        calls.append(args)
        return [
            ("token", int(plan.started_at.timestamp()) - 1, 0.1),
            ("token", int(plan.started_at.timestamp()), 0.2),
            ("token", int(plan.finished_at.timestamp()), 0.3),
            ("token", int(plan.finished_at.timestamp()) + 1, 0.4),
        ]

    rows = match_minute._fetch_plan(  # noqa: SLF001
        plan,
        object(),
        fetch,
        transient_retries=2,
        transient_backoff_seconds=0.25,
    )

    assert rows == [
        ("token", int(plan.started_at.timestamp()), 0.2),
        ("token", int(plan.finished_at.timestamp()), 0.3),
    ]
    assert calls[0][2:6] == (1782907200, 1782907380, 1, 300)


def test_fetch_plan_rejects_empty_in_game_history():
    plan = match_minute.MatchMinuteTokenPlan(
        market_id="market",
        token_id="token",
        started_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
        finished_at=datetime(2026, 7, 1, 0, 1, tzinfo=timezone.utc),
    )
    with pytest.raises(RuntimeError, match="Empty in-game"):
        match_minute._fetch_plan(  # noqa: SLF001
            plan,
            object(),
            lambda *_: [],
            transient_retries=0,
            transient_backoff_seconds=0,
        )


@pytest.mark.parametrize(
    ("history", "message"),
    [
        (None, "Transient CLOB failure"),
        ([("other", 1_783_036_800, 0.5)], "mismatched token"),
        ([("token", 1_783_036_800, 1.1)], "invalid probability"),
    ],
)
def test_fetch_plan_rejects_failed_or_invalid_history(history, message):
    plan = match_minute.MatchMinuteTokenPlan(
        market_id="market",
        token_id="token",
        started_at=datetime(2026, 7, 3, tzinfo=timezone.utc),
        finished_at=datetime(2026, 7, 3, 0, 1, tzinfo=timezone.utc),
    )
    with pytest.raises((RuntimeError, ValueError), match=message):
        match_minute._fetch_plan(  # noqa: SLF001
            plan,
            object(),
            lambda *_: history,
            transient_retries=0,
            transient_backoff_seconds=0,
        )


def test_sync_is_atomic_and_does_not_use_hourly_ledger(monkeypatch):
    conn = duckdb.connect(":memory:")
    conn.execute("create schema polymarket_wc2026_ops")
    conn.execute(
        "create table polymarket_wc2026_ops.token_sync_ledger "
        "(clobTokenId varchar primary key, last_sync_timestamp bigint)"
    )
    conn.execute(
        "insert into polymarket_wc2026_ops.token_sync_ledger values ('hourly', 7)"
    )
    plan = match_minute.MatchMinuteTokenPlan(
        market_id="market",
        token_id="token",
        started_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
        finished_at=datetime(2026, 7, 1, 0, 1, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        match_minute, "select_match_minute_token_plans", lambda _: [plan]
    )
    persisted = []

    summary = match_minute.sync_match_minute_odds_history(
        conn,
        workers=1,
        requests_per_second=1000,
        client_factory=object,
        fetch_window_fn=lambda *_: [("token", int(plan.started_at.timestamp()), 0.5)],
        persist_fn=lambda rows, _: persisted.extend(rows),
    )

    assert summary["tokens"] == 1
    assert len(persisted) == 1
    assert conn.execute(
        "select * from polymarket_wc2026_ops.token_sync_ledger"
    ).fetchall() == [("hourly", 7)]
    conn.close()


def test_sync_reuses_default_worker_client(monkeypatch):
    conn = duckdb.connect(":memory:")
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    plans = [
        match_minute.MatchMinuteTokenPlan(
            market_id=f"market-{index}",
            token_id=f"token-{index}",
            started_at=start,
            finished_at=start + timedelta(minutes=1),
        )
        for index in range(2)
    ]
    monkeypatch.setattr(
        match_minute, "select_match_minute_token_plans", lambda _: plans
    )
    clients = []

    def build_client(*args, **kwargs):
        value = object()
        clients.append((args, kwargs, value))
        return value

    monkeypatch.setattr(match_minute, "build_client", build_client)
    persisted = []
    match_minute.sync_match_minute_odds_history(
        conn,
        workers=1,
        requests_per_second=1000,
        fetch_window_fn=lambda _client, token, *_args: [
            (token, int(start.timestamp()), 0.5)
        ],
        persist_fn=lambda rows, _: persisted.extend(rows),
    )

    assert len(clients) == 1
    assert len(persisted) == 2
    conn.close()


def test_sync_cancels_and_never_persists_after_fetch_failure(monkeypatch):
    conn = duckdb.connect(":memory:")
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    plans = [
        match_minute.MatchMinuteTokenPlan(
            market_id=f"market-{index}",
            token_id=f"token-{index}",
            started_at=start,
            finished_at=start + timedelta(minutes=1),
        )
        for index in range(2)
    ]
    monkeypatch.setattr(
        match_minute, "select_match_minute_token_plans", lambda _: plans
    )

    with pytest.raises(RuntimeError, match="Transient CLOB failure"):
        match_minute.sync_match_minute_odds_history(
            conn,
            workers=0,
            requests_per_second=1000,
            client_factory=object,
            fetch_window_fn=lambda *_: None,
            persist_fn=lambda *_: pytest.fail("failed fetch must not persist"),
        )
    conn.close()
