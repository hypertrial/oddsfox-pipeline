from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

from oddsfox_pipeline.ingestion.polymarket import soccer_match
from oddsfox_pipeline.ingestion.polymarket.match_minute import MatchMinuteTokenPlan
from oddsfox_pipeline.ingestion.polymarket.odds.minute_batch import MinuteFetchResult
from oddsfox_pipeline.ingestion.polymarket.soccer_match import (
    _terminal_empty_token_ids,
    build_soccer_match_result_registry,
    refresh_soccer_match_result_registry,
    select_soccer_match_minute_token_plans,
)
from oddsfox_pipeline.naming import SCOPE_SOCCER
from oddsfox_pipeline.storage.duckdb.dlt_batch import load_match_minute_fetch_audit
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    bootstrap_polymarket_tables,
)

KICKOFF = datetime(2025, 1, 2, 12, tzinfo=timezone.utc)


def test_registry_refresh_uses_persisted_snapshot_columns_without_row_order():
    conn = duckdb.connect(":memory:")
    conn.execute("create schema polymarket_soccer_raw")
    conn.execute("create schema polymarket_soccer_ops")
    bootstrap_polymarket_tables(conn, scope_name=SCOPE_SOCCER)

    summary = refresh_soccer_match_result_registry(conn)

    columns = {
        item[0]
        for item in conn.execute(
            "select * from polymarket_soccer_raw.markets limit 0"
        ).description
    }
    assert summary == {"events": 0, "matches": 0, "markets": 0, "excluded_events": 0}
    assert "id" in columns
    assert "row_order" not in columns


def _event(**overrides):
    return {
        "event_id": "event-1",
        "event_title": "Atlético-MG vs. São Paulo FC",
        "event_start_at": KICKOFF + timedelta(minutes=2),
        "finished_at": KICKOFF + timedelta(hours=2),
        "created_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
        **overrides,
    }


def _market(index: int, role: str, *, structured: bool = True, **overrides):
    questions = {
        "home_win": "Will Atlético-MG beat São Paulo FC?",
        "draw": "Will Atlético-MG vs. São Paulo FC end in a draw?",
        "away_win": "Will São Paulo FC beat Atlético-MG?",
    }
    labels = {
        "home_win": "Atlético-MG",
        "draw": "Draw (Atlético-MG vs. São Paulo FC)",
        "away_win": "São Paulo FC",
    }
    return {
        "market_id": f"market-{index}",
        "sports_market_type": "moneyline" if structured else None,
        "group_item_title": labels[role],
        "question": questions[role],
        "outcomes": '["Yes", "No"]',
        "clob_token_ids": f'["yes-{index}", "no-{index}"]',
        "game_start_time": KICKOFF,
        **overrides,
    }


def test_registry_maps_structured_roles_tokens_and_explicit_timing():
    markets = [
        _market(index, role)
        for index, role in enumerate(("home_win", "draw", "away_win"))
    ]
    result = build_soccer_match_result_registry([_event()], {"event-1": markets})

    assert result.exclusions == ()
    assert [row["result_role"] for row in result.rows] == [
        "home_win",
        "draw",
        "away_win",
    ]
    assert {row["yes_token_id"] for row in result.rows} == {
        "yes-0",
        "yes-1",
        "yes-2",
    }
    assert all(row["window_start_at"] == KICKOFF for row in result.rows)
    assert all(row["kickoff_source"] == "market_game_start_time" for row in result.rows)
    assert all(row["timing_status"] == "explicit_finish" for row in result.rows)
    assert all(row["timing_confidence"] == "high" for row in result.rows)
    assert all(row["coverage_tier"] == "guaranteed_tag_era" for row in result.rows)


def test_registry_strips_competition_prefix_without_weakening_draw_mapping():
    markets = [
        _market(index, role)
        for index, role in enumerate(("home_win", "draw", "away_win"))
    ]
    result = build_soccer_match_result_registry(
        [_event(event_title="Brasileirão: Atlético-MG vs. São Paulo FC")],
        {"event-1": markets},
    )

    assert {(row["home_team"], row["away_team"]) for row in result.rows} == {
        ("Atlético-MG", "São Paulo FC")
    }

    markets[1]["group_item_title"] = "Draw or Atlético-MG"
    markets[1]["question"] = "Will Atlético-MG or a draw occur?"
    rejected = build_soccer_match_result_registry(
        [_event(event_title="Brasileirão: Atlético-MG vs. São Paulo FC")],
        {"event-1": markets},
    )
    assert rejected.rows == ()
    assert rejected.exclusions[0]["exclusion_reason"] == (
        "incomplete_match_result_markets"
    )


def test_registry_accepts_only_exact_reciprocal_null_type_questions():
    markets = [
        _market(index, role, structured=False, group_item_title="ignored")
        for index, role in enumerate(("home_win", "draw", "away_win"))
    ]
    admitted = build_soccer_match_result_registry([_event()], {"event-1": markets})
    assert len(admitted.rows) == 3

    markets[0]["question"] = "Atlético-MG to win"
    rejected = build_soccer_match_result_registry([_event()], {"event-1": markets})
    assert rejected.rows == ()
    assert rejected.exclusions[0]["exclusion_reason"] == (
        "incomplete_match_result_markets"
    )


def test_registry_rejects_partial_kickoff_and_duplicate_tokens():
    markets = [
        _market(index, role)
        for index, role in enumerate(("home_win", "draw", "away_win"))
    ]
    markets[1]["game_start_time"] = None
    result = build_soccer_match_result_registry([_event()], {"event-1": markets})
    assert result.exclusions[0]["exclusion_reason"] == (
        "missing_or_inconsistent_kickoff"
    )

    markets[1]["game_start_time"] = KICKOFF
    markets[2]["clob_token_ids"] = '["yes-0", "no-2"]'
    result = build_soccer_match_result_registry([_event()], {"event-1": markets})
    assert result.exclusions[0]["exclusion_reason"] == "duplicate_clob_token"

    markets[2]["clob_token_ids"] = '["yes-2", "no-2"]'
    markets[2]["market_id"] = "market-0"
    result = build_soccer_match_result_registry([_event()], {"event-1": markets})
    assert result.exclusions[0]["exclusion_reason"] == (
        "duplicate_or_missing_market_id"
    )


def test_registry_uses_earliest_capped_closure_and_pre_tag_tier():
    markets = [
        _market(
            index,
            role,
            event_finished_time=KICKOFF + timedelta(hours=6 - index),
        )
        for index, role in enumerate(("home_win", "draw", "away_win"))
    ]
    result = build_soccer_match_result_registry(
        [
            _event(
                finished_at=None,
                created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            )
        ],
        {"event-1": markets},
    )
    assert all(
        row["window_end_at"] == KICKOFF + timedelta(hours=4) for row in result.rows
    )
    assert all(row["timing_status"] == "inferred_closure" for row in result.rows)
    assert all(row["timing_confidence"] == "medium" for row in result.rows)
    assert all(row["coverage_tier"] == "pre_tag_best_effort" for row in result.rows)


def test_registry_ignores_closure_at_kickoff():
    markets = [
        _market(index, role, end_date=KICKOFF)
        for index, role in enumerate(("home_win", "draw", "away_win"))
    ]
    result = build_soccer_match_result_registry(
        [_event(finished_at=None, closed_at=KICKOFF)],
        {"event-1": markets},
    )

    assert all(
        row["window_end_at"] == KICKOFF + timedelta(hours=5) for row in result.rows
    )
    assert all(row["timing_status"] == "inferred_five_hour_cap" for row in result.rows)


def test_plan_selection_produces_six_tokens_and_even_date_sample():
    conn = duckdb.connect(":memory:")
    conn.execute("SET TimeZone='UTC'")
    conn.execute("create schema polymarket_soccer_raw")
    conn.execute("create schema polymarket_soccer_ops")
    bootstrap_polymarket_tables(conn, scope_name=SCOPE_SOCCER)
    rows = []
    roles = ("home_win", "draw", "away_win")
    for game in range(5):
        started = KICKOFF + timedelta(days=game)
        finished = started + timedelta(hours=2)
        for index, role in enumerate(roles):
            rows.append(
                (
                    f"event-{game}",
                    f"market-{game}-{index}",
                    role,
                    "Home",
                    "Away",
                    f"yes-{game}-{index}",
                    f"no-{game}-{index}",
                    started,
                    finished,
                    "market_game_start_time",
                    "explicit_finish",
                    "high",
                    "guaranteed_tag_era",
                    finished,
                )
            )
    conn.executemany(
        "insert into polymarket_soccer_ops.match_result_registry values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    all_plans = select_soccer_match_minute_token_plans(
        conn,
        completion_grace_minutes=0,
        now=KICKOFF + timedelta(days=10),
    )
    sample = select_soccer_match_minute_token_plans(
        conn,
        completion_grace_minutes=0,
        game_sample_size=3,
        now=KICKOFF + timedelta(days=10),
    )

    assert len(all_plans) == 30
    assert len(sample) == 18
    assert {plan.market_id.split("-")[1] for plan in sample} == {"0", "2", "4"}

    plan = all_plans[0]
    terminal_at = plan.finished_at + timedelta(hours=73)
    load_match_minute_fetch_audit(
        [
            {
                "fetch_run_id": "empty-run",
                "market_id": plan.market_id,
                "clobTokenId": plan.token_id,
                "fetch_status": "empty",
                "raw_published": False,
                "fidelity_minutes": 1,
                "exact_window_start_at": plan.started_at,
                "exact_window_end_at": plan.finished_at,
                "request_start_epoch": int(plan.started_at.timestamp()),
                "request_end_epoch": int(plan.finished_at.timestamp()),
                "source_row_count": 0,
                "in_game_row_count": 0,
                "in_game_history_sha256": None,
                "source_endpoint": "https://clob.polymarket.com/prices-history",
                "fetch_started_at": plan.finished_at,
                "fetch_finished_at": plan.finished_at,
                "error_type": "EmptyHistory",
                "error_message": "empty",
            }
        ],
        conn,
        scope_name=SCOPE_SOCCER,
    )
    assert _terminal_empty_token_ids(
        conn,
        [plan],
        empty_retry_hours=72,
        now=terminal_at,
    ) == {plan.token_id}
    assert conn.execute(
        "select empty_retry_hours, terminal_at from polymarket_soccer_ops.match_minute_odds_terminal_unavailable"
    ).fetchone() == (72, terminal_at.replace(tzinfo=None))


def test_sync_fails_when_all_newly_due_fetches_fail_despite_reuse(monkeypatch):
    conn = duckdb.connect(":memory:")
    conn.execute("SET TimeZone='UTC'")
    conn.execute("create schema polymarket_soccer_raw")
    conn.execute("create schema polymarket_soccer_ops")
    bootstrap_polymarket_tables(conn, scope_name=SCOPE_SOCCER)
    reused = MatchMinuteTokenPlan(
        market_id="market-reused",
        token_id="token-reused",
        started_at=KICKOFF,
        finished_at=KICKOFF + timedelta(hours=2),
    )
    due = MatchMinuteTokenPlan(
        market_id="market-due",
        token_id="token-due",
        started_at=KICKOFF,
        finished_at=KICKOFF + timedelta(hours=2),
    )
    captured_at = KICKOFF + timedelta(hours=3)

    def result(plan, status):
        return MinuteFetchResult(
            plan=plan,
            fetch_status=status,
            history=(),
            request_start_epoch=int(plan.started_at.timestamp()),
            request_end_epoch=int(plan.finished_at.timestamp()),
            source_row_count=0,
            history_sha256="a" * 64 if status == "success" else None,
            fetch_started_at=captured_at,
            fetch_finished_at=captured_at,
            error_type="ApiError" if status == "error" else None,
            error_message="failed" if status == "error" else None,
        )

    monkeypatch.setattr(
        soccer_match,
        "select_soccer_match_minute_token_plans",
        lambda *_args, **_kwargs: [reused, due],
    )
    monkeypatch.setattr(
        soccer_match, "_terminal_empty_token_ids", lambda *_a, **_k: set()
    )
    monkeypatch.setattr(
        soccer_match,
        "resolve_minute_token_reuse",
        lambda *_args, **_kwargs: (None, {reused.token_id}, {}),
    )
    monkeypatch.setattr(
        soccer_match,
        "fetch_and_write_minute_history_parquet_shards",
        lambda *_args, **_kwargs: ([result(due, "error")], [], {}),
    )
    cleaned = []
    monkeypatch.setattr(
        soccer_match,
        "cleanup_minute_odds_publish_cache",
        cleaned.append,
    )

    with pytest.raises(RuntimeError, match="All due"):
        soccer_match.sync_soccer_match_minute_odds_history(conn, log=object())

    assert len(cleaned) == 1
    assert conn.execute(
        "select fetch_status, raw_published from polymarket_soccer_ops.match_minute_odds_fetch_audit order by clobTokenId"
    ).fetchall() == [("error", False)]


def test_sync_audits_only_newly_due_tokens(monkeypatch):
    conn = duckdb.connect(":memory:")
    plans = [
        MatchMinuteTokenPlan(
            market_id=f"market-{index // 2}",
            token_id=f"token-{index}",
            started_at=KICKOFF,
            finished_at=KICKOFF + timedelta(hours=2),
        )
        for index in range(12)
    ]
    reused = {plan.token_id for plan in plans[:6]}
    due = plans[6:]
    captured_audit: list[dict] = []
    captured_reuse: list[set[str]] = []

    monkeypatch.setattr(
        soccer_match,
        "select_soccer_match_minute_token_plans",
        lambda *_args, **_kwargs: plans,
    )
    monkeypatch.setattr(
        soccer_match, "_terminal_empty_token_ids", lambda *_args, **_kwargs: set()
    )
    monkeypatch.setattr(
        soccer_match,
        "resolve_minute_token_reuse",
        lambda *_args, **_kwargs: (None, reused, {}),
    )
    monkeypatch.setattr(
        soccer_match,
        "fetch_and_write_minute_history_parquet_shards",
        lambda *_args, **_kwargs: (
            [
                MinuteFetchResult(
                    plan=plan,
                    fetch_status="success",
                    history=(),
                    request_start_epoch=int(plan.started_at.timestamp()),
                    request_end_epoch=int(plan.finished_at.timestamp()),
                    source_row_count=1,
                    history_sha256="a" * 64,
                    fetch_started_at=KICKOFF,
                    fetch_finished_at=KICKOFF,
                    history_row_count=1,
                )
                for plan in due
            ],
            [Path("fixture.parquet")],
            {"max_inflight_futures": 6},
        ),
    )
    monkeypatch.setattr(
        soccer_match,
        "load_match_minute_fetch_audit",
        lambda rows, *_args, **_kwargs: captured_audit.extend(rows),
    )
    monkeypatch.setattr(
        soccer_match,
        "load_match_minute_odds_history_stage",
        lambda *_args, **kwargs: captured_reuse.append(kwargs["reuse_token_ids"]),
    )
    monkeypatch.setattr(
        soccer_match, "cleanup_minute_odds_publish_cache", lambda _: None
    )

    summary = soccer_match.sync_soccer_match_minute_odds_history(conn, log=object())

    assert len(captured_audit) == 6
    assert {row["clobTokenId"] for row in captured_audit} == {
        plan.token_id for plan in due
    }
    assert captured_reuse == [reused]
    assert summary["attempted_tokens"] == 6
    assert summary["reused_tokens"] == 6
    assert summary["audit_amplification"] == 1.0
    conn.close()
