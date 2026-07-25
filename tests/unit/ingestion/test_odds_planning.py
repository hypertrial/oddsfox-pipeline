"""Unit tests for Polymarket odds sync planning."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from hypothesis import given
from hypothesis import strategies as st

pytest.importorskip("duckdb")

from oddsfox_pipeline.ingestion.polymarket.odds import sync as odds_sync
from oddsfox_pipeline.ingestion.polymarket.odds.engine.bootstrap import (
    bootstrap_planning,
)
from oddsfox_pipeline.ingestion.polymarket.odds.support import (
    OddsSyncOptions,
    TokenPlan,
)


def _valid_token(seed: int) -> str:
    return f"{seed:030x}12"


def test_build_single_token_plan_budget_and_latest_branches():
    tok = "b" * 33 + "12"
    budgets = {tok: 2}
    plan, skip, _ = odds_sync.build_single_token_plan(
        token_id=tok,
        market_id="m",
        closed=False,
        created_ts=100,
        latest_timestamps={},
        fully_checked_tokens=set(),
        persisted_skips={},
        seen_tokens=set(),
        now_ts=200,
        fidelity=1440,
        force=False,
        rebuild_history=False,
        overlap_seconds=0,
        recent_seconds=0,
        empty_token_skip_budgets=budgets,
        empty_token_skip_runs=2,
    )
    assert plan is None
    assert skip == "empty_cache_skip"
    assert budgets[tok] == 1

    tok2 = "c" * 33 + "12"
    plan, skip, _ = odds_sync.build_single_token_plan(
        token_id=tok2,
        market_id="m",
        closed=False,
        created_ts=100,
        latest_timestamps={tok2: 150},
        fully_checked_tokens=set(),
        persisted_skips={},
        seen_tokens=set(),
        now_ts=200,
        fidelity=1440,
        force=False,
        rebuild_history=False,
        overlap_seconds=20,
        recent_seconds=10,
        empty_token_skip_budgets={},
        empty_token_skip_runs=2,
    )
    assert skip is None
    assert plan is not None
    assert plan.start_ts == 130

    tok3 = "d" * 33 + "12"
    _, skip, _ = odds_sync.build_single_token_plan(
        token_id=tok3,
        market_id="m",
        closed=False,
        created_ts=100,
        latest_timestamps={tok3: 195},
        fully_checked_tokens=set(),
        persisted_skips={},
        seen_tokens=set(),
        now_ts=200,
        fidelity=1440,
        force=False,
        rebuild_history=False,
        overlap_seconds=0,
        recent_seconds=10,
        empty_token_skip_budgets={},
        empty_token_skip_runs=0,
    )
    assert skip == "recent_skip"

    tok4 = "e" * 33 + "12"
    _, skip, _ = odds_sync.build_single_token_plan(
        token_id=tok4,
        market_id="m",
        closed=False,
        created_ts=250,
        latest_timestamps={},
        fully_checked_tokens=set(),
        persisted_skips={},
        seen_tokens=set(),
        now_ts=200,
        fidelity=1440,
        force=True,
        rebuild_history=False,
        overlap_seconds=0,
        recent_seconds=0,
        empty_token_skip_budgets={},
        empty_token_skip_runs=0,
    )
    assert skip == "already_current"


@given(
    created_ts=st.integers(min_value=1, max_value=2_000_000_000),
    duration=st.integers(min_value=1, max_value=31_536_000),
    overlap_seconds=st.integers(min_value=0, max_value=86_400),
    latest_offset=st.none() | st.integers(min_value=0, max_value=31_536_000),
)
def test_build_single_token_plan_property_emits_bounded_windows(
    created_ts,
    duration,
    overlap_seconds,
    latest_offset,
):
    now_ts = created_ts + duration
    latest_ts = (
        None if latest_offset is None else min(now_ts - 1, created_ts + latest_offset)
    )
    token = _valid_token(created_ts + duration + overlap_seconds)
    seen_tokens: set[str] = set()

    plan, skip, invalid = odds_sync.build_single_token_plan(
        token_id=token,
        market_id="m",
        closed=False,
        created_ts=created_ts,
        latest_timestamps={} if latest_ts is None else {token: latest_ts},
        fully_checked_tokens=set(),
        persisted_skips={},
        seen_tokens=seen_tokens,
        now_ts=now_ts,
        fidelity=60,
        force=True,
        rebuild_history=False,
        overlap_seconds=overlap_seconds,
        recent_seconds=0,
        empty_token_skip_budgets={},
        empty_token_skip_runs=0,
    )

    assert invalid is None
    assert skip is None
    assert plan is not None
    assert plan.created_at_ts == created_ts
    assert created_ts <= plan.start_ts < plan.end_ts == now_ts
    if latest_ts is not None:
        assert plan.start_ts == max(created_ts, latest_ts - overlap_seconds)

    duplicate, duplicate_skip, _ = odds_sync.build_single_token_plan(
        token_id=token,
        market_id="m",
        closed=False,
        created_ts=created_ts,
        latest_timestamps={},
        fully_checked_tokens=set(),
        persisted_skips={},
        seen_tokens=seen_tokens,
        now_ts=now_ts,
        fidelity=60,
        force=True,
        rebuild_history=False,
        overlap_seconds=overlap_seconds,
        recent_seconds=0,
        empty_token_skip_budgets={},
        empty_token_skip_runs=0,
    )
    assert duplicate is None
    assert duplicate_skip == "dup_token"


def test_force_does_not_reopen_closed_fully_checked_token():
    tok = "f" * 33 + "12"
    _, skip, _ = odds_sync.build_single_token_plan(
        token_id=tok,
        market_id="m",
        closed=True,
        created_ts=1_600_000_000,
        latest_timestamps={tok: 1_700_000_000},
        fully_checked_tokens={tok},
        persisted_skips={},
        seen_tokens=set(),
        now_ts=1_900_000_000,
        fidelity=1,
        force=True,
        rebuild_history=False,
        overlap_seconds=120,
        recent_seconds=60,
        empty_token_skip_budgets={},
        empty_token_skip_runs=0,
    )
    assert skip == "closed_done"


def test_iter_token_plans_paged_collects_invalids_and_done_value():
    valid = "w" * 33 + "12"

    def pages():
        yield [
            ("pre", f'["{valid}"]', "2022-01-01 00:00:00", False),
            ("badjson", "{bad", "2024-01-01 00:00:00", False),
            ("noneonly", "[null]", "2024-01-01 00:00:00", False),
            ("mixed", f'[null, "short", "{valid}"]', "2024-01-01 00:00:00", False),
        ]

    invalid_batches = []
    gen = odds_sync.iter_token_plans_paged(
        now_ts=1_900_000_000,
        clob_cutoff_date="2023-01-01",
        fidelity=1440,
        force=True,
        rebuild_history=True,
        overlap_minutes=0,
        skip_recent_minutes=0,
        market_page_size=10,
        on_invalid_tokens_batch=invalid_batches.append,
        iter_markets_with_tokens_fn=lambda **kwargs: pages(),
        get_token_sync_snapshot_fn=lambda token_ids, **kwargs: ({}, set(), {}),
    )

    yielded = []
    done_value = None
    while True:
        try:
            yielded.append(next(gen))
        except StopIteration as done:
            done_value = done.value
            break

    planning_state, invalid_tokens = done_value
    assert len(yielded) == 1
    assert planning_state.pre_clob_markets == 1
    assert planning_state.invalid_token == 1
    assert invalid_batches == [[("short", "invalid token id format")]]
    assert invalid_tokens == {"short": "invalid token id format"}


def test_iter_token_plans_paged_force_passes_ended_market_grace():
    token_id = "g" * 33 + "12"
    full_calls = []

    def full_pages(**kwargs):
        full_calls.append(kwargs)
        yield [("fresh", f'["{token_id}"]', "2024-01-01 00:00:00", False)]

    def due_pages(**kwargs):
        raise AssertionError(f"unexpected due iterator call: {kwargs}")

    plans = list(
        odds_sync.iter_token_plans_paged(
            now_ts=1_900_000_000,
            clob_cutoff_date="2023-01-01",
            fidelity=1440,
            force=True,
            rebuild_history=False,
            overlap_minutes=0,
            skip_recent_minutes=0,
            market_page_size=10,
            ended_market_grace_days=7,
            iter_markets_with_tokens_fn=full_pages,
            iter_due_market_tokens_fn=due_pages,
            get_token_sync_snapshot_fn=lambda token_ids, **kwargs: ({}, set(), {}),
        )
    )

    assert len(plans) == 1
    assert full_calls
    assert full_calls[0]["ended_market_grace_days"] == 7


def test_bootstrap_planning_force_counts_with_ended_market_grace():
    class Runtime:
        count_kwargs = None

        def ensure_duck_db(self):
            return None

        def snapshot_raw_layer(self):
            return {}

        def save_skipped_tokens(self, records):
            return None

        def count_candidate_market_tokens(self, **kwargs):
            self.count_kwargs = kwargs
            return {"candidate_tokens": 4, "candidate_markets": 2}

        def count_due_market_token_exclusions(self, **kwargs):
            del kwargs
            return {"scope_skip": 0, "ended_market_skip": 0}

    runtime = Runtime()
    boot = bootstrap_planning(
        runtime,
        options=odds_sync.OddsSyncOptions(
            clob_cutoff_date="2023-01-01",
            fidelity=1440,
            force=True,
            rebuild_history=False,
            overlap_minutes=0,
            skip_recent_minutes=0,
            market_page_size=10,
            reconcile_ledger=False,
            short_range_first=True,
            market_scope="wc2026",
            ended_market_grace_days=7,
            min_volume=None,
            max_volume=None,
            history_backfill_days=0,
            empty_token_skip_budgets=None,
            empty_token_skip_runs=0,
        ),
        plan_iterator_factory=lambda **kwargs: iter(()),
    )

    assert runtime.count_kwargs["due_only"] is False
    assert runtime.count_kwargs["ended_market_grace_days"] == 7
    assert boot.candidate_tokens == 4
    assert boot.candidate_markets == 2


def test_iter_token_plans_paged_skips_unparseable_created_at():
    def pages():
        yield [("badtime", '["tok"]', None, False)]

    assert (
        list(
            odds_sync.iter_token_plans_paged(
                now_ts=1_900_000_000,
                clob_cutoff_date="2023-01-01",
                fidelity=1440,
                force=True,
                rebuild_history=True,
                overlap_minutes=0,
                skip_recent_minutes=0,
                market_page_size=10,
                iter_markets_with_tokens_fn=lambda **kwargs: pages(),
                get_token_sync_snapshot_fn=lambda token_ids, **kwargs: ({}, set(), {}),
            )
        )
        == []
    )


def test_iter_token_plans_paged_due_only_uses_due_iterator_and_scheduler_state():
    token_id = "d" * 33 + "12"
    called = {"due": 0, "full": 0}

    def due_pages(**kwargs):
        called["due"] += 1
        yield [("m1", token_id, "2024-01-01 00:00:00", False)]

    def full_pages(**kwargs):
        called["full"] += 1
        yield []

    plans = list(
        odds_sync.iter_token_plans_paged(
            now_ts=1_900_000_000,
            clob_cutoff_date="2023-01-01",
            fidelity=1440,
            force=False,
            rebuild_history=False,
            overlap_minutes=0,
            skip_recent_minutes=0,
            market_page_size=10,
            iter_due_market_tokens_fn=due_pages,
            iter_markets_with_tokens_fn=full_pages,
            count_due_market_token_exclusions_fn=lambda **kwargs: {
                "scope_skip": 0,
                "ended_market_skip": 0,
            },
            get_token_sync_snapshot_fn=lambda *args, **kwargs: (
                {token_id: 100},
                set(),
                {},
                {
                    token_id: odds_sync.TokenSyncSchedulerState(
                        empty_run_streak=2,
                    )
                },
            ),
        )
    )
    assert called == {"due": 1, "full": 0}
    assert len(plans) == 1
    assert plans[0].token_id == token_id
    assert plans[0].empty_run_streak == 2


def test_iter_token_plans_paged_due_only_skips_bad_rows():
    token_id = "e" * 33 + "12"

    def due_pages(**kwargs):
        yield [
            ("badtime", token_id, None, False),
            ("old", token_id, "2022-01-01 00:00:00", False),
            ("blank", "   ", "2024-01-01 00:00:00", False),
            ("good", token_id, "2024-01-01 00:00:00", False),
        ]

    gen = odds_sync.iter_token_plans_paged(
        now_ts=1_900_000_000,
        clob_cutoff_date="2023-01-01",
        fidelity=1440,
        force=False,
        rebuild_history=False,
        overlap_minutes=0,
        skip_recent_minutes=0,
        market_page_size=10,
        iter_due_market_tokens_fn=due_pages,
        iter_markets_with_tokens_fn=lambda **kwargs: iter(()),
        count_due_market_token_exclusions_fn=lambda **kwargs: {
            "scope_skip": 0,
            "ended_market_skip": 0,
        },
        get_token_sync_snapshot_fn=lambda *args, **kwargs: ({}, set(), {}, {}),
    )
    plans = []
    done_value = None
    while True:
        try:
            plans.append(next(gen))
        except StopIteration as done:
            done_value = done.value
            break
    planning_state, _ = done_value
    assert len(plans) == 1
    assert plans[0].market_id == "good"
    assert planning_state.pre_clob_markets == 1


def test_build_single_token_plan_history_backfill_floor():
    tok = "f" * 33 + "12"
    now_ts = 2_000_000_000
    floor_ts = now_ts - 45 * 86400
    plan, skip, _ = odds_sync.build_single_token_plan(
        token_id=tok,
        market_id="m",
        closed=False,
        created_ts=100,
        latest_timestamps={tok: now_ts - 3600},
        fully_checked_tokens=set(),
        persisted_skips={tok: "old"},
        seen_tokens=set(),
        now_ts=now_ts,
        fidelity=1,
        force=False,
        rebuild_history=False,
        overlap_seconds=60,
        recent_seconds=999999,
        history_backfill_floor_ts=floor_ts,
    )
    assert skip is None
    assert plan is not None
    assert plan.start_ts == floor_ts
    assert plan.end_ts == now_ts
    assert plan.fidelity == 1


def test_iter_token_plans_paged_history_backfill_uses_full_iterator():
    captured = {}

    def markets_pages(**kwargs):
        captured.update(kwargs)
        return iter(())

    gen = odds_sync.iter_token_plans_paged(
        now_ts=2_000_000_000,
        clob_cutoff_date="2023-01-01",
        fidelity=1,
        force=False,
        rebuild_history=False,
        overlap_minutes=0,
        skip_recent_minutes=0,
        market_page_size=10,
        history_backfill_days=45,
        min_volume=5_000.0,
        iter_due_market_tokens_fn=lambda **kwargs: iter(()),
        iter_markets_with_tokens_fn=markets_pages,
    )
    try:
        while True:
            next(gen)
    except StopIteration:
        pass
    assert captured.get("min_volume") == 5_000.0
    assert captured.get("json_array_only") is True


def test_empty_retry_next_check_supports_uncapped_and_capped_delay():
    checked_at = odds_sync.datetime(2024, 1, 1, tzinfo=odds_sync.timezone.utc)
    uncapped = odds_sync._empty_retry_next_check(
        checked_at,
        empty_run_streak=3,
        base_seconds=10,
        max_seconds=0,
    )
    capped = odds_sync._empty_retry_next_check(
        checked_at,
        empty_run_streak=3,
        base_seconds=10,
        max_seconds=20,
    )
    assert (uncapped - checked_at).total_seconds() == 40
    assert (capped - checked_at).total_seconds() == 20


def test_parse_cutoff_invalid():
    assert odds_sync._parse_cutoff_date("not-a-date").year == 2023


def test_build_single_token_plan_keys():
    now = 1_700_000_000
    tok = "t" * 33 + "12"
    seen = set()
    budgets = {tok: 1}
    plan, skip, inv = odds_sync.build_single_token_plan(
        token_id=tok,
        market_id="m",
        closed=False,
        created_ts=1_600_000_000,
        latest_timestamps={},
        fully_checked_tokens=set(),
        persisted_skips={},
        seen_tokens=seen,
        now_ts=now,
        fidelity=1440,
        force=False,
        rebuild_history=False,
        overlap_seconds=0,
        recent_seconds=0,
        empty_token_skip_budgets=budgets,
        empty_token_skip_runs=1,
    )
    assert skip == "empty_cache_skip"

    tok2 = "u" * 33 + "12"
    plan2, sk2, _ = odds_sync.build_single_token_plan(
        token_id=tok2,
        market_id="m",
        closed=True,
        created_ts=1_600_000_000,
        latest_timestamps={},
        fully_checked_tokens={tok2},
        persisted_skips={},
        seen_tokens=set(),
        now_ts=now,
        fidelity=1440,
        force=False,
        rebuild_history=False,
        overlap_seconds=0,
        recent_seconds=999999999,
        empty_token_skip_budgets=None,
        empty_token_skip_runs=0,
    )
    assert sk2 == "closed_done" or plan2 is None


def test_iter_token_plans_paged_uses_current_market_iterator_signature():
    seen = {}

    def iter_side(**kwargs):
        seen.update(kwargs)
        return iter(())

    gen = odds_sync.iter_token_plans_paged(
        now_ts=1_800_000_000,
        clob_cutoff_date="2024-01-01",
        fidelity=1440,
        force=True,
        rebuild_history=True,
        overlap_minutes=0,
        skip_recent_minutes=0,
        market_page_size=100,
        iter_markets_with_tokens_fn=iter_side,
        get_token_sync_snapshot_fn=lambda *a, **k: ({}, set(), {}),
    )
    assert list(gen) == []
    assert seen["json_array_only"] is True


def test_build_planning_context_uses_raw_snapshot():
    planning_state = odds_sync.PlanningState(plans=6, closed_done=2, recent_skip=1)
    context = odds_sync._build_planning_context(
        {
            "market_tokens_distinct_tokens": 10,
            "odds_history_distinct_tokens": 7,
            "token_odds_daily_distinct_tokens": 5,
            "ledger_distinct_tokens": 8,
            "ledger_fully_checked_tokens": 2,
            "token_sync_skips_distinct_tokens": 1,
            "market_tokens_without_history": 3,
            "history_tokens_without_market_tokens": 0,
            "token_sync_skips_by_reason": {"invalid token id format": 1},
        },
        planning_state,
        invalid_tokens=1,
    )
    assert context["planned_tokens"] == 6
    assert context["history_coverage_vs_market_tokens"] == 0.7
    assert context["token_sync_skips_by_reason"] == {"invalid token id format": 1}


def test_build_single_token_plan_all_skips():
    now = 1_800_000_000
    tok_dup = "d" * 33 + "12"
    seen = {tok_dup}
    _, sk, _ = odds_sync.build_single_token_plan(
        token_id=tok_dup,
        market_id="m",
        closed=False,
        created_ts=1,
        latest_timestamps={},
        fully_checked_tokens=set(),
        persisted_skips={},
        seen_tokens=seen,
        now_ts=now,
        fidelity=1440,
        force=False,
        rebuild_history=False,
        overlap_seconds=0,
        recent_seconds=0,
    )
    assert sk == "dup_token"

    bad = "short"
    _, sk2, inv = odds_sync.build_single_token_plan(
        token_id=bad,
        market_id="m",
        closed=False,
        created_ts=1,
        latest_timestamps={},
        fully_checked_tokens=set(),
        persisted_skips={},
        seen_tokens=set(),
        now_ts=now,
        fidelity=1440,
        force=False,
        rebuild_history=False,
        overlap_seconds=0,
        recent_seconds=0,
    )
    assert sk2 == "invalid_token" and inv

    tok = "e" * 33 + "12"
    _, sk3, _ = odds_sync.build_single_token_plan(
        token_id=tok,
        market_id="m",
        closed=False,
        created_ts=1,
        latest_timestamps={},
        fully_checked_tokens=set(),
        persisted_skips={tok: "x"},
        seen_tokens=set(),
        now_ts=now,
        fidelity=1440,
        force=False,
        rebuild_history=False,
        overlap_seconds=0,
        recent_seconds=0,
    )
    assert sk3 == "persisted_skip"


def test_iter_token_plans_paged_reconcile_and_invalid_batch():
    page = [
        (
            "mx",
            json.dumps(["f" * 33 + "12"]),
            "2024-06-01 00:00:00",
            False,
        )
    ]

    def iter_kw(**kwargs):
        yield page

    seen_batches = []

    def on_inv(batch):
        seen_batches.append(batch)

    gen = odds_sync.iter_token_plans_paged(
        now_ts=1_900_000_000,
        clob_cutoff_date="2020-01-01",
        fidelity=1440,
        force=True,
        rebuild_history=True,
        overlap_minutes=0,
        skip_recent_minutes=0,
        market_page_size=10,
        reconcile_ledger=True,
        on_invalid_tokens_batch=on_inv,
        iter_markets_with_tokens_fn=iter_kw,
        get_token_sync_snapshot_fn=lambda ids, **kw: ({}, set(), {}),
    )
    plans = list(gen)
    assert isinstance(plans, list)


def test_iter_token_plans_paged_allowlist_and_denylist_skip_tokens():
    tok_keep = "k" * 33 + "12"
    tok_skip = "s" * 33 + "12"
    page = [
        (
            "mx",
            json.dumps([tok_skip, tok_keep]),
            "2024-06-01 00:00:00",
            False,
        )
    ]

    def iter_pages(**_kwargs):
        yield page

    def sync_snapshot(_ids, **_kwargs):
        return {}, set(), {}

    common = {
        "now_ts": 1_900_000_000,
        "clob_cutoff_date": "2020-01-01",
        "fidelity": 1440,
        "force": True,
        "rebuild_history": False,
        "overlap_minutes": 0,
        "skip_recent_minutes": 0,
        "market_page_size": 10,
        "iter_markets_with_tokens_fn": iter_pages,
        "get_token_sync_snapshot_fn": sync_snapshot,
    }

    allowlisted = list(
        odds_sync.iter_token_plans_paged(
            **common,
            token_id_allowlist={tok_keep},
        )
    )
    denied = list(
        odds_sync.iter_token_plans_paged(
            **common,
            token_id_denylist={tok_skip},
        )
    )

    assert [plan.token_id for plan in allowlisted] == [tok_keep]
    assert [plan.token_id for plan in denied] == [tok_keep]


def test_iter_token_plans_paged_empty_tokens_list():
    def pages():
        yield [
            (
                "m1",
                "[]",
                "2024-06-01 00:00:00",
                False,
            ),
        ]

    gen = odds_sync.iter_token_plans_paged(
        now_ts=1_800_000_000,
        clob_cutoff_date="2020-01-01",
        fidelity=1440,
        force=True,
        rebuild_history=True,
        overlap_minutes=0,
        skip_recent_minutes=0,
        market_page_size=50,
        short_range_first=False,
        iter_markets_with_tokens_fn=lambda **k: pages(),
        get_token_sync_snapshot_fn=lambda *a, **k: ({}, set(), {}),
    )
    assert list(gen) == []


def test_iter_token_plans_paged_reconcile_short_first_off():
    tid = "b" * 33 + "12"

    def pages():
        yield [
            (
                "m1",
                json.dumps([tid]),
                "2024-06-01 00:00:00",
                False,
            ),
        ]

    seen = []

    def on_inv(batch):
        seen.extend(batch)

    gen = odds_sync.iter_token_plans_paged(
        now_ts=1_800_000_000,
        clob_cutoff_date="2020-01-01",
        fidelity=1440,
        force=True,
        rebuild_history=True,
        overlap_minutes=0,
        skip_recent_minutes=0,
        market_page_size=50,
        reconcile_ledger=True,
        short_range_first=False,
        on_invalid_tokens_batch=on_inv,
        iter_markets_with_tokens_fn=lambda **k: pages(),
        get_token_sync_snapshot_fn=lambda *a, **k: ({tid: 1}, set(), {}),
    )
    plans = list(gen)
    assert plans


def test_iter_token_plans_paged_accepts_prebuilt_options(monkeypatch):
    from oddsfox_pipeline.ingestion.polymarket.odds.support import OddsSyncOptions

    captured: dict = {}
    options = OddsSyncOptions(force=True, rebuild_history=True)

    def fake_paged(*_args, **kwargs):
        captured.update(kwargs)
        return iter(())

    monkeypatch.setattr(odds_sync._planning_mod, "iter_token_plans_paged", fake_paged)
    list(odds_sync.iter_token_plans_paged(options=options))

    assert captured["options"] is options


def test_parse_created_at_preserves_all_supported_timestamp_semantics():
    naive = datetime(2026, 7, 18, 17, 1, 2)
    aware = datetime(
        2026,
        7,
        18,
        19,
        1,
        2,
        tzinfo=timezone(timedelta(hours=2)),
    )
    expected = datetime(2026, 7, 18, 17, 1, 2, tzinfo=timezone.utc)

    assert odds_sync._parse_created_at(None) is None
    assert odds_sync._parse_created_at(naive) == expected
    parsed_aware = odds_sync._parse_created_at(aware)
    assert parsed_aware == expected
    assert parsed_aware.tzinfo is timezone.utc
    assert odds_sync._parse_created_at("2026-07-18T17:01:02") == expected
    assert odds_sync._parse_created_at("2026-07-18 17:01:02.987654") == expected


def test_parse_cutoff_date_has_exact_fallback_and_operational_log(caplog):
    assert odds_sync._parse_cutoff_date("2026-07-18") == datetime(
        2026, 7, 18, tzinfo=timezone.utc
    )

    with caplog.at_level("ERROR"):
        fallback = odds_sync._parse_cutoff_date("bad-date")

    assert fallback == datetime(2023, 1, 1, tzinfo=timezone.utc)
    assert caplog.messages == ["Invalid clob_cutoff_date 'bad-date'; using 2023-01-01"]


def test_build_single_token_plan_boundary_contract_is_exact():
    token = "z" * 33 + "12"
    plan, skip, invalid = odds_sync.build_single_token_plan(
        token_id=token,
        market_id="market",
        closed=False,
        created_ts=100,
        latest_timestamps={token: 150},
        fully_checked_tokens={token},
        persisted_skips={},
        seen_tokens=set(),
        now_ts=200,
        fidelity=60,
        force=False,
        rebuild_history=False,
        overlap_seconds=10,
        recent_seconds=50,
        empty_token_skip_budgets={token: 0},
        empty_token_skip_runs=1,
    )

    assert skip is None
    assert invalid is None
    assert plan == odds_sync.TokenPlan(
        token_id=token,
        market_id="market",
        is_closed=False,
        created_at_ts=100,
        start_ts=140,
        end_ts=200,
        fidelity=60,
        empty_run_streak=0,
    )

    at_now, at_now_skip, _ = odds_sync.build_single_token_plan(
        token_id="y" * 33 + "12",
        market_id="market",
        closed=False,
        created_ts=200,
        latest_timestamps={},
        fully_checked_tokens=set(),
        persisted_skips={},
        seen_tokens=set(),
        now_ts=200,
        fidelity=60,
        force=True,
        rebuild_history=False,
        overlap_seconds=0,
        recent_seconds=0,
    )
    assert at_now is None
    assert at_now_skip == "already_current"


def test_empty_skip_budget_removes_only_the_exhausted_token():
    token = "x" * 33 + "12"
    other = "w" * 33 + "12"
    budgets = {token: 1, other: 4}

    plan, skip, invalid = odds_sync.build_single_token_plan(
        token_id=token,
        market_id="market",
        closed=False,
        created_ts=100,
        latest_timestamps={},
        fully_checked_tokens=set(),
        persisted_skips={},
        seen_tokens=set(),
        now_ts=200,
        fidelity=60,
        force=False,
        rebuild_history=False,
        overlap_seconds=0,
        recent_seconds=0,
        empty_token_skip_budgets=budgets,
        empty_token_skip_runs=1,
    )

    assert (plan, skip, invalid) == (None, "empty_cache_skip", None)
    assert budgets == {other: 4}

    no_skip, no_skip_reason, _ = odds_sync.build_single_token_plan(
        token_id=other,
        market_id="market",
        closed=False,
        created_ts=100,
        latest_timestamps={},
        fully_checked_tokens=set(),
        persisted_skips={},
        seen_tokens=set(),
        now_ts=200,
        fidelity=60,
        force=False,
        rebuild_history=False,
        overlap_seconds=0,
        recent_seconds=0,
        empty_token_skip_budgets={other: 1},
        empty_token_skip_runs=0,
    )
    assert no_skip_reason is None
    assert no_skip is not None


def test_force_and_backfill_bypass_only_routine_skips():
    token = "v" * 33 + "12"
    common = {
        "token_id": token,
        "market_id": "market",
        "closed": True,
        "created_ts": 100,
        "latest_timestamps": {},
        "fully_checked_tokens": {token},
        "persisted_skips": {token: "previous"},
        "now_ts": 200,
        "fidelity": 60,
        "overlap_seconds": 0,
        "recent_seconds": 0,
        "empty_token_skip_budgets": {token: 2},
        "empty_token_skip_runs": 1,
    }

    forced, forced_skip, _ = odds_sync.build_single_token_plan(
        **common,
        seen_tokens=set(),
        force=True,
        rebuild_history=False,
    )
    assert forced is None
    assert forced_skip == "closed_done"

    rebuilt, rebuilt_skip, _ = odds_sync.build_single_token_plan(
        **common,
        seen_tokens=set(),
        force=False,
        rebuild_history=True,
    )
    assert rebuilt_skip is None
    assert rebuilt is not None
    assert rebuilt.start_ts == 100


def _collect_plans_and_result(iterator):
    plans = []
    while True:
        try:
            plans.append(next(iterator))
        except StopIteration as done:
            return plans, done.value


def test_paged_planning_forwards_every_full_scan_option(monkeypatch):
    planning = odds_sync._planning_mod
    now_ts = 1_800_000_000
    tokens = [_valid_token(1001), _valid_token(1002), _valid_token(1003)]
    budgets = {tokens[0]: 4}
    iterator_calls = []
    snapshot_calls = []
    build_calls = []

    def pages(**kwargs):
        iterator_calls.append(kwargs)
        yield [
            (
                "market-1",
                json.dumps(tokens[:2]),
                "2024-01-01 00:00:00",
                True,
            )
        ]
        yield [
            (
                "market-2",
                json.dumps(tokens[2:]),
                "2024-01-02 00:00:00",
                False,
            )
        ]

    def snapshot(token_ids, **kwargs):
        snapshot_calls.append((token_ids, kwargs))
        return (
            {},
            set(),
            {},
            {
                token: odds_sync.TokenSyncSchedulerState(empty_run_streak=index + 2)
                for index, token in enumerate(tokens)
            },
        )

    def build(**kwargs):
        build_calls.append(kwargs)
        index = tokens.index(kwargs["token_id"])
        return (
            TokenPlan(
                token_id=kwargs["token_id"],
                market_id=kwargs["market_id"],
                is_closed=kwargs["closed"],
                created_at_ts=kwargs["created_ts"],
                start_ts=100 + index * 10,
                end_ts=200,
                fidelity=kwargs["fidelity"],
                empty_run_streak=kwargs["empty_run_streak"],
            ),
            None,
            None,
        )

    monkeypatch.setattr(planning, "build_single_token_plan", build)
    options = OddsSyncOptions(
        clob_cutoff_date="2024-01-01",
        fidelity=17,
        force=True,
        overlap_minutes=2,
        skip_recent_minutes=3,
        market_page_size=23,
        reconcile_ledger=True,
        short_range_first=False,
        market_scope="scope-a",
        ended_market_grace_days=4,
        min_volume=5.5,
        max_volume=99.5,
        history_backfill_days=1,
        empty_token_skip_budgets=budgets,
        empty_token_skip_runs=7,
    )

    plans, (state, invalid) = _collect_plans_and_result(
        planning.iter_token_plans_paged(
            now_ts=now_ts,
            options=options,
            iter_markets_with_tokens_fn=pages,
            get_token_sync_snapshot_fn=snapshot,
        )
    )

    assert [plan.token_id for plan in plans] == tokens
    assert state.plans == 3
    assert state.pre_clob_markets == 0
    assert invalid == {}
    assert iterator_calls == [
        {
            "page_size": 23,
            "cutoff_created_at": "2024-01-01 00:00:00",
            "json_array_only": True,
            "market_scope": "scope-a",
            "ended_market_grace_days": 4,
            "min_volume": 5.5,
            "max_volume": 99.5,
        }
    ]
    assert len(snapshot_calls) == 2
    assert [set(call[0]) for call in snapshot_calls] == [
        set(tokens[:2]),
        {tokens[2]},
    ]
    assert [call[1] for call in snapshot_calls] == [
        {
            "reconcile_with_history": True,
            "repair_ledger": True,
            "include_scheduler_state": True,
        },
        {
            "reconcile_with_history": True,
            "repair_ledger": True,
            "include_scheduler_state": True,
        },
    ]
    assert len(build_calls) == 3
    for index, call in enumerate(build_calls):
        assert call["token_id"] == tokens[index]
        assert call["market_id"] == ("market-1" if index < 2 else "market-2")
        assert call["closed"] is (index < 2)
        assert call["now_ts"] == now_ts
        assert call["fidelity"] == 17
        assert call["force"] is True
        assert call["rebuild_history"] is False
        assert call["overlap_seconds"] == 120
        assert call["recent_seconds"] == 180
        assert call["history_backfill_floor_ts"] == now_ts - 86400
        assert call["empty_run_streak"] == index + 2
        assert call["empty_token_skip_budgets"] is budgets
        assert call["empty_token_skip_runs"] == 7


def test_paged_planning_preserves_zero_overlap_and_recent_windows(monkeypatch):
    planning = odds_sync._planning_mod
    token = _valid_token(1500)
    calls = []

    def build(**kwargs):
        calls.append(kwargs)
        return None, None, None

    monkeypatch.setattr(planning, "build_single_token_plan", build)
    plans, _ = _collect_plans_and_result(
        planning.iter_token_plans_paged(
            now_ts=1_800_000_000,
            options=OddsSyncOptions(
                clob_cutoff_date="2024-01-01",
                force=True,
                overlap_minutes=0,
                skip_recent_minutes=0,
            ),
            iter_markets_with_tokens_fn=lambda **kwargs: iter(
                [
                    [
                        (
                            "market",
                            json.dumps([token]),
                            "2024-01-01 00:00:00",
                            False,
                        )
                    ]
                ]
            ),
            get_token_sync_snapshot_fn=lambda token_ids, **kwargs: ({}, set(), {}),
        )
    )

    assert plans == []
    assert len(calls) == 1
    assert calls[0]["overlap_seconds"] == 0
    assert calls[0]["recent_seconds"] == 0


@pytest.mark.parametrize(
    ("exclusion_counts", "expected_scope", "expected_ended"),
    [
        ({"scope_skip": 2, "ended_market_skip": 3}, 2, 3),
        ({}, 0, 0),
    ],
)
def test_paged_planning_due_scan_counts_and_boundaries_are_exact(
    exclusion_counts,
    expected_scope,
    expected_ended,
):
    planning = odds_sync._planning_mod
    token = _valid_token(2001)
    exclusion_calls = []
    due_calls = []
    snapshot_calls = []

    def count_exclusions(**kwargs):
        exclusion_calls.append(kwargs)
        return exclusion_counts

    def due_pages(**kwargs):
        due_calls.append(kwargs)
        yield [
            ("old-1", token, "2023-12-31 23:59:58", False),
            ("old-2", token, "2023-12-31 23:59:59", False),
            ("boundary", token, "2024-01-01 00:00:00", True),
        ]

    def snapshot(token_ids, **kwargs):
        snapshot_calls.append((token_ids, kwargs))
        return {}, set(), {}, {}

    plans, (state, invalid) = _collect_plans_and_result(
        planning.iter_token_plans_paged(
            now_ts=1_800_000_000,
            options=OddsSyncOptions(
                clob_cutoff_date="2024-01-01",
                market_page_size=31,
                market_scope="scope-b",
                ended_market_grace_days=6,
                min_volume=7.5,
                max_volume=88.5,
                short_range_first=False,
            ),
            iter_due_market_tokens_fn=due_pages,
            count_due_market_token_exclusions_fn=count_exclusions,
            get_token_sync_snapshot_fn=snapshot,
        )
    )

    assert [plan.market_id for plan in plans] == ["boundary"]
    assert plans[0].is_closed is True
    assert state.pre_clob_markets == 2
    assert state.scope_skip == expected_scope
    assert state.ended_market_skip == expected_ended
    assert invalid == {}
    expected_args = {
        "cutoff_created_at": "2024-01-01 00:00:00",
        "market_scope": "scope-b",
        "ended_market_grace_days": 6,
        "min_volume": 7.5,
        "max_volume": 88.5,
    }
    assert exclusion_calls == [expected_args]
    assert due_calls == [{"page_size": 31, **expected_args}]
    assert snapshot_calls == [
        (
            [token],
            {"include_scheduler_state": True},
        )
    ]


def test_paged_planning_rejects_non_list_tokens_and_avoids_empty_callback(
    monkeypatch,
):
    planning = odds_sync._planning_mod
    token = _valid_token(3001)
    build_calls = []
    callback_calls = []

    def pages(**kwargs):
        yield [
            ("empty", json.dumps([]), "2024-01-01 00:00:00", False),
        ]
        yield [
            ("bad-time", json.dumps([token]), None, False),
            ("old-1", json.dumps([token]), "2023-12-31 23:59:58", False),
            ("old-2", json.dumps([token]), "2023-12-31 23:59:59", False),
            ("mapping", json.dumps({"token": token}), "2024-01-01 00:00:00", False),
            ("valid", json.dumps([token]), "2024-01-01 00:00:00", False),
        ]

    original_build = planning.build_single_token_plan

    def build(**kwargs):
        build_calls.append(kwargs["token_id"])
        return original_build(**kwargs)

    monkeypatch.setattr(planning, "build_single_token_plan", build)
    plans, (state, invalid) = _collect_plans_and_result(
        planning.iter_token_plans_paged(
            now_ts=1_800_000_000,
            options=OddsSyncOptions(
                clob_cutoff_date="2024-01-01",
                force=True,
                short_range_first=False,
            ),
            on_invalid_tokens_batch=callback_calls.append,
            iter_markets_with_tokens_fn=pages,
            get_token_sync_snapshot_fn=lambda token_ids, **kwargs: ({}, set(), {}),
        )
    )

    assert [plan.market_id for plan in plans] == ["valid"]
    assert build_calls == [token]
    assert callback_calls == []
    assert state.plans == 1
    assert state.pre_clob_markets == 2
    assert invalid == {}


def test_paged_planning_scope_counts_and_sort_order_are_exact(monkeypatch):
    planning = odds_sync._planning_mod
    tokens = [_valid_token(4001), _valid_token(4002), _valid_token(4003)]
    windows = {
        tokens[0]: (50, 100, 20),
        tokens[1]: (80, 100, 10),
        tokens[2]: (80, 100, 5),
    }

    def pages(**kwargs):
        yield [
            ("market", json.dumps(tokens), "2024-01-01 00:00:00", False),
        ]

    def build(**kwargs):
        start, end, created = windows[kwargs["token_id"]]
        return (
            TokenPlan(
                token_id=kwargs["token_id"],
                market_id=kwargs["market_id"],
                is_closed=False,
                created_at_ts=created,
                start_ts=start,
                end_ts=end,
                fidelity=1,
            ),
            None,
            None,
        )

    monkeypatch.setattr(planning, "build_single_token_plan", build)
    common = {
        "now_ts": 1_800_000_000,
        "iter_markets_with_tokens_fn": pages,
        "get_token_sync_snapshot_fn": lambda token_ids, **kwargs: ({}, set(), {}),
    }

    sorted_plans, (sorted_state, _) = _collect_plans_and_result(
        planning.iter_token_plans_paged(
            **common,
            options=OddsSyncOptions(
                clob_cutoff_date="2024-01-01",
                force=True,
                short_range_first=True,
            ),
        )
    )
    assert [plan.token_id for plan in sorted_plans] == [
        tokens[2],
        tokens[1],
        tokens[0],
    ]
    assert sorted_state.plans == 3

    filtered_plans, (filtered_state, _) = _collect_plans_and_result(
        planning.iter_token_plans_paged(
            **common,
            options=OddsSyncOptions(
                clob_cutoff_date="2024-01-01",
                force=True,
                short_range_first=False,
            ),
            token_id_allowlist={tokens[0]},
        )
    )
    assert [plan.token_id for plan in filtered_plans] == [tokens[0]]
    assert filtered_state.plans == 1
    assert filtered_state.scope_skip == 2

    denied_plans, (denied_state, _) = _collect_plans_and_result(
        planning.iter_token_plans_paged(
            **common,
            options=OddsSyncOptions(
                clob_cutoff_date="2024-01-01",
                force=True,
                short_range_first=False,
            ),
            token_id_denylist={tokens[1], tokens[2]},
        )
    )
    assert [plan.token_id for plan in denied_plans] == [tokens[0]]
    assert denied_state.plans == 1
    assert denied_state.scope_skip == 2
