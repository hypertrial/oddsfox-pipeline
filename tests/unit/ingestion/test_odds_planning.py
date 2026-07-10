"""Unit tests for Polymarket odds sync planning."""

from __future__ import annotations

import json

import pytest
from hypothesis import given
from hypothesis import strategies as st

pytest.importorskip("duckdb")

from oddsfox_pipeline.ingestion.polymarket.odds import sync as odds_sync
from oddsfox_pipeline.ingestion.polymarket.odds.engine.bootstrap import (
    bootstrap_planning,
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
