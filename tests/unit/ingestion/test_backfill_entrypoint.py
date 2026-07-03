"""Unit tests for markets/backfill backfill entrypoint."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from tests.unit.ingestion.backfill_test_support import (
    bf_end_dates,
    bf_event_slugs,
    bf_events_fallback,
    bf_gamma,
    bf_slugs,
    bf_tokens,
)

from oddsfox_pipeline.ingestion.polymarket.markets import backfill as bf


@pytest.fixture
def no_sleep_tqdm(monkeypatch):
    monkeypatch.setattr(
        bf,
        "tqdm",
        lambda *a, **k: MagicMock(__enter__=lambda s: s, __exit__=lambda *x: None),
    )
    monkeypatch.setattr(bf_gamma.time, "sleep", lambda s: None)
    monkeypatch.setattr(bf_events_fallback.time, "sleep", lambda s: None)


def test_backfill_tokens_early_and_empty(monkeypatch):
    monkeypatch.setattr(bf_tokens, "get_backfill_fully_checked", lambda t: True)
    bf.backfill_tokens(force=False)

    monkeypatch.setattr(bf_tokens, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_tokens, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(bf_tokens, "get_markets_without_tokens", lambda limit=None: [])
    monkeypatch.setattr(bf_tokens, "set_backfill_fully_checked", lambda *a: None)
    bf.backfill_tokens(max_markets=None, force=True)


def test_backfill_slugs_early(monkeypatch):
    monkeypatch.setattr(bf_slugs, "get_backfill_fully_checked", lambda t: True)
    bf.backfill_slugs(force=False)


def test_backfill_end_dates_early(monkeypatch):
    monkeypatch.setattr(bf_end_dates, "get_backfill_fully_checked", lambda t: True)
    bf.backfill_end_dates(force=False)


def test_backfill_event_slugs_early_and_progress(monkeypatch, no_sleep_tqdm):
    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: True)
    bf.backfill_event_slugs(force=False)

    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", lambda limit=None: []
    )
    monkeypatch.setattr(bf_event_slugs, "set_backfill_fully_checked", lambda *a: None)
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a: None)
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 0)
    bf.backfill_event_slugs(max_markets=None, force=True)

    gm_calls = []

    def get_event_slugs_twice(limit=None):
        gm_calls.append(limit)
        if len(gm_calls) == 1:
            return ["1", "2"]
        return []

    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", get_event_slugs_twice
    )
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 5)

    def proc(**kw):
        for mid in kw["market_ids"]:
            kw["on_record_saved"](mid)
        return (2, 2, 1)

    monkeypatch.setattr(bf_event_slugs, "_process_market_chunks", proc)
    monkeypatch.setattr(
        bf_event_slugs,
        "_fill_from_events_endpoint",
        lambda *a, **k: (
            0,
            {
                "events_fallback_pages": 0,
                "events_fallback_truncated": False,
                "events_fallback_remaining_ids": 0,
            },
        ),
    )
    monkeypatch.setattr(bf_gamma, "APIClient", lambda *a, **k: MagicMock())
    bf.backfill_event_slugs(max_markets=None, force=False)


def test_backfill_event_slugs_negative_progress_clamped(monkeypatch, no_sleep_tqdm):
    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    gm_calls = []

    def get_event_slugs_twice(limit=None):
        gm_calls.append(limit)
        if len(gm_calls) == 1:
            return ["1"]
        return []

    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", get_event_slugs_twice
    )
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: -5)

    def proc(**kw):
        for mid in kw["market_ids"]:
            kw["on_record_saved"](mid)
        return (1, 1, 1)

    monkeypatch.setattr(bf_event_slugs, "_process_market_chunks", proc)
    monkeypatch.setattr(
        bf_event_slugs,
        "_fill_from_events_endpoint",
        lambda *a, **k: (
            0,
            {
                "events_fallback_pages": 0,
                "events_fallback_truncated": False,
                "events_fallback_remaining_ids": 0,
            },
        ),
    )
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a: None)
    monkeypatch.setattr(bf_event_slugs, "set_backfill_fully_checked", lambda *a: None)
    monkeypatch.setattr(bf_gamma, "APIClient", lambda *a, **k: MagicMock())
    bf.backfill_event_slugs(max_markets=None, force=False)


def test_backfill_tokens_with_markets_finally(no_sleep_tqdm, monkeypatch):
    monkeypatch.setattr(bf_tokens, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_tokens, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_tokens, "get_markets_without_tokens", lambda limit=None: ["10"]
    )
    monkeypatch.setattr(bf_tokens, "set_backfill_fully_checked", lambda *a: None)

    client = MagicMock()
    client.get.return_value = [{"id": "10", "clobTokenIds": ["z"]}]
    with patch.object(bf_gamma, "APIClient", lambda *a, **k: client):
        with patch.object(bf_tokens, "_process_market_chunks", return_value=(1, 1, 1)):
            bf.backfill_tokens(batch_size=1, max_markets=1, force=True)


def test_backfill_slugs_run_finally(no_sleep_tqdm, monkeypatch):
    monkeypatch.setattr(bf_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_slugs, "get_markets_without_slugs", lambda limit=None: ["20"]
    )
    monkeypatch.setattr(bf_slugs, "set_backfill_fully_checked", lambda *a: None)
    client = MagicMock()
    client.get.return_value = [{"id": "20", "slug": "s"}]
    with patch.object(bf_gamma, "APIClient", lambda *a, **k: client):
        with patch.object(bf_slugs, "_process_market_chunks", return_value=(1, 1, 1)):
            bf.backfill_slugs(batch_size=1, max_markets=1, force=True)


def test_backfill_end_dates_run(no_sleep_tqdm, monkeypatch):
    monkeypatch.setattr(bf_end_dates, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_end_dates, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_end_dates, "get_markets_without_end_date", lambda limit=None: ["30"]
    )
    monkeypatch.setattr(bf_end_dates, "set_backfill_fully_checked", lambda *a: None)
    client = MagicMock()
    client.get.return_value = [{"id": "30", "endDate": "2025-01-01"}]
    with patch.object(bf_gamma, "APIClient", lambda *a, **k: client):
        with patch.object(
            bf_end_dates, "_process_market_chunks", lambda **kw: (1, 1, 1)
        ):
            bf.backfill_end_dates(batch_size=1, max_markets=1, force=True)


def test_backfill_end_dates_max_markets_flag(monkeypatch, no_sleep_tqdm):
    monkeypatch.setattr(bf_end_dates, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_end_dates, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_end_dates, "get_markets_without_end_date", lambda limit=None: ["40"]
    )
    monkeypatch.setattr(bf_end_dates, "set_backfill_fully_checked", lambda *a: None)
    with patch.object(bf_gamma, "APIClient", MagicMock):
        with patch.object(
            bf_end_dates, "_process_market_chunks", lambda **kw: (1, 0, 1)
        ):
            bf.backfill_end_dates(batch_size=1, max_markets=5, force=True)


def test_backfill_empty_partial_runs_do_not_mark_complete(monkeypatch):
    flags = []

    monkeypatch.setattr(bf_tokens, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_tokens, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_tokens,
        "set_backfill_fully_checked",
        lambda task, val: flags.append((task, val)),
    )
    monkeypatch.setattr(bf_tokens, "get_markets_without_tokens", lambda limit=None: [])
    bf.backfill_tokens(max_markets=1, force=True)

    monkeypatch.setattr(bf_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_slugs,
        "set_backfill_fully_checked",
        lambda task, val: flags.append((task, val)),
    )
    monkeypatch.setattr(bf_slugs, "get_markets_without_slugs", lambda limit=None: [])
    bf.backfill_slugs(max_markets=1, force=True)

    monkeypatch.setattr(bf_end_dates, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_end_dates, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_end_dates,
        "set_backfill_fully_checked",
        lambda task, val: flags.append((task, val)),
    )
    monkeypatch.setattr(
        bf_end_dates, "get_markets_without_end_date", lambda limit=None: []
    )
    bf.backfill_end_dates(max_markets=1, force=True)

    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_event_slugs,
        "set_backfill_fully_checked",
        lambda task, val: flags.append((task, val)),
    )
    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", lambda limit=None: []
    )
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a: None)
    bf.backfill_event_slugs(max_markets=1, force=True)

    assert flags == []


def test_backfill_event_slugs_stale_ledger_does_not_skip_work(
    monkeypatch, no_sleep_tqdm
):
    """Large saved progress must not skip unresolved rows (no list slicing resume)."""
    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    gm_calls = []

    def get_event_slugs_twice(limit=None):
        gm_calls.append(limit)
        if len(gm_calls) == 1:
            return ["a", "b"]
        return []

    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", get_event_slugs_twice
    )
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 2)
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a: None)
    monkeypatch.setattr(bf_event_slugs, "set_backfill_fully_checked", lambda *a: None)
    chunks = {"n": 0}

    def proc(**kw):
        chunks["n"] += 1
        assert kw["market_ids"] == ["a", "b"]
        for mid in kw["market_ids"]:
            kw["on_record_saved"](mid)
        return (2, 2, 1)

    monkeypatch.setattr(bf_event_slugs, "_process_market_chunks", proc)

    def boom(*a, **k):
        raise RuntimeError("fallback should not run when all resolved")

    monkeypatch.setattr(bf_event_slugs, "_fill_from_events_endpoint", boom)
    monkeypatch.setattr(bf_gamma, "APIClient", lambda *a, **k: MagicMock())
    bf.backfill_event_slugs(max_markets=None, force=False)
    assert chunks["n"] == 1


def test_backfill_event_slugs_fallback_finally(no_sleep_tqdm, monkeypatch):
    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 0)
    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", lambda limit=None: ["50"]
    )
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a: None)
    monkeypatch.setattr(bf_event_slugs, "set_backfill_fully_checked", lambda *a: None)

    client = MagicMock()

    with patch.object(bf_gamma, "APIClient", lambda *a, **k: client):
        with patch.object(
            bf_event_slugs, "_process_market_chunks", return_value=(1, 0, 1)
        ):
            with patch.object(
                bf_event_slugs,
                "_fill_from_events_endpoint",
                return_value=(
                    0,
                    {
                        "events_fallback_pages": 0,
                        "events_fallback_truncated": False,
                        "events_fallback_remaining_ids": 0,
                    },
                ),
            ):
                bf.backfill_event_slugs(batch_size=1, max_markets=1, force=True)


def test_backfill_event_slugs_without_fallback_when_remaining_empty(
    monkeypatch, no_sleep_tqdm
):
    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 0)
    gm_calls = []

    def get_event_slugs_twice(limit=None):
        gm_calls.append(limit)
        if len(gm_calls) == 1:
            return ["50"]
        return []

    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", get_event_slugs_twice
    )
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a: None)
    monkeypatch.setattr(bf_event_slugs, "set_backfill_fully_checked", lambda *a: None)
    client = MagicMock()
    with patch.object(bf_gamma, "APIClient", lambda *a, **k: client):
        with patch.object(
            bf_event_slugs,
            "_process_market_chunks",
            side_effect=lambda **kw: (kw["on_record_saved"]("50"), (1, 1, 1))[1],
        ):
            with patch.object(
                bf_event_slugs,
                "_fill_from_events_endpoint",
                side_effect=RuntimeError("should not run"),
            ):
                bf.backfill_event_slugs(batch_size=1, max_markets=None, force=False)


def test_backfill_event_slugs_incomplete_full_run_updates_progress(
    monkeypatch, no_sleep_tqdm
):
    progress = []
    flags = []
    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 0)
    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", lambda limit=None: ["1", "2"]
    )
    monkeypatch.setattr(
        bf_event_slugs, "_process_market_chunks", lambda **kw: (1, 1, 1)
    )
    monkeypatch.setattr(
        bf_event_slugs,
        "_fill_from_events_endpoint",
        lambda *a, **k: (
            0,
            {
                "events_fallback_pages": 0,
                "events_fallback_truncated": False,
                "events_fallback_remaining_ids": 2,
            },
        ),
    )
    monkeypatch.setattr(
        bf_event_slugs,
        "set_backfill_progress",
        lambda task, value: progress.append((task, value)),
    )
    monkeypatch.setattr(
        bf_event_slugs,
        "set_backfill_fully_checked",
        lambda task, value: flags.append((task, value)),
    )
    monkeypatch.setattr(bf_gamma, "APIClient", lambda *a, **k: MagicMock())
    bf.backfill_event_slugs(batch_size=1, max_markets=None, force=False)
    assert progress[-1] == ("event_slugs", 1)
    assert flags[-1] == ("event_slugs", False)


def test_backfill_event_slugs_truncated_fallback_never_marks_fully_checked(
    monkeypatch, no_sleep_tqdm
):
    flags = []
    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 0)
    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", lambda limit=None: ["99"]
    )
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a, **k: None)
    monkeypatch.setattr(
        bf_event_slugs,
        "set_backfill_fully_checked",
        lambda task, value: flags.append((task, value)),
    )
    with patch.object(bf_gamma, "APIClient", lambda *a, **k: MagicMock()):
        with patch.object(
            bf_event_slugs, "_process_market_chunks", return_value=(1, 0, 1)
        ):
            with patch.object(
                bf_event_slugs,
                "_fill_from_events_endpoint",
                return_value=(
                    0,
                    {
                        "events_fallback_pages": 1,
                        "events_fallback_truncated": True,
                        "events_fallback_remaining_ids": 1,
                    },
                ),
            ):
                out = bf.backfill_event_slugs(
                    batch_size=1, max_markets=None, force=False
                )
    assert out["events_fallback_truncated"] is True
    assert flags[-1] == ("event_slugs", False)


def test_backfill_event_slugs_force_true_processes_full_unresolved_list(
    monkeypatch, no_sleep_tqdm
):
    seen = []
    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 999)
    gm_calls = []

    def gm(limit=None):
        gm_calls.append(limit)
        if len(gm_calls) == 1:
            return ["10", "20"]
        return []

    monkeypatch.setattr(bf_event_slugs, "get_markets_without_event_slugs", gm)

    def proc(**kw):
        seen.append(list(kw["market_ids"]))
        for mid in kw["market_ids"]:
            kw["on_record_saved"](mid)
        return (2, 2, 1)

    monkeypatch.setattr(bf_event_slugs, "_process_market_chunks", proc)
    monkeypatch.setattr(
        bf_event_slugs,
        "_fill_from_events_endpoint",
        lambda *a, **k: (
            0,
            {
                "events_fallback_pages": 0,
                "events_fallback_truncated": False,
                "events_fallback_remaining_ids": 0,
            },
        ),
    )
    monkeypatch.setattr(bf_gamma, "APIClient", lambda *a, **k: MagicMock())
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a, **k: None)
    monkeypatch.setattr(
        bf_event_slugs, "set_backfill_fully_checked", lambda *a, **k: None
    )
    bf.backfill_event_slugs(max_markets=None, force=True)
    assert seen == [["10", "20"]]


def test_backfill_event_slugs_db_mismatch_after_run_warns_and_skips_fully_checked(
    monkeypatch, no_sleep_tqdm, caplog
):
    import logging

    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 0)
    gm_calls = []

    def gm(limit=None):
        gm_calls.append(limit)
        if len(gm_calls) == 1:
            return ["7"]
        if len(gm_calls) == 2:
            return ["7"]
        return []

    monkeypatch.setattr(bf_event_slugs, "get_markets_without_event_slugs", gm)

    def proc(**kw):
        kw["on_record_saved"]("7")
        return (1, 1, 1)

    monkeypatch.setattr(bf_event_slugs, "_process_market_chunks", proc)
    monkeypatch.setattr(
        bf_event_slugs,
        "_fill_from_events_endpoint",
        lambda *a, **k: (
            0,
            {
                "events_fallback_pages": 0,
                "events_fallback_truncated": False,
                "events_fallback_remaining_ids": 0,
            },
        ),
    )
    flags = []
    monkeypatch.setattr(bf_gamma, "APIClient", lambda *a, **k: MagicMock())
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a, **k: None)
    monkeypatch.setattr(
        bf_event_slugs,
        "set_backfill_fully_checked",
        lambda task, value: flags.append((task, value)),
    )
    caplog.set_level(logging.WARNING)
    bf.backfill_event_slugs(max_markets=None, force=False)
    assert flags[-1] == ("event_slugs", False)
    assert any(
        "DuckDB still has markets without event_slug" in r.message
        for r in caplog.records
    )


def test_backfill_end_dates_partial_run_marks_false(monkeypatch, no_sleep_tqdm):
    flags = []
    monkeypatch.setattr(bf_end_dates, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_end_dates, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_end_dates, "get_markets_without_end_date", lambda limit=None: ["40"]
    )
    monkeypatch.setattr(
        bf_end_dates,
        "set_backfill_fully_checked",
        lambda task, value: flags.append((task, value)),
    )
    with patch.object(bf_gamma, "APIClient", MagicMock):
        with patch.object(
            bf_end_dates, "_process_market_chunks", lambda **kw: (1, 1, 1)
        ):
            bf.backfill_end_dates(batch_size=1, max_markets=5, force=True)
    assert flags[-1] == ("end_dates", False)


def test_backfill_end_dates_full_run_marks_completion(monkeypatch, no_sleep_tqdm):
    flags = []
    monkeypatch.setattr(bf_end_dates, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_end_dates, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_end_dates, "get_markets_without_end_date", lambda limit=None: ["40"]
    )
    monkeypatch.setattr(
        bf_end_dates,
        "set_backfill_fully_checked",
        lambda task, value: flags.append((task, value)),
    )
    with patch.object(bf_gamma, "APIClient", MagicMock):
        with patch.object(
            bf_end_dates, "_process_market_chunks", lambda **kw: (1, 1, 1)
        ):
            bf.backfill_end_dates(batch_size=1, max_markets=None, force=True)
    assert flags[-1] == ("end_dates", True)


def test_backfill_tokens_progress_start_complete(no_sleep_tqdm, monkeypatch):
    calls = []
    monkeypatch.setattr(bf_tokens, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_tokens, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_tokens, "get_markets_without_tokens", lambda limit=None: ["1"]
    )
    monkeypatch.setattr(bf_tokens, "set_backfill_fully_checked", lambda *a: None)
    client = MagicMock()
    client.get.return_value = [{"id": "1", "clobTokenIds": ["z"]}]
    with patch.object(bf_gamma, "APIClient", lambda *a, **k: client):
        bf.backfill_tokens(
            batch_size=1,
            max_markets=1,
            force=True,
            progress_callback=lambda p, d: calls.append((p, d)),
            progress_every_n_batches=1,
        )
    assert any(d.get("stage") == "start" for _, d in calls)
    assert any(d.get("stage") == "complete" for _, d in calls)


def test_backfill_slugs_progress_start_complete(no_sleep_tqdm, monkeypatch):
    calls = []
    monkeypatch.setattr(bf_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(bf_slugs, "get_markets_without_slugs", lambda limit=None: ["1"])
    monkeypatch.setattr(bf_slugs, "set_backfill_fully_checked", lambda *a: None)
    client = MagicMock()
    client.get.return_value = [{"id": "1", "slug": "s"}]
    with patch.object(bf_gamma, "APIClient", lambda *a, **k: client):
        bf.backfill_slugs(
            batch_size=1,
            max_markets=1,
            force=True,
            progress_callback=lambda p, d: calls.append((p, d)),
            progress_every_n_batches=1,
        )
    assert any(d.get("stage") == "start" for _, d in calls)
    assert any(d.get("stage") == "complete" for _, d in calls)


def test_backfill_end_dates_progress_start_complete(no_sleep_tqdm, monkeypatch):
    calls = []
    monkeypatch.setattr(bf_end_dates, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_end_dates, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_end_dates, "get_markets_without_end_date", lambda limit=None: ["1"]
    )
    monkeypatch.setattr(bf_end_dates, "set_backfill_fully_checked", lambda *a: None)
    client = MagicMock()
    client.get.return_value = [{"id": "1", "endDate": "2025-01-01"}]
    with patch.object(bf_gamma, "APIClient", lambda *a, **k: client):
        bf.backfill_end_dates(
            batch_size=1,
            max_markets=1,
            force=True,
            progress_callback=lambda p, d: calls.append((p, d)),
            progress_every_n_batches=1,
        )
    assert any(d.get("stage") == "start" for _, d in calls)
    assert any(d.get("stage") == "complete" for _, d in calls)


def test_backfill_event_slugs_progress_fallback_start(no_sleep_tqdm, monkeypatch):
    calls = []
    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 0)
    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", lambda limit=None: ["50"]
    )
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a: None)
    monkeypatch.setattr(bf_event_slugs, "set_backfill_fully_checked", lambda *a: None)
    with patch.object(bf_gamma, "APIClient", lambda *a, **k: MagicMock()):
        with patch.object(
            bf_event_slugs, "_process_market_chunks", return_value=(1, 0, 1)
        ):
            with patch.object(
                bf_event_slugs,
                "_fill_from_events_endpoint",
                return_value=(
                    0,
                    {
                        "events_fallback_pages": 1,
                        "events_fallback_truncated": False,
                        "events_fallback_remaining_ids": 0,
                    },
                ),
            ):
                bf.backfill_event_slugs(
                    batch_size=1,
                    max_markets=1,
                    force=True,
                    progress_callback=lambda p, d: calls.append((p, d)),
                )
    assert any(d.get("stage") == "start" for _, d in calls)
    assert any(d.get("stage") == "events_fallback_start" for _, d in calls)
    assert any(d.get("stage") == "complete" for _, d in calls)
