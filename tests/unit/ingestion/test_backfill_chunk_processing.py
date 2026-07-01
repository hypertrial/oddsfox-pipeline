"""Unit tests for markets/backfill chunk processing."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from tests.unit.ingestion.backfill_test_support import (
    bf_events_fallback,
    bf_gamma,
)

from oddsfox.ingestion.polymarket.markets import backfill as bf


@pytest.fixture
def no_sleep_tqdm(monkeypatch):
    monkeypatch.setattr(
        bf,
        "tqdm",
        lambda *a, **k: MagicMock(__enter__=lambda s: s, __exit__=lambda *x: None),
    )
    monkeypatch.setattr(bf_gamma.time, "sleep", lambda s: None)
    monkeypatch.setattr(bf_events_fallback.time, "sleep", lambda s: None)


def test_process_market_chunks_with_records_and_on_saved(no_sleep_tqdm, monkeypatch):
    monkeypatch.setattr(bf, "ensure_duck_db", lambda: None)
    saved_ids = []

    def on_saved(mid):
        saved_ids.append(mid)

    client = MagicMock()
    client.get.return_value = [{"id": "1", "clobTokenIds": ["x"]}]
    processed, saved, _api = bf._process_market_chunks(
        client=client,
        market_ids=["1"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=bf._extract_tokens_record,
        save_batch=lambda x: None,
        on_record_saved=on_saved,
    )
    assert processed >= 1
    assert saved == 1
    assert saved_ids == ["1"]


def test_process_market_chunks_save_failure_no_false_progress(
    no_sleep_tqdm, monkeypatch
):
    monkeypatch.setattr(bf, "ensure_duck_db", lambda: None)
    saved_ids = []

    def on_saved(mid):
        saved_ids.append(mid)

    client = MagicMock()
    client.get.return_value = [{"id": "1", "slug": "slug-1"}]

    def failing_save(_rows):
        raise RuntimeError("save failed")

    processed, saved, _api = bf._process_market_chunks(
        client=client,
        market_ids=["1"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=bf._extract_slug_record,
        save_batch=failing_save,
        on_record_saved=on_saved,
    )
    assert processed == 1
    assert saved == 0
    assert saved_ids == []


def test_process_market_chunks_record_none_without_callback(no_sleep_tqdm, monkeypatch):
    monkeypatch.setattr(bf, "ensure_duck_db", lambda: None)
    client = MagicMock()
    client.get.return_value = [{"id": "1", "slug": ""}]
    processed, saved, _api = bf._process_market_chunks(
        client=client,
        market_ids=["1"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=bf._extract_slug_record,
        save_batch=lambda x: (_ for _ in ()).throw(RuntimeError("should not save")),
    )
    assert processed == 1
    assert saved == 0


def test_process_market_chunks_record_saved_without_on_saved(
    no_sleep_tqdm, monkeypatch
):
    monkeypatch.setattr(bf, "ensure_duck_db", lambda: None)
    saved_batches = []
    client = MagicMock()
    client.get.return_value = [{"id": "1", "slug": "slug-1"}]
    processed, saved, _api = bf._process_market_chunks(
        client=client,
        market_ids=["1"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=bf._extract_slug_record,
        save_batch=lambda rows: saved_batches.extend(rows),
    )
    assert processed == 1
    assert saved == 1
    assert saved_batches == [("slug-1", "1")]


def test_process_market_chunks_count_errors_request_exception(
    no_sleep_tqdm, monkeypatch
):
    import requests

    monkeypatch.setattr(bf, "ensure_duck_db", lambda: None)
    client = MagicMock()
    client.get.side_effect = requests.RequestException("gamma down")
    processed, saved, _api = bf._process_market_chunks(
        client=client,
        market_ids=["1"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=bf._extract_tokens_record,
        save_batch=lambda x: None,
        count_errors_as_processed=True,
    )
    assert processed >= 1
    processed2, _, _ = bf._process_market_chunks(
        client=client,
        market_ids=["1"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=bf._extract_tokens_record,
        save_batch=lambda x: None,
        count_errors_as_processed=False,
    )
    assert processed2 == 0


def test_process_market_chunks_count_errors(no_sleep_tqdm, monkeypatch):
    monkeypatch.setattr(bf, "ensure_duck_db", lambda: None)
    client = MagicMock()
    client.get.side_effect = RuntimeError("api")
    processed, saved, _api = bf._process_market_chunks(
        client=client,
        market_ids=["1", "2"],
        batch_size=2,
        desc="t",
        include_events=False,
        extract_record=bf._extract_tokens_record,
        save_batch=lambda x: None,
        count_errors_as_processed=True,
    )
    assert processed >= 2


def test_fill_from_events_empty_remaining():
    c = MagicMock()
    saved, meta = bf._fill_from_events_endpoint(c, set())
    assert saved == 0
    assert meta["events_fallback_pages"] == 0


def test_fill_from_events_pagination(no_sleep_tqdm, monkeypatch):
    c = MagicMock()
    c.get.side_effect = [
        [
            {
                "slug": "es",
                "markets": [{"id": "99"}],
            }
        ],
        [],
    ]
    remaining = {"99"}
    with patch.object(bf_events_fallback, "save_event_slugs_batch", lambda x: None):
        n, meta = bf._fill_from_events_endpoint(c, remaining)
    assert n >= 1
    assert meta["events_fallback_truncated"] is False


def test_fill_from_events_skip_bad_slug(no_sleep_tqdm, monkeypatch):
    c = MagicMock()
    c.get.side_effect = [
        [{"slug": None, "markets": [{"id": "1"}]}],
        [{"slug": "s", "markets": []}],
        [],
    ]
    with patch.object(bf_events_fallback, "save_event_slugs_batch", lambda x: None):
        bf._fill_from_events_endpoint(c, {"1"})


def test_fill_from_events_unmatched_market_keeps_remaining(no_sleep_tqdm):
    c = MagicMock()
    c.get.side_effect = [[{"slug": "s", "markets": [{"id": "other"}]}], []]
    remaining = {"1"}
    with patch.object(bf_events_fallback, "save_event_slugs_batch", lambda x: None):
        saved, meta = bf._fill_from_events_endpoint(c, remaining)
    assert saved == 0
    assert remaining == {"1"}
    assert meta["events_fallback_remaining_ids"] == 1


def test_process_market_chunks_missing_returned_market(no_sleep_tqdm, monkeypatch):
    """Market id in chunk but not in API response — no record, still processed."""
    monkeypatch.setattr(bf, "ensure_duck_db", lambda: None)
    client = MagicMock()
    client.get.return_value = [{"id": "other", "clobTokenIds": ["x"]}]
    processed, saved, _api = bf._process_market_chunks(
        client=client,
        market_ids=["wanted"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=bf._extract_tokens_record,
        save_batch=lambda x: None,
    )
    assert processed >= 1


def test_process_market_chunks_progress_callback(monkeypatch):
    monkeypatch.setattr(bf, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf,
        "tqdm",
        lambda *a, **k: MagicMock(__enter__=lambda s: s, __exit__=lambda *x: None),
    )
    calls = []

    def cb(phase, payload):
        calls.append((phase, payload))

    client = MagicMock()
    client.get.return_value = [{"id": "1", "slug": "s"}]
    bf._process_market_chunks(
        client=client,
        market_ids=["1"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=bf._extract_slug_record,
        save_batch=lambda x: None,
        progress_phase="test_phase",
        progress_callback=cb,
        progress_every_n_batches=1,
    )
    assert any(phase == "test_phase" for phase, _ in calls)
    assert any("batch_index" in pl for _, pl in calls)


def test_fill_from_events_max_pages_truncates():
    c = MagicMock()
    c.get.return_value = {
        "events": [{"slug": "s", "markets": [{"id": "9"}]}],
        "next_cursor": "cursor-2",
    }
    remaining = {"9", "orphan"}
    with patch.object(bf_events_fallback, "save_event_slugs_batch", lambda x: None):
        saved, meta = bf._fill_from_events_endpoint(c, remaining, max_pages=1)
    assert saved >= 0
    assert meta["events_fallback_pages"] == 1
    assert meta["events_fallback_truncated"] is True
    assert "orphan" in remaining


def test_fill_from_events_no_progress_truncates():
    c = MagicMock()
    c.get.return_value = {
        "events": [{"slug": "s", "markets": [{"id": "not-target"}]}],
        "next_cursor": "cursor-2",
    }
    remaining = {"target"}
    with patch.object(bf_events_fallback, "save_event_slugs_batch", lambda x: None):
        saved, meta = bf._fill_from_events_endpoint(
            c,
            remaining,
            max_pages=None,
            max_pages_without_progress=2,
        )
    assert saved == 0
    assert meta["events_fallback_pages"] == 2
    assert meta["events_fallback_truncated"] is True
    assert "target" in remaining


def test_process_market_chunks_disables_tqdm_when_stderr_not_tty(monkeypatch):
    monkeypatch.setattr(bf_gamma.sys.stderr, "isatty", lambda: False)
    kwargs_seen = []

    def fake_tqdm(*a, **kw):
        kwargs_seen.append(kw)
        return MagicMock(__enter__=lambda s: s, __exit__=lambda *x: None)

    monkeypatch.setattr(bf_gamma, "tqdm", fake_tqdm)
    client = MagicMock()
    client.get.return_value = []
    bf._process_market_chunks(
        client=client,
        market_ids=["1"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=bf._extract_slug_record,
        save_batch=lambda x: None,
    )
    assert kwargs_seen[0]["disable"] is True


def test_fill_from_events_progress_callback():
    calls = []
    c = MagicMock()
    c.get.side_effect = [
        {"events": [{"slug": "s", "markets": [{"id": "1"}]}], "next_cursor": None}
    ]
    with patch.object(bf_events_fallback, "save_event_slugs_batch", lambda x: None):
        bf._fill_from_events_endpoint(
            c,
            {"1"},
            progress_callback=lambda ph, pl: calls.append((ph, pl)),
            progress_every_pages=1,
            progress_phase="fb",
        )
    assert any(ph == "fb" for ph, _ in calls)


def test_fill_from_events_empty_keyset_page_stops():
    c = MagicMock()
    c.get.return_value = {"events": [], "next_cursor": "unused"}
    saved, meta = bf._fill_from_events_endpoint(c, {"target"})
    assert saved == 0
    assert meta["events_fallback_pages"] == 1
    assert meta["events_fallback_truncated"] is False


def test_fill_from_events_uses_keyset_cursor():
    calls = []
    c = MagicMock()
    c.get.side_effect = [
        {
            "events": [{"slug": "s", "markets": [{"id": "nope"}]}],
            "next_cursor": "cursor-2",
        },
        {
            "events": [{"slug": "s2", "markets": [{"id": "target"}]}],
            "next_cursor": None,
        },
    ]
    with patch.object(
        bf_events_fallback, "save_event_slugs_batch", lambda x: calls.append(x)
    ):
        saved, meta = bf._fill_from_events_endpoint(c, {"target"})
    assert saved == 1
    assert meta["events_fallback_pages"] == 2
    first_params = c.get.call_args_list[0].kwargs["params"]
    second_params = c.get.call_args_list[1].kwargs["params"]
    assert "next_cursor" not in first_params
    assert second_params["next_cursor"] == "cursor-2"
