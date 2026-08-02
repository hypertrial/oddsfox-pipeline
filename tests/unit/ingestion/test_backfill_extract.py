"""Unit tests for markets/backfill extract."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from tests.unit.ingestion.backfill_test_support import bf_extract, bf_gamma


def test_extract_tokens_json_string_success():
    raw = json.dumps(["a", "b"])
    assert bf_extract._extract_tokens_record("1", {"clobTokenIds": raw}) == ("1", raw)


def test_extract_tokens_invalid_list_type():
    assert bf_extract._extract_tokens_record("1", {"clobTokenIds": 123}) is None


def test_extract_tokens_invalid_json_and_empty_list():
    assert bf_extract._extract_tokens_record("1", {"clobTokenIds": "{bad"}) is None
    assert bf_extract._extract_tokens_record("1", {"clobTokenIds": []}) is None


def test_extract_event_slug_non_list_events():
    assert bf_extract._extract_event_slug_record("1", {"events": "x"}) is None


def test_extract_event_slug_bad_first_event():
    assert bf_extract._extract_event_slug_record("1", {"events": [123]}) is None


def test_extract_end_date_iso_alt():
    assert (
        bf_extract._extract_end_date_record("1", {"endDateIso": "2020-01-01"})[0]
        == "2020-01-01"
    )


def test_extract_end_date_missing():
    assert bf_extract._extract_end_date_record("1", {}) is None


def test_extract_slug_empty():
    assert bf_extract._extract_slug_record("1", {"slug": ""}) is None


def test_extract_event_slug_empty_slug():
    assert (
        bf_extract._extract_event_slug_record("1", {"events": [{"slug": ""}]}) is None
    )


def test_iter_gamma_events_keyset_stops_on_empty_events_page():
    from oddsfox_pipeline.ingestion.polymarket.gamma_events import (
        iter_gamma_events_keyset,
    )

    client = MagicMock()
    client.get.return_value = {"events": [], "next_cursor": "cursor-2"}
    pages = list(iter_gamma_events_keyset(client, max_pages=5))
    assert len(pages) == 1
    assert pages[0][0] == []


def test_iter_gamma_events_keyset_stops_on_non_advancing_cursor():
    from oddsfox_pipeline.ingestion.polymarket.gamma_events import (
        iter_gamma_events_keyset,
    )

    client = MagicMock()
    client.get.side_effect = [
        {
            "events": [{"id": "1", "slug": "world-cup-winner"}],
            "next_cursor": "stuck-cursor",
        },
        {"events": [], "next_cursor": "stuck-cursor"},
    ]
    pages = list(iter_gamma_events_keyset(client, max_pages=None))
    # Page 1 yields events; page 2 echoes the same cursor with no new rows (EOF).
    assert len(pages) == 2
    assert pages[0][0] == [{"id": "1", "slug": "world-cup-winner"}]
    assert pages[1][0] == []
    assert pages[1][1].truncated is False
    assert client.get.call_count == 2
    first_params = client.get.call_args_list[0].kwargs["params"]
    second_params = client.get.call_args_list[1].kwargs["params"]
    assert "after_cursor" not in first_params
    assert second_params["after_cursor"] == "stuck-cursor"
    assert "next_cursor" not in second_params


def test_iter_gamma_events_keyset_non_advancing_duplicate_data_is_eof():
    from oddsfox_pipeline.ingestion.polymarket.gamma_events import (
        iter_gamma_events_keyset,
    )

    client = MagicMock()
    client.get.side_effect = [
        {
            "events": [{"id": "1", "slug": "world-cup-winner"}],
            "next_cursor": "stuck-cursor",
        },
        {
            "events": [{"id": "1", "slug": "world-cup-winner"}],
            "next_cursor": "stuck-cursor",
        },
    ]
    pages = list(iter_gamma_events_keyset(client, max_pages=None))
    assert len(pages) == 2
    assert pages[1][0] == []
    assert pages[1][1].truncated is False
    assert client.get.call_count == 2


def test_iter_gamma_events_keyset_full_stuck_page_uses_offset_fallback():
    from oddsfox_pipeline.ingestion.polymarket.gamma_events import (
        iter_gamma_events_keyset,
    )

    first = [{"id": "1"}, {"id": "2"}]
    client = MagicMock()
    client.get.side_effect = [
        {"events": first, "next_cursor": "stuck-cursor"},
        {"events": first, "next_cursor": "stuck-cursor"},
        first,
        [{"id": "3"}, {"id": "4"}],
        [{"id": "5"}],
    ]
    progress = MagicMock()

    pages = list(
        iter_gamma_events_keyset(
            client,
            max_pages=None,
            fetch_limit=2,
            progress_callback=progress,
            progress_every_pages=2,
        )
    )

    assert [[event["id"] for event in page] for page, _ in pages] == [
        ["1", "2"],
        ["3", "4"],
        ["5"],
    ]
    assert pages[-1][1].truncated is False
    assert client.get.call_args_list[2].args[0] == "/events"
    assert client.get.call_args_list[2].kwargs["params"]["offset"] == 0
    assert client.get.call_args_list[3].kwargs["params"]["offset"] == 2
    assert client.get.call_args_list[4].kwargs["params"]["offset"] == 4
    assert progress.call_args.args[1]["keyset_fallback"] is True


def test_iter_gamma_events_keyset_offset_fallback_does_not_assume_same_order():
    from oddsfox_pipeline.ingestion.polymarket.gamma_events import (
        iter_gamma_events_keyset,
    )

    first = [{"id": "1"}, {"id": "2"}]
    client = MagicMock()
    client.get.side_effect = [
        {"events": first, "next_cursor": "stuck-cursor"},
        {"events": first, "next_cursor": "stuck-cursor"},
        [{"id": "3"}, {"id": "1"}],
        [{"id": "2"}, {"id": "4"}],
        [{"id": "5"}],
    ]

    pages = list(iter_gamma_events_keyset(client, max_pages=None, fetch_limit=2))

    assert [[event["id"] for event in page] for page, _ in pages] == [
        ["1", "2"],
        ["3"],
        ["4"],
        ["5"],
    ]
    assert pages[-1][1].truncated is False


def test_iter_gamma_events_keyset_full_stuck_page_honors_page_cap():
    from oddsfox_pipeline.ingestion.polymarket.gamma_events import (
        iter_gamma_events_keyset,
    )

    first = [{"id": "1"}, {"id": "2"}]
    client = MagicMock()
    client.get.side_effect = [
        {"events": first, "next_cursor": "stuck-cursor"},
        {"events": first, "next_cursor": "stuck-cursor"},
    ]

    pages = list(iter_gamma_events_keyset(client, max_pages=2, fetch_limit=2))

    assert pages[-1][0] == []
    assert pages[-1][1].truncated is True
    assert client.get.call_count == 2


def test_iter_gamma_events_keyset_stalled_offset_fallback_is_truncated():
    from oddsfox_pipeline.ingestion.polymarket.gamma_events import (
        iter_gamma_events_keyset,
    )

    first = [{"id": "1"}, {"id": "2"}]
    client = MagicMock()
    client.get.side_effect = [
        {"events": first, "next_cursor": "stuck-cursor"},
        {"events": first, "next_cursor": "stuck-cursor"},
        first,
        first,
    ]

    pages = list(iter_gamma_events_keyset(client, max_pages=None, fetch_limit=2))

    assert pages[-1][0] == []
    assert pages[-1][1].truncated is True
    assert client.get.call_count == 4


def test_iter_gamma_events_keyset_closed_filter():
    from oddsfox_pipeline.ingestion.polymarket.gamma_events import (
        iter_gamma_events_keyset,
    )

    client = MagicMock()
    client.get.return_value = {
        "events": [{"id": "1", "slug": "open-event"}],
        "next_cursor": None,
    }
    list(iter_gamma_events_keyset(client, max_pages=5, keyset_closed=False))
    params = client.get.call_args.kwargs.get("params") or {}
    assert params.get("closed") is False

    client.reset_mock()
    client.get.return_value = {
        "events": [{"id": "2", "slug": "any-event"}],
        "next_cursor": None,
    }
    list(iter_gamma_events_keyset(client, max_pages=5))
    params = client.get.call_args.kwargs.get("params") or {}
    assert "closed" not in params


def test_iter_gamma_events_keyset_tag_and_volume_filters():
    from oddsfox_pipeline.ingestion.polymarket.gamma_events import (
        iter_gamma_events_keyset,
    )

    client = MagicMock()
    client.get.return_value = {
        "events": [{"id": "1", "slug": "world-cup-winner"}],
        "next_cursor": None,
    }
    progress = MagicMock()
    list(
        iter_gamma_events_keyset(
            client,
            max_pages=5,
            keyset_closed=False,
            keyset_tag_slug="fifa-world-cup",
            keyset_series_id="series-1",
            keyset_related_tags=True,
            keyset_volume_min=5000,
            progress_callback=progress,
        )
    )
    params = client.get.call_args.kwargs.get("params") or {}
    assert params.get("closed") is False
    assert params.get("tag_slug") == "fifa-world-cup"
    assert params.get("series_id") == "series-1"
    assert params.get("related_tags") == "true"
    assert params.get("volume_min") == 5000
    assert progress.call_args.args[1]["keyset_series_id"] == "series-1"


def test_iter_gamma_events_keyset_related_tags_param():
    from oddsfox_pipeline.ingestion.polymarket.gamma_events import (
        iter_gamma_events_keyset,
    )

    client = MagicMock()
    client.get.return_value = {
        "events": [{"id": "1", "slug": "wc-event"}],
        "next_cursor": None,
    }
    list(iter_gamma_events_keyset(client, max_pages=5, keyset_related_tags=True))
    params = client.get.call_args.kwargs.get("params") or {}
    assert params.get("related_tags") == "true"


def test_gamma_client_forwards_requests_per_second(monkeypatch):
    captured = []

    def ctor(*a, **kw):
        captured.append(kw)
        return MagicMock()

    monkeypatch.setattr(bf_gamma, "APIClient", ctor)
    bf_gamma._gamma_client(12.5)
    assert captured[0]["requests_per_second"] == 12.5
    bf_gamma._gamma_client(None)
    assert captured[1]["requests_per_second"] is None
