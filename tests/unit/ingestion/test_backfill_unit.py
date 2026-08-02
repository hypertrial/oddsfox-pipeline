import json
from unittest.mock import MagicMock

import pytest
import requests
from tests.unit.ingestion.backfill_test_support import (
    bf_events_fallback,
    bf_gamma,
    bf_metadata,
    patch_ensure_duck_db,
)
from tests.unit.storage.duckdb_storage_test_support import (
    T_MT,
    _insert_minimal_market,
)

from oddsfox_pipeline.ingestion.polymarket.markets import backfill as bf
from oddsfox_pipeline.storage.duckdb.market_scope_registry import (
    RegistryRow,
    upsert_registry_rows,
)


@pytest.fixture
def no_tqdm(monkeypatch):
    def fake_tqdm(*args, **kwargs):
        del args, kwargs
        return MagicMock(__enter__=lambda s: s, __exit__=lambda *x: None)

    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.markets.backfill.tqdm",
        fake_tqdm,
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.markets.backfill.metadata.tqdm",
        fake_tqdm,
    )


@pytest.fixture
def duck_ready(monkeypatch):
    patch_ensure_duck_db(monkeypatch)
    monkeypatch.setattr(
        bf_metadata, "mark_market_metadata_unresolved", lambda *args, **kwargs: None
    )


def test_chunk_helpers():
    assert bf._chunk_market_ids(["a", "b", "c"], 2) == [["a", "b"], ["c"]]
    mc = MagicMock()
    mc.get.return_value = []
    bf._fetch_markets_batch(mc, ["1"], include_events=True)
    mc.get.assert_called()
    assert bf._extract_tokens_record("1", {"clobTokenIds": ["x"]}) == (
        "1",
        json.dumps(["x"]),
    )
    assert bf._extract_tokens_record("1", {"clobTokenIds": "not-json"}) is None
    assert bf._extract_slug_record("1", {"slug": "s"}) == ("s", "1")
    assert bf._extract_event_slug_record("1", {"events": [{"slug": "e"}]}) == ("e", "1")
    assert (
        bf._extract_end_date_record("1", {"endDate": "2020-01-01"})[0] == "2020-01-01"
    )


def test_process_market_chunks_error_and_empty(no_tqdm, duck_ready, monkeypatch):
    client = MagicMock()
    client.get.return_value = []
    processed, saved, _api = bf._process_market_chunks(
        client=client,
        market_ids=["1"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=lambda mid, m: (mid, "v"),
        save_batch=lambda rows: None,
        count_errors_as_processed=True,
    )
    assert processed >= 0


def test_process_market_chunks_exception_branch(no_tqdm, duck_ready, monkeypatch):
    client = MagicMock()
    client.get.side_effect = RuntimeError("x")

    processed, saved, _api = bf._process_market_chunks(
        client=client,
        market_ids=["1"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=lambda mid, m: (mid, "v"),
        save_batch=lambda rows: None,
        count_errors_as_processed=False,
    )
    assert processed == 0


def test_fill_from_events_endpoint(no_tqdm, duck_ready, monkeypatch):
    monkeypatch.setattr(bf_events_fallback, "save_event_slugs_batch", lambda b: None)
    monkeypatch.setattr(bf_events_fallback.time, "sleep", lambda s: None)
    client = MagicMock()
    client.get.side_effect = [
        {
            "events": [{"slug": "ev", "markets": [{"id": "5"}]}],
            "next_cursor": None,
        },
    ]
    n, meta = bf._fill_from_events_endpoint(client, {"5"})
    assert n == 1
    assert meta["events_fallback_truncated"] is False


def test_enrich_market_metadata_no_fields_enabled():
    out = bf.enrich_market_metadata(
        include_tokens=False,
        include_slugs=False,
        include_event_slugs=False,
        include_end_dates=False,
    )
    assert out["skipped"] is True
    assert out["reason"] == "no_fields_enabled"
    assert out["errors"] == 0
    assert out["failed_batches"] == []
    assert out["has_errors"] is False


def test_enrich_market_metadata_fully_checked_skip(monkeypatch):
    monkeypatch.setattr(bf_metadata, "get_backfill_fully_checked", lambda key: True)
    out = bf.enrich_market_metadata(force=False)
    assert out["skipped"] is True
    assert out["reason"] == "fully_checked"
    assert out["errors"] == 0
    assert out["failed_batches"] == []
    assert out["has_errors"] is False


def test_enrich_market_metadata_empty_eligible_sets_ledger(
    monkeypatch, duck_ready, no_tqdm
):
    monkeypatch.setattr(bf_metadata, "get_backfill_fully_checked", lambda key: False)
    monkeypatch.setattr(
        bf_metadata, "get_markets_missing_any_metadata", lambda **kwargs: []
    )
    checked = []
    monkeypatch.setattr(
        bf_metadata,
        "set_backfill_fully_checked",
        lambda key, val: checked.append((key, val)),
    )
    out = bf.enrich_market_metadata(max_markets=None)
    assert out["eligible"] == 0
    assert out["errors"] == 0
    assert out["failed_batches"] == []
    assert out["has_errors"] is False
    assert ("tokens", True) in checked

    checked.clear()
    out_capped = bf.enrich_market_metadata(max_markets=10)
    assert out_capped["eligible"] == 0
    assert checked == []


def test_enrich_market_metadata_fetches_once_and_saves_all_fields(
    monkeypatch, duck_ready, no_tqdm
):
    saved = {"tokens": [], "slugs": [], "event_slugs": [], "end_dates": []}
    fully_checked = []
    fetch_calls = []
    client = MagicMock()

    monkeypatch.setattr(bf_metadata, "get_backfill_fully_checked", lambda key: False)
    monkeypatch.setattr(
        bf_metadata,
        "get_markets_missing_any_metadata",
        lambda **kwargs: ["1"],
    )
    monkeypatch.setattr(
        bf_metadata, "_gamma_client", lambda requests_per_second=None: client
    )

    def fetch_markets(client_arg, chunk, include_events=False):
        fetch_calls.append((client_arg, tuple(chunk), include_events))
        return [
            {
                "id": "1",
                "clobTokenIds": ["tok1", "tok2"],
                "slug": "world-cup-2026-winner",
                "events": [{"slug": "world-cup-2026"}],
                "endDate": "2026-07-19T00:00:00Z",
            }
        ]

    monkeypatch.setattr(bf_metadata, "_fetch_markets_batch", fetch_markets)
    monkeypatch.setattr(
        bf_metadata,
        "save_tokens_batch",
        lambda rows, scope_name=None: saved["tokens"].extend(rows),
    )
    monkeypatch.setattr(
        bf_metadata,
        "save_slugs_batch",
        lambda rows, scope_name=None: saved["slugs"].extend(rows),
    )
    monkeypatch.setattr(
        bf_metadata,
        "save_event_slugs_batch",
        lambda rows, scope_name=None: saved["event_slugs"].extend(rows),
    )
    monkeypatch.setattr(
        bf_metadata,
        "save_end_dates_batch",
        lambda rows, scope_name=None: saved["end_dates"].extend(rows),
    )
    monkeypatch.setattr(
        bf_metadata,
        "set_backfill_fully_checked",
        lambda key, value: fully_checked.append((key, value)),
    )

    out = bf.enrich_market_metadata(batch_size=50, max_markets=None)

    assert fetch_calls == [(client, ("1",), True)]
    assert out["api_requests"] == 1
    assert out["errors"] == 0
    assert out["failed_batches"] == []
    assert out["has_errors"] is False
    assert out["saved"] == {
        "tokens": 1,
        "slugs": 1,
        "event_slugs": 1,
        "end_dates": 1,
    }
    assert saved["tokens"] == [("1", json.dumps(["tok1", "tok2"]))]
    assert saved["slugs"] == [("world-cup-2026-winner", "1")]
    assert saved["event_slugs"] == [("world-cup-2026", "1")]
    assert saved["end_dates"] == [("2026-07-19T00:00:00Z", "1")]
    assert sorted(fully_checked) == [
        ("end_dates", True),
        ("event_slugs", True),
        ("slugs", True),
        ("tokens", True),
    ]


def test_backfill_creates_token_row_for_zero_volume_event_child(
    duck, monkeypatch, no_tqdm
):
    with duck.get_connection() as conn:
        _insert_minimal_market(
            conn,
            "zero-volume-child",
            volume=0.0,
            slug="world-cup-child",
            event_slug="world-cup-event",
            event_id="event-eligible",
            end_date="2026-07-19 00:00:00",
        )
    upsert_registry_rows(
        [
            RegistryRow(
                market_id="zero-volume-child",
                event_slug="world-cup-event",
                event_id="event-eligible",
                source="event_catalog",
            )
        ]
    )
    client = MagicMock()
    monkeypatch.setattr(bf_metadata, "_gamma_client", lambda *_a, **_k: client)
    monkeypatch.setattr(
        bf_metadata,
        "_fetch_markets_batch",
        lambda *_args, **_kwargs: [
            {
                "id": "zero-volume-child",
                "clobTokenIds": ["token-yes", "token-no"],
            }
        ],
    )

    summary = bf.enrich_market_metadata(
        force=True,
        include_tokens=True,
        include_slugs=False,
        include_event_slugs=False,
        include_end_dates=False,
    )

    with duck.get_connection() as conn:
        row = conn.execute(
            f"select market_id, clobTokenIds from {T_MT} "
            "where market_id = 'zero-volume-child'"
        ).fetchone()
    assert summary["saved"]["tokens"] == 1
    assert row == ("zero-volume-child", '["token-yes", "token-no"]')


def test_enrich_market_metadata_records_unresolved_event_slug_cooldown(
    monkeypatch, duck_ready, no_tqdm
):
    unresolved = []
    client = MagicMock()

    monkeypatch.setattr(bf_metadata, "get_backfill_fully_checked", lambda key: False)
    monkeypatch.setattr(
        bf_metadata,
        "get_markets_missing_any_metadata",
        lambda **kwargs: ["1"],
    )
    monkeypatch.setattr(
        bf_metadata, "_gamma_client", lambda requests_per_second=None: client
    )
    monkeypatch.setattr(
        bf_metadata,
        "_fetch_markets_batch",
        lambda client_arg, chunk, include_events=False: [
            {"id": "1", "events": [], "slug": "world-cup-2026-winner"}
        ],
    )
    monkeypatch.setattr(
        bf_metadata,
        "_fill_from_events_endpoint",
        lambda *args, **kwargs: (
            0,
            {
                "events_fallback_pages": 1,
                "events_fallback_truncated": False,
                "events_fallback_remaining_ids": 1,
            },
        ),
    )
    monkeypatch.setattr(
        bf_metadata, "save_slugs_batch", lambda rows, scope_name=None: None
    )
    monkeypatch.setattr(bf_metadata, "set_backfill_fully_checked", lambda *args: None)
    monkeypatch.setattr(
        bf_metadata,
        "mark_market_metadata_unresolved",
        lambda rows, retry_after_hours, scope_name=None: unresolved.extend(
            [(tuple(row), retry_after_hours) for row in rows]
        ),
    )

    out = bf.enrich_market_metadata(
        include_tokens=False,
        include_slugs=False,
        include_end_dates=False,
        event_slug_unresolved_retry_hours=12,
    )

    assert out["unresolved_event_slugs"] == 1
    assert unresolved == [
        (("1", "event_slug", "missing from Gamma market and events payload"), 12)
    ]


def test_enrich_market_metadata_skips_missing_market_rows(
    monkeypatch, duck_ready, no_tqdm
):
    monkeypatch.setattr(bf_metadata, "get_backfill_fully_checked", lambda key: False)
    monkeypatch.setattr(
        bf_metadata, "get_markets_missing_any_metadata", lambda **kwargs: ["1", "2"]
    )
    monkeypatch.setattr(
        bf_metadata, "_gamma_client", lambda requests_per_second=None: MagicMock()
    )
    monkeypatch.setattr(
        bf_metadata,
        "_fetch_markets_batch",
        lambda *args, **kwargs: [{"id": "1", "slug": "s", "clobTokenIds": ["t"]}],
    )
    monkeypatch.setattr(
        bf_metadata, "save_tokens_batch", lambda rows, scope_name=None: None
    )
    monkeypatch.setattr(
        bf_metadata, "save_slugs_batch", lambda rows, scope_name=None: None
    )
    monkeypatch.setattr(bf_metadata, "set_backfill_fully_checked", lambda *a: None)
    out = bf.enrich_market_metadata(batch_size=50, include_slugs=False)
    assert out["processed"] == 2


def test_enrich_market_metadata_max_markets_clears_ledger(
    monkeypatch, duck_ready, no_tqdm
):
    monkeypatch.setattr(bf_metadata, "get_backfill_fully_checked", lambda key: False)
    monkeypatch.setattr(
        bf_metadata, "get_markets_missing_any_metadata", lambda **kwargs: ["1"]
    )
    monkeypatch.setattr(
        bf_metadata, "_gamma_client", lambda requests_per_second=None: MagicMock()
    )
    monkeypatch.setattr(
        bf_metadata,
        "_fetch_markets_batch",
        lambda *args, **kwargs: [{"id": "1", "slug": "s", "clobTokenIds": ["t"]}],
    )
    monkeypatch.setattr(
        bf_metadata, "save_tokens_batch", lambda rows, scope_name=None: None
    )
    flags = []
    monkeypatch.setattr(
        bf_metadata,
        "set_backfill_fully_checked",
        lambda key, val: flags.append((key, val)),
    )
    bf.enrich_market_metadata(batch_size=50, max_markets=5)
    assert ("tokens", False) in flags


def test_enrich_market_metadata_partial_field_extraction(
    monkeypatch, duck_ready, no_tqdm
):
    monkeypatch.setattr(bf_metadata, "get_backfill_fully_checked", lambda key: False)
    monkeypatch.setattr(
        bf_metadata, "get_markets_missing_any_metadata", lambda **kwargs: ["1"]
    )
    monkeypatch.setattr(
        bf_metadata, "_gamma_client", lambda requests_per_second=None: MagicMock()
    )
    monkeypatch.setattr(
        bf_metadata,
        "_fetch_markets_batch",
        lambda *args, **kwargs: [
            {"id": "1", "slug": "", "clobTokenIds": [], "events": []}
        ],
    )
    monkeypatch.setattr(bf_metadata, "set_backfill_fully_checked", lambda *a: None)
    out = bf.enrich_market_metadata(
        include_tokens=True,
        include_slugs=True,
        include_event_slugs=False,
        include_end_dates=False,
        max_markets=1,
    )
    assert out["saved"]["tokens"] == 0
    assert out["saved"]["slugs"] == 0

    monkeypatch.setattr(
        bf_metadata,
        "_fetch_markets_batch",
        lambda *args, **kwargs: [
            {"id": "2", "slug": "has-slug", "clobTokenIds": '["t"]'}
        ],
    )
    monkeypatch.setattr(
        bf_metadata, "get_markets_missing_any_metadata", lambda **kwargs: ["2"]
    )
    slug_rows = []
    monkeypatch.setattr(
        bf_metadata,
        "save_slugs_batch",
        lambda rows, scope_name=None: slug_rows.extend(rows),
    )
    monkeypatch.setattr(
        bf_metadata, "save_tokens_batch", lambda rows, scope_name=None: None
    )
    bf.enrich_market_metadata(
        include_tokens=True,
        include_slugs=True,
        include_event_slugs=False,
        include_end_dates=False,
        max_markets=1,
    )
    assert slug_rows == [("has-slug", "2")]


def test_enrich_market_metadata_slugs_only_ledger(monkeypatch, duck_ready, no_tqdm):
    monkeypatch.setattr(bf_metadata, "get_backfill_fully_checked", lambda key: False)
    monkeypatch.setattr(
        bf_metadata, "get_markets_missing_any_metadata", lambda **kwargs: []
    )
    monkeypatch.setattr(bf_metadata, "set_backfill_fully_checked", lambda *a: None)
    out = bf.enrich_market_metadata(
        include_tokens=False,
        include_slugs=True,
        include_event_slugs=False,
        include_end_dates=False,
        max_markets=None,
    )
    assert out["eligible"] == 0


def test_enrich_market_metadata_batch_errors(monkeypatch, duck_ready, no_tqdm):
    monkeypatch.setattr(bf_metadata, "get_backfill_fully_checked", lambda key: False)
    monkeypatch.setattr(
        bf_metadata, "get_markets_missing_any_metadata", lambda **kwargs: ["1"]
    )
    monkeypatch.setattr(
        bf_metadata, "_gamma_client", lambda requests_per_second=None: MagicMock()
    )

    def boom(*args, **kwargs):
        raise requests.RequestException("gamma down")

    monkeypatch.setattr(bf_metadata, "_fetch_markets_batch", boom)
    checked = []
    monkeypatch.setattr(
        bf_metadata,
        "set_backfill_fully_checked",
        lambda *a: checked.append(a),
    )
    out = bf.enrich_market_metadata(batch_size=50, include_tokens=True)
    assert out["processed"] == 0
    assert out["errors"] == 1
    assert out["has_errors"] is True
    assert out["failed_batches"] == [
        {
            "batch_index": 1,
            "market_ids": ["1"],
            "error_type": "RequestException",
            "error": "gamma down",
        }
    ]
    assert out["fully_checked_set"] is False
    assert ("tokens", False) in checked

    def boom_generic(*args, **kwargs):
        raise RuntimeError("unexpected")

    monkeypatch.setattr(bf_metadata, "_fetch_markets_batch", boom_generic)
    out2 = bf.enrich_market_metadata(batch_size=50, include_tokens=True)
    assert out2["processed"] == 0
    assert out2["errors"] == 1
    assert out2["failed_batches"][0]["error_type"] == "RuntimeError"


def test_enrich_market_metadata_progress_callbacks(monkeypatch, duck_ready, no_tqdm):
    progress_events: list[tuple[str, dict]] = []
    monkeypatch.setattr(bf_metadata, "get_backfill_fully_checked", lambda key: False)
    monkeypatch.setattr(
        bf_metadata, "get_markets_missing_any_metadata", lambda **kwargs: ["1"]
    )
    monkeypatch.setattr(
        bf_metadata, "_gamma_client", lambda requests_per_second=None: MagicMock()
    )
    monkeypatch.setattr(
        bf_metadata,
        "_fetch_markets_batch",
        lambda *args, **kwargs: [
            {"id": "1", "slug": "s", "clobTokenIds": ["t"], "events": []}
        ],
    )
    monkeypatch.setattr(
        bf_metadata, "save_tokens_batch", lambda rows, scope_name=None: None
    )
    monkeypatch.setattr(
        bf_metadata, "save_slugs_batch", lambda rows, scope_name=None: None
    )
    monkeypatch.setattr(
        bf_metadata,
        "_fill_from_events_endpoint",
        lambda *args, **kwargs: (0, {"events_fallback_pages": 0}),
    )
    monkeypatch.setattr(bf_metadata, "set_backfill_fully_checked", lambda *a: None)

    bf.enrich_market_metadata(
        include_event_slugs=True,
        progress_callback=lambda phase, payload: progress_events.append(
            (phase, payload)
        ),
        progress_every_n_batches=1,
    )
    assert (
        sum(1 for phase, _ in progress_events if phase == "enrich_market_metadata") >= 2
    )
    assert all(
        {"errors", "failed_batches", "has_errors"} <= payload.keys()
        for phase, payload in progress_events
        if phase == "enrich_market_metadata"
    )
    assert any(
        payload.get("stage") == "events_fallback_start"
        for _, payload in progress_events
    )
