from __future__ import annotations

import json

import duckdb
import pytest

from oddsfox_pipeline.ingestion.polymarket import catalog
from oddsfox_pipeline.storage.duckdb.polymarket_catalog import (
    catalog_crawl_status,
    save_catalog_page,
    start_catalog_crawl,
)


def _market(market_id: str, **values):
    return {
        "id": market_id,
        "question": f"Will market {market_id} resolve Yes?",
        "slug": f"market-{market_id}",
        "outcomes": ["Yes", "No"],
        **values,
    }


def test_tradability_requires_durable_evidence():
    assert catalog.tradability_evidence({"volume": "1000000", "active": True}) == ()
    assert catalog.tradability_evidence({"conditionId": "0x1"}) == ()
    assert catalog.tradability_evidence({"clobTokenIds": '["1","2"]'}) == (
        "clob_token_ids",
    )
    assert catalog.tradability_evidence(
        {
            "conditionId": "0x1",
            "ready": True,
            "enableOrderBook": True,
            "acceptingOrdersTimestamp": "2026-01-01T00:00:00Z",
        }
    ) == ("enable_order_book", "accepting_orders_timestamp", "condition_deployed")
    assert catalog.tradability_evidence(
        {"fundedTimestamp": "2026-01-01T00:00:00Z"}
    ) == ("funded_timestamp",)
    assert catalog.tradability_evidence({"conditionId": "0x1", "funded": True}) == (
        "condition_deployed",
    )
    with pytest.raises(catalog.CatalogConflictError, match="clob_token_ids"):
        catalog.tradability_evidence({"clobTokenIds": "not-json"})


def test_normalization_is_deterministic_and_preserves_many_to_many_edges():
    event = {
        "id": "10",
        "title": "Election\r\n2026",
        "tags": [{"id": "2", "label": "Politics"}],
        "markets": [_market("20", clobTokenIds=["1", "2"])],
    }
    market = {
        **_market("20", conditionId="0x20", enableOrderBook=True),
        "events": [event, {"id": "11", "title": "Second event"}],
    }
    pages = [
        {
            "pass_name": "markets_open",
            "payload_json": json.dumps({"markets": [market]}),
        },
        {"pass_name": "events_open", "payload_json": json.dumps({"events": [event]})},
    ]
    forward = catalog.normalize_catalog_pages(
        pages, crawl_id="crawl", observed_at="2026-01-01T00:00:00Z"
    )
    reverse = catalog.normalize_catalog_pages(
        reversed(pages), crawl_id="crawl", observed_at="2026-01-01T00:00:00Z"
    )
    assert forward == reverse
    events, markets, edges = forward
    assert {row["event_id"] for row in events} == {"10", "11"}
    assert [row["market_id"] for row in markets] == ["20"]
    assert markets[0]["tradability_evidence_json"] == (
        '["clob_token_ids","enable_order_book"]'
    )
    assert {(row["event_id"], row["market_id"]) for row in edges} == {
        ("10", "20"),
        ("11", "20"),
    }
    assert events[0]["content_text"].startswith("Type: event\nID: 10")
    assert "Election\n2026" in events[0]["content_text"]


def test_source_prose_is_retained_but_edge_text_removes_unsafe_controls():
    title = " Event\x00 title "
    pages = [
        {
            "pass_name": "events_open",
            "payload_json": json.dumps(
                {
                    "events": [
                        {
                            "id": "10",
                            "title": title,
                            "markets": [
                                _market(
                                    "20",
                                    outcomes=["Y\x00es", "No\r\nchange"],
                                    tags=[{"id": "1", "label": "Ta\x00g"}],
                                    clobTokenIds=["1", "2"],
                                )
                            ],
                        }
                    ]
                }
            ),
        }
    ]
    events, markets, edges = catalog.normalize_catalog_pages(
        pages, crawl_id="crawl", observed_at="2026-01-01T00:00:00Z"
    )
    assert events[0]["title"] == title
    assert "\x00" not in events[0]["content_text"]
    assert "\x00" not in edges[0]["content_text"]
    assert markets[0]["outcomes_json"] == r'["Yes","No\nchange"]'
    assert markets[0]["tags_json"] == '[{"id":"1","label":"Tag"}]'


def test_event_market_and_edge_text_have_exact_stable_format():
    event = {
        "id": "10",
        "title": "Election 2026",
        "description": "Event description.",
        "category": "Politics",
        "tags": [{"id": "1", "label": "Elections"}],
        "active": True,
        "startDate": "2026-01-01T00:00:00Z",
        "markets": [
            {
                "id": "20",
                "question": "Will the candidate win?",
                "description": "Market description.",
                "resolutionSource": "https://example.test/rules",
                "category": "Politics",
                "outcomes": '["Yes", "No"]',
                "tags": [{"id": "1", "label": "Elections"}],
                "closed": True,
                "endDate": "2026-11-04T00:00:00Z",
                "clobTokenIds": '["1", "2"]',
            }
        ],
    }
    events, markets, edges = catalog.normalize_catalog_pages(
        [
            {
                "pass_name": "events_open",
                "payload_json": json.dumps({"events": [event]}),
            }
        ],
        crawl_id="crawl",
        observed_at="2026-01-01T00:00:00Z",
    )
    assert events[0]["content_text"] == (
        "Type: event\n"
        "ID: 10\n"
        "Title: Election 2026\n"
        "Description: Event description.\n"
        "Category: Politics\n"
        "Tags: Elections\n"
        "Status: active\n"
        "Start: 2026-01-01T00:00:00Z"
    )
    assert markets[0]["content_text"] == (
        "Type: market\n"
        "ID: 20\n"
        "Question: Will the candidate win?\n"
        "Description: Market description.\n"
        "Resolution source: https://example.test/rules\n"
        "Category: Politics\n"
        "Outcomes: Yes, No\n"
        "Tags: Elections\n"
        "Status: closed, tradable\n"
        "End: 2026-11-04T00:00:00Z"
    )
    assert edges[0]["content_text"] == (
        'Polymarket event "Election 2026" contains market "Will the candidate win?".'
    )


@pytest.mark.parametrize(
    "payload, message",
    [
        ({"events": [{"title": "missing id"}]}, "malformed event ID"),
        ({"events": {}}, "non-array events"),
        ({"events": ["not an object"]}, "non-object row"),
    ],
)
def test_malformed_payloads_fail_closed(payload, message):
    with pytest.raises((ValueError, catalog.CatalogConflictError), match=message):
        catalog.normalize_catalog_pages(
            [{"pass_name": "events_open", "payload_json": json.dumps(payload)}],
            crawl_id="crawl",
            observed_at="2026-01-01T00:00:00Z",
        )


def test_nested_text_arrays_reject_schema_drift_and_deduplicate_objects():
    with pytest.raises(catalog.CatalogConflictError, match="outcomes"):
        catalog.normalize_catalog_pages(
            [
                {
                    "pass_name": "markets_open",
                    "payload_json": json.dumps(
                        {"markets": [_market("20", outcomes="not-json")]}
                    ),
                }
            ],
            crawl_id="crawl",
            observed_at="2026-01-01T00:00:00Z",
        )

    events, _, _ = catalog.normalize_catalog_pages(
        [
            {
                "pass_name": "events_open",
                "payload_json": json.dumps(
                    {
                        "events": [
                            {
                                "id": "10",
                                "tags": [
                                    {"id": "1", "label": "Politics"},
                                    {"label": "Politics", "id": "1"},
                                ],
                            }
                        ]
                    }
                ),
            }
        ],
        crawl_id="crawl",
        observed_at="2026-01-01T00:00:00Z",
    )
    assert events[0]["tags_json"] == '[{"id":"1","label":"Politics"}]'


def test_conflicting_condition_ids_fail_closed():
    pages = [
        {
            "pass_name": "markets_open",
            "payload_json": json.dumps(
                {"markets": [_market("20", conditionId="0x1", ready=True)]}
            ),
        },
        {
            "pass_name": "markets_closed",
            "payload_json": json.dumps(
                {"markets": [_market("20", conditionId="0x2", ready=True)]}
            ),
        },
    ]
    with pytest.raises(catalog.CatalogConflictError, match="condition_id"):
        catalog.normalize_catalog_pages(
            pages, crawl_id="crawl", observed_at="2026-01-01T00:00:00Z"
        )


def test_complete_four_pass_crawl_activates_atomically(monkeypatch):
    conn = duckdb.connect(":memory:")

    def fake_get(_client, endpoint, *, params):
        closed = params["closed"]
        if endpoint == "/events/keyset":
            events = [
                {
                    "id": "10" if not closed else "11",
                    "title": "Open" if not closed else "Closed",
                    "markets": [
                        _market(
                            "20" if not closed else "21",
                            clobTokenIds=["1", "2"] if not closed else [],
                        )
                    ],
                }
            ]
            return {"events": events}
        markets = [
            {
                **_market("20" if not closed else "22", enableOrderBook=True),
                "events": [] if closed else [{"id": "10", "title": "Open"}],
            }
        ]
        return {"markets": markets}

    monkeypatch.setattr(catalog, "gamma_get", fake_get)
    summary = catalog.collect_polymarket_catalog(
        conn, crawl_id="crawl-1", client=object()
    )
    assert summary["events"] == 2
    assert summary["markets"] == 3
    assert summary["qualifying_markets"] == 2
    assert summary["passes"]["events_open"]["source_rows"] == 1
    assert catalog_crawl_status(conn, "crawl-1")["status"] == "complete"
    counts = conn.execute(
        """
        select
          (select count(*) from polymarket_catalog_raw.event_snapshots),
          (select count(*) from polymarket_catalog_raw.market_snapshots),
          (select count(*) from polymarket_catalog_raw.event_market_snapshots)
        """
    ).fetchone()
    assert counts == (2, 3, 2)


def test_endpoint_passes_use_official_limits_and_market_tags(monkeypatch):
    conn = duckdb.connect(":memory:")
    calls = []

    def fake_get(_client, endpoint, *, params):
        calls.append((endpoint, params))
        key = "events" if endpoint.startswith("/events") else "markets"
        return {key: []}

    monkeypatch.setattr(catalog, "gamma_get", fake_get)
    catalog.collect_polymarket_catalog(conn, crawl_id="crawl", client=object())
    assert [params["closed"] for _, params in calls] == [False, True, False, True]
    assert [params["limit"] for _, params in calls] == [500, 500, 100, 100]
    assert all("include_tag" not in params for _, params in calls[:2])
    assert all(params["include_tag"] is True for _, params in calls[2:])


def test_valid_checkpoint_cursor_is_resumed(monkeypatch):
    conn = duckdb.connect(":memory:")
    start_catalog_crawl(conn, "crawl")
    save_catalog_page(
        conn,
        crawl_id="crawl",
        pass_name="events_open",
        page_number=0,
        payload={"events": [{"id": "10"}], "next_cursor": "cursor-1"},
        next_cursor="cursor-1",
        is_complete=False,
    )
    calls = []

    def fake_get(_client, endpoint, *, params):
        calls.append((endpoint, params.copy()))
        key = "events" if endpoint.startswith("/events") else "markets"
        return {key: []}

    monkeypatch.setattr(catalog, "gamma_get", fake_get)
    catalog.collect_polymarket_catalog(conn, crawl_id="crawl", client=object())
    assert calls[0][1]["after_cursor"] == "cursor-1"
    assert catalog_crawl_status(conn, "crawl")["status"] == "complete"


def test_nonadvancing_cursor_fails_without_activation(monkeypatch):
    conn = duckdb.connect(":memory:")

    def fake_get(_client, endpoint, *, params):
        key = "events" if endpoint.startswith("/events") else "markets"
        return {key: [{"id": "1"}], "next_cursor": "stalled"}

    monkeypatch.setattr(catalog, "gamma_get", fake_get)
    with pytest.raises(RuntimeError, match="non-advancing"):
        catalog.collect_polymarket_catalog(conn, crawl_id="crawl", client=object())
    assert catalog_crawl_status(conn, "crawl")["status"] == "failed"
    assert (
        conn.execute(
            "select count(*) from polymarket_catalog_raw.event_snapshots"
        ).fetchone()[0]
        == 0
    )


def test_empty_page_with_unresolved_cursor_fails_without_activation(monkeypatch):
    conn = duckdb.connect(":memory:")

    def fake_get(_client, endpoint, *, params):
        key = "events" if endpoint.startswith("/events") else "markets"
        return {key: [], "next_cursor": "unresolved"}

    monkeypatch.setattr(catalog, "gamma_get", fake_get)
    with pytest.raises(RuntimeError, match="unresolved cursor"):
        catalog.collect_polymarket_catalog(conn, crawl_id="crawl", client=object())
    assert catalog_crawl_status(conn, "crawl")["status"] == "failed"


def test_acquisition_rejects_malformed_response_before_activation(monkeypatch):
    conn = duckdb.connect(":memory:")
    monkeypatch.setattr(catalog, "gamma_get", lambda *_args, **_kwargs: [])
    with pytest.raises(ValueError, match="non-object payload"):
        catalog.collect_polymarket_catalog(conn, crawl_id="crawl", client=object())
    assert catalog_crawl_status(conn, "crawl")["status"] == "failed"
    assert conn.execute(
        "select issue_type, detail from polymarket_catalog_ops.crawl_issues"
    ).fetchall() == [
        ("ValueError", "catalog pass events_open returned a non-object payload")
    ]


def test_truncated_pass_does_not_activate(monkeypatch):
    conn = duckdb.connect(":memory:")
    monkeypatch.setattr(
        catalog,
        "gamma_get",
        lambda *_args, **_kwargs: {"events": [{"id": "1"}], "next_cursor": "x"},
    )
    with pytest.raises(RuntimeError, match="max_pages"):
        catalog.collect_polymarket_catalog(
            conn, crawl_id="crawl-1", max_pages=1, client=object()
        )
    assert catalog_crawl_status(conn, "crawl-1")["status"] == "failed"
    assert (
        conn.execute(
            "select count(*) from polymarket_catalog_raw.event_snapshots"
        ).fetchone()[0]
        == 0
    )


def test_failed_later_crawl_preserves_completed_observations(monkeypatch):
    conn = duckdb.connect(":memory:")

    def complete_get(_client, endpoint, *, params):
        if endpoint == "/events/keyset" and params["closed"] is False:
            return {
                "events": [
                    {
                        "id": "10",
                        "title": "Retained event",
                        "markets": [_market("20", enableOrderBook=True)],
                    }
                ]
            }
        key = "events" if endpoint.startswith("/events") else "markets"
        return {key: []}

    monkeypatch.setattr(catalog, "gamma_get", complete_get)
    catalog.collect_polymarket_catalog(conn, crawl_id="complete", client=object())

    def failed_get(_client, endpoint, *, params):
        key = "events" if endpoint.startswith("/events") else "markets"
        return {key: [], "next_cursor": "unresolved"}

    monkeypatch.setattr(catalog, "gamma_get", failed_get)
    with pytest.raises(RuntimeError, match="unresolved cursor"):
        catalog.collect_polymarket_catalog(conn, crawl_id="failed", client=object())

    assert conn.execute(
        "select crawl_id, event_id from polymarket_catalog_raw.event_snapshots"
    ).fetchall() == [("complete", "10")]
    assert catalog_crawl_status(conn, "complete")["status"] == "complete"
    assert catalog_crawl_status(conn, "failed")["status"] == "failed"
