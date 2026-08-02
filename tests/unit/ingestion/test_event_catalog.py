from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from oddsfox_pipeline.ingestion.polymarket import event_catalog as catalog
from oddsfox_pipeline.ingestion.polymarket.gamma_events import EventsPageMeta


def _event(
    event_id: str = "1",
    *,
    related_event_id: str = "1",
    markets: list[dict[str, Any]] | None = None,
    volume: Any = "120000",
) -> dict[str, Any]:
    return {
        "id": event_id,
        "slug": f"fifwc-event-{event_id}",
        "title": "2026 FIFA World Cup",
        "volume": volume,
        "tags": [
            {"id": "tag-1", "slug": catalog.WC2026_EVENT_TAG},
            {"id": "tag-2", "slug": catalog.WC2026_RECALL_TAG},
        ],
        "series": [{"id": "series-1", "slug": "soccer-fifwc"}],
        "markets": markets
        if markets is not None
        else [
            {
                "id": "market-1",
                "events": [
                    {"id": related_event_id, "slug": f"event-{related_event_id}"}
                ],
            }
        ],
    }


def _patch_series(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        catalog,
        "gamma_get",
        lambda *_args, **_kwargs: [{"id": "series-1", "slug": "soccer-fifwc"}],
    )


def test_catalog_helpers_reject_or_ignore_malformed_nested_payloads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 8, 2, tzinfo=timezone.utc)
    assert catalog._tag_rows({"tags": []}, observed_at) == []
    assert (
        catalog._tag_rows(
            {
                "id": "1",
                "tags": ["bad", {}, {"slug": " Direct-Tag "}],
            },
            observed_at,
        )[0]["tag_slug"]
        == "direct-tag"
    )
    assert catalog._series_slugs({"seriesSlug": " Direct-Series "}) == ["direct-series"]
    assert catalog._event_market_rows({}, observed_at) == ([], [])

    bridges, markets = catalog._event_market_rows(
        {
            "id": "1",
            "slug": "event-1",
            "markets": [
                "bad",
                {},
                {
                    "id": "market-1",
                    "events": ["bad", {}, {"id": "2"}, {"id": "2"}],
                },
            ],
        },
        observed_at,
    )
    assert [(row["event_id"], row["source_ordinal"]) for row in bridges] == [
        ("2", 2),
        ("1", 1),
    ]
    assert [item["id"] for item in markets[0]["events"]] == ["2", "1"]

    merged_market = catalog._merge_market_payload(
        {
            "eventGameId": None,
            "eventTitle": None,
            "events": [{"id": "1"}],
            "fallback": "previous",
        },
        {
            "eventGameId": "game",
            "eventTitle": "title",
            "events": ["bad", {"id": "2"}],
            "fallback": None,
        },
    )
    assert merged_market["fallback"] == "previous"
    assert [item["id"] for item in merged_market["events"]] == ["2", "1"]

    assert catalog._referenced_event_ids(
        [{"markets": ["bad", {"events": [{}, {"id": "2"}]}]}]
    ) == {"2"}
    inventory, child_markets, memberships = catalog._partition_inventory(
        {
            "1": {
                "markets": [
                    "bad",
                    {},
                    {"id": "market-1", "events": ["bad", {}, {"id": "2"}]},
                ]
            }
        }
    )
    assert inventory == (("1", (("market-1", ("1", "2")),)),)
    assert (child_markets, memberships) == (1, 2)
    assert catalog._merge_event_payloads([{}, _event()])["1"]["id"] == "1"

    monkeypatch.setattr(catalog, "gamma_get", lambda *_a, **_k: [])
    with pytest.raises(RuntimeError, match="exactly one"):
        catalog._fixture_series_id(object())


def test_catalog_converges_on_membership_inventory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_series(monkeypatch)

    def pages(*_args: Any, **_kwargs: Any):
        yield [_event()], EventsPageMeta(pages_done=1, truncated=False)

    monkeypatch.setattr(catalog, "iter_gamma_events_keyset", pages)
    batch = catalog.collect_wc2026_event_catalog(
        client=object(), observed_at=datetime(2026, 8, 2, tzinfo=timezone.utc)
    )

    assert len(batch.summary["scan_partitions"]) == 10
    assert {
        "related_2026_tag_recall:open",
        "related_2026_tag_recall:closed",
    } <= set(batch.summary["scan_partitions"])
    for partition in batch.summary["scan_partitions"].values():
        assert partition["event_count"] == 1
        assert partition["child_market_count"] == 1
        assert partition["membership_count"] == 1
        assert len(partition["membership_inventory_sha256"]) == 64
        assert len(partition["attempts"]) == 2


def test_catalog_rejects_membership_drift_with_stable_event_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_series(monkeypatch)
    calls = 0

    def pages(*_args: Any, **_kwargs: Any):
        nonlocal calls
        calls += 1
        related = "1" if calls % 2 else "2"
        yield (
            [_event(related_event_id=related)],
            EventsPageMeta(pages_done=1, truncated=False),
        )

    monkeypatch.setattr(catalog, "iter_gamma_events_keyset", pages)
    with pytest.raises(RuntimeError, match="scan_unstable"):
        catalog.collect_wc2026_event_catalog(client=object())


def test_catalog_rejects_any_truncated_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_series(monkeypatch)

    def pages(*_args: Any, **_kwargs: Any):
        yield [_event()], EventsPageMeta(pages_done=1, truncated=True)

    monkeypatch.setattr(catalog, "iter_gamma_events_keyset", pages)
    with pytest.raises(RuntimeError, match="scan truncated"):
        catalog.collect_wc2026_event_catalog(client=object())


def test_catalog_scan_ignores_non_mapping_and_unidentified_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_series(monkeypatch)

    def pages(*_args: Any, **_kwargs: Any):
        yield ["bad", {}, _event()], EventsPageMeta(pages_done=1, truncated=False)

    monkeypatch.setattr(catalog, "iter_gamma_events_keyset", pages)

    batch = catalog.collect_wc2026_event_catalog(client=object())

    assert {row["event_id"] for row in batch.event_snapshots} == {"1"}


def test_catalog_skips_merged_market_payload_without_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_series(monkeypatch)

    def pages(*_args: Any, **_kwargs: Any):
        yield [_event(markets=[])], EventsPageMeta(pages_done=1, truncated=False)

    monkeypatch.setattr(catalog, "iter_gamma_events_keyset", pages)
    monkeypatch.setattr(
        catalog,
        "_event_market_rows",
        lambda *_args: ([], [{}]),
    )

    batch = catalog.collect_wc2026_event_catalog(client=object())

    assert batch.market_payloads == ()


def test_catalog_fetches_dangling_events_and_preserves_all_memberships(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_series(monkeypatch)
    source = _event(
        markets=[
            {
                "id": "market-1",
                "events": [
                    {"id": "2", "slug": "event-2"},
                    {"id": "1", "slug": "event-1"},
                ],
            }
        ]
    )

    def pages(*_args: Any, **_kwargs: Any):
        yield [source], EventsPageMeta(pages_done=1, truncated=False)

    monkeypatch.setattr(catalog, "iter_gamma_events_keyset", pages)
    monkeypatch.setattr(
        catalog,
        "fetch_gamma_event_by_id",
        lambda _client, event_id: _event(event_id, markets=[]),
    )
    batch = catalog.collect_wc2026_event_catalog(client=object())

    assert {row["event_id"] for row in batch.event_snapshots} == {"1", "2"}
    source_endpoints = {
        row["event_id"]: row["source_endpoint"] for row in batch.event_snapshots
    }
    assert source_endpoints == {"1": "/events/keyset", "2": "/events/2"}
    assert [
        (row["event_id"], row["source_ordinal"], row["is_enclosing_event"])
        for row in batch.event_market_snapshots
    ] == [("1", 1, True), ("2", 0, False)]


def test_catalog_rejects_missing_dangling_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_series(monkeypatch)

    def pages(*_args: Any, **_kwargs: Any):
        yield (
            [_event(related_event_id="2")],
            EventsPageMeta(
                pages_done=1,
                truncated=False,
            ),
        )

    monkeypatch.setattr(catalog, "iter_gamma_events_keyset", pages)
    monkeypatch.setattr(catalog, "fetch_gamma_event_by_id", lambda *_args: None)

    with pytest.raises(RuntimeError, match="references missing Gamma event 2"):
        catalog.collect_wc2026_event_catalog(client=object())


def test_catalog_duplicate_bridge_keeps_enclosing_flag_with_minimum_ordinal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_series(monkeypatch)
    enclosing = _event(
        "1",
        markets=[
            {
                "id": "market-shared",
                "events": [
                    {"id": "2", "slug": "event-2"},
                    {"id": "1", "slug": "event-1"},
                ],
            }
        ],
    )
    duplicate = _event(
        "2",
        markets=[
            {
                "id": "market-shared",
                "events": [
                    {"id": "1", "slug": "event-1"},
                    {"id": "2", "slug": "event-2"},
                ],
            }
        ],
    )

    def pages(*_args: Any, **_kwargs: Any):
        yield [enclosing, duplicate], EventsPageMeta(pages_done=1, truncated=False)

    monkeypatch.setattr(catalog, "iter_gamma_events_keyset", pages)
    batch = catalog.collect_wc2026_event_catalog(client=object())

    rows = {
        (row["event_id"], row["market_id"]): row for row in batch.event_market_snapshots
    }
    assert rows[("1", "market-shared")]["source_ordinal"] == 0
    assert rows[("1", "market-shared")]["is_enclosing_event"] is True
    assert rows[("2", "market-shared")]["source_ordinal"] == 0
    assert rows[("2", "market-shared")]["is_enclosing_event"] is True


def test_catalog_keeps_unknown_volume_without_applying_child_market_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_series(monkeypatch)
    event = _event(volume="not-a-number")
    event["markets"][0]["volumeNum"] = 1

    requests: list[dict[str, Any]] = []

    def pages(*_args: Any, **kwargs: Any):
        requests.append(kwargs)
        yield [event], EventsPageMeta(pages_done=1, truncated=False)

    monkeypatch.setattr(catalog, "iter_gamma_events_keyset", pages)
    batch = catalog.collect_wc2026_event_catalog(client=object())

    assert batch.event_snapshots[0]["event_volume_usd_lifetime_reported"] is None
    assert batch.summary["volume_unknown_events"] == 1
    assert batch.summary["volume_scan_floor_usd"] is None
    assert {row["id"] for row in batch.market_payloads} == {"market-1"}
    assert requests
    assert all(request["keyset_volume_min"] is None for request in requests)


@pytest.mark.parametrize("volume", [-1.0, float("nan"), float("inf"), float("-inf")])
def test_catalog_treats_invalid_volume_as_unknown(
    monkeypatch: pytest.MonkeyPatch, volume: float
) -> None:
    _patch_series(monkeypatch)

    def pages(*_args: Any, **_kwargs: Any):
        yield [_event(volume=volume)], EventsPageMeta(pages_done=1, truncated=False)

    monkeypatch.setattr(catalog, "iter_gamma_events_keyset", pages)
    batch = catalog.collect_wc2026_event_catalog(client=object())

    assert batch.event_snapshots[0]["event_volume_usd_lifetime_reported"] is None
    assert batch.summary["eligible_events_as_observed"] == 0
    assert batch.summary["volume_unknown_events"] == 1


def test_slug_prefix_recall_uses_unfiltered_keyset_and_local_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_series(monkeypatch)
    requests: list[dict[str, Any]] = []
    match = _event("match", related_event_id="match")
    miss = _event("miss", related_event_id="miss")
    miss["slug"] = "unrelated-event"

    def pages(*_args: Any, **kwargs: Any):
        requests.append(kwargs)
        yield [match, miss], EventsPageMeta(pages_done=1, truncated=False)

    monkeypatch.setattr(catalog, "iter_gamma_events_keyset", pages)
    batch = catalog.collect_wc2026_event_catalog(client=object())

    assert (
        batch.summary["scan_partitions"]["wc2026_event_slug_prefix_recall:open"][
            "event_count"
        ]
        == 1
    )
    slug_requests = [
        request
        for request in requests
        if "wc2026_event_catalog_wc2026_event_slug_prefix_recall"
        in request["progress_task"]
    ]
    assert slug_requests
    assert all(request["keyset_tag_slug"] is None for request in slug_requests)
    assert all(request["keyset_series_id"] is None for request in slug_requests)
    rows = {row["event_id"]: row for row in batch.event_snapshots}
    assert "wc2026_event_slug_prefix_recall" in rows["match"]["candidate_sources_json"]
    assert (
        "wc2026_event_slug_prefix_recall" not in rows["miss"]["candidate_sources_json"]
    )


def test_related_tag_recall_accepts_related_only_event_as_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_series(monkeypatch)
    related = _event("related", related_event_id="related")
    related["slug"] = "unrelated-event"
    related["tags"] = [{"id": "tag-soccer", "slug": "soccer"}]
    related["series"] = []
    requests: list[dict[str, Any]] = []

    def pages(*_args: Any, **kwargs: Any):
        requests.append(kwargs)
        events = [related] if kwargs["keyset_related_tags"] else []
        yield events, EventsPageMeta(pages_done=1, truncated=False)

    monkeypatch.setattr(catalog, "iter_gamma_events_keyset", pages)
    batch = catalog.collect_wc2026_event_catalog(client=object())

    assert {row["event_id"] for row in batch.event_snapshots} == {"related"}
    row = batch.event_snapshots[0]
    assert row["candidate_sources_json"] == '["related_2026_tag_recall"]'
    assert catalog.WC2026_EVENT_TAG not in row["tags_json"]
    related_requests = [
        request for request in requests if request["keyset_related_tags"]
    ]
    assert related_requests
    assert all(
        request["keyset_tag_slug"] == catalog.WC2026_EVENT_TAG
        for request in related_requests
    )
