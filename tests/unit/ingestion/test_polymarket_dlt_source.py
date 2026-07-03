from oddsfox_pipeline.ingestion.polymarket.dlt_source import (
    collect_raw_markets,
    normalize_market_payloads_for_dlt,
    polymarket_markets_source,
)
from oddsfox_pipeline.storage.duckdb.dlt_batch import DLT_STRICT_SCHEMA_CONTRACT


def test_polymarket_markets_source_yields_prefetched_rows():
    rows = [
        {
            "id": "m1",
            "question": "Who will win?",
            "category": "Sports",
            "description": "Winner market",
            "outcomes": '["Yes", "No"]',
            "volume": 100.0,
            "active": True,
            "closed": False,
            "created_at": "2025-01-01 00:00:00",
            "scraped_at": "2025-01-02 00:00:00",
            "end_date": "2026-07-19 00:00:00",
            "slug": "2026-fifa-world-cup-winner",
            "event_slug": "2026-fifa-world-cup-winner",
            "event_id": "99",
        }
    ]
    resource = polymarket_markets_source(rows=rows).resources["markets"]

    assert list(resource) == rows


def test_markets_resource_has_frozen_columns_and_types_contract():
    resource = polymarket_markets_source().resources["markets"]

    assert resource.schema_contract == DLT_STRICT_SCHEMA_CONTRACT
    assert resource.columns["id"]["data_type"] == "text"
    assert resource.columns["volume"]["data_type"] == "double"
    assert resource.columns["created_at"]["data_type"] == "timestamp"


def test_normalize_market_payloads_for_dlt_matches_raw_market_contract():
    rows = normalize_market_payloads_for_dlt(
        [
            {
                "id": "m1",
                "question": "Who will win the 2026 FIFA World Cup?",
                "category": "Sports",
                "description": "Winner market",
                "outcomes": ["Yes", "No"],
                "volumeNum": "12345.67",
                "active": True,
                "closed": False,
                "createdAt": "2025-01-01T00:00:00Z",
                "endDate": "2026-07-19T00:00:00Z",
                "clobTokenIds": ["tok_yes", "tok_no"],
                "slug": "2026-fifa-world-cup-winner",
                "events": [{"id": 99, "slug": "2026-fifa-world-cup-winner"}],
            }
        ]
    )

    assert rows == [
        {
            "id": "m1",
            "question": "Who will win the 2026 FIFA World Cup?",
            "category": "Sports",
            "description": "Winner market",
            "outcomes": '["Yes", "No"]',
            "volume": 12345.67,
            "active": True,
            "closed": False,
            "created_at": "2025-01-01 00:00:00",
            "scraped_at": rows[0]["scraped_at"],
            "end_date": "2026-07-19 00:00:00",
            "slug": "2026-fifa-world-cup-winner",
            "event_slug": "2026-fifa-world-cup-winner",
            "event_id": "99",
        }
    ]


def test_normalize_market_payloads_for_dlt_dedupes_by_id_last_wins():
    base = {
        "category": "Sports",
        "description": "Winner market",
        "outcomes": ["Yes", "No"],
        "volumeNum": "100.0",
        "active": True,
        "closed": False,
        "createdAt": "2025-01-01T00:00:00Z",
        "endDate": "2026-07-19T00:00:00Z",
        "clobTokenIds": ["tok_yes", "tok_no"],
        "slug": "2026-fifa-world-cup-winner",
        "events": [{"id": 99, "slug": "2026-fifa-world-cup-winner"}],
    }
    rows = normalize_market_payloads_for_dlt(
        [
            {"id": "dup", "question": "old question", **base},
            {"id": "dup", "question": "new question", **base},
        ]
    )

    assert len(rows) == 1
    assert rows[0]["id"] == "dup"
    assert rows[0]["question"] == "new question"


def test_collect_raw_markets_targeted(monkeypatch):
    client = object()
    config = object()
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.dlt_source.build_client", lambda: client
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.dlt_source.load_market_scope_config",
        lambda **_kwargs: config,
    )

    seen = {}

    def targeted(got_client, *, config):
        seen["client"] = got_client
        seen["config"] = config
        return {}, [{"id": "m1"}], {}

    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.dlt_source.refresh_registry_and_collect_markets_targeted",
        targeted,
    )

    assert collect_raw_markets(discovery_mode="targeted") == [{"id": "m1"}]
    assert seen == {"client": client, "config": config}


def test_collect_raw_markets_full_keyset_resolves_tags(monkeypatch):
    client = object()
    config = object()
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.dlt_source.build_client", lambda: client
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.dlt_source.load_market_scope_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.dlt_source.resolve_keyset_tag_slugs",
        lambda slugs, *, config, client: ["world-cup"],
    )

    seen = {}

    def from_events(got_client, **kwargs):
        seen["client"] = got_client
        seen.update(kwargs)
        return {}, [{"id": "m2"}], {}

    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.dlt_source.refresh_registry_and_collect_markets_from_events",
        from_events,
    )

    assert collect_raw_markets(
        max_event_pages=3,
        max_pages_without_progress=2,
        keyset_closed=None,
        keyset_tag_slugs=["seed"],
        keyset_volume_min=None,
    ) == [{"id": "m2"}]
    assert seen["client"] is client
    assert seen["config"] is config
    assert seen["max_pages"] == 3
    assert seen["max_pages_without_progress"] == 2
    assert seen["keyset_closed"] is None
    assert seen["keyset_tag_slugs"] == ["world-cup"]
    assert seen["keyset_volume_min"] is None
