"""Unit tests for selected-scope predicates."""

from __future__ import annotations

from unittest.mock import MagicMock

from tests.unit.ingestion.market_scope_test_support import slug_only_cfg

from oddsfox_pipeline.ingestion.polymarket import market_scope as scope_mod
from oddsfox_pipeline.ingestion.polymarket.market_scope import (
    MARKET_SCOPE_ALL,
    MarketScopeConfig,
    collect_scope_markets_from_events,
    event_in_scope,
    event_matches_scope_config,
    event_matches_scope_tags,
    is_market_scope_row,
    load_market_scope_config,
)
from oddsfox_pipeline.ingestion.polymarket.market_scope import (
    predicates as scope_predicates_mod,
)


def test_event_in_scope_rejects_related_pass_without_wc_tag():
    cfg = MarketScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
        event_tags=("fifa-world-cup",),
    )
    event = {
        "slug": "unrelated-esports-finals",
        "tags": [{"slug": "esports"}],
    }
    assert not event_in_scope(
        event,
        config=cfg,
        keyset_tag_slug="fifa-world-cup",
        keyset_related_tags=True,
        scope_tag_slugs=cfg.event_tags,
    )


def test_event_in_scope_related_pass_keeps_wc_tagged_event():
    cfg = MarketScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
        event_tags=("fifa-world-cup",),
    )
    event = {
        "slug": "world-cup-group-a-winner",
        "tags": [{"slug": "fifa-world-cup"}],
    }
    assert event_in_scope(
        event,
        config=cfg,
        keyset_tag_slug="fifa-world-cup",
        keyset_related_tags=True,
        scope_tag_slugs=cfg.event_tags,
    )


def test_event_in_scope_matches_tag_without_prefix_slug():
    cfg = MarketScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
        event_tags=("2026-fifa-world-cup", "fifa-world-cup"),
    )
    event = {
        "slug": "world-cup-group-a-winner",
        "tags": [{"slug": "fifa-world-cup"}, {"slug": "soccer"}],
    }
    assert event_in_scope(event, config=cfg)
    assert event_in_scope(event, config=cfg, keyset_tag_slug="fifa-world-cup")
    assert event_matches_scope_tags(event, config=cfg)
    assert not event_matches_scope_config("world-cup-group-a-winner", config=cfg)


def test_event_in_scope_rejects_crawl_only_discovered_tag():
    """Crawl-only tags must not widen strict scope admission."""
    cfg = MarketScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
        event_tags=("fifa-world-cup", "2026-fifa-world-cup", "world-cup"),
    )
    event = {
        "slug": "copa-america-final",
        "tags": [{"slug": "argentina"}],
    }
    assert not event_in_scope(
        event,
        config=cfg,
        keyset_tag_slug="argentina",
        scope_tag_slugs=cfg.event_tags,
    )


def test_crawl_tag_allowed_skips_broad_and_keeps_wc_tags():
    scope = ("fifa-world-cup",)
    seed = ("fifa-world-cup",)
    denylist = ("sports", "portugal")
    assert scope_predicates_mod._parse_tag_discovery_keywords(" A, b ") == ("a", "b")
    assert not scope_mod._crawl_tag_allowed(
        "sports", scope_tags=scope, seed_tags=seed, denylist=denylist
    )
    assert not scope_mod._crawl_tag_allowed(
        "portugal", scope_tags=scope, seed_tags=seed, denylist=denylist
    )
    assert scope_mod._crawl_tag_allowed(
        "world-cup-qualifiers",
        scope_tags=scope,
        seed_tags=seed,
        denylist=denylist,
        keyword_gate=True,
    )
    assert scope_mod._crawl_tag_allowed(
        "fifa-world-cup", scope_tags=scope, seed_tags=seed, denylist=denylist
    )
    assert scope_mod._crawl_tag_allowed(
        "argentina",
        scope_tags=scope,
        seed_tags=seed,
        denylist=denylist,
        keyword_gate=False,
    )


def test_crawl_tag_allowed_scope_seed_always_crawl_even_when_denylisted():
    scope = ("sports",)
    seed = ("sports",)
    denylist = ("sports",)
    assert scope_mod._crawl_tag_allowed(
        "sports", scope_tags=scope, seed_tags=seed, denylist=denylist
    )


def test_crawl_tag_allowed_denylist_blocks_keyword_match():
    scope = ("fifa-world-cup",)
    seed = ("fifa-world-cup",)
    denylist = ("world-cup-qualifiers",)
    assert not scope_mod._crawl_tag_allowed(
        "world-cup-qualifiers",
        scope_tags=scope,
        seed_tags=seed,
        denylist=denylist,
        keyword_gate=True,
    )


def test_is_market_scope_row_matches_event_tags():
    cfg = MarketScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
        event_tags=("fifa-world-cup",),
    )
    assert is_market_scope_row(
        market_id="x",
        event_slug="world-cup-group-a-winner",
        event_tags=("fifa-world-cup",),
        config=cfg,
    )


def test_is_market_scope_strict_by_allowlisted_event_slug():
    cfg = MarketScopeConfig(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=("2026-fifa-world-cup",),
        market_ids=(),
        registry_max_event_pages=None,
    )
    assert is_market_scope_row(
        market_id="1",
        event_slug="2026-fifa-world-cup-winner-595",
        config=cfg,
    )


def test_is_market_scope_strict_excludes_unrelated_market():
    cfg = load_market_scope_config()
    assert not is_market_scope_row(
        market_id="x",
        question="Premier League 2026",
        description="No world cup here",
        config=cfg,
    )


def test_event_matches_scope_config_prefix():
    cfg = MarketScopeConfig(
        event_slugs=(),
        event_slug_prefixes=("2026-fifa-world-cup",),
        market_ids=(),
        registry_max_event_pages=None,
    )
    assert event_matches_scope_config("2026-fifa-world-cup-winner-595", config=cfg)


def test_keyset_tag_pass_keeps_non_prefix_event_slug():
    cfg = MarketScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
        event_tags=("fifa-world-cup", "2026-fifa-world-cup"),
    )
    client = MagicMock()
    client.get.return_value = {
        "events": [
            {
                "id": "ev-group-a",
                "slug": "world-cup-group-a-winner",
                "tags": [{"slug": "fifa-world-cup"}],
                "markets": [{"id": "m-group-a"}],
            },
        ],
        "next_cursor": None,
    }
    markets, meta = collect_scope_markets_from_events(
        client,
        config=cfg,
        max_pages=5,
        keyset_tag_slugs=["fifa-world-cup"],
    )
    assert len(markets) == 1
    assert markets[0]["id"] == "m-group-a"
    params = client.get.call_args.kwargs.get("params") or {}
    assert params.get("tag_slug") == "fifa-world-cup"
    assert meta["keyset_tag_slugs"] == ["fifa-world-cup"]


def test_resolve_market_scope_discovery_validates_explicit_tag_slugs():
    import pytest

    cfg = slug_only_cfg()
    resolved = scope_predicates_mod.resolve_market_scope_discovery(
        cfg,
        max_pages=None,
        max_pages_without_progress=None,
        keyset_tag_slugs=["FIFA-World-Cup"],
    )
    assert resolved.keyset_tag_slugs == ("fifa-world-cup",)
    with pytest.raises(ValueError, match="Invalid event slug token"):
        scope_predicates_mod.resolve_market_scope_discovery(
            cfg,
            max_pages=None,
            max_pages_without_progress=None,
            keyset_tag_slugs=["bad tag"],
        )


def test_resolve_keyset_crawl_tags_discovery_failure(monkeypatch):
    cfg = slug_only_cfg(event_tags=("fifa-world-cup",))
    client = MagicMock()

    def _boom(*_a: object, **_k: object) -> object:
        raise RuntimeError("discovery down")

    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.market_scope_tags.discover_market_scope_tag_slugs",
        _boom,
    )
    slugs, sources = scope_predicates_mod.resolve_keyset_crawl_tags(
        None,
        config=cfg,
        client=client,
        tag_discovery=True,
    )
    assert slugs == ["fifa-world-cup"]
    assert sources["fifa-world-cup"] == {"seed"}


def test_resolve_keyset_crawl_tags_discovery_no_log_when_unchanged(monkeypatch):
    from types import SimpleNamespace

    cfg = slug_only_cfg(event_tags=("fifa-world-cup",))
    client = MagicMock()
    discovered = SimpleNamespace(
        tag_slugs=["fifa-world-cup"],
        sources={"fifa-world-cup": {"discovered"}},
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.market_scope_tags.discover_market_scope_tag_slugs",
        lambda *a, **k: discovered,
    )
    slugs, _sources = scope_predicates_mod.resolve_keyset_crawl_tags(
        None,
        config=cfg,
        client=client,
        tag_discovery=True,
    )
    assert slugs == ["fifa-world-cup"]


def test_resolve_keyset_crawl_tags_discovery_expands(monkeypatch):
    from types import SimpleNamespace

    cfg = slug_only_cfg(event_tags=("fifa-world-cup",))
    client = MagicMock()
    discovered = SimpleNamespace(
        tag_slugs=["fifa-world-cup", "extra-tag"],
        sources={"extra-tag": {"discovered"}},
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.market_scope_tags.discover_market_scope_tag_slugs",
        lambda *a, **k: discovered,
    )
    slugs, sources = scope_predicates_mod.resolve_keyset_crawl_tags(
        None,
        config=cfg,
        client=client,
        tag_discovery=True,
    )
    assert slugs == ["extra-tag", "fifa-world-cup"]
    assert sources["extra-tag"] == {"discovered"}
    assert sources["fifa-world-cup"] == {"seed"}


def test_event_tag_slugs_skips_blank_slug() -> None:
    assert scope_predicates_mod._event_tag_slugs(
        {"tags": ["not-a-dict", {"slug": ""}, {"slug": "  "}, {"slug": "WC"}]}
    ) == frozenset({"wc"})


def test_parse_tag_discovery_keywords_default() -> None:
    assert scope_predicates_mod._parse_tag_discovery_keywords(None)
    assert scope_predicates_mod._parse_tag_discovery_keywords("  ")


def test_predicate_helpers_cover_remaining_branches() -> None:
    assert scope_predicates_mod.event_matches_scope_tags(None) is False
    assert scope_predicates_mod.event_in_scope(None) is False
    assert (
        scope_predicates_mod._crawl_tag_allowed(None, scope_tags=(), seed_tags=())
        is True
    )
    assert (
        scope_predicates_mod._crawl_tag_allowed(
            "  ", scope_tags=("fifa-world-cup",), seed_tags=()
        )
        is False
    )
    cfg = slug_only_cfg(event_tags=("fifa-world-cup",))
    assert scope_predicates_mod.is_market_scope_row(
        market_id="x",
        event_slug="2026-fifa-world-cup-extra",
        market_scope=MARKET_SCOPE_ALL,
    )
    assert scope_predicates_mod.is_market_scope_row(
        market_id="x",
        event_slug="2026-fifa-world-cup-extra",
        config=cfg,
    )
    assert scope_predicates_mod.is_market_scope_row(
        market_id="x",
        event_tags=["fifa-world-cup"],
        config=cfg,
    )
    assert not scope_predicates_mod.is_market_scope_row(
        market_id="zzz",
        event_tags=["unrelated"],
        config=cfg,
    )
    denied = scope_predicates_mod._filter_crawl_tag_slugs(
        ["blocked-tag"],
        scope_tags=("fifa-world-cup",),
        seed_tags=(),
    )
    assert denied == []
