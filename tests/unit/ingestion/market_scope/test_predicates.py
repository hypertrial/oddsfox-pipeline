"""Unit tests for WC2026 scope predicates."""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from hypothesis import given
from hypothesis import strategies as st
from tests.unit.ingestion.market_scope.support import slug_only_cfg

from oddsfox_pipeline.ingestion.polymarket import market_scope as scope_mod
from oddsfox_pipeline.ingestion.polymarket.market_scope import (
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
from oddsfox_pipeline.ingestion.polymarket.market_scope.config import (
    _validate_slug_token,
)
from oddsfox_pipeline.ingestion.polymarket.market_scope_tags import (
    _normalize_slug_token,
)

_VALID_TAG_SLUGS = st.from_regex(r"[A-Za-z0-9][A-Za-z0-9-]{0,32}", fullmatch=True)
_INVALID_SCOPE_SLUGS = st.one_of(
    st.text(min_size=0, max_size=16).map(lambda value: f"-{value}"),
    st.text(min_size=0, max_size=16).map(lambda value: f"a/{value}"),
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


@given(_VALID_TAG_SLUGS)
def test_scope_slug_normalization_property_lowercases_valid_slugs(slug):
    expected = slug.lower()

    assert _validate_slug_token(f" {slug.upper()} ") == expected
    assert _normalize_slug_token(f" {slug.upper()} ") == expected


@given(_VALID_TAG_SLUGS)
def test_event_scope_tag_property_matches_normalized_tags(slug):
    normalized = slug.lower()
    cfg = MarketScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
        event_tags=(normalized,),
    )
    event = {"slug": "anything", "tags": [{"slug": slug.upper()}]}

    assert event_matches_scope_tags(event, config=cfg)
    assert event_in_scope(event, config=cfg)


@given(_INVALID_SCOPE_SLUGS)
def test_invalid_scope_slug_property_rejects_unsafe_tokens(value):
    with pytest.raises(ValueError):
        _validate_slug_token(value)


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
        category="sports",
        description="No world cup here",
        slug="premier-league-2026",
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
    import pytest

    with pytest.raises(ValueError, match="Unknown Polymarket market scope"):
        scope_predicates_mod.is_market_scope_row(
            market_id="x",
            event_slug="2026-fifa-world-cup-extra",
            market_scope="all",
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


def test_resolve_market_scope_discovery_preserves_every_effective_option() -> None:
    cfg = slug_only_cfg(
        event_tags=("scope-tag",),
        keyset_closed=False,
        keyset_related_tags=False,
        keyset_volume_min=3.5,
        tag_discovery=False,
        registry_max_event_pages=41,
        tag_closure_rounds=2,
        tag_crawl_max=17,
    )

    resolved = scope_predicates_mod.resolve_market_scope_discovery(
        cfg,
        max_pages=7,
        max_pages_without_progress=11,
        keyset_closed=True,
        keyset_tag_slugs=(" Explicit-Tag ",),
        keyset_related_tags=True,
        keyset_volume_min=12.5,
        tag_discovery=True,
    )

    assert resolved == scope_predicates_mod.ResolvedMarketScopeDiscovery(
        keyset_closed=True,
        keyset_related_tags=True,
        keyset_volume_min=12.5,
        keyset_tag_slugs=("explicit-tag",),
        tag_discovery=True,
        pass_page_cap=7,
        total_page_budget=7,
        max_pages_without_progress=11,
        scope_tag_slugs=("scope-tag",),
        scope_for_passes=("scope-tag",),
        seed_tag_slugs=("scope-tag",),
        max_closure_rounds=2,
        max_crawl_tags=17,
    )

    uncapped = scope_predicates_mod.resolve_market_scope_discovery(
        slug_only_cfg(
            event_tags=(),
            registry_max_event_pages=41,
            keyset_volume_min=9.5,
            tag_closure_rounds=-1,
            tag_crawl_max=0,
        ),
        max_pages=None,
        max_pages_without_progress=None,
    )
    assert uncapped.pass_page_cap == 41
    assert uncapped.keyset_closed is False
    assert uncapped.keyset_related_tags is False
    assert uncapped.keyset_volume_min == 9.5
    assert uncapped.scope_for_passes is None
    assert uncapped.max_closure_rounds == 0
    assert uncapped.max_crawl_tags is None
    one = scope_predicates_mod.resolve_market_scope_discovery(
        slug_only_cfg(tag_crawl_max=1),
        max_pages=None,
        max_pages_without_progress=None,
    )
    assert one.max_crawl_tags == 1


def test_keyset_option_helpers_distinguish_explicit_false_and_zero() -> None:
    cfg = slug_only_cfg(
        keyset_closed=True,
        keyset_related_tags=False,
        keyset_volume_min=9.5,
    )

    assert scope_predicates_mod._resolve_keyset_closed(False, cfg) is False
    assert scope_predicates_mod._resolve_keyset_closed(None, cfg) is True
    assert scope_predicates_mod._resolve_keyset_closed(None, None) is None
    assert scope_predicates_mod._resolve_keyset_related_tags(True, cfg) is True
    assert scope_predicates_mod._resolve_keyset_related_tags(None, cfg) is False
    assert scope_predicates_mod._resolve_keyset_related_tags(None, None) is True
    assert scope_predicates_mod._resolve_keyset_volume_min(0.0, cfg) == 0.0
    assert scope_predicates_mod._resolve_keyset_volume_min(None, cfg) == 9.5
    assert scope_predicates_mod._resolve_keyset_volume_min(None, None) is None


def test_crawl_tag_rules_normalize_each_collection_independently() -> None:
    assert scope_predicates_mod._crawl_tag_allowed(
        " SCOPE ",
        scope_tags=(" scope ",),
        seed_tags=(),
        denylist=("scope",),
    )
    assert scope_predicates_mod._crawl_tag_allowed(
        " SEED ",
        scope_tags=(),
        seed_tags=(" seed ",),
        denylist=("seed",),
    )
    assert not scope_predicates_mod._crawl_tag_allowed(
        " BLOCKED ",
        scope_tags=(),
        seed_tags=(),
        denylist=(" blocked ",),
        keyword_gate=False,
    )
    assert scope_predicates_mod._crawl_tag_allowed(
        "anything",
        scope_tags=(),
        seed_tags=(),
        denylist=(),
        keyword_gate=False,
    )
    assert not scope_predicates_mod._crawl_tag_allowed(
        "anything",
        scope_tags=(),
        seed_tags=(),
        denylist=(),
    )


def test_filter_crawl_tags_passes_policy_and_logs_exact_rejection(
    monkeypatch, caplog
) -> None:
    cfg = slug_only_cfg(
        tag_crawl_denylist=("blocked",),
        tag_closure_keyword_gate=True,
        tag_discovery_keywords=("world",),
    )
    calls: list[tuple[object, ...]] = []

    def allowed(slug, **kwargs):
        calls.append((slug, kwargs))
        return slug == "kept"

    monkeypatch.setattr(scope_predicates_mod, "_crawl_tag_allowed", allowed)
    caplog.set_level(logging.INFO, logger=scope_predicates_mod.__name__)

    assert scope_predicates_mod._filter_crawl_tag_slugs(
        ("kept", "blocked"),
        scope_tags=("scope",),
        seed_tags=("seed",),
        config=cfg,
    ) == ["kept"]
    assert calls == [
        (
            "kept",
            {
                "scope_tags": ("scope",),
                "seed_tags": ("seed",),
                "denylist": ("blocked",),
                "keyword_gate": True,
                "keywords": ("world",),
            },
        ),
        (
            "blocked",
            {
                "scope_tags": ("scope",),
                "seed_tags": ("seed",),
                "denylist": ("blocked",),
                "keyword_gate": True,
                "keywords": ("world",),
            },
        ),
    ]
    assert caplog.messages == [
        "Skipping market-scope crawl tag blocked (denylist or keyword gate)"
    ]


def test_event_predicates_reject_wrong_shapes_and_use_exact_inputs(monkeypatch) -> None:
    cfg = slug_only_cfg(
        event_slugs=("exact-event",),
        event_slug_prefixes=("prefix-",),
        event_tags=("scope-tag",),
    )

    assert not scope_predicates_mod.event_matches_scope_tags([], config=cfg)
    assert not scope_predicates_mod.event_matches_scope_tags("event", config=cfg)
    assert not scope_predicates_mod.event_matches_scope_tags(
        {"tags": [{"slug": "scope-tag"}]},
        config=slug_only_cfg(event_tags=()),
    )
    assert scope_predicates_mod.event_matches_scope_config(" EXACT-EVENT ", config=cfg)
    assert scope_predicates_mod.event_matches_scope_config("PREFIX-child", config=cfg)
    assert not scope_predicates_mod.event_matches_scope_config(None, config=cfg)
    assert not scope_predicates_mod.event_in_scope([], config=cfg)
    assert not scope_predicates_mod.event_in_scope("event", config=cfg)
    assert scope_predicates_mod.event_in_scope(
        {"slug": "unrelated"},
        config=cfg,
        keyset_tag_slug="scope-tag",
    )
    assert scope_predicates_mod.event_in_scope(
        {"slug": " EXACT-EVENT "},
        config=cfg,
    )

    calls: list[tuple[object, object, object]] = []

    def matches_tags(event, *, config, scope_tag_slugs):
        calls.append((event, config, scope_tag_slugs))
        return False

    monkeypatch.setattr(scope_predicates_mod, "event_matches_scope_tags", matches_tags)
    event = {"slug": "unrelated"}
    assert scope_predicates_mod.event_in_scope(
        event,
        config=cfg,
        keyset_tag_slug="scope-tag",
        keyset_related_tags=False,
        scope_tag_slugs=("scope-tag",),
    )
    assert not scope_predicates_mod.event_in_scope(
        event,
        config=cfg,
        keyset_tag_slug="other-tag",
        keyset_related_tags=False,
        scope_tag_slugs=("scope-tag",),
    )
    assert calls == [(event, cfg, ("scope-tag",))]


def test_event_in_scope_forwards_normal_branch_arguments(monkeypatch) -> None:
    cfg = slug_only_cfg(event_tags=("scope-tag",))
    event = {"slug": "event-slug"}
    config_calls: list[tuple[object, object]] = []
    tag_calls: list[tuple[object, object, object]] = []

    def matches_config(slug, *, config):
        config_calls.append((slug, config))
        return False

    def matches_tags(candidate, *, config, scope_tag_slugs):
        tag_calls.append((candidate, config, scope_tag_slugs))
        return True

    monkeypatch.setattr(
        scope_predicates_mod, "event_matches_scope_config", matches_config
    )
    monkeypatch.setattr(scope_predicates_mod, "event_matches_scope_tags", matches_tags)

    assert scope_predicates_mod.event_in_scope(
        event,
        config=cfg,
        scope_tag_slugs=("explicit-scope",),
    )
    assert config_calls == [("event-slug", cfg)]
    assert tag_calls == [(event, cfg, ("explicit-scope",))]


def test_event_in_scope_does_not_invent_a_missing_slug() -> None:
    cfg = slug_only_cfg(
        event_slugs=(),
        event_slug_prefixes=("x",),
        event_tags=(),
    )

    assert not scope_predicates_mod.event_in_scope({"id": "event"}, config=cfg)


def test_keyset_crawl_resolution_preserves_sources_calls_and_logs(
    monkeypatch, caplog
) -> None:
    cfg = slug_only_cfg(
        event_tags=("seed-tag",),
        tag_discovery_keywords=("world",),
        tag_discovery=True,
    )
    explicit = scope_predicates_mod.resolve_keyset_crawl_tags(
        ("one", "two"),
        config=cfg,
        client=object(),
        tag_discovery=True,
    )
    assert explicit == (
        ["one", "two"],
        {"one": {"explicit"}, "two": {"explicit"}},
    )

    seen: dict[str, object] = {}

    def discover(client, *, seed_slugs, keywords):
        seen.update(
            client=client,
            seed_slugs=seed_slugs,
            keywords=keywords,
        )
        return SimpleNamespace(
            tag_slugs=("seed-tag", "new-tag"),
            sources={"new-tag": {"keyword"}},
        )

    client = object()
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.market_scope_tags.discover_market_scope_tag_slugs",
        discover,
    )
    caplog.set_level(logging.INFO, logger=scope_predicates_mod.__name__)
    resolved = scope_predicates_mod.resolve_keyset_crawl_tags(
        None,
        config=cfg,
        client=client,
        tag_discovery=True,
    )
    assert resolved == (
        ["new-tag", "seed-tag"],
        {"seed-tag": {"seed"}, "new-tag": {"keyword"}},
    )
    assert seen == {
        "client": client,
        "seed_slugs": ["seed-tag"],
        "keywords": ("world",),
    }
    assert caplog.messages == [
        "Market-scope tag discovery expanded crawl tags from ['seed-tag'] "
        "to ['new-tag', 'seed-tag']"
    ]
    assert scope_predicates_mod.resolve_keyset_tag_slugs(
        None,
        config=cfg,
        client=None,
        tag_discovery=False,
    ) == ["seed-tag"]


def test_disabled_discovery_never_calls_client_discovery(monkeypatch) -> None:
    calls = 0

    def unexpected(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise AssertionError("discovery must stay disabled")

    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.market_scope_tags.discover_market_scope_tag_slugs",
        unexpected,
    )

    assert scope_predicates_mod.resolve_keyset_crawl_tags(
        None,
        config=slug_only_cfg(event_tags=("seed-tag",), tag_discovery=False),
        client=object(),
        tag_discovery=False,
    ) == (["seed-tag"], {"seed-tag": {"seed"}})
    assert calls == 0


def test_keyset_tag_wrapper_forwards_every_argument(monkeypatch) -> None:
    cfg = slug_only_cfg(event_tags=("seed-tag",))
    client = object()
    calls: list[tuple[object, object, object, object]] = []

    def resolve(tags, *, config, client, tag_discovery):
        calls.append((tags, config, client, tag_discovery))
        return ["resolved"], {"resolved": {"test"}}

    monkeypatch.setattr(scope_predicates_mod, "resolve_keyset_crawl_tags", resolve)

    assert scope_predicates_mod.resolve_keyset_tag_slugs(
        ("explicit",),
        config=cfg,
        client=client,
        tag_discovery=True,
    ) == ["resolved"]
    assert calls == [(("explicit",), cfg, client, True)]


def test_keyset_discovery_failure_log_is_operationally_complete(
    monkeypatch, caplog
) -> None:
    def fail(*args, **kwargs):
        raise RuntimeError("discovery down")

    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.market_scope_tags.discover_market_scope_tag_slugs",
        fail,
    )
    caplog.set_level(logging.WARNING, logger=scope_predicates_mod.__name__)

    result = scope_predicates_mod.resolve_keyset_crawl_tags(
        None,
        config=slug_only_cfg(event_tags=("seed-tag",), tag_discovery=True),
        client=object(),
        tag_discovery=True,
    )

    assert result == (["seed-tag"], {"seed-tag": {"seed"}})
    assert caplog.messages == [
        "Market-scope tag discovery failed; using configured event_tags only"
    ]
    assert caplog.records[0].exc_info is not None


def test_keyset_discovery_failure_passes_exact_logger_arguments(monkeypatch) -> None:
    def fail(*args, **kwargs):
        raise RuntimeError("discovery down")

    warning = MagicMock()
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.market_scope_tags.discover_market_scope_tag_slugs",
        fail,
    )
    monkeypatch.setattr(scope_predicates_mod.logger, "warning", warning)

    scope_predicates_mod.resolve_keyset_crawl_tags(
        None,
        config=slug_only_cfg(event_tags=("seed-tag",), tag_discovery=True),
        client=object(),
        tag_discovery=True,
    )

    warning.assert_called_once_with(
        "Market-scope tag discovery failed; using configured event_tags only",
        exc_info=True,
    )


def test_is_market_scope_row_distinguishes_each_admission_route() -> None:
    cfg = slug_only_cfg(
        market_ids=("market-id",),
        event_slugs=("exact-event",),
        event_slug_prefixes=("prefix-",),
        event_tags=("scope-tag",),
    )

    assert scope_predicates_mod.is_market_scope_row(
        market_id="other", in_registry=True, config=cfg
    )
    assert scope_predicates_mod.is_market_scope_row(market_id="market-id", config=cfg)
    assert scope_predicates_mod.is_market_scope_row(
        market_id="other", event_slug="EXACT-EVENT", config=cfg
    )
    assert scope_predicates_mod.is_market_scope_row(
        market_id="other", event_slug="PREFIX-child", config=cfg
    )
    assert scope_predicates_mod.is_market_scope_row(
        market_id="other", event_tags=(" SCOPE-TAG ",), config=cfg
    )
    assert not scope_predicates_mod.is_market_scope_row(
        market_id="other", event_slug="", event_tags=("other",), config=cfg
    )
    assert not scope_predicates_mod.is_market_scope_row(
        market_id="other",
        config=slug_only_cfg(
            event_slugs=(),
            event_slug_prefixes=("x",),
            event_tags=("none",),
        ),
    )
