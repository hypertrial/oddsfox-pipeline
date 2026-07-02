"""Unit tests for market-scope config helpers."""

from __future__ import annotations

import pytest

from oddsfox.ingestion.polymarket.market_scope import (
    MARKET_SCOPE_ALL,
    MarketScopeConfig,
    event_matches_scope_config,
    is_market_scope_row,
    load_market_scope_config,
    market_scope_predicate_sql,
    market_scope_sql,
    validate_market_scope,
)
from oddsfox.ingestion.polymarket.market_scope import config as scope_config_mod
from oddsfox.ingestion.polymarket.scope_sql import DEFAULT_MARKET_SCOPE


def test_validate_market_scope_accepts_slug_like_scopes():
    assert validate_market_scope("custom-scope") == "custom-scope"
    with pytest.raises(ValueError, match="slug-like"):
        validate_market_scope("bad scope")


def test_load_market_scope_config_includes_default_wc2026_preset(monkeypatch):
    monkeypatch.delenv("POLYMARKET_MARKET_SCOPE", raising=False)
    monkeypatch.delenv("POLYMARKET_SCOPE_EVENT_TAGS", raising=False)
    cfg = load_market_scope_config()
    assert cfg.scope_name == "wc2026"
    assert "2026-fifa-world-cup-winner" in cfg.event_slugs
    assert cfg.event_slug_prefixes
    assert "2026-fifa-world-cup" in cfg.event_tags
    assert "fifa-world-cup" in cfg.event_tags
    assert "world-cup" in cfg.event_tags
    assert cfg.keyset_closed is False
    assert cfg.keyset_volume_min == 10000.0


def test_market_scope_config_and_sql_helpers():
    assert scope_config_mod._parse_csv_list("") == ()
    assert scope_config_mod._parse_csv_list(" a , b ") == ("a", "b")
    with pytest.raises(ValueError, match="Invalid event slug"):
        scope_config_mod._validate_slug_token("bad slug!")

    cfg = MarketScopeConfig(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=("mid-1",),
        registry_max_event_pages=None,
    )
    assert cfg.default_event_slug == "2026-fifa-world-cup-winner-595"

    sql = market_scope_sql(DEFAULT_MARKET_SCOPE)
    assert "market_scope_registry" in sql
    assert "scope_name = 'wc2026'" in sql
    assert "event_slug" not in sql
    assert market_scope_sql(MARKET_SCOPE_ALL) == ""
    assert market_scope_predicate_sql(MARKET_SCOPE_ALL) == "TRUE"
    assert not event_matches_scope_config(None)
    assert is_market_scope_row(market_id="seed1", in_registry=True, config=cfg)
    assert is_market_scope_row(
        market_id="x",
        market_scope=MARKET_SCOPE_ALL,
        config=cfg,
    )


def test_load_market_scope_config_yaml_validation(tmp_path, monkeypatch):
    monkeypatch.delenv("POLYMARKET_MARKET_SCOPE", raising=False)
    monkeypatch.delenv("POLYMARKET_SCOPE_EVENT_SLUGS", raising=False)
    monkeypatch.delenv("POLYMARKET_SCOPE_EVENT_SLUG_PREFIXES", raising=False)
    monkeypatch.delenv("POLYMARKET_SCOPE_EVENT_TAGS", raising=False)
    good = tmp_path / "good.yml"
    good.write_text(
        """
default_scope: custom
scopes:
  custom:
    event_slug_prefixes:
      - pre
    event_slugs: []
    event_tags: []
    market_ids: []
""",
        encoding="utf-8",
    )
    cfg = load_market_scope_config(seed_path=good)
    assert cfg.scope_name == "custom"
    assert cfg.event_slug_prefixes == ("pre",)

    bad = tmp_path / "bad.yml"
    bad.write_text("not-a-dict", encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid YAML root"):
        load_market_scope_config(seed_path=bad)

    bad2 = tmp_path / "bad2.yml"
    bad2.write_text(
        """
default_scope: custom
scopes:
  custom:
    event_slugs: 1
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="event_slugs must be"):
        load_market_scope_config(seed_path=bad2)

    bad3 = tmp_path / "bad3.yml"
    bad3.write_text(
        """
default_scope: missing
scopes:
  custom: {}
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Unknown Polymarket market scope"):
        load_market_scope_config(seed_path=bad3)

    bad4 = tmp_path / "bad4.yml"
    bad4.write_text(
        """
default_scope: custom
scopes:
  custom: []
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="scopes.custom must be"):
        load_market_scope_config(seed_path=bad4)

    bad5 = tmp_path / "bad5.yml"
    bad5.write_text("default_scope: custom\nscopes: {}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="scopes must be"):
        load_market_scope_config(seed_path=bad5)


def test_market_scope_yaml_scalar_validation_helpers(tmp_path):
    path = tmp_path / "seed.yml"

    with pytest.raises(ValueError, match="must be an integer"):
        scope_config_mod._optional_int(True, key="n", path=path)
    with pytest.raises(ValueError, match="must be an integer"):
        scope_config_mod._optional_int("bad", key="n", path=path)

    with pytest.raises(ValueError, match="must be a number"):
        scope_config_mod._optional_float(True, key="f", path=path)
    with pytest.raises(ValueError, match="must be a number"):
        scope_config_mod._optional_float("bad", key="f", path=path)

    assert scope_config_mod._optional_bool("yes", key="b", path=path) is True
    assert scope_config_mod._optional_bool("off", key="b", path=path) is False
    with pytest.raises(ValueError, match="must be a boolean"):
        scope_config_mod._optional_bool("maybe", key="b", path=path)
