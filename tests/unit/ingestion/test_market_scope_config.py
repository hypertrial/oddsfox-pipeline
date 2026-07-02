"""Unit tests for market-scope config helpers."""

from __future__ import annotations

import pytest

from oddsfox.ingestion.polymarket.market_scope import (
    MARKET_SCOPE_ALL,
    MarketScopeConfig,
    default_market_scopes_seed_path,
    event_matches_scope_config,
    is_market_scope_row,
    load_market_scope_config,
    market_scope_predicate_sql,
    market_scope_sql,
    validate_market_scope,
    validate_market_scopes,
)
from oddsfox.ingestion.polymarket.market_scope import config as scope_config_mod
from oddsfox.ingestion.polymarket.scope_sql import DEFAULT_MARKET_SCOPE


def test_validate_market_scope_accepts_slug_like_scopes():
    assert validate_market_scope("custom-scope") == "custom-scope"
    with pytest.raises(ValueError, match="slug-like"):
        validate_market_scope("bad scope")


def test_validate_market_scopes_csv_dedupes_and_preserves_order(monkeypatch):
    monkeypatch.delenv("POLYMARKET_MARKET_SCOPES", raising=False)
    assert validate_market_scopes("wc2026,us-politics,wc2026") == (
        "wc2026",
        "us-politics",
    )
    assert validate_market_scopes(["nba", "nfl"]) == ("nba", "nfl")
    with pytest.raises(ValueError, match="at least one scope"):
        validate_market_scopes([])
    with pytest.raises(ValueError, match="at least one scope"):
        validate_market_scopes(" , ")


def test_merge_scope_sync_summaries_handles_empty_and_multi_scope():
    from oddsfox.orchestration import assets_polymarket as assets_mod

    assert assets_mod._merge_scope_sync_summaries([])["total_fetched"] == 0
    merged = assets_mod._merge_scope_sync_summaries(
        [
            {"scope_name": "wc2026", "total_fetched": 1, "registry_refreshed": True},
            {"scope_name": "nba", "total_fetched": 2, "registry_refreshed": False},
        ]
    )
    assert merged["scope_names"] == ["wc2026", "nba"]
    assert merged["total_fetched"] == 3
    assert merged["registry_refreshed"] is False


def test_validate_market_scopes_defaults_to_settings(monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_MARKET_SCOPES", "nba,nfl")
    from oddsfox.config._reload_settings import reload_all_settings_modules

    reload_all_settings_modules()
    assert validate_market_scopes() == ("nba", "nfl")


def test_market_scopes_csv_skips_empty_tokens(monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_MARKET_SCOPES", "wc2026,,nba")
    from oddsfox.config._reload_settings import reload_all_settings_modules

    reload_all_settings_modules()
    from oddsfox.config.settings_polymarket import POLYMARKET_MARKET_SCOPES

    assert POLYMARKET_MARKET_SCOPES == ("wc2026", "nba")


def test_market_scopes_csv_empty_env_falls_back_to_default(monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_MARKET_SCOPES", "   ")
    from oddsfox.config._reload_settings import reload_all_settings_modules

    reload_all_settings_modules()
    from oddsfox.config.settings_polymarket import POLYMARKET_MARKET_SCOPES

    assert POLYMARKET_MARKET_SCOPES == ("wc2026",)


def test_snapshot_refreshed_scope_names_legacy_single_scope():
    from oddsfox.orchestration import assets_polymarket as assets_mod

    assert assets_mod._snapshot_refreshed_scope_names({"scope_name": "wc2026"}) == [
        "wc2026"
    ]
    assert assets_mod._snapshot_refreshed_scope_names({}) == []


def test_load_market_scope_config_includes_default_wc2026_preset(monkeypatch):
    monkeypatch.delenv("POLYMARKET_MARKET_SCOPES", raising=False)
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


def test_all_seed_presets_load_cleanly():
    import yaml

    seed = yaml.safe_load(default_market_scopes_seed_path().read_text(encoding="utf-8"))
    for scope_name in seed["scopes"]:
        cfg = load_market_scope_config(scope_name=scope_name)
        assert cfg.scope_name == scope_name


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
    multi_sql = market_scope_sql(["wc2026", "us-politics"])
    assert "scope_name IN ('wc2026', 'us-politics')" in multi_sql
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
    monkeypatch.delenv("POLYMARKET_MARKET_SCOPES", raising=False)
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
    cfg = load_market_scope_config(seed_path=good, scope_name="custom")
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
        load_market_scope_config(seed_path=bad2, scope_name="custom")

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
        load_market_scope_config(seed_path=bad3, scope_name="missing")

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
        load_market_scope_config(seed_path=bad4, scope_name="custom")

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
