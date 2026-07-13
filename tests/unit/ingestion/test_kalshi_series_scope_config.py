"""Unit tests for Kalshi market-scope seed loading."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from oddsfox_pipeline.ingestion.kalshi.series_scope.config import (
    KalshiMarketScopeConfig,
    default_market_scopes_seed_path,
    load_market_scope_config,
    market_suffix_excluded,
    scope_config_hash,
)


def test_default_seed_path_points_at_packaged_yaml():
    path = default_market_scopes_seed_path()
    assert path.name == "market_scopes.yml"
    assert path.is_file()


def test_load_market_scope_config_wc2026_preset():
    cfg = load_market_scope_config()
    assert cfg.scope_name == "wc2026"
    assert "KXMENWORLDCUP" in cfg.series_tickers
    assert "KXWCSTAGEOFELIM" in cfg.series_tickers
    assert "KXWCADVANCE" in cfg.series_tickers
    assert cfg.excluded_market_suffixes["KXWCSTAGEOFELIM"] == ("FW",)


def test_all_seed_presets_load_cleanly():
    seed = yaml.safe_load(default_market_scopes_seed_path().read_text(encoding="utf-8"))
    for scope_name in seed["scopes"]:
        cfg = load_market_scope_config(scope_name=scope_name)
        assert cfg.scope_name == scope_name


def test_load_market_scope_config_unknown_scope_raises(tmp_path: Path):
    seed_path = tmp_path / "market_scopes.yml"
    seed_path.write_text(
        """
default_scope: wc2026
scopes:
  wc2026:
    series_tickers: [KXMENWORLDCUP]
""".strip()
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Unknown Kalshi scope"):
        load_market_scope_config(scope_name="missing", seed_path=seed_path)


def test_scope_config_hash_is_stable_for_same_config():
    cfg = KalshiMarketScopeConfig(
        scope_name="wc2026",
        series_tickers=("KXMENWORLDCUP",),
        excluded_market_suffixes={"KXWCSTAGEOFELIM": ("FW",)},
    )
    assert scope_config_hash(cfg) == scope_config_hash(cfg)


def test_market_suffix_excluded_matches_series_suffix_rules():
    cfg = load_market_scope_config()
    assert market_suffix_excluded(
        cfg,
        series_ticker="KXWCSTAGEOFELIM",
        market_ticker="KXWCSTAGEOFELIM-TEAM-FW",
    )
    assert not market_suffix_excluded(
        cfg,
        series_ticker="KXWCSTAGEOFELIM",
        market_ticker="KXWCSTAGEOFELIM-TEAM",
    )
    assert not market_suffix_excluded(
        cfg,
        series_ticker="KXMENWORLDCUP",
        market_ticker="KXMENWORLDCUP-WINNER",
    )


def test_load_market_scope_config_validation_errors(tmp_path: Path):
    base_seed = tmp_path / "base.yml"
    base_seed.write_text(
        """
default_scope: wc2026
scopes:
  wc2026:
    series_tickers: [KXWC]
""".strip()
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="scope_name must not be empty"):
        load_market_scope_config(scope_name="   ", seed_path=base_seed)

    bad_seed = tmp_path / "bad_seed.yml"
    bad_seed.write_text("[]\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid market scope seed"):
        load_market_scope_config(seed_path=bad_seed)

    invalid_scope = tmp_path / "invalid_scope.yml"
    invalid_scope.write_text(
        """
default_scope: wc2026
scopes:
  wc2026: not-a-dict
""".strip()
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Invalid scope block"):
        load_market_scope_config(seed_path=invalid_scope)

    bad_series = tmp_path / "bad_series.yml"
    bad_series.write_text(
        """
default_scope: wc2026
scopes:
  wc2026:
    series_tickers: not-a-list
""".strip()
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="series_tickers must be a list"):
        load_market_scope_config(seed_path=bad_series)

    empty_series = tmp_path / "empty_series.yml"
    empty_series.write_text(
        """
default_scope: wc2026
scopes:
  wc2026:
    series_tickers: ["  "]
""".strip()
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="series_tickers must not be empty"):
        load_market_scope_config(seed_path=empty_series)

    suffixes = tmp_path / "suffixes.yml"
    suffixes.write_text(
        """
default_scope: wc2026
scopes:
  wc2026:
    series_tickers: [KXWC]
    excluded_market_suffixes:
      KXWC: [FW, ""]
      bad-key: FW
""".strip()
        + "\n",
        encoding="utf-8",
    )
    cfg = load_market_scope_config(seed_path=suffixes)
    assert cfg.excluded_market_suffixes["KXWC"] == ("FW",)

    non_dict_excluded = tmp_path / "non_dict_excluded.yml"
    non_dict_excluded.write_text(
        """
default_scope: wc2026
scopes:
  wc2026:
    series_tickers: [KXWC]
    excluded_market_suffixes: not-a-dict
""".strip()
        + "\n",
        encoding="utf-8",
    )
    cfg = load_market_scope_config(seed_path=non_dict_excluded)
    assert cfg.excluded_market_suffixes == {}
