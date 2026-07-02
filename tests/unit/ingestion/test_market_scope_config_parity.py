"""Selected market-scope defaults stay aligned across Python and dbt."""

from __future__ import annotations

from pathlib import Path

import yaml

from oddsfox.config.settings_polymarket import DEFAULT_POLYMARKET_MARKET_SCOPE
from oddsfox.ingestion.polymarket.market_scope import load_market_scope_config
from oddsfox.ingestion.polymarket.scope_sql import DEFAULT_MARKET_SCOPE

REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_PROJECT = REPO_ROOT / "dbt" / "dbt_project.yml"
ENV_EXAMPLE = REPO_ROOT / ".env.example"
SCOPE_SEED = (
    REPO_ROOT
    / "src"
    / "oddsfox"
    / "ingestion"
    / "polymarket"
    / "seeds"
    / "market_scopes.yml"
)


def _env_example_value(key: str) -> str | None:
    for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1].strip()
    return None


def test_default_market_scope_matches_dbt_active_scope(monkeypatch) -> None:
    monkeypatch.delenv("POLYMARKET_MARKET_SCOPES", raising=False)
    cfg = load_market_scope_config()
    raw = yaml.safe_load(DBT_PROJECT.read_text(encoding="utf-8")) or {}
    vars_block = raw.get("vars") or {}
    seed = yaml.safe_load(SCOPE_SEED.read_text(encoding="utf-8")) or {}

    assert DEFAULT_POLYMARKET_MARKET_SCOPE == DEFAULT_MARKET_SCOPE == "wc2026"
    assert seed.get("default_scope") == DEFAULT_POLYMARKET_MARKET_SCOPE
    assert _env_example_value("POLYMARKET_MARKET_SCOPES") == DEFAULT_MARKET_SCOPE
    assert vars_block.get("active_market_scopes") == [cfg.scope_name] == ["wc2026"]


def test_market_scope_keyset_discovery_defaults(monkeypatch) -> None:
    monkeypatch.delenv("POLYMARKET_SCOPE_KEYSET_CLOSED", raising=False)
    monkeypatch.delenv("POLYMARKET_SCOPE_KEYSET_VOLUME_MIN", raising=False)
    from oddsfox.config._reload_settings import reload_all_settings_modules

    reload_all_settings_modules()
    from oddsfox.config.settings_polymarket import (
        POLYMARKET_SCOPE_KEYSET_CLOSED as closed,
    )
    from oddsfox.config.settings_polymarket import (
        POLYMARKET_SCOPE_KEYSET_VOLUME_MIN as volume_min,
    )

    assert closed is False
    assert volume_min == 10000.0


def test_market_scope_keyset_discovery_omit_filters_via_empty_env(monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_SCOPE_KEYSET_CLOSED", "any")
    monkeypatch.setenv("POLYMARKET_SCOPE_KEYSET_VOLUME_MIN", "")
    from oddsfox.config._reload_settings import reload_all_settings_modules

    reload_all_settings_modules()
    from oddsfox.config.settings_polymarket import (
        POLYMARKET_SCOPE_KEYSET_CLOSED as closed,
    )
    from oddsfox.config.settings_polymarket import (
        POLYMARKET_SCOPE_KEYSET_VOLUME_MIN as volume_min,
    )

    assert closed is None
    assert volume_min is None


def test_load_market_scope_config_includes_world_cup_tag(monkeypatch) -> None:
    monkeypatch.delenv("POLYMARKET_SCOPE_EVENT_TAGS", raising=False)
    cfg = load_market_scope_config()
    assert "world-cup" in cfg.event_tags
