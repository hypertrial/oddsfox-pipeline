"""WC2026 market-scope defaults stay aligned across Python and dbt."""

from __future__ import annotations

import csv
from pathlib import Path

import yaml

from oddsfox_pipeline.config.settings_polymarket import (
    DEFAULT_POLYMARKET_WC2026_MARKET_SCOPE,
    POLYMARKET_WC2026_HOURLY_WINDOW_DAYS,
    POLYMARKET_WC2026_HOURLY_WINDOW_HOURS,
    POLYMARKET_WC2026_KNOCKOUT_MIN_VOLUME_USD,
    WC2026_CONTRACT_DEFAULTS,
)
from oddsfox_pipeline.ingestion.polymarket.market_scope import load_market_scope_config
from oddsfox_pipeline.ingestion.polymarket.scope_sql import DEFAULT_MARKET_SCOPE
from oddsfox_pipeline.orchestration.config import HourlyOddsSyncConfig

REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_PROJECT = REPO_ROOT / "dbt" / "dbt_project.yml"
ENV_EXAMPLE = REPO_ROOT / ".env.example"
SCOPE_SEED = (
    REPO_ROOT
    / "src"
    / "oddsfox_pipeline"
    / "ingestion"
    / "polymarket"
    / "seeds"
    / "market_scopes.yml"
)
DBT_CONTRACT_SEED = REPO_ROOT / "dbt" / "seeds" / "polymarket_wc2026_contract.csv"


def test_default_market_scope_matches_dbt_contract() -> None:
    cfg = load_market_scope_config()
    raw = yaml.safe_load(DBT_PROJECT.read_text(encoding="utf-8")) or {}
    seed = yaml.safe_load(SCOPE_SEED.read_text(encoding="utf-8")) or {}

    assert DEFAULT_POLYMARKET_WC2026_MARKET_SCOPE == DEFAULT_MARKET_SCOPE == "wc2026"
    assert seed.get("default_scope") == DEFAULT_POLYMARKET_WC2026_MARKET_SCOPE
    assert tuple((seed.get("scopes") or {}).keys()) == ("wc2026", "us_midterms_2026")
    assert "MARKET_SCOPES" not in ENV_EXAMPLE.read_text(encoding="utf-8")
    assert "active_market_scopes" not in (raw.get("vars") or {})
    assert cfg.scope_name == "wc2026"


def test_market_scope_keyset_discovery_defaults(monkeypatch) -> None:
    monkeypatch.delenv("POLYMARKET_WC2026_SCOPE_KEYSET_CLOSED", raising=False)
    monkeypatch.delenv("POLYMARKET_WC2026_SCOPE_KEYSET_VOLUME_MIN", raising=False)
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules

    reload_all_settings_modules()
    from oddsfox_pipeline.config.settings_polymarket import (
        POLYMARKET_WC2026_SCOPE_KEYSET_CLOSED as closed,
    )
    from oddsfox_pipeline.config.settings_polymarket import (
        POLYMARKET_WC2026_SCOPE_KEYSET_VOLUME_MIN as volume_min,
    )

    assert closed is False
    assert volume_min == 5000.0


def test_wc2026_contract_seed_matches_python_defaults() -> None:
    with DBT_CONTRACT_SEED.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 1
    row = rows[0]
    hourly_cfg = HourlyOddsSyncConfig()
    scope_seed = yaml.safe_load(SCOPE_SEED.read_text(encoding="utf-8")) or {}
    wc2026_scope = (scope_seed.get("scopes") or {})["wc2026"]
    env_example = ENV_EXAMPLE.read_text(encoding="utf-8")

    assert row["scope_name"] == "wc2026"
    assert row["scope_name"] == WC2026_CONTRACT_DEFAULTS["scope_name"]
    assert (
        float(row["knockout_min_volume_usd"])
        == POLYMARKET_WC2026_KNOCKOUT_MIN_VOLUME_USD
    )
    assert (
        wc2026_scope["keyset_volume_min"] == POLYMARKET_WC2026_KNOCKOUT_MIN_VOLUME_USD
    )
    expected_env = (
        "POLYMARKET_WC2026_SCOPE_KEYSET_VOLUME_MIN="
        f"{int(POLYMARKET_WC2026_KNOCKOUT_MIN_VOLUME_USD)}"
    )
    assert expected_env in env_example
    assert int(row["hourly_window_days"]) == hourly_cfg.history_backfill_days
    assert int(row["hourly_window_hours"]) == hourly_cfg.window_hours
    assert int(row["hourly_window_days"]) == POLYMARKET_WC2026_HOURLY_WINDOW_DAYS
    assert int(row["hourly_window_hours"]) == POLYMARKET_WC2026_HOURLY_WINDOW_HOURS


def test_market_scope_keyset_discovery_omit_filters_via_empty_env(monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_WC2026_SCOPE_KEYSET_CLOSED", "any")
    monkeypatch.setenv("POLYMARKET_WC2026_SCOPE_KEYSET_VOLUME_MIN", "")
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules

    reload_all_settings_modules()
    from oddsfox_pipeline.config.settings_polymarket import (
        POLYMARKET_WC2026_SCOPE_KEYSET_CLOSED as closed,
    )
    from oddsfox_pipeline.config.settings_polymarket import (
        POLYMARKET_WC2026_SCOPE_KEYSET_VOLUME_MIN as volume_min,
    )

    assert closed is None
    assert volume_min is None


def test_load_market_scope_config_includes_world_cup_tag(monkeypatch) -> None:
    monkeypatch.delenv("POLYMARKET_WC2026_SCOPE_EVENT_TAGS", raising=False)
    cfg = load_market_scope_config()
    assert "world-cup" in cfg.event_tags
