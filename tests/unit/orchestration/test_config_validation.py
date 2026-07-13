from __future__ import annotations

import pytest

from oddsfox_pipeline.orchestration.config import (
    DbtBuildConfig,
    GuardrailConfig,
    HourlyOddsSyncConfig,
    MarketScopeRegistryConfig,
    MarketsSyncConfig,
    MetadataBackfillConfig,
    OddsSyncConfig,
    wc2026_knockout_match_odds_full_pipeline_run_config,
)


def test_guardrail_config_rejects_invalid_timeout_and_snapshot_level():
    assert GuardrailConfig(raw_snapshot_level=" Full ").raw_snapshot_level == "full"

    with pytest.raises(ValueError, match="hard_timeout"):
        GuardrailConfig(
            no_progress_soft_timeout_seconds=10,
            no_progress_hard_timeout_seconds=5,
        )

    with pytest.raises(ValueError, match="raw_snapshot_level"):
        GuardrailConfig(raw_snapshot_level="deep")


def test_paged_config_rejects_nonpositive_no_progress_limits():
    legacy_key = "scope" + "_names"
    assert legacy_key not in MarketsSyncConfig.model_fields
    assert legacy_key not in MarketScopeRegistryConfig.model_fields

    with pytest.raises(ValueError, match="max_pages_without_progress"):
        MarketsSyncConfig(max_pages_without_progress=0)

    with pytest.raises(ValueError, match="max_pages_without_progress"):
        MarketScopeRegistryConfig(max_pages_without_progress=0)


def test_metadata_config_rejects_invalid_rps():
    with pytest.raises(ValueError, match="gamma_requests_per_second"):
        MetadataBackfillConfig(gamma_requests_per_second=0)


def test_odds_config_validates_volume_bounds():
    legacy_key = "scope" + "_names"
    assert legacy_key not in OddsSyncConfig.model_fields
    assert OddsSyncConfig(min_volume=None).min_volume is None
    assert OddsSyncConfig(min_volume=1).min_volume == 1.0

    with pytest.raises(ValueError, match="volume bounds"):
        OddsSyncConfig(max_volume=-1)


def test_hourly_odds_config_defaults_to_knockout_30_day_sync():
    cfg = HourlyOddsSyncConfig()

    assert cfg.fidelity == 60
    assert cfg.force is True
    assert cfg.overlap_minutes == 60
    assert cfg.window_hours == 720
    assert cfg.history_backfill_days == 30
    assert cfg.routine_interval_hours == 1
    assert cfg.min_volume == 5000.0
    assert cfg.max_volume is None


def test_dbt_build_config_accepts_fixed_scope_selectors():
    cfg = DbtBuildConfig(
        full_refresh=True,
        dbt_select="+tag:polymarket,tag:wc2026",
        dbt_exclude="tag:kalshi",
    )

    assert cfg.full_refresh is True
    assert cfg.dbt_select == "+tag:polymarket,tag:wc2026"
    assert cfg.dbt_exclude == "tag:kalshi"


def test_combined_match_odds_config_preserves_history_and_bypasses_volume_floor():
    ops = wc2026_knockout_match_odds_full_pipeline_run_config()["ops"]

    assert ops["polymarket_wc2026_raw_markets"]["config"]["keyset_volume_min"] == 0.0
    assert (
        ops["polymarket_wc2026_ops_market_scope_registry"]["config"][
            "keyset_volume_min"
        ]
        == 0.0
    )
    assert (
        ops["polymarket_wc2026_raw_token_odds_history_hourly"]["config"]["min_volume"]
        is None
    )
    assert ops["oddsfox_dbt"]["config"]["full_refresh"] is False
    assert ops["oddsfox_dbt"]["config"]["dbt_select"] == "+tag:cross_domain"
    assert ops["oddsfox_dbt"]["config"]["dbt_exclude"] is None
