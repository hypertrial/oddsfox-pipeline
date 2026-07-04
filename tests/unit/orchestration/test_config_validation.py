from __future__ import annotations

import pytest

from oddsfox_pipeline.orchestration.config import (
    GuardrailConfig,
    HourlyOddsSyncConfig,
    MarketScopeRegistryConfig,
    MarketsSyncConfig,
    MetadataBackfillConfig,
    OddsSyncConfig,
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
