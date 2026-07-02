from __future__ import annotations

import pytest

from oddsfox.orchestration.config import (
    GuardrailConfig,
    MarketsSyncConfig,
    MetadataBackfillConfig,
    OddsSyncConfig,
    Wc2026RegistryConfig,
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
    with pytest.raises(ValueError, match="max_pages_without_progress"):
        MarketsSyncConfig(max_pages_without_progress=0)

    with pytest.raises(ValueError, match="max_pages_without_progress"):
        Wc2026RegistryConfig(max_pages_without_progress=0)


def test_metadata_config_rejects_invalid_rps_and_scope():
    with pytest.raises(ValueError, match="gamma_requests_per_second"):
        MetadataBackfillConfig(gamma_requests_per_second=0)

    with pytest.raises(ValueError, match="market_scope"):
        MetadataBackfillConfig(market_scope="not-a-scope")


def test_odds_config_validates_scope_and_volume_bounds():
    assert OddsSyncConfig(market_scope="all").market_scope == "all"
    assert OddsSyncConfig(min_volume=None).min_volume is None
    assert OddsSyncConfig(min_volume=1).min_volume == 1.0

    with pytest.raises(ValueError, match="volume bounds"):
        OddsSyncConfig(max_volume=-1)
