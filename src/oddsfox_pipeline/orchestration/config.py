from __future__ import annotations

from typing import Literal

from dagster import Config
from pydantic import Field, field_validator, model_validator

from oddsfox_pipeline.config.settings import (
    DEFAULT_ODDS_FIDELITY_MINUTES,
    KALSHI_WC2026_HOURLY_WINDOW_DAYS,
    KALSHI_WC2026_HOURLY_WINDOW_HOURS,
    MIN_ODDS_FIDELITY_MINUTES,
    POLYMARKET_US_MIDTERMS_2026_HOURLY_WINDOW_DAYS,
    POLYMARKET_US_MIDTERMS_2026_HOURLY_WINDOW_HOURS,
    POLYMARKET_US_MIDTERMS_2026_MIN_VOLUME_USD,
    POLYMARKET_WC2026_HOURLY_WINDOW_DAYS,
    POLYMARKET_WC2026_HOURLY_WINDOW_HOURS,
    POLYMARKET_WC2026_KNOCKOUT_MIN_VOLUME_USD,
)
from oddsfox_pipeline.orchestration.scope_registry import (
    KALSHI_WC2026_SCOPE,
    POLYMARKET_US_MIDTERMS_2026_SCOPE,
    POLYMARKET_WC2026_SCOPE,
)

DEFAULT_EVENT_SLUG_FALLBACK_MAX_PAGES = 20_000
DEFAULT_EVENT_SLUG_FALLBACK_MAX_NO_PROGRESS_PAGES = 25
DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS = 60
DEFAULT_NO_PROGRESS_SOFT_TIMEOUT_SECONDS = 900
DEFAULT_NO_PROGRESS_HARD_TIMEOUT_SECONDS = 2700
DEFAULT_DBT_NO_PROGRESS_HARD_TIMEOUT_SECONDS = 3600
DEFAULT_PROGRESS_POLL_SECONDS = 5


class GuardrailConfig(Config):
    progress_log_interval_seconds: int = Field(
        default=DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS, ge=1
    )
    no_progress_soft_timeout_seconds: int | None = Field(
        default=DEFAULT_NO_PROGRESS_SOFT_TIMEOUT_SECONDS,
        ge=1,
    )
    no_progress_hard_timeout_seconds: int | None = Field(
        default=DEFAULT_NO_PROGRESS_HARD_TIMEOUT_SECONDS,
        ge=1,
    )
    progress_poll_seconds: int = Field(default=DEFAULT_PROGRESS_POLL_SECONDS, ge=1)
    raw_snapshot_level: str = Field(default="basic")

    @model_validator(mode="after")
    def _validate_soft_hard_timeouts(self) -> "GuardrailConfig":
        soft = self.no_progress_soft_timeout_seconds
        hard = self.no_progress_hard_timeout_seconds
        if soft is not None and hard is not None and hard <= soft:
            raise ValueError(
                "no_progress_hard_timeout_seconds must be greater than "
                "no_progress_soft_timeout_seconds when both are set"
            )
        return self

    @field_validator("raw_snapshot_level")
    @classmethod
    def _validate_raw_snapshot_level(cls, v: str) -> str:
        s = str(v).strip().lower()
        if s not in ("basic", "full"):
            raise ValueError("raw_snapshot_level must be 'basic' or 'full'")
        return s


class MarketsSyncConfig(GuardrailConfig):
    progress_log_interval_pages: int = Field(default=10, ge=1)
    discovery_mode: Literal["targeted", "full_keyset"] = "full_keyset"
    force_full_discovery: bool = False
    max_event_pages: int | None = None
    keyset_closed: bool | None = None
    keyset_tag_slugs: list[str] | None = None
    keyset_volume_min: float | None = Field(
        default=POLYMARKET_WC2026_KNOCKOUT_MIN_VOLUME_USD, ge=0
    )
    max_pages_without_progress: int | None = None

    @field_validator("max_pages_without_progress")
    @classmethod
    def _max_pages_without_progress_nonnegative(cls, v: int | None) -> int | None:
        if v is not None and v < 1:
            raise ValueError("max_pages_without_progress must be >= 1 when set")
        return v


class MarketScopeRegistryConfig(GuardrailConfig):
    max_event_pages: int | None = None
    keyset_closed: bool | None = None
    keyset_tag_slugs: list[str] | None = None
    keyset_volume_min: float | None = Field(
        default=POLYMARKET_WC2026_KNOCKOUT_MIN_VOLUME_USD, ge=0
    )
    max_pages_without_progress: int | None = None
    skip_if_snapshot_refreshed: bool = True
    force_refresh: bool = False

    @field_validator("max_pages_without_progress")
    @classmethod
    def _max_pages_without_progress_nonnegative(cls, v: int | None) -> int | None:
        if v is not None and v < 1:
            raise ValueError("max_pages_without_progress must be >= 1 when set")
        return v


class MetadataBackfillConfig(GuardrailConfig):
    batch_size: int = Field(default=50, ge=1, le=200)
    max_markets: int | None = None
    force: bool = False
    include_slugs: bool = True
    include_event_slugs: bool = True
    include_end_dates: bool = True
    gamma_requests_per_second: float | None = Field(default=None)
    event_slug_fallback_max_pages: int | None = Field(
        default=DEFAULT_EVENT_SLUG_FALLBACK_MAX_PAGES
    )
    event_slug_fallback_max_pages_without_progress: int | None = Field(
        default=DEFAULT_EVENT_SLUG_FALLBACK_MAX_NO_PROGRESS_PAGES
    )
    progress_log_interval_batches: int = Field(default=10, ge=1)
    event_slug_fallback_progress_pages: int = Field(default=25, ge=1)

    @field_validator("gamma_requests_per_second")
    @classmethod
    def _gamma_rps_positive(cls, v: float | None) -> float | None:
        if v is not None and v <= 0:
            raise ValueError("gamma_requests_per_second must be positive when set")
        return v


class OddsSyncConfig(GuardrailConfig):
    workers: int = 40
    batch_size: int = 50_000
    progress_log_interval_tokens: int = Field(default=100, ge=1)
    fidelity: int = Field(
        default=DEFAULT_ODDS_FIDELITY_MINUTES, ge=MIN_ODDS_FIDELITY_MINUTES
    )
    requests_per_second: int | None = 40
    auto_tune_rps: bool = True
    auto_tune_max_rps: int | None = Field(default=60, ge=1)
    force: bool = False
    clob_cutoff: str = "2023-01-01"
    skip_recent_minutes: int = 15
    overlap_minutes: int = 5
    window_hours: int = 8760
    rebuild_history: bool = False
    reconcile_ledger: bool = False
    empty_skip_runs: int = 2
    routine_interval_hours: int = Field(default=6, ge=1)
    empty_retry_base_hours: int = Field(default=24, ge=1)
    empty_retry_max_hours: int = Field(default=168, ge=1)
    error_retry_minutes: int = Field(default=30, ge=1)
    transient_retries: int = 2
    transient_backoff_seconds: float = 0.25
    short_range_first: bool = True
    market_page_size: int = 2000
    ended_market_grace_days: int | None = Field(default=7, ge=0)
    min_volume: float | None = Field(default=POLYMARKET_WC2026_KNOCKOUT_MIN_VOLUME_USD)
    max_volume: float | None = None
    history_backfill_days: int = Field(default=0, ge=0)

    @field_validator("min_volume", "max_volume")
    @classmethod
    def _validate_volume_bound(cls, v: float | None) -> float | None:
        if v is None:
            return None
        parsed = float(v)
        if parsed < 0:
            raise ValueError("volume bounds must be >= 0")
        return parsed


class HourlyOddsSyncConfig(OddsSyncConfig):
    fidelity: int = Field(default=60, ge=MIN_ODDS_FIDELITY_MINUTES)
    force: bool = True
    skip_recent_minutes: int = 1
    overlap_minutes: int = 60
    window_hours: int = POLYMARKET_WC2026_HOURLY_WINDOW_HOURS
    history_backfill_days: int = Field(
        default=POLYMARKET_WC2026_HOURLY_WINDOW_DAYS,
        ge=0,
    )
    routine_interval_hours: int = Field(default=1, ge=1)
    min_volume: float | None = Field(default=POLYMARKET_WC2026_KNOCKOUT_MIN_VOLUME_USD)
    max_volume: float | None = None
    ended_market_grace_days: int | None = Field(default=7, ge=0)


class DbtBuildConfig(GuardrailConfig):
    progress_log_interval_events: int = Field(default=20, ge=1)
    no_progress_hard_timeout_seconds: int | None = Field(
        default=DEFAULT_DBT_NO_PROGRESS_HARD_TIMEOUT_SECONDS,
        ge=1,
    )
    full_refresh: bool = False
    dbt_select: str | None = None
    dbt_exclude: str | None = None
    fetch_dbt_metadata: bool = False


def polymarket_us_midterms_2026_full_refresh_events_run_config() -> dict:
    markets_cfg = MarketsSyncConfig(
        discovery_mode="targeted",
        force_full_discovery=True,
        max_pages_without_progress=None,
    )
    registry_cfg = MarketScopeRegistryConfig(
        force_refresh=True,
        max_pages_without_progress=None,
    )
    metadata_cfg = MetadataBackfillConfig()
    return {
        "ops": {
            "polymarket_us_midterms_2026_raw_markets": {
                "config": markets_cfg.model_dump()
            },
            "polymarket_us_midterms_2026_ops_market_scope_registry": {
                "config": registry_cfg.model_dump()
            },
            "polymarket_us_midterms_2026_raw_market_metadata_backfill": {
                "config": metadata_cfg.model_dump()
            },
        }
    }


def polymarket_us_midterms_2026_hourly_odds_run_config() -> dict:
    odds_cfg = HourlyOddsSyncConfig(
        fidelity=60,
        force=True,
        skip_recent_minutes=1,
        overlap_minutes=60,
        window_hours=POLYMARKET_US_MIDTERMS_2026_HOURLY_WINDOW_HOURS,
        history_backfill_days=POLYMARKET_US_MIDTERMS_2026_HOURLY_WINDOW_DAYS,
        routine_interval_hours=1,
        min_volume=POLYMARKET_US_MIDTERMS_2026_MIN_VOLUME_USD,
        max_volume=None,
        ended_market_grace_days=7,
    )
    return {
        "ops": {
            "polymarket_us_midterms_2026_raw_token_odds_history_hourly": {
                "config": odds_cfg.model_dump()
            },
        }
    }


def polymarket_wc2026_dbt_build_run_config() -> dict:
    dbt_cfg = DbtBuildConfig(
        full_refresh=True,
        dbt_select=POLYMARKET_WC2026_SCOPE.dbt_select,
        dbt_exclude=POLYMARKET_WC2026_SCOPE.dbt_exclude,
    )
    return {"ops": {"oddsfox_dbt": {"config": dbt_cfg.model_dump()}}}


def polymarket_us_midterms_2026_dbt_build_run_config() -> dict:
    dbt_cfg = DbtBuildConfig(
        full_refresh=True,
        dbt_select=POLYMARKET_US_MIDTERMS_2026_SCOPE.dbt_select,
        dbt_exclude=POLYMARKET_US_MIDTERMS_2026_SCOPE.dbt_exclude,
    )
    return {"ops": {"oddsfox_dbt": {"config": dbt_cfg.model_dump()}}}


def polymarket_wc2026_full_refresh_events_run_config() -> dict:
    markets_cfg = MarketsSyncConfig(
        discovery_mode="full_keyset",
        force_full_discovery=True,
        max_pages_without_progress=None,
    )
    registry_cfg = MarketScopeRegistryConfig(
        force_refresh=True,
        max_pages_without_progress=None,
    )
    metadata_cfg = MetadataBackfillConfig()
    return {
        "ops": {
            "polymarket_wc2026_raw_markets": {"config": markets_cfg.model_dump()},
            "polymarket_wc2026_ops_market_scope_registry": {
                "config": registry_cfg.model_dump()
            },
            "polymarket_wc2026_raw_market_metadata_backfill": {
                "config": metadata_cfg.model_dump()
            },
        }
    }


def polymarket_wc2026_hourly_odds_run_config() -> dict:
    odds_cfg = HourlyOddsSyncConfig()
    return {
        "ops": {
            "polymarket_wc2026_raw_token_odds_history_hourly": {
                "config": odds_cfg.model_dump()
            },
        }
    }


class KalshiMarketsSyncConfig(GuardrailConfig):
    progress_log_interval_pages: int = Field(default=10, ge=1)


class KalshiMarketScopeRegistryConfig(GuardrailConfig):
    skip_if_snapshot_refreshed: bool = True
    force_refresh: bool = False


class KalshiHourlyOddsSyncConfig(GuardrailConfig):
    progress_log_interval_markets: int = Field(default=10, ge=1)
    window_hours: int = Field(default=KALSHI_WC2026_HOURLY_WINDOW_HOURS, ge=1)
    history_backfill_days: int = Field(
        default=KALSHI_WC2026_HOURLY_WINDOW_DAYS,
        ge=0,
    )
    force: bool = True
    routine_interval_hours: int = Field(default=1, ge=1)


def kalshi_wc2026_full_refresh_events_run_config() -> dict:
    markets_cfg = KalshiMarketsSyncConfig()
    registry_cfg = KalshiMarketScopeRegistryConfig(force_refresh=True)
    return {
        "ops": {
            "kalshi_wc2026_raw_markets": {"config": markets_cfg.model_dump()},
            "kalshi_wc2026_ops_market_scope_registry": {
                "config": registry_cfg.model_dump()
            },
        }
    }


def kalshi_wc2026_hourly_odds_run_config() -> dict:
    odds_cfg = KalshiHourlyOddsSyncConfig()
    return {
        "ops": {
            "kalshi_wc2026_raw_market_candlesticks_hourly": {
                "config": odds_cfg.model_dump()
            },
        }
    }


def kalshi_wc2026_dbt_build_run_config() -> dict:
    dbt_cfg = DbtBuildConfig(
        full_refresh=True,
        dbt_select=KALSHI_WC2026_SCOPE.dbt_select,
        dbt_exclude=KALSHI_WC2026_SCOPE.dbt_exclude,
    )
    return {"ops": {"oddsfox_dbt": {"config": dbt_cfg.model_dump()}}}
