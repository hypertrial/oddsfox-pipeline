from __future__ import annotations

from typing import Literal

from dagster import Config
from pydantic import Field, field_validator, model_validator

from oddsfox_pipeline.config.settings import (
    DEFAULT_ODDS_FIDELITY_MINUTES,
    KALSHI_WC2026_HOURLY_WINDOW_DAYS,
    KALSHI_WC2026_HOURLY_WINDOW_HOURS,
    MIN_ODDS_FIDELITY_MINUTES,
    ODDS_REQUESTS_PER_SECOND,
    POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
    POLYMARKET_WC2026_HOURLY_WINDOW_HOURS,
)
from oddsfox_pipeline.orchestration.shipped_scopes import (
    KALSHI_WC2026_SCOPE,
    POLYMARKET_WC2026_SCOPE,
)
from oddsfox_pipeline.publishing.polygon_settlement import (
    DEFAULT_POLYGON_SETTLEMENT_AUDIT_ROOT,
    PolygonSettlementAuditSpec,
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
    discovery_mode: Literal["targeted", "full_keyset"] = "targeted"
    refresh_registry: bool = False
    force_full_discovery: bool = False
    max_event_pages: int | None = None
    keyset_closed: bool | None = None
    keyset_tag_slugs: list[str] | None = None
    keyset_volume_min: float | None = Field(default=None, ge=0)
    max_pages_without_progress: int | None = None

    @field_validator("max_pages_without_progress")
    @classmethod
    def _max_pages_without_progress_nonnegative(cls, v: int | None) -> int | None:
        if v is not None and v < 1:
            raise ValueError("max_pages_without_progress must be >= 1 when set")
        return v


class MarketScopeRegistryConfig(GuardrailConfig):
    progress_log_interval_pages: int = Field(default=10, ge=1)
    max_event_pages: int | None = None
    keyset_closed: bool | None = None
    keyset_tag_slugs: list[str] | None = None
    keyset_volume_min: float | None = Field(default=None, ge=0)
    max_pages_without_progress: int | None = None
    # Routine jobs set False; exhaustive recall is the dedicated audit job.
    include_slug_prefix_recall: bool = True
    slug_prefix_recall_max_pages_without_progress: int | None = Field(default=500, ge=1)
    reset_event_catalog_checkpoint: bool = False
    skip_if_snapshot_refreshed: bool = True
    force_refresh: bool = False
    apply_event_volume_eligibility_gate: bool = True

    @field_validator(
        "max_pages_without_progress",
        "slug_prefix_recall_max_pages_without_progress",
    )
    @classmethod
    def _max_pages_without_progress_nonnegative(cls, v: int | None) -> int | None:
        if v is not None and v < 1:
            raise ValueError("max_pages_without_progress fields must be >= 1 when set")
        return v


class MetadataEnrichmentConfig(GuardrailConfig):
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
    requests_per_second: int | None = ODDS_REQUESTS_PER_SECOND
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
    batch_group_size: int = Field(default=20, ge=1, le=20)
    routine_interval_hours: int = Field(default=6, ge=1)
    empty_retry_base_hours: int = Field(default=24, ge=1)
    empty_retry_max_hours: int = Field(default=168, ge=1)
    error_retry_minutes: int = Field(default=30, ge=1)
    transient_retries: int = 2
    transient_backoff_seconds: float = 0.25
    short_range_first: bool = True
    market_page_size: int = 2000
    ended_market_grace_days: int | None = Field(default=7, ge=0)
    min_volume: float | None = Field(default=POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD)
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
    """WC2026 hourly odds defaults collect from market creation.

    The 30-day pipeline-policy window remains a dbt presentation bound. Raw
    history is retained from market creation so later temporal pipelines can
    widen their query window without re-fetching. Child markets of admitted
    events are not filtered by market-grain volume or ended-market grace.
    """

    fidelity: int = Field(default=60, ge=MIN_ODDS_FIDELITY_MINUTES)
    force: bool = False
    skip_recent_minutes: int = 1
    overlap_minutes: int = 60
    window_hours: int = POLYMARKET_WC2026_HOURLY_WINDOW_HOURS
    history_backfill_days: int = Field(default=0, ge=0)
    routine_interval_hours: int = Field(default=1, ge=1)
    auto_tune_max_rps: int | None = Field(default=90, ge=1)
    min_volume: float | None = None
    max_volume: float | None = None
    ended_market_grace_days: int | None = None


class MatchMinuteOddsSyncConfig(GuardrailConfig):
    workers: int = Field(default=40, ge=1, le=100)
    requests_per_second: int = Field(default=40, ge=1)
    batch_group_size: int = Field(default=20, ge=1, le=20)
    window_hours: int = Field(default=24, ge=1)
    auto_tune_rps: bool = True
    auto_tune_max_rps: int | None = Field(default=90, ge=1)
    transient_retries: int = Field(default=2, ge=0)
    transient_backoff_seconds: float = Field(default=0.25, ge=0)


class FuturesMinuteOddsSyncConfig(GuardrailConfig):
    workers: int = Field(default=40, ge=1, le=100)
    requests_per_second: int = Field(default=40, ge=1)
    batch_group_size: int = Field(default=20, ge=1, le=20)
    window_hours: int = Field(default=24, ge=1)
    auto_tune_rps: bool = True
    auto_tune_max_rps: int | None = Field(default=90, ge=1)
    transient_retries: int = Field(default=2, ge=0)
    transient_backoff_seconds: float = Field(default=0.25, ge=0)


class MatchOrderBookBackfillConfig(GuardrailConfig):
    requests_per_minute: int = Field(default=50, ge=1, le=60)
    monthly_credit_budget: int = Field(default=20_000, ge=1, le=25_000)
    transient_retries: int = Field(default=4, ge=0, le=10)
    transient_backoff_seconds: float = Field(default=1.0, ge=0, le=120)
    force: bool = False
    manifest_path: str | None = None


class PolygonSettlementSyncConfig(GuardrailConfig):
    requests_per_second: float = Field(default=5.0, gt=0)
    workers: int = Field(default=5, ge=1)
    initial_block_chunk_size: int = Field(default=8_000, ge=250, le=20_000)
    initial_receipt_batch_size: int = Field(default=20, ge=5, le=50)
    transient_retries: int = Field(default=4, ge=0)
    transient_backoff_seconds: float = Field(default=0.5, ge=0)
    expected_duckdb_path: str | None = None


class PolygonSettlementReleaseConfig(Config):
    dataset_version: str
    output_root: str = str(DEFAULT_POLYGON_SETTLEMENT_AUDIT_ROOT)

    @model_validator(mode="after")
    def _validate_release(self) -> "PolygonSettlementReleaseConfig":
        PolygonSettlementAuditSpec(dataset_version=self.dataset_version)
        if not self.output_root.strip():
            raise ValueError("output_root must not be blank")
        return self


class DbtBuildConfig(GuardrailConfig):
    progress_log_interval_events: int = Field(default=20, ge=1)
    no_progress_hard_timeout_seconds: int | None = Field(
        default=DEFAULT_DBT_NO_PROGRESS_HARD_TIMEOUT_SECONDS,
        ge=1,
    )
    full_refresh: bool = False
    dbt_select: str | None = None
    dbt_exclude: str | None = "tag:polygon_settlement tag:pmxt_order_book"
    fetch_dbt_metadata: bool = False
    expected_duckdb_path: str | None = None


def polymarket_wc2026_dbt_build_run_config() -> dict:
    dbt_cfg = DbtBuildConfig(
        full_refresh=False,
        dbt_select=POLYMARKET_WC2026_SCOPE.dbt_select,
        dbt_exclude=POLYMARKET_WC2026_SCOPE.dbt_exclude,
    )
    return {"ops": {"oddsfox_dbt": {"config": dbt_cfg.model_dump()}}}


def polymarket_wc2026_full_pipeline_run_config() -> dict:
    ops: dict = {}
    for cfg in (
        polymarket_wc2026_full_refresh_events_run_config(),
        polymarket_wc2026_hourly_odds_run_config(),
        polymarket_wc2026_dbt_build_run_config(),
    ):
        ops.update(cfg["ops"])
    return {"ops": ops}


def polymarket_wc2026_full_refresh_events_run_config() -> dict:
    markets_cfg = MarketsSyncConfig(
        discovery_mode="full_keyset",
        refresh_registry=True,
        force_full_discovery=True,
        max_pages_without_progress=None,
    )
    registry_cfg = MarketScopeRegistryConfig(
        force_refresh=True,
        max_pages_without_progress=None,
        include_slug_prefix_recall=False,
    )
    # Re-evaluate missing metadata on each full registry refresh.
    metadata_cfg = MetadataEnrichmentConfig(force=True)
    return {
        "ops": {
            "polymarket_wc2026_raw_markets": {"config": markets_cfg.model_dump()},
            "polymarket_wc2026_raw_event_catalog": {
                "config": registry_cfg.model_dump()
            },
            "polymarket_wc2026_ops_market_scope_registry": {
                "config": registry_cfg.model_dump()
            },
            "polymarket_wc2026_raw_market_metadata_enrichment": {
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


def polymarket_wc2026_match_minute_odds_run_config() -> dict:
    markets = MarketsSyncConfig(
        discovery_mode="full_keyset",
        refresh_registry=True,
        force_full_discovery=True,
        keyset_closed=True,
        keyset_volume_min=0.0,
        max_pages_without_progress=None,
    )
    registry = MarketScopeRegistryConfig(
        force_refresh=True,
        keyset_closed=True,
        keyset_volume_min=0.0,
        apply_event_volume_eligibility_gate=False,
        max_pages_without_progress=None,
        # Keep slug-prefix recall exhaustive for the 104/248/496 fixture contract.
        include_slug_prefix_recall=True,
        slug_prefix_recall_max_pages_without_progress=None,
    )
    dbt = DbtBuildConfig(
        full_refresh=False,
        dbt_select="+polymarket_wc2026_match_minute_odds",
    )
    return {
        "ops": {
            "polymarket_wc2026_raw_markets": {"config": markets.model_dump()},
            "polymarket_wc2026_raw_event_catalog": {"config": registry.model_dump()},
            "polymarket_wc2026_ops_market_scope_registry": {
                "config": registry.model_dump()
            },
            "polymarket_wc2026_raw_market_metadata_enrichment": {
                "config": MetadataEnrichmentConfig().model_dump()
            },
            "polymarket_wc2026_raw_match_token_odds_history_minute": {
                "config": MatchMinuteOddsSyncConfig().model_dump()
            },
            "oddsfox_dbt": {"config": dbt.model_dump()},
        }
    }


def polymarket_wc2026_minute_odds_run_config() -> dict:
    """Unified minute-odds backfill: match windows + futures tournament span."""
    base = polymarket_wc2026_match_minute_odds_run_config()
    ops = dict(base["ops"])
    # Include open markets so in-tournament futures are discoverable; match-minute
    # selection still requires closed game markets for the 104/248/496 contract.
    markets = MarketsSyncConfig(
        discovery_mode="full_keyset",
        refresh_registry=True,
        force_full_discovery=True,
        keyset_closed=None,
        keyset_volume_min=0.0,
        max_pages_without_progress=None,
    )
    registry = MarketScopeRegistryConfig(
        force_refresh=True,
        keyset_closed=None,
        keyset_volume_min=0.0,
        apply_event_volume_eligibility_gate=True,
        max_pages_without_progress=None,
        include_slug_prefix_recall=True,
        slug_prefix_recall_max_pages_without_progress=None,
    )
    ops["polymarket_wc2026_raw_markets"] = {"config": markets.model_dump()}
    ops["polymarket_wc2026_raw_event_catalog"] = {"config": registry.model_dump()}
    ops["polymarket_wc2026_ops_market_scope_registry"] = {
        "config": registry.model_dump()
    }
    ops["polymarket_wc2026_raw_futures_token_odds_history_minute"] = {
        "config": FuturesMinuteOddsSyncConfig().model_dump()
    }
    ops["oddsfox_dbt"] = {
        "config": DbtBuildConfig(
            full_refresh=False,
            dbt_select="+polymarket_wc2026_market_minute_odds_data_quality",
            dbt_exclude=None,
        ).model_dump()
    }
    return {"ops": ops}


def polymarket_wc2026_event_catalog_recall_audit_run_config() -> dict:
    """Exhaustive slug-prefix recall audit (manual / rare completeness check)."""
    markets_cfg = MarketsSyncConfig(
        discovery_mode="full_keyset",
        refresh_registry=True,
        force_full_discovery=True,
        max_pages_without_progress=None,
    )
    registry_cfg = MarketScopeRegistryConfig(
        force_refresh=True,
        max_pages_without_progress=None,
        include_slug_prefix_recall=True,
        slug_prefix_recall_max_pages_without_progress=None,
    )
    metadata_cfg = MetadataEnrichmentConfig(force=True)
    return {
        "ops": {
            "polymarket_wc2026_raw_markets": {"config": markets_cfg.model_dump()},
            "polymarket_wc2026_raw_event_catalog": {
                "config": registry_cfg.model_dump()
            },
            "polymarket_wc2026_ops_market_scope_registry": {
                "config": registry_cfg.model_dump()
            },
            "polymarket_wc2026_raw_market_metadata_enrichment": {
                "config": metadata_cfg.model_dump()
            },
        }
    }


def polymarket_wc2026_match_order_book_run_config(
    *, manifest_path: str | None = None
) -> dict:
    dbt = DbtBuildConfig(
        full_refresh=False,
        dbt_select="+tag:pmxt_order_book",
        dbt_exclude="tag:polygon_settlement tag:match_minute",
    )
    return {
        "ops": {
            "polymarket_wc2026_raw_match_order_book_snapshots": {
                "config": MatchOrderBookBackfillConfig(
                    manifest_path=manifest_path
                ).model_dump()
            },
            "oddsfox_dbt": {"config": dbt.model_dump()},
        }
    }


def polymarket_wc2026_market_portrait_run_config(
    *, manifest_path: str | None = None
) -> dict:
    config = polymarket_wc2026_match_order_book_run_config(manifest_path=manifest_path)
    config["ops"]["polymarket_wc2026_raw_match_trades"] = {
        "config": MatchOrderBookBackfillConfig(manifest_path=manifest_path).model_dump()
    }
    config["ops"]["oddsfox_dbt"]["config"]["dbt_select"] = (
        "+tag:pmxt_order_book +tag:market_portrait"
    )
    return config


def polymarket_wc2026_polygon_settlement_backfill_run_config(
    *,
    expected_duckdb_path: str | None = None,
    requests_per_second: float = 5.0,
    workers: int = 5,
    initial_block_chunk_size: int = 8_000,
    initial_receipt_batch_size: int = 20,
) -> dict:
    dbt = DbtBuildConfig(
        full_refresh=False,
        dbt_select="+polymarket_wc2026_polygon_settlement_minute_odds",
        dbt_exclude=None,
        expected_duckdb_path=expected_duckdb_path,
    )
    return {
        "ops": {
            "polymarket_wc2026_raw_polygon_settlement_fills": {
                "config": PolygonSettlementSyncConfig(
                    expected_duckdb_path=expected_duckdb_path,
                    requests_per_second=requests_per_second,
                    workers=workers,
                    initial_block_chunk_size=initial_block_chunk_size,
                    initial_receipt_batch_size=initial_receipt_batch_size,
                ).model_dump()
            },
            "oddsfox_dbt": {"config": dbt.model_dump()},
        }
    }


def polymarket_wc2026_polygon_settlement_release_run_config(
    *,
    dataset_version: str,
    output_root: str = str(DEFAULT_POLYGON_SETTLEMENT_AUDIT_ROOT),
) -> dict:
    release = PolygonSettlementReleaseConfig(
        dataset_version=dataset_version,
        output_root=output_root,
    )
    return {
        "ops": {
            "polymarket_wc2026_release_polygon_settlement_odds_bundle": {
                "config": release.model_dump()
            }
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
    force: bool = False
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
        full_refresh=False,
        dbt_select=KALSHI_WC2026_SCOPE.dbt_select,
        dbt_exclude=KALSHI_WC2026_SCOPE.dbt_exclude,
    )
    return {"ops": {"oddsfox_dbt": {"config": dbt_cfg.model_dump()}}}
