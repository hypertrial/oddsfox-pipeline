"""Dagster schedules."""

from __future__ import annotations

from dagster import DefaultScheduleStatus, ScheduleDefinition

from oddsfox_pipeline.config.settings import (
    KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED,
    POLYMARKET_SOCCER_DAILY_SCHEDULE_ENABLED,
)
from oddsfox_pipeline.orchestration.config import (
    kalshi_wc2026_hourly_odds_run_config,
    polymarket_soccer_full_pipeline_run_config,
)
from oddsfox_pipeline.orchestration.jobs import (
    international_results_historical_ingest,
    kalshi_wc2026_hourly_odds_ingest,
    polymarket_soccer_full_pipeline,
)

international_results_daily_schedule = ScheduleDefinition(
    name="international_results_daily_schedule",
    job=international_results_historical_ingest,
    cron_schedule="15 2 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Daily public 2006+ international results, shootout, and goals refresh.",
)

kalshi_wc2026_hourly_odds_schedule = ScheduleDefinition(
    name="kalshi_wc2026_hourly_odds_schedule",
    job=kalshi_wc2026_hourly_odds_ingest,
    cron_schedule="0 * * * *",
    run_config=kalshi_wc2026_hourly_odds_run_config(),
    default_status=(
        DefaultScheduleStatus.RUNNING
        if KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED
        else DefaultScheduleStatus.STOPPED
    ),
    description=(
        "Hourly Kalshi WC2026 candlestick refresh. Controlled by "
        "KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED."
    ),
)

polymarket_soccer_daily_schedule = ScheduleDefinition(
    name="polymarket_soccer_daily_schedule",
    job=polymarket_soccer_full_pipeline,
    cron_schedule="0 4 * * *",
    run_config=polymarket_soccer_full_pipeline_run_config(),
    default_status=(
        DefaultScheduleStatus.RUNNING
        if POLYMARKET_SOCCER_DAILY_SCHEDULE_ENABLED
        else DefaultScheduleStatus.STOPPED
    ),
    description=(
        "Daily soccer catalog catch-up, match-result minute ingestion, and dbt "
        "publication. Controlled by POLYMARKET_SOCCER_DAILY_SCHEDULE_ENABLED."
    ),
)

__all__ = [
    "international_results_daily_schedule",
    "kalshi_wc2026_hourly_odds_schedule",
    "polymarket_soccer_daily_schedule",
]
