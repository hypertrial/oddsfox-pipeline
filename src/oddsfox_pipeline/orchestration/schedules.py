"""Dagster schedules."""

from __future__ import annotations

from dagster import DefaultScheduleStatus, ScheduleDefinition

from oddsfox_pipeline.config.settings import (
    POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SCHEDULE_ENABLED,
    POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED,
)
from oddsfox_pipeline.orchestration.config import (
    polymarket_us_midterms_2026_hourly_odds_run_config,
    polymarket_wc2026_hourly_odds_run_config,
)
from oddsfox_pipeline.orchestration.jobs import (
    polymarket_us_midterms_2026_hourly_odds_ingest,
    polymarket_wc2026_hourly_odds_ingest,
)

polymarket_wc2026_hourly_odds_schedule = ScheduleDefinition(
    name="polymarket_wc2026_hourly_odds_schedule",
    job=polymarket_wc2026_hourly_odds_ingest,
    cron_schedule="0 * * * *",
    run_config=polymarket_wc2026_hourly_odds_run_config(),
    default_status=(
        DefaultScheduleStatus.RUNNING
        if POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED
        else DefaultScheduleStatus.STOPPED
    ),
    description=(
        "Hourly WC2026 odds refresh at CLOB fidelity=60. Controlled by "
        "POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED."
    ),
)

polymarket_us_midterms_2026_hourly_odds_schedule = ScheduleDefinition(
    name="polymarket_us_midterms_2026_hourly_odds_schedule",
    job=polymarket_us_midterms_2026_hourly_odds_ingest,
    cron_schedule="0 * * * *",
    run_config=polymarket_us_midterms_2026_hourly_odds_run_config(),
    default_status=(
        DefaultScheduleStatus.RUNNING
        if POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SCHEDULE_ENABLED
        else DefaultScheduleStatus.STOPPED
    ),
    description=(
        "Hourly US midterms 2026 odds refresh at CLOB fidelity=60. Controlled by "
        "POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SCHEDULE_ENABLED."
    ),
)

__all__ = [
    "polymarket_us_midterms_2026_hourly_odds_schedule",
    "polymarket_wc2026_hourly_odds_schedule",
]
