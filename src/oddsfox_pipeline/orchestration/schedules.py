"""Dagster schedules."""

from __future__ import annotations

from dagster import DefaultScheduleStatus, ScheduleDefinition

from oddsfox_pipeline.config.settings import (
    WC2026_POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED,
)
from oddsfox_pipeline.orchestration.config import wc2026_hourly_odds_run_config
from oddsfox_pipeline.orchestration.jobs import wc2026_hourly_odds_ingest

wc2026_hourly_odds_schedule = ScheduleDefinition(
    name="wc2026_hourly_odds_schedule",
    job=wc2026_hourly_odds_ingest,
    cron_schedule="0 * * * *",
    run_config=wc2026_hourly_odds_run_config(),
    default_status=(
        DefaultScheduleStatus.RUNNING
        if WC2026_POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED
        else DefaultScheduleStatus.STOPPED
    ),
    description=(
        "Hourly WC2026 odds refresh at CLOB fidelity=60. Controlled by "
        "WC2026_POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED."
    ),
)

__all__ = ["wc2026_hourly_odds_schedule"]
