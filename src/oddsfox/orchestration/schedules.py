"""Dagster schedules."""

from __future__ import annotations

import logging

from dagster import DefaultScheduleStatus, ScheduleDefinition

from oddsfox.config.settings import (
    POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED,
    POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED,
)
from oddsfox.orchestration.config import minutely_odds_cold_run_config
from oddsfox.orchestration.jobs import polymarket_minutely_odds_ingest

logger = logging.getLogger(__name__)

_both_minutely_flags_enabled = (
    POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED
    and POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED
)
if _both_minutely_flags_enabled:
    logger.warning(
        "Both POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED and "
        "POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED are true; "
        "keeping only polymarket_minutely_odds_live_schedule RUNNING."
    )

_standard_minutely_running = (
    POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED and not _both_minutely_flags_enabled
)

polymarket_minutely_odds_schedule = ScheduleDefinition(
    name="polymarket_minutely_odds_schedule",
    job=polymarket_minutely_odds_ingest,
    cron_schedule="*/5 * * * *",
    default_status=(
        DefaultScheduleStatus.RUNNING
        if _standard_minutely_running
        else DefaultScheduleStatus.STOPPED
    ),
    description=(
        "Every 5 minutes: minutely odds for WC2026 whale markets. Controlled by "
        "POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED."
    ),
)

polymarket_minutely_odds_cold_schedule = ScheduleDefinition(
    name="polymarket_minutely_odds_cold_schedule",
    job=polymarket_minutely_odds_ingest,
    cron_schedule="0 * * * *",
    run_config=minutely_odds_cold_run_config(),
    default_status=(
        DefaultScheduleStatus.RUNNING
        if _standard_minutely_running
        else DefaultScheduleStatus.STOPPED
    ),
    description=(
        "Hourly conservative minutely odds refresh for WC2026 whale markets. Enabled "
        "with POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED."
    ),
)

polymarket_minutely_odds_live_schedule = ScheduleDefinition(
    name="polymarket_minutely_odds_live_schedule",
    job=polymarket_minutely_odds_ingest,
    cron_schedule="*/1 * * * *",
    default_status=(
        DefaultScheduleStatus.RUNNING
        if POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED
        else DefaultScheduleStatus.STOPPED
    ),
    description=(
        "Every minute during tournament: live minutely odds refresh. Gated by "
        "POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED."
    ),
)

__all__ = [
    "polymarket_minutely_odds_cold_schedule",
    "polymarket_minutely_odds_live_schedule",
    "polymarket_minutely_odds_schedule",
]
