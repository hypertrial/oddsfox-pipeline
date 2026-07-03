"""Dagster schedules."""

from __future__ import annotations

import logging

from dagster import DefaultScheduleStatus, ScheduleDefinition

from oddsfox_pipeline.config.settings import (
    POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED,
    POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED,
    POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED,
)
from oddsfox_pipeline.orchestration.config import (
    hourly_odds_run_config,
    minutely_odds_cold_run_config,
)
from oddsfox_pipeline.orchestration.jobs import (
    polymarket_hourly_odds_ingest,
    polymarket_minutely_odds_ingest,
)

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
        "Every 5 minutes: minutely odds for selected-scope whale markets. Controlled by "
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
        "Hourly conservative minutely odds refresh for selected-scope whale markets. Enabled "
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
        "Every minute: live selected-scope minutely odds refresh. Gated by "
        "POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED."
    ),
)

polymarket_hourly_odds_schedule = ScheduleDefinition(
    name="polymarket_hourly_odds_schedule",
    job=polymarket_hourly_odds_ingest,
    cron_schedule="0 * * * *",
    run_config=hourly_odds_run_config(),
    default_status=(
        DefaultScheduleStatus.RUNNING
        if POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED
        else DefaultScheduleStatus.STOPPED
    ),
    description=(
        "Hourly selected-scope odds refresh at CLOB fidelity=60. Controlled by "
        "POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED."
    ),
)

__all__ = [
    "polymarket_hourly_odds_schedule",
    "polymarket_minutely_odds_cold_schedule",
    "polymarket_minutely_odds_live_schedule",
    "polymarket_minutely_odds_schedule",
]
