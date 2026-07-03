"""Dagster schedules."""

from __future__ import annotations

import logging

from dagster import DefaultScheduleStatus, ScheduleDefinition

from oddsfox_pipeline.config.settings import (
    WC2026_POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED,
    WC2026_POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED,
    WC2026_POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED,
)
from oddsfox_pipeline.orchestration.config import (
    wc2026_hourly_odds_run_config,
    wc2026_minutely_odds_cold_run_config,
)
from oddsfox_pipeline.orchestration.jobs import (
    wc2026_hourly_odds_ingest,
    wc2026_minutely_odds_ingest,
)

logger = logging.getLogger(__name__)

_both_minutely_flags_enabled = (
    WC2026_POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED
    and WC2026_POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED
)
if _both_minutely_flags_enabled:
    logger.warning(
        "Both WC2026_POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED and "
        "WC2026_POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED are true; "
        "keeping only wc2026_minutely_odds_live_schedule RUNNING."
    )

_standard_minutely_running = (
    WC2026_POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED
    and not _both_minutely_flags_enabled
)

wc2026_minutely_odds_schedule = ScheduleDefinition(
    name="wc2026_minutely_odds_schedule",
    job=wc2026_minutely_odds_ingest,
    cron_schedule="*/5 * * * *",
    default_status=(
        DefaultScheduleStatus.RUNNING
        if _standard_minutely_running
        else DefaultScheduleStatus.STOPPED
    ),
    description=(
        "Every 5 minutes: minutely odds for WC2026 markets. Controlled by "
        "WC2026_POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED."
    ),
)

wc2026_minutely_odds_cold_schedule = ScheduleDefinition(
    name="wc2026_minutely_odds_cold_schedule",
    job=wc2026_minutely_odds_ingest,
    cron_schedule="0 * * * *",
    run_config=wc2026_minutely_odds_cold_run_config(),
    default_status=(
        DefaultScheduleStatus.RUNNING
        if _standard_minutely_running
        else DefaultScheduleStatus.STOPPED
    ),
    description=(
        "Hourly conservative minutely odds refresh for WC2026 markets. Enabled "
        "with WC2026_POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED."
    ),
)

wc2026_minutely_odds_live_schedule = ScheduleDefinition(
    name="wc2026_minutely_odds_live_schedule",
    job=wc2026_minutely_odds_ingest,
    cron_schedule="*/1 * * * *",
    default_status=(
        DefaultScheduleStatus.RUNNING
        if WC2026_POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED
        else DefaultScheduleStatus.STOPPED
    ),
    description=(
        "Every minute: live WC2026 minutely odds refresh. Gated by "
        "WC2026_POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED."
    ),
)

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

__all__ = [
    "wc2026_hourly_odds_schedule",
    "wc2026_minutely_odds_cold_schedule",
    "wc2026_minutely_odds_live_schedule",
    "wc2026_minutely_odds_schedule",
]
