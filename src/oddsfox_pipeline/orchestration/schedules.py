"""Dagster schedules."""

from __future__ import annotations

from dagster import DefaultScheduleStatus, ScheduleDefinition

from oddsfox_pipeline.config.settings import (
    KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED,
    POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SCHEDULE_ENABLED,
    POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED,
    WC2026_KNOCKOUT_MATCH_ODDS_HOURLY_SCHEDULE_ENABLED,
)
from oddsfox_pipeline.orchestration.config import (
    kalshi_wc2026_hourly_odds_run_config,
    polymarket_us_midterms_2026_hourly_odds_run_config,
    polymarket_wc2026_hourly_odds_run_config,
    wc2026_knockout_match_odds_full_pipeline_run_config,
)
from oddsfox_pipeline.orchestration.jobs import (
    kalshi_wc2026_hourly_odds_ingest,
    polymarket_us_midterms_2026_hourly_odds_ingest,
    polymarket_wc2026_hourly_odds_ingest,
    wc2026_knockout_match_odds_full_pipeline,
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

wc2026_knockout_match_odds_hourly_schedule = ScheduleDefinition(
    name="wc2026_knockout_match_odds_hourly_schedule",
    job=wc2026_knockout_match_odds_full_pipeline,
    cron_schedule="0 * * * *",
    run_config=wc2026_knockout_match_odds_full_pipeline_run_config(),
    default_status=(
        DefaultScheduleStatus.RUNNING
        if WC2026_KNOCKOUT_MATCH_ODDS_HOURLY_SCHEDULE_ENABLED
        else DefaultScheduleStatus.STOPPED
    ),
    description=(
        "Atomic hourly WC2026 knockout fixture and match-advance odds refresh. "
        "Controlled by WC2026_KNOCKOUT_MATCH_ODDS_HOURLY_SCHEDULE_ENABLED."
    ),
)

__all__ = [
    "kalshi_wc2026_hourly_odds_schedule",
    "polymarket_us_midterms_2026_hourly_odds_schedule",
    "polymarket_wc2026_hourly_odds_schedule",
    "wc2026_knockout_match_odds_hourly_schedule",
]
