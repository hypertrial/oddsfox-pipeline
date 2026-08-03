"""Kalshi / HTTP tuning for the WC2026 pipeline."""

from __future__ import annotations

from oddsfox_pipeline.config._env import _env_bool, _env_int

KALSHI_API_URL = "https://external-api.kalshi.com/trade-api/v2"

KALSHI_REQUESTS_PER_SECOND = _env_int("KALSHI_REQUESTS_PER_SECOND", 5)

WC2026_KALSHI_CONTRACT_DEFAULTS = {
    "scope_name": "wc2026",
    "hourly_window_days": 63,
    "hourly_window_hours": 1512,
    "live_freshness_hours": 3,
    "results_freshness_hours": 12,
}
KALSHI_WC2026_HOURLY_WINDOW_DAYS = int(
    WC2026_KALSHI_CONTRACT_DEFAULTS["hourly_window_days"]
)
KALSHI_WC2026_HOURLY_WINDOW_HOURS = int(
    WC2026_KALSHI_CONTRACT_DEFAULTS["hourly_window_hours"]
)
DEFAULT_KALSHI_WC2026_MARKET_SCOPE = str(WC2026_KALSHI_CONTRACT_DEFAULTS["scope_name"])
KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED = _env_bool(
    "KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED",
    False,
)

__all__ = [
    "DEFAULT_KALSHI_WC2026_MARKET_SCOPE",
    "KALSHI_API_URL",
    "KALSHI_REQUESTS_PER_SECOND",
    "KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED",
    "KALSHI_WC2026_HOURLY_WINDOW_DAYS",
    "KALSHI_WC2026_HOURLY_WINDOW_HOURS",
    "WC2026_KALSHI_CONTRACT_DEFAULTS",
]
