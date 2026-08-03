# Enable schedules

Enable an hourly schedule only after the corresponding manual registry, odds,
and dbt jobs complete successfully against the intended warehouse.

## Available schedules

| Schedule | Target job | `.env` flag |
| --- | --- | --- |
| `polymarket_wc2026_hourly_odds_schedule` | `polymarket_wc2026_hourly_odds_ingest` | `POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED` |
| `kalshi_wc2026_hourly_odds_schedule` | `kalshi_wc2026_hourly_odds_ingest` | `KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED` |

Both use hourly fidelity (`fidelity=60`). They are stopped by default. Polygon
settlement jobs remain unscheduled.

## Enable one source

Change only the required `.env` value. Example for Polymarket WC2026 only:

```dotenv
POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=true
KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
```

Restart `uv run make dagster-dev` after changing schedule configuration, then
confirm the expected schedule is running in the Dagster UI.

!!! warning "Do not enable schedules as a first-run shortcut"

    A schedule repeats the odds job; it does not repair failed discovery,
    configuration, schema, or dbt state. Complete a manual full run first.

Next, use [Validate and recover](validate-and-recover.md) to monitor freshness
and handle gaps.
