# Enable schedules

Enable an hourly schedule only after the corresponding manual registry, odds,
and dbt jobs complete successfully against the intended warehouse.

## Available schedules

| Schedule | Target job | `.env` flag |
| --- | --- | --- |
| `kalshi_wc2026_hourly_odds_schedule` | `kalshi_wc2026_hourly_odds_ingest` | `KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED` |

Kalshi hourly odds use trade API candlesticks with `period_interval=60` (one
hourly bucket). The schedule is stopped by default. Polygon settlement jobs
remain unscheduled.

Polymarket WC2026 has no Dagster schedule. WC2026 Polymarket events are
complete; run `polymarket_wc2026_hourly_odds_ingest` or
`polymarket_wc2026_full_pipeline` manually when you need a one-off refresh.

## Enable Kalshi hourly odds

Change only the required `.env` value:

```dotenv
KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=true
```

Restart `uv run make dagster-dev` after changing schedule configuration, then
confirm the expected schedule is running in the Dagster UI.

!!! warning "Do not enable schedules as a first-run shortcut"

    A schedule repeats the odds job; it does not repair failed discovery,
    configuration, schema, or dbt state. Complete a manual full run first.

Next, use [Validate and recover](validate-and-recover.md) to monitor freshness
and handle gaps.
