# Run cross-platform knockout match odds

Use this guide to refresh the neutral WC2026 knockout match mart that compares
Polymarket and Kalshi match-advance closes. This is **not** a
`scripts/run_scope.py` ref. It is a fixed job outside the three shipped
scope chooser entries.

## Why a dedicated job

`wc2026_knockout_match_odds_full_pipeline` refreshes the OpenFootball fixture
mirror, both provider registries, both hourly odds sources, permanent provider
facts, and the neutral mart/observability models in one run. The combined job
selects `+tag:cross_domain`.

Source-specific Polymarket and Kalshi dbt jobs **exclude** that tag. Running
`polymarket:wc2026` or `kalshi:wc2026` alone does not publish an atomic
cross-provider comparison. Prefer this job whenever analysts need
`wc2026_marts.wc2026_knockout_match_hourly_odds` to reflect both sources
together.

## Prerequisites

- Local install and `.env` from [Quickstart](../getting-started/index.md).
- Keep schedules disabled until a manual combined run succeeds. The four
  schedule flags should remain `false` unless you intentionally enable one.
- Network access for FIFA/OpenFootball fixture refresh plus Polymarket and
  Kalshi public APIs used by the combined job.

You may run the combined job on a cold warehouse. When provider scopes already
exist, the job still refreshes both sides atomically for the neutral mart.

## Run the job

```bash
uv run python -m dagster job execute \
  -m oddsfox_pipeline.orchestration.definitions \
  -j wc2026_knockout_match_odds_full_pipeline
```

## Confirm success

The public comparison surface is:

```text
wc2026_marts.wc2026_knockout_match_hourly_odds
```

Inspect coverage and hard findings in:

- `wc2026_observability.wc2026_knockout_match_odds_coverage`
- `wc2026_observability.wc2026_knockout_match_odds_data_quality`

Then query with [Query recipes](query-recipes.md) or the
[Data dictionary](../reference/data-dictionary.md) cross-platform section.

Local technical smoke (opt-in live network) is `uv run make live-smoke`. See
[Development](../development/index.md).

## Schedules

The stopped-by-default schedule
`wc2026_knockout_match_odds_hourly_schedule` targets this same job. Enable it
only after a manual combined run is healthy, via
`WC2026_KNOCKOUT_MATCH_ODDS_HOURLY_SCHEDULE_ENABLED=true`. See
[Enable schedules](enable-schedules.md).

Next: [Operators](../audiences/operators.md) or
[Orchestration](../reference/orchestration.md) for the full job map.
