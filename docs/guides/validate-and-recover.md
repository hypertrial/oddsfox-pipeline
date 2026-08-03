# Validate and recover

Use deterministic checks, warehouse inspection, and targeted reruns to verify
pipeline health without turning routine gaps into full warehouse rebuilds.

## Run deterministic checks

Validate all six product pipelines offline (expect roughly 25–40 minutes locally;
not a `ci-fast` substitute):

```bash
uv run make pipelines-deterministic
```

This runs, in order: `integration-dagster` (all registered jobs, including
mocked externals plus Polymarket/Kalshi real DuckDB/dbt E2E),
`integration-dbt` (production marts plus isolated order-book and portrait dbt),
`dbt-polygon-settlement-ci`, `dbt-match-minute-ci`, and `dbt-build-ci`.

| Pipeline | Covered by |
| --- | --- |
| Polymarket WC2026 | `integration-dagster`, `integration-dbt`, `dbt-build-ci` |
| Kalshi WC2026 | `integration-dagster`, `integration-dbt`, `dbt-build-ci` |
| Polygon settlement history | `integration-dagster` (mocked job), `dbt-polygon-settlement-ci` |
| Match-minute odds | `integration-dagster` (mocked job), `dbt-match-minute-ci` |
| Match order book | `integration-dagster` (mocked job), `integration-dbt` |
| Market portrait | `integration-dagster` (mocked job), `integration-dbt` |

Verify every registered public job with mocked external APIs:

```bash
uv run make dagster-jobs-smoke
```

Validate high-risk SQL branches, exact mart outputs, source freshness, and data
quality:

```bash
uv run make dbt-unit
uv run make golden-dbt
uv run make dbt-source-freshness-ci
uv run make data-quality
uv run make dbt-polygon-settlement-ci
uv run make dbt-match-minute-ci
```

These checks use disposable DuckDB state. The Polygon target synthesizes a
complete published scan/fill/chunk fixture, builds only
`tag:polygon_settlement`, and asserts the exact 39,120-row dense mart without
network access. `contract-http` is replay-only and
manual or nightly; it is not part of the default deterministic gate.

## Inspect a warehouse safely

Prefer the read-only profiler over opening the live warehouse read-write:

```bash
uv run python scripts/profile_warehouse.py
```

For current analysis, inspect the matching `*_data_quality` and
`*_ingestion_run_observability` relations before trusting stale or missing prices.

## Recover a failed path

- Re-run `polymarket_wc2026_hourly_odds_ingest` for routine WC2026 odds gaps.
- Re-run `kalshi_wc2026_hourly_odds_ingest` for Kalshi candlestick gaps.
- Re-run `international_results_wc2026_match_results_ingest` after fixture or
  score updates.
- Re-run `polymarket_wc2026_polygon_settlement_backfill` after a transient RPC
  or chunk failure. It resumes compatible successful gaps and preserves the
  previous canonical snapshot until atomic publication. Adjacent successful
  leaves are coalesced for gap planning, boundary headers are batch-revalidated,
  and only uncovered exchange-specific ranges are scheduled. A valid published
  v4 scan short-circuits locally without RPC credentials.
- Run the matching `*_dbt_build` after repairing raw or ops tables.
- Use `make prune-odds-history` to prune WC2026 raw odds; preview the script with
  `--dry-run` before changing retention.
- Use `make compact-warehouse` after pruning or large refreshes to reclaim
  DuckDB file space.

Next, use [Troubleshooting](troubleshooting.md) for a specific symptom or the
[orchestration reference](../reference/orchestration.md) for exact job names.
