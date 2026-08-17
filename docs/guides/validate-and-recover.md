# Validate and recover

Use deterministic checks, warehouse inspection, and targeted reruns to verify
pipeline health without turning routine gaps into full warehouse rebuilds.

## Run deterministic checks

Validate all six product pipelines offline (expect roughly 25–40 minutes locally;
not a `ci-fast` substitute):

```bash
uv run make pipelines-deterministic
```

| Pipeline | Covered by |
| --- | --- |
| Polymarket WC2026 | `integration-dagster`, `integration-dbt`, `dbt-build-ci` |
| Kalshi WC2026 | `integration-dagster`, `integration-dbt`, `dbt-build-ci` |
| Polygon settlement history | `integration-dagster` (mocked job), `dbt-polygon-settlement-ci` |
| Match-minute odds | `integration-dagster` (mocked job), `dbt-match-minute-ci` |
| Match order book | `integration-dagster` (mocked job), `integration-dbt` |
| Market portrait | `integration-dagster` (mocked job), `integration-dbt` |

For the full Makefile target inventory (`dagster-jobs-smoke`, `dbt-unit`,
`golden-dbt`, `data-quality`, isolated dbt CI targets, and more), see
[AGENTS.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/AGENTS.md#targeted-commands).

## Inspect a warehouse safely

Prefer the read-only profiler over opening the live warehouse read-write:

```bash
uv run python scripts/profile_warehouse.py
```

For Polymarket golden-mart analysis, inspect
`polymarket_wc2026_ingestion_run_observability` before trusting stale or
missing prices. Kalshi and match-minute paths also expose matching `*_data_quality`
relations.

For soccer, use `make soccer-production-health`. Warning-only state returns
zero; active critical state returns one; missing or unreadable monitoring state
returns two. Inspect `polymarket_soccer_pipeline_alerts` for the stable code and
remediation before rerunning work.

| Condition | Recovery |
| --- | --- |
| Preflight failure | Correct the reported schema, token collision, snapshot writeability, or critical disk issue; no external request was made. |
| Catalog instability | Preserve checkpoints, wait for Gamma to stabilize, then rerun the catalog job. |
| Partial CLOB publication | Keep successful tokens; rerun the minute job for due failed tokens. |
| Aged retries | Inspect token fetch status and retry; terminal unavailable tokens are reported separately. |
| Stale running run | Confirm the process is gone, stop any DuckDB holder, then launch a new run. |
| dbt failure | Repair the reported model/contract and rerun soccer dbt; raw publication remains intact. |
| Data or resource drift | Compare the two latest successful runs and inspect exclusions, request rate, query work, and host load. |
| Low disk | Free space under the runtime/warehouse volume; below the critical floor preflight blocks. |

## Recover a failed path

- Re-run `polymarket_wc2026_hourly_odds_ingest` for routine WC2026 odds gaps.
- Re-run `kalshi_wc2026_hourly_odds_ingest` for Kalshi candlestick gaps.
- Re-run `international_results_wc2026_match_results_ingest` after fixture or
  score updates when you are on Kalshi or match-minute paths (not required for
  the Polymarket golden-mart `full`/`dbt` jobs).
- Re-run `polymarket_wc2026_polygon_settlement_backfill` after a transient RPC
  or chunk failure. It resumes compatible successful gaps and preserves the
  previous canonical snapshot until atomic publication. Adjacent successful
  leaves are coalesced for gap planning, boundary headers are batch-revalidated,
  and only uncovered exchange-specific ranges are scheduled. A valid published
  v4 scan short-circuits locally without RPC credentials.
- Run the matching `*_dbt_build` after repairing raw or ops tables.
- For warehouse pruning and compaction, see
  [Troubleshooting](troubleshooting.md#large-warehouse-file).

Next, use [Troubleshooting](troubleshooting.md) for a specific symptom or the
[orchestration reference](../reference/orchestration.md) for exact job names.
