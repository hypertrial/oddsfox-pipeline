# Troubleshooting

Use this page when a local run fails. Most fixes assume schedules are disabled
and only one process is writing to the DuckDB warehouse. The current runbooks
target the WC2026 Polymarket implementation.

## DuckDB Lock Errors

Only one read-write connection can hold the DuckDB file.

Fix:

1. Stop Dagster and any Python shells using the warehouse.
2. Retry the job.
3. Use `scripts/profile_warehouse.py --snapshot-copy` for read-only inspection while another process is active.

## dbt Cannot Find Profile

Use the packaged profiles directory:

```bash
uv run make dbt-parse
```

If running dbt directly:

```bash
uv run python -m dbt.cli.main parse --project-dir dbt --profiles-dir dbt/profiles
```

## dlt ContainerInjectableContextMangled

If `wc2026_polymarket_raw_markets` fails during extract with:

```text
ContainerInjectableContextMangled: When restoring context `DestinationCapabilitiesContext` ...
```

an older build nested a second dlt pipeline (registry staging) inside the markets pipeline extract. Pull the latest code: the asset now fetches and normalizes markets before calling `dlt.run()`, so only one dlt pipeline runs at a time.

Fix:

1. Pull the latest code.
2. Stop Dagster, then rerun `wc2026_polymarket_raw_markets`.

## dlt Market Schema Conflict

If dlt cannot load `wc2026_polymarket_raw.markets` because the local table schema does
not match the current source contract, drop the table and rerun
`wc2026_polymarket_raw_markets`:

```sql
DROP TABLE IF EXISTS wc2026_polymarket_raw.markets;
```

## Markets vs Snapshot Responsibilities

- `wc2026_polymarket_raw_markets` owns `wc2026_polymarket_raw.markets` rows (dlt merge on `id`).
- `wc2026_polymarket_markets_snapshot` refreshes the WC2026 registry and writes `wc2026_polymarket_raw.market_tokens` only; it does not upsert markets rows.

If markets metadata looks stale after a snapshot run, materialize `wc2026_polymarket_raw_markets` first.

## Stale Warehouse

For local development, the simplest reset is to stop Dagster and remove the DuckDB file:

```bash
rm -f oddsfox.duckdb oddsfox.duckdb.wal oddsfox.duckdb-shm
```

Then rerun the quickstart.

## API or Network Failures

- Lower `MARKETS_REQUESTS_PER_SECOND` or `ODDS_REQUESTS_PER_SECOND`.
- Re-run the failed Dagster job; token sync state is ledgered.
- Check `wc2026_polymarket_ops.pipeline_run_events` and `wc2026_polymarket_ops.sync_run_metrics` for the latest run payloads.
- If the latest sync metrics include `pipeline_run_event_append_failed`, the
  ingestion run continued but the append-only telemetry event failed to land;
  inspect `pipeline_run_event_append_error` and rerun after fixing storage.

## Large Warehouse File

DuckDB files do not always shrink after rebuilds or deletes. Stop writers, then:

1. Prune old raw odds points (default: keep the trailing 365 days):

```bash
uv run make prune-odds-history
# or preview first:
uv run python scripts/prune_odds_history.py --dry-run
```

2. Reclaim dead space left in the file:

```bash
uv run make compact-warehouse
```
