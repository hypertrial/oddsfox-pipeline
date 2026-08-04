# Troubleshooting

Use this page when a local run fails. Most fixes assume schedules are disabled
and only one process is writing to the DuckDB warehouse. The runbooks cover both
the shipped Polymarket WC2026 scope (`wc2026`).

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

If `polymarket/wc2026/raw/markets` (`polymarket_wc2026_raw_markets` op) fails
during extract with:

```text
ContainerInjectableContextMangled: When restoring context `DestinationCapabilitiesContext` ...
```

an older build nested a second dlt pipeline (registry staging) inside the markets pipeline extract. Pull the latest code: the asset now fetches and normalizes markets before calling `dlt.run()`, so only one dlt pipeline runs at a time.

Fix:

1. Pull the latest code.
2. Stop Dagster, then rerun `polymarket/wc2026/raw/markets`.

## dlt Market Schema Conflict

If dlt cannot load `polymarket_wc2026_raw.markets` because the local table schema does
not match the current source contract, drop the table and rerun
`polymarket/wc2026/raw/markets`:

```sql
DROP TABLE IF EXISTS polymarket_wc2026_raw.markets;
```

## Markets vs Snapshot Responsibilities

- `polymarket/wc2026/raw/markets` owns `polymarket_wc2026_raw.markets` rows (dlt merge on `id`).
- `polymarket/wc2026/raw/markets_snapshot` refreshes the WC2026 registry and writes `polymarket_wc2026_raw.market_tokens` only; it does not upsert markets rows.

If markets metadata looks stale after a snapshot run, materialize
`polymarket/wc2026/raw/markets` first.

## Stale Warehouse

For local development, the simplest reset is to stop Dagster and remove the DuckDB file:

```bash
rm -f oddsfox.duckdb oddsfox.duckdb.wal oddsfox.duckdb-shm
```

Then rerun the quickstart.

## API or Network Failures

- Lower `MARKETS_REQUESTS_PER_SECOND` or `ODDS_REQUESTS_PER_SECOND`.
- Re-run the failed job; token sync state is ledgered.
- Dagster `run_monitoring` and `run_retries` in `dagster_instance.yaml` mark
  orphaned runs failed and retry from the last successful step.
- Transient connection or 5xx failures on network-heavy assets may raise Dagster
  `RetryRequested` for a bounded automatic retry. Wrapped Gamma/CLOB read
  timeouts are classified as transient.
- A retried `polymarket_wc2026_raw_event_catalog` crawl resumes already-converged
  partitions from `polymarket_wc2026_ops.event_catalog_scan_checkpoint` instead
  of restarting every partition from page 0. Checkpoints clear after a successful
  warehouse merge. Set `reset_event_catalog_checkpoint=true` in run config to
  discard checkpoints.
- Routine full-pipeline / registry-refresh runs skip the platform-wide
  slug-prefix recall scan. For a rare completeness re-check, run
  `uv run make event-catalog-recall-audit`.
- Check `polymarket_wc2026_ops.ingestion_run_events` and
  `polymarket_wc2026_ops.sync_run_metrics` for WC2026 run payloads.
- Summarize the latest task outcomes locally:

```bash
uv run python scripts/run_health.py --limit 20
# Inspect another warehouse read-only without bootstrapping DUCKDB_PATH:
uv run python scripts/run_health.py --duckdb-path /path/to/other.duckdb --limit 20
```

- If the latest sync metrics include `ingestion_run_event_append_failed`, the
  ingestion run continued but the append-only telemetry event failed to land;
  inspect `ingestion_run_event_append_error` and rerun after fixing storage.

## Interrupted dbt incremental hourly odds

`int_polymarket_wc2026_token_hourly_odds` uses `delete+insert`. If a prior
Polymarket hourly-odds `oddsfox_dbt` run was killed mid-build, the next
non-`full_refresh` build that selects that model detects the interrupted flag in
`scrape_metadata`, runs a targeted `--full-refresh` for that model, then
continues the ordinary build. Kalshi-only and other isolated dbt selects do not
arm or recover this flag. If builds still look stale, rerun
`polymarket_wc2026_dbt_build` with `full_refresh: true`
in run config.

## Polygon Settlement RPC Failures

The manual Polygon settlement pipeline requires `POLYGON_RPC_URL` and
`POLYGON_RPC_PROVIDER_LABEL`. The primary endpoint must report chain ID 137 and
support the `finalized` block tag. Finality or chain preflight failures are
terminal; transient RPC or chunk failures are resumable.

Re-run `polymarket_wc2026_polygon_settlement_backfill` after a transient
failure. Inspect `polymarket_wc2026_ops.polygon_settlement_scan_runs` and
`polymarket_wc2026_ops.polygon_settlement_scan_chunks` for sanitized progress
and errors. Do not paste full endpoint URLs into logs or issue reports.

For RPC configuration, chunk tuning, live smoke, disposable checkpoints, and
seed authoring, see
[Recreate the Polygon settlement mart](recreate-polygon-settlement-mart.md).

## Polygon dbt Graph Is Missing

`make dbt-build` intentionally excludes `tag:polygon_settlement` and
`tag:pmxt_order_book`, so ordinary credential-free builds cannot publish from
empty historical raw tables. For Polygon settlement, use:

```bash
uv run make dbt-polygon-settlement-ci
```

for replay-only fixture validation, or run the unscheduled Polygon backfill
against a disposable/selected warehouse. The backfill's fail-closed gate
requires the current seed-matched published scan, complete chunk coverage,
nonempty fills, and exactly 39,120 mart rows.

## Polygon Audit Or Export Already Exists

`make polygon-settlement-release` refuses to overwrite an existing internal
audit version, and `make polygon-settlement-export` refuses to overwrite its
allowlisted technical export. Choose a new SemVer only for an intentional new
snapshot; do not delete or replace an immutable version merely to rerun either
command. There is no mutable `latest` alias or upload step.

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

## Tests Writing To Production Warehouse

Symptom: unexpected rows appear in `oddsfox.duckdb` after `make test`.

Cause: `.env` sets `DUCKDB_PATH` to the real warehouse and some tests only
override `DUCKDB_NAME`, which loses to `DUCKDB_PATH` precedence.

Fix:

1. Remove or comment out `DUCKDB_PATH` in `.env` for local test runs, or
2. Use the shared `duck` fixture / `isolate_duckdb_test_env()` pattern in new
   storage tests (see [Development](../development/index.md)).

## Warehouse Writes Land in a Different Checkout

Symptom: jobs or dbt builds report success, but the repo-root
`oddsfox.duckdb` has no new schemas or row counts.

Cause: `.env` sets an absolute `DUCKDB_PATH` pointing at another checkout or
machine path. `DUCKDB_PATH` takes precedence over `DUCKDB_NAME`, so ingestion and
dbt write to that file instead of the warehouse in the current repo.

Fix:

1. Point `DUCKDB_PATH` at the warehouse you intend to query (for example the
   repo-root `oddsfox.duckdb` in this checkout), or
2. Unset `DUCKDB_PATH` and rely on `DUCKDB_NAME=oddsfox.duckdb` so the path
   resolves relative to the repo root.
