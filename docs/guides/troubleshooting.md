# Troubleshooting

Use this page when a local run fails. Most fixes assume schedules are disabled
and only one process is writing to the DuckDB warehouse. The runbooks cover both
shipped Polymarket scopes (`wc2026` and `us_midterms_2026`).

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
- Re-run the failed Dagster job; token sync state is ledgered.
- Check `polymarket_wc2026_ops.pipeline_run_events` and
  `polymarket_wc2026_ops.sync_run_metrics` for WC2026 run payloads.
- Check `polymarket_us_midterms_2026_ops.pipeline_run_events` and
  `polymarket_us_midterms_2026_ops.sync_run_metrics` for US midterms run payloads.
- If the latest sync metrics include `pipeline_run_event_append_failed`, the
  ingestion run continued but the append-only telemetry event failed to land;
  inspect `pipeline_run_event_append_error` and rerun after fixing storage.

## Polygon Settlement RPC Failures

The manual Polygon flow requires both `POLYGON_RPC_URL` and
`POLYGON_RPC_PROVIDER_LABEL`. The primary endpoint must report chain ID 137 and
support the `finalized` block tag. Seed authoring additionally requires archive
history for event-block verification.

- A finality or chain preflight failure is terminal; use a compatible Polygon
  endpoint rather than changing the fixed chain/finality contract.
- Provider range-limit errors trigger adaptive chunk splitting. Other RPC
  errors are never interpreted as an empty log range.
- Runtime discovery requests only `OrdersMatched`, then expands seeded active
  tokens through bounded JSON-RPC receipt and header batches. Exact authored
  token/exchange/block bounds filter discoveries before receipts. Batch
  responses may arrive out of order but missing, duplicate, malformed, null,
  or errored items fail closed; only recognized provider size limits split.
- Re-run `polymarket_wc2026_polygon_settlement_backfill` after a transient
  failure. Successful leaf chunks for the same deterministic scan are reused;
  the prior published fill snapshot remains intact until a complete scan commits.
- Inspect `polymarket_wc2026_ops.polygon_settlement_scan_runs` and
  `polymarket_wc2026_ops.polygon_settlement_scan_chunks`. These tables contain
  only sanitized provider origins/labels and errors; do not paste full endpoint
  URLs into logs or issue reports.

`make polygon-settlement-live-smoke` preserves its disposable checkpoint unless
`POLYGON_SETTLEMENT_LIVE_SMOKE_RESET=true` is supplied. Its runtime files are
under `.cache/polygon_settlement/` on the same volume as the repository. If the
repository is not on the intended SSD, move the checkout before running; the
target intentionally does not hardcode a host-specific mount path.

During a run, inspect the atomic JSON in `.cache/polygon_settlement/status/` for
exchange/range progress, safe aggregate RPC/receipt/fill counts, and rates. The
file deliberately omits endpoint, transaction, wallet, token, and payload data.
A completed compatible v4 rerun is expected to return offline with no RPC
credentials. Use `POLYGON_SETTLEMENT_LIVE_SMOKE_RESET=true` only to request a
new scan intentionally.

If a provider times out on the default initial 8,000-block log request, resume
the same scan with a smaller starting leaf instead of resetting it:

```bash
POLYGON_SETTLEMENT_LIVE_SMOKE_INITIAL_BLOCK_CHUNK_SIZE=2000 \
  uv run make polygon-settlement-live-smoke
```

Successful leaves remain reusable. A transport timeout still fails closed; it
is not treated as an empty result or silently converted into a range split.

If optional secondary verification is absent, mismatched, or unavailable, the
release records a warning. It does not invalidate a primary finalized scan.

## Polygon dbt Graph Is Missing

`make dbt-build` intentionally excludes `tag:polygon_settlement`, so ordinary
credential-free builds cannot publish from empty Polygon raw tables. Use:

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

Symptom: Dagster jobs or dbt builds report success, but the repo-root
`oddsfox.duckdb` has no new schemas or row counts.

Cause: `.env` sets an absolute `DUCKDB_PATH` pointing at another checkout or
machine path. `DUCKDB_PATH` takes precedence over `DUCKDB_NAME`, so ingestion and
dbt write to that file instead of the warehouse in the current repo.

Fix:

1. Point `DUCKDB_PATH` at the warehouse you intend to query (for example the
   repo-root `oddsfox.duckdb` in this checkout), or
2. Unset `DUCKDB_PATH` and rely on `DUCKDB_NAME=oddsfox.duckdb` so the path
   resolves relative to the repo root.

## Midterms Metadata Backfill Uses Wrong Markets

Symptom: `polymarket/us_midterms_2026/raw/market_metadata_backfill` queries WC2026
markets or returns zero due markets.

Cause: an older build exited `active_polymarket_scope` before scoped queries ran.

Fix: pull the latest code, reset a polluted warehouse if needed (`rm oddsfox.duckdb*`),
and rerun the midterms registry refresh job.

## Empty Midterms Observability

Symptom: `polymarket_us_midterms_2026_observability.polymarket_us_midterms_2026_sync_run_observability`
has zero rows after a successful midterms run.

Cause: an older build wrote `pipeline_run_events` to the WC2026 ops schema.

Fix: pull the latest code and rerun `polymarket_us_midterms_2026_hourly_odds_ingest`
or the full midterms pipeline. Confirm rows land in
`polymarket_us_midterms_2026_ops.pipeline_run_events`.
