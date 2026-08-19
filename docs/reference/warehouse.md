# Warehouse

Physical schema inventory for contributors. Mart guarantees live in
[Data contracts](data-contracts.md); analyst columns, joins, and common mistakes
live in [Data dictionary](data-dictionary.md). Analysts should start with
[Query the warehouse](../guides/query-the-warehouse.md),
[Query recipes](../guides/query-recipes.md), and the data dictionary rather
than this page.

The local warehouse is DuckDB. By default it is `oddsfox.duckdb` in the repo
root. OddsFox Pipeline is designed for prediction-market data; the v0.2.x warehouse
schemas and relation names are source-specific because adapters ship in parallel.

## Raw layer

Raw schemas hold landed source payloads and append-only observation history
before staging and marts. Prefer qualified names (`polymarket_wc2026_raw`,
event-catalog snapshots, match-minute history). Pipeline raw schemas are limited
to Polymarket, PMXT, Kalshi, and Polygon data.

Schema: `polymarket_catalog_raw`

- `markets`: platform-wide Gamma market landing table written by
  `scripts/sync_polymarket_markets_catalog.py` (`GET /markets/keyset`,
  volume ≥ $100k, open + closed). Replaced each sync; not a Dagster scope asset.

Schema: `polymarket_wc2026_raw`

- `event_market_payload_snapshots`: append-only Gamma child-market metadata at
  `(market_id, observed_at)` grain. Market scope registry refresh uses the shared
  event-catalog ingestion path; the dlt-owned `markets` table remains exclusive
  to the existing odds ingestion path.
- `markets`: dlt-owned Gamma market landing table with frozen column/type contract.
- `market_tokens`: one row per market with CLOB token JSON; current batches are
  extracted from the same Gamma payload as `markets` and finalized into this
  canonical table with `INSERT OR REPLACE`. Enrichment may temporarily widen
  this set; hourly odds planning and dbt staging market tokens use the latest
  `event_market_payload_snapshots.clob_token_ids` catalog instead.
- `odds_history`: append-only point-in-time CLOB token prices. The composite
  primary key `(clobTokenId, timestamp)` makes replays idempotent; an observed
  source point is never rewritten. Pruning policy is documented under
  [Scripts](scripts.md#warehouse) (`prune_odds_history.py`).
- `match_minute_odds_history`: exact-window CLOB observations for the selected
  match markets, keyed by `(clobTokenId, timestamp)` with fixed fidelity `1`.
  Canonical storage is an immutable partitioned Parquet snapshot registered as
  a DuckDB view; a successful dedicated run advances the active snapshot
  atomically, so upstream-deleted observations disappear. Failed fetch or
  storage runs leave the prior snapshot unchanged. This relation is isolated
  from `odds_history` and its sync ledger.
- `futures_minute_odds_history`: tournament-span minute observations for
  non-match WC2026 futures markets, same fidelity contract as match-minute.
  Publish uses temporary Parquet shards plus an immutable snapshot (see raw
  storage notes below). Raw retains every CLOB token; the unified minute mart
  reads publish-time primary-token OHLC.
- `polygon_settlement_fills`: the current canonical, wallet- and
  order-payload-redacted Polygon V2 settlement snapshot. Grain is
  `(chain_id, exchange_address,
  transaction_hash, passive_log_index, normalized_leg_ordinal)`. Rows retain
  deterministic chain ordering, proposition/token orientation, exact source
  integer amounts, decimal price/volume, normalization kind, and audit hashes.
  They intentionally omit wallets, order hashes, signatures, raw topics/data,
  calldata, oracle prose, and RPC URLs. A successful scan atomically replaces
  the complete snapshot; a failed scan leaves the prior publication unchanged.
- `token_odds_daily`: daily token aggregates rebuilt by custom SQL finalizers from
  canonical `odds_history`.

Schema: `oddsfox_reference`

- Contains the exact closed-world table inventory from the active
  `oddsfox.reference.v1` bundle published by OddsFox Scraper.
- The source-neutral loader verifies the manifest, all payload checksums, schema
  fingerprints, primary keys, and immutable bundle identity before starting a
  transaction. Failed loads preserve the last known-good schema.
- Market models read these source-neutral tables directly. Pipeline does not
  transform, scrape, or own the underlying non-market data.

Schema: `kalshi_wc2026_raw`

- `events`: dlt-owned Kalshi event landing table.
- `markets`: dlt-owned Kalshi market landing table.
- `market_candlesticks_hourly`: hourly OHLC candlesticks for admitted registry
  markets; written by the Python candlestick sync.

## Ops Tables

Schema: `polymarket_wc2026_ops`

- `market_scope_registry`: market ids admitted to the WC2026 market scope; current batches
  land through dlt staging before the canonical upsert preserves existing non-null event fields.
- `token_sync_ledger`: per-token sync progress kept in custom SQL because cursor
  and scheduler-state merges are stateful.
- `token_sync_skips`: persisted skip reasons kept in custom SQL to preserve `created_at`.
- `ingestion_run_events`: append-only run metrics landed through dlt staging.
- `sync_run_metrics`: latest sync metrics and short history. If appending to
  `ingestion_run_events` fails, the latest payload includes
  `ingestion_run_event_append_failed` and `ingestion_run_event_append_error`.
- `scrape_metadata`: small key/value metadata used by backfill progress helpers.
- `market_metadata_unresolved`: retry ledger for unresolved metadata fields.
- `match_minute_odds_fetch_audit`: append-only one-row-per-`(fetch_run_id,
  clobTokenId)` evidence for every dedicated minute fetch. It retains request
  windows, status, row counts, deterministic history SHA-256, sanitized errors,
  and whether the complete run was atomically published. Rows are retained
  indefinitely; the unscheduled job adds 496 per run.
- `futures_minute_odds_fetch_audit`: same append-only shape for futures-minute
  fetches (`window_row_count` / `window_history_sha256`). Empty in-window
  tokens stay unpublished; only success rows flip `raw_published` with the
  immutable Parquet snapshot publish.
- `polygon_settlement_scan_runs`: one row per deterministic scan identity,
  including manifest/normalizer versions, finalized head, sanitized provider
  label/origin, exact target ranges, publication status, and advisory secondary
  verification state.
- `polygon_settlement_scan_chunks`: resumable leaf-range evidence keyed by scan,
  exchange, and inclusive block range. Successful chunks record boundary and
  scoped-event hashes plus duration, HTTP/log/receipt/header call counts,
  discovery filtering, receipt/log counts, retries, and adaptive splits. Counts
  are non-identifying and internally reconciled; RPC errors are failures, never
  empty results.
- `polygon_settlement_fill_stage`: unpublished normalized legs for an in-flight
  scan. It is cleared only through the transactional recovery/publication path
  and is not an analyst surface.

Schema: `kalshi_wc2026_ops`

- `market_scope_registry`: market tickers admitted to the Kalshi WC2026 scope.
- `candlestick_sync_ledger`: per-market candlestick sync progress and scheduling
  state.
- `ingestion_run_events`: append-only run metrics landed through custom SQL.
- `sync_run_metrics`: latest sync metrics and short history for Kalshi tasks.

## dbt Schemas

- `polymarket_wc2026_staging`
- `polymarket_wc2026_intermediate`
- `polymarket_wc2026_marts`
- `polymarket_wc2026_observability`
- `oddsfox_reference` (transactionally loaded, Scraper-owned tables)
- `kalshi_wc2026_staging`
- `kalshi_wc2026_intermediate`
- `kalshi_wc2026_marts`
- `kalshi_wc2026_observability`

## dbt Intermediate

Representative inventory only; see `dbt/models/**/intermediate/` for the full set.

Schema: `polymarket_wc2026_intermediate`

- `int_polymarket_wc2026_token_working_set`: materialized canonical one-row-per-token
  join of market tokens to market labels, state, and volume.
- `int_polymarket_wc2026_markets`: markets admitted by the fixed WC2026 scope
  registry with sticky event-volume eligibility; one row per admitted market with
  enclosing-event metadata from the event catalog.
- `int_polymarket_wc2026_event_latest`: latest snapshot per WC2026 event from
  raw event catalog history.
- `int_polymarket_wc2026_primary_market_token`: one primary CLOB token per
  admitted market (Yes when present, otherwise `outcome_index` 0).
- `int_polymarket_wc2026_token_hourly_odds`: incremental hourly OHLC price fact
  for raw CLOB tokens across full lifetime history.
- `int_polymarket_wc2026_match_working_set`: match-grain working set for
  minute odds and order-book pipelines.
- `int_polymarket_wc2026_match_token_minute_odds`: incremental minute-grain
  token odds fact for the match-minute mart.
- `int_polymarket_wc2026_match_minute_odds_candidate`: candidate rows before
  the match-minute publication gate.
- `int_polymarket_wc2026_match_minute_publication_gate`: publication gate for
  the match-minute mart.
- `int_polymarket_wc2026_match_order_book_levels`: PMXT order-book levels for
  the match order-book mart.
- `int_polymarket_wc2026_match_order_book_publication_gate`: publication gate
  for the match order-book mart.
- `int_polymarket_wc2026_match_trade_publication_gate`: publication gate for the
  market-portrait trades bundle.
- `int_polymarket_wc2026_polygon_settlement_*`: working set, token-minute
  aggregates, publication gate, and quality-summary models for the Polygon
  settlement pipeline.

Schema: `kalshi_wc2026_intermediate`

- `int_kalshi_wc2026_markets`: markets admitted by the fixed Kalshi WC2026 scope.
- `int_kalshi_wc2026_market_hourly_odds`: incremental hourly OHLC fact from raw
  candlesticks in the pipeline policy trailing window.
- `int_kalshi_wc2026_stage_classification` and
  `int_kalshi_wc2026_group_winner_classification`: shared classifiers for
  stage and group-winner marts.

## dbt Observability

Schema: `polymarket_wc2026_observability`

- `polymarket_wc2026_polygon_settlement_data_quality`: one-row publication gate
  for the complete seed, current matching published scan, gap-free finalized
  ranges, nonempty fills, valid normalization/price/volume/OHLC, exact 150/210
  axes, and 39,120-row inventory.
- `polymarket_wc2026_polygon_settlement_token_coverage`: one row for each of the
  496 oriented tokens with expected/observed minutes, fill/derived counts,
  volumes, timestamps, and coverage ratio.
- `polymarket_wc2026_polygon_settlement_quality_issues`: current hard errors and
  advisory warnings. No-fill/sparse sides, derived-fill prevalence, pair-price
  deviations, and secondary-provider status are warnings and never modify
  prices.
- `polymarket_wc2026_match_minute_odds_data_quality`: expected-versus-mapped
  games, results provenance, markets, tokens, timing, audit status, minute rows,
  boundary/interior completeness, pair deviations, cadence, warning/error
  counts, elapsed-axis integrity, and publication-blocking issue keys.
- `polymarket_wc2026_match_minute_token_coverage`: one row per mapped token with
  expected/observed buckets, raw and fetch counts, first/last offsets, maximum
  gap, distinct prices, ratio, and latest fetch provenance.
- `polymarket_wc2026_match_minute_odds_quality_issues`: stable current warning or
  error keys with entity IDs, measured values, thresholds, and explanations.
- `polymarket_wc2026_ingestion_run_observability`: run-level ingestion, market-discovery provenance, and odds-sync telemetry.

Schema: `kalshi_wc2026_observability`

- `kalshi_wc2026_ingestion_run_observability`: run-level Kalshi ingestion telemetry.
- `kalshi_wc2026_stage_coverage`: classified market coverage and hourly
  completeness metrics.
- `kalshi_wc2026_data_quality`: DQ findings for sparse coverage and stale or
  missing live odds.

## dlt Landing And Canonical Tables

Canonical raw and ops table names and schemas remain stable. dlt owns batch
landing for `markets`, `market_tokens`, `odds_history`,
`market_scope_registry`, and `ingestion_run_events`; stage tables and `_dlt*`
metadata tables are internal implementation details.

Non-market relations are not dlt or custom Pipeline raw storage. They are loaded
as one validated Scraper reference bundle into `oddsfox_reference`; see
[Data contracts](data-contracts.md).

`kalshi_wc2026_raw.events` and `kalshi_wc2026_raw.markets` are created by
`kalshi_wc2026_raw_markets`. `kalshi_wc2026_raw.market_candlesticks_hourly` is
custom SQL storage updated by the hourly candlestick sync asset.

`polymarket_wc2026_raw.match_minute_odds_history` and
`polymarket_wc2026_raw.futures_minute_odds_history` are DuckDB views over an
immutable partitioned Parquet snapshot under
`${ODDSFOX_RUNTIME_ROOT:-.cache/runtime}/minute-odds-snapshots/<scope>/<match|futures>/`.
After CLOB fetch, the sync spills fresh histories to temporary Parquet shards
under `minute-odds-publish/<fetch_run_id>/` (warehouse lock released), then
promotes them into a checksummed snapshot (`raw/` + `primary_ohlc/` buckets,
`manifest.json`, atomic `CURRENT` pointer). Tokens whose prior published window
bounds still match are reused without a CLOB refetch; only dirty token buckets
are rewritten. DuckDB then re-registers the stable raw + primary-OHLC relation
names and flips matching audit rows `raw_published` in one short transaction.
Soccer writes audit rows only for due token attempts; unchanged tokens reconcile
against their latest published exact-window audit instead of being copied into
every catch-up run. The fetch pool caps submitted futures, writes completed
histories into bounded Arrow/Parquet batches, and releases each token payload
before the remaining plan completes.
Views prefer the `CURRENT` symlink path so a later pointer advance is visible at
query time without re-registering. Partial publishes merge into the prior
snapshot (changed tokens only); they do not drop out-of-plan prior tokens.
Publish uses `temp_directory` under the runtime root and a default
`memory_limit` of `12GB` (`ODDSFOX_MINUTE_PUBLISH_MEMORY_LIMIT`). Do not overlap
two publishers of the same minute raw relation while the lock is released for
spill; cross-relation concurrency (match vs futures) is the intended unlock win.
Snapshot files are written and `CURRENT` advances before the DuckDB register
transaction. If that transaction fails, DuckDB audit flags roll back and
`CURRENT` is restored to the predecessor snapshot (the failed snapshot directory
is left for forensics and cleaned by later `retain_snapshots`). Staging is
deleted when promote fails before `CURRENT` advances. Existing dbt source names
are unchanged. Measure publish-only speed with
`make futures-minute-publish-benchmark` (disposable DuckDB only; never opens the
operator warehouse). Measure the dbt rebuild with
`make minute-odds-dbt-benchmark` (same disposable policy; default
`performance` tier ~10M primary rows).

Soccer catalog observations remain append-only in the snapshot relations. The
same converged merge atomically upserts one-row-per-key current projections in
`polymarket_soccer_raw.events` and `polymarket_soccer_raw.markets`; registry and
dbt staging read those projections without ranking all prior observations. The
public observed and dense soccer mart names are views over private
`delete+insert` incremental relations keyed by `(market_id,
odds_minute_epoch)`. A deterministic registry/audit revision replaces the full
inclusive window only for dirty markets and removes markets that are no longer
eligible. Measure cold and warm behavior with the disposable
`make soccer-minute-performance-benchmark` target.

For a cheaper live end-to-end check of the unified minute path without refetching
every market, use `make minute-odds-live-smoke`. It always asserts a disposable
`.cache/minute_odds_live_smoke.duckdb` **and** a disposable
`.cache/runtime/smoke/minute-odds-live` `ODDSFOX_RUNTIME_ROOT` so sampled
publishes cannot GC or shrink operator production snapshots. It still proves the
full 104/248/496 match inventory before sampling, then samples about 5% of match
markets and 5% of futures markets independently (all tokens retained per selected
market), caps sampled futures windows to their final 24 hours, builds the unified
minute mart plus DQ, and validates via
`scripts/validate_polymarket_wc2026_minute_odds_live_smoke.py` into an ignored
JSON report under `.cache/runtime/smoke/minute-odds/`. Cold runs reset the
disposable warehouse and smoke runtime root and refresh catalog by default; warm
reruns use
`MINUTE_ODDS_LIVE_SMOKE_RESET=false MINUTE_ODDS_LIVE_SMOKE_REFRESH_CATALOG=false`
while still forcing match and futures refresh. It does not prove the full
104/248/496 match publication gate; use `make match-minute-live-smoke` (disposable
DuckDB **and** `.cache/runtime/smoke/match-minute-live` runtime root) or the
production minute backfill for that contract.

The Polygon settlement tables are custom transactional SQL, not dlt. Completed
chunks and their scoped hashes are durable resume points. Publication first
proves the finalized target ranges have no gaps or overlaps, then replaces
`polygon_settlement_fills` and marks its scan published in one transaction.
The committed market seed is the only runtime fixture/semantic dependency; this
path does not read the Gamma/CLOB raw tables, international-results tables, or
the runtime OpenFootball table.

The v4 live-smoke warehouse is
`.cache/polygon_settlement/benchmarks/v4/live_smoke.duckdb`. Atomic redacted
progress snapshots are stored under `.cache/polygon_settlement/status/`; they
contain aggregate rates and counts only, never endpoints, transactions,
wallets, token IDs, or payloads. A compatible published scan validates its
local provenance, exchange-specific coverage, and canonical fill count before
returning offline.

`polymarket_wc2026_raw.markets` is created by `polymarket_wc2026_raw_markets`.
That asset performs the single Gamma market discovery pass and persists token
mappings from the same payload after dlt market landing succeeds. The
`polymarket_wc2026_raw_markets_snapshot` asset is local lineage/accounting only.
The `dbt-build-ci` target creates an empty source fixture only in its disposable
DuckDB database.

Manual reset:

```sql
DROP TABLE IF EXISTS polymarket_wc2026_raw.markets;
```

Then materialize `polymarket_wc2026_raw_markets`.
