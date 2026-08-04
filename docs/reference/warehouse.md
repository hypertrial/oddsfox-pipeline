# Warehouse

Physical schema inventory for contributors. Mart guarantees live in
[Data contracts](data-contracts.md); analyst columns, joins, and common mistakes
live in [Data dictionary](data-dictionary.md). Analysts should start with
[Query the warehouse](../guides/query-the-warehouse.md),
[Query recipes](../guides/query-recipes.md), and the data dictionary rather
than this page.

The local warehouse is DuckDB. By default it is `oddsfox.duckdb` in the repo
root. OddsFox Pipeline is designed for prediction-market data; the v0.1.x warehouse
schemas and relation names are source-specific because adapters ship in parallel.

## Raw layer

Raw schemas hold landed source payloads and append-only observation history
before staging and marts. Prefer qualified names (`polymarket_wc2026_raw`,
event-catalog snapshots, match-minute history). Private collector layout
`oddsfox.raw.v1` is documented in [Strategy contracts](strategy-contracts.md).

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
  A successful dedicated run replaces this complete snapshot atomically, so
  upstream-deleted observations disappear. Failed fetch or storage runs leave
  the prior snapshot unchanged. This table is isolated from `odds_history` and
  its sync ledger.
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

Schema: `international_results_wc2026_raw`

- `match_results`: WC2026-only FIFA World Cup fixture/result rows from
  `martj42/international_results`. Ingestion resolves the latest commit affecting
  `results.csv`, downloads that immutable revision, and stores its revision,
  exact-byte SHA-256, immutable URL, and load time on every full-replacement row.

Schema: `openfootball_wc2026_raw`

- `schedule_fixtures`: full-replacement 104-match FIFA-numbered schedule mirror,
  including schedule fixtures for group and knockout rounds and match 103.
  Stores published match number, stage, group label, kickoff, official
  home/away assignment, venue, pinned OpenFootball source URL, the final
  one-based line number of the exact source slice in the legacy
  `source_line_number` field, that slice's SHA-256 in `source_line_hash`, and
  load timestamp.

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
- `international_results_wc2026_staging`
- `international_results_wc2026_intermediate`
- `international_results_wc2026_marts`
- `international_results_wc2026_observability`
- `openfootball_wc2026_staging`
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
- `int_polymarket_wc2026_primary_market_token`: one Yes-outcome CLOB token per
  admitted market.
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

Schema: `international_results_wc2026_intermediate`

- `int_international_results_wc2026_match_teams`: exploded home/away team rows
  from the WC2026 fixture/result source.

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

Schema: `international_results_wc2026_observability`

- `international_results_wc2026_data_quality`: warning findings for unresolved
  tied-knockout advancers or stale source loads, plus an error when a populated
  snapshot does not share one valid immutable revision and payload SHA-256.

## dlt Landing And Canonical Tables

Canonical raw and ops table names and schemas remain stable. dlt owns batch
landing for `markets`, `market_tokens`, `odds_history`,
`market_scope_registry`, and `ingestion_run_events`; stage tables and `_dlt*`
metadata tables are internal implementation details.

`international_results_wc2026_raw.match_results` is custom SQL storage, not dlt,
because the source is a single CSV and a full WC2026 replacement is simpler than
batch finalization.

`openfootball_wc2026_raw.schedule_fixtures` is also custom SQL storage. Its
parser reads the exact pinned OpenFootball `2026--usa/cup.txt` and
`2026--usa/cup_finals.txt` bytes, validates their file hashes and the complete
1–104 match/stage/group/time/team/venue contract, and records each exact source
slice hash before an atomic full replacement. The group-stage source is grouped
by group rather than official match number, so a reviewed 72-entry slice-hash
map binds those fixtures to FIFA IDs. The manifest separately pins the official
FIFA schedule document used for that identity review. Knockout-only consumers
explicitly filter match IDs 73–104.

`kalshi_wc2026_raw.events` and `kalshi_wc2026_raw.markets` are created by
`kalshi_wc2026_raw_markets`. `kalshi_wc2026_raw.market_candlesticks_hourly` is
custom SQL storage updated by the hourly candlestick sync asset.

`polymarket_wc2026_raw.match_minute_odds_history` is custom dlt-staged storage
with primary key `(clobTokenId, timestamp)`. Every row records its selected
market, fixed fidelity `1`, exact Gamma timing window, and ingestion timestamp.
The stage is loaded before a transaction replaces the canonical table and marks
all matching fetch-audit rows published; either both changes commit or neither
does.

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
