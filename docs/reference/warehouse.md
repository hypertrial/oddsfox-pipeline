# Warehouse

The local warehouse is DuckDB. By default it is `oddsfox.duckdb` in the repo
root. OddsFox Pipeline is designed for prediction-market data; the v0.1.x warehouse
schemas and relation names are source-specific because adapters ship in parallel.
For public mart guarantees, see
[Data contracts](data-contracts.md).

If you are analyzing data rather than operating the pipeline, start with the
[Query the warehouse](../guides/query-the-warehouse.md),
[Query recipes](../guides/query-recipes.md), and
[Data dictionary](data-dictionary.md).

## Raw Tables

Schema: `polymarket_wc2026_raw`

- `markets`: dlt-owned Gamma market landing table with frozen column/type contract.
- `market_tokens`: one row per market with CLOB token JSON; current batches are
  extracted from the same Gamma payload as `markets` and finalized into this
  canonical table with `INSERT OR REPLACE`.
- `odds_history`: point-in-time CLOB token prices. Indexed by the composite
  primary key `(clobTokenId, timestamp)` for idempotent upserts. Operators may
  prune rows older than 365 days with `make prune-odds-history` (manual; not automatic).
  Current batches land through dlt staging, then finalize with duplicate `(clobTokenId, timestamp)` last-write-wins semantics.
- `match_minute_odds_history`: exact-window CLOB observations for the selected
  match markets, keyed by `(clobTokenId, timestamp)` with fixed fidelity `1`.
  A successful dedicated run replaces this complete snapshot atomically, so
  upstream-deleted observations disappear. Failed fetch or storage runs leave
  the prior snapshot unchanged. This table is isolated from `odds_history` and
  its sync ledger.
- `token_odds_daily`: daily token aggregates rebuilt by custom SQL finalizers from
  canonical `odds_history`.

Schema: `international_results_wc2026_raw`

- `match_results`: WC2026-only FIFA World Cup fixture/result rows from
  `martj42/international_results`. Ingestion resolves the latest commit affecting
  `results.csv`, downloads that immutable revision, and stores its revision,
  exact-byte SHA-256, immutable URL, and load time on every full-replacement row.

Schema: `openfootball_wc2026_raw`

- `knockout_fixtures`: full-replacement FIFA-numbered knockout schedule mirror,
  including match 103. Stores published match number, stage, kickoff, official
  home/away assignment, venue, source line/hash, and load timestamp.

Schema: `polymarket_us_midterms_2026_raw`

- `markets`: dlt-owned Gamma market landing for targeted US midterms event slugs.
- `market_tokens`: one row per market with CLOB token JSON; finalized from the
  same Gamma payload as `markets`.
- `odds_history`: point-in-time CLOB token prices with the same idempotent
  `(clobTokenId, timestamp)` primary key as WC2026.
- `token_odds_daily`: daily token aggregates rebuilt from canonical `odds_history`.

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
- `pipeline_run_events`: append-only run metrics landed through dlt staging.
- `sync_run_metrics`: latest sync metrics and short history. If appending to
  `pipeline_run_events` fails, the latest payload includes
  `pipeline_run_event_append_failed` and `pipeline_run_event_append_error`.
- `scrape_metadata`: small key/value metadata used by backfill progress helpers.
- `market_metadata_unresolved`: retry ledger for unresolved metadata fields.
- `match_minute_odds_fetch_audit`: append-only one-row-per-`(fetch_run_id,
  clobTokenId)` evidence for every dedicated minute fetch. It retains request
  windows, status, row counts, deterministic history SHA-256, sanitized errors,
  and whether the complete run was atomically published. Rows are retained
  indefinitely; the unscheduled job adds 496 per run.

Schema: `kalshi_wc2026_ops`

- `market_scope_registry`: market tickers admitted to the Kalshi WC2026 scope.
- `candlestick_sync_ledger`: per-market candlestick sync progress and scheduling
  state.
- `pipeline_run_events`: append-only run metrics landed through custom SQL.
- `sync_run_metrics`: latest sync metrics and short history for Kalshi tasks.

Schema: `polymarket_us_midterms_2026_ops`

- `market_scope_registry`: market ids admitted to the US midterms 2026 scope.
- `token_sync_ledger`: per-token sync progress for the midterms namespace.
- `token_sync_skips`: persisted skip reasons for midterms tokens.
- `pipeline_run_events`: append-only run metrics for midterms ingestion jobs.
- `sync_run_metrics`: latest sync metrics and short history for midterms tasks.
- `scrape_metadata`: shared key/value metadata (global across scopes).
- `market_metadata_unresolved`: retry ledger for unresolved midterms metadata.

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
- `wc2026_intermediate`
- `wc2026_marts`
- `wc2026_observability`
- `polymarket_us_midterms_2026_staging`
- `polymarket_us_midterms_2026_intermediate`
- `polymarket_us_midterms_2026_marts`
- `polymarket_us_midterms_2026_observability`
- `kalshi_wc2026_staging`
- `kalshi_wc2026_intermediate`
- `kalshi_wc2026_marts`
- `kalshi_wc2026_observability`

## dbt Intermediate

Schema: `polymarket_wc2026_intermediate`

- `int_polymarket_wc2026_token_universe`: materialized canonical one-row-per-token
  join of market tokens to market labels, state, and volume.
- `int_polymarket_wc2026_markets`: markets admitted by the fixed WC2026 scope;
  one row per `(scope_name, market_id)` with the knockout volume floor from the
  WC2026 contract seed.
- `int_polymarket_wc2026_market_tokens`: WC2026 subset of the token universe.
- `int_polymarket_wc2026_token_hourly_odds`: incremental hourly OHLC price fact
  for raw CLOB tokens in the WC2026 contract trailing window.
- `int_polymarket_wc2026_knockout_market_classification`: shared real-team
  knockout market classifier used by public knockout marts and observability.
- `int_polymarket_wc2026_match_advance_tokens`: exact team-to-advance outcomes
  mapped to official FIFA fixture sides without the progression volume floor.
- `int_polymarket_wc2026_match_hourly_odds`: permanent incremental match-token
  hourly fact with a short late-arrival lookback and no age deletion.

Schema: `polymarket_us_midterms_2026_intermediate`

- `int_polymarket_us_midterms_2026_token_universe`: one-row-per-token join of
  market tokens to market labels, state, and volume.
- `int_polymarket_us_midterms_2026_markets`: markets admitted by the fixed US
  midterms scope at or above the contract volume floor.
- `int_polymarket_us_midterms_2026_market_tokens`: midterms subset of the token
  universe.
- `int_polymarket_us_midterms_2026_token_hourly_odds`: incremental hourly OHLC
  price fact for raw CLOB tokens in the contract trailing window.

Schema: `kalshi_wc2026_intermediate`

- `int_kalshi_wc2026_markets`: markets admitted by the fixed Kalshi WC2026 scope.
- `int_kalshi_wc2026_market_hourly_odds`: incremental hourly OHLC fact from raw
  candlesticks in the contract trailing window.
- `int_kalshi_wc2026_stage_classification` and
  `int_kalshi_wc2026_group_winner_classification`: shared classifiers for public
  stage and group-winner marts.
- `int_kalshi_wc2026_match_advance_markets`: exact `KXWCADVANCE` sides mapped to
  official FIFA fixtures.
- `int_kalshi_wc2026_match_hourly_odds`: permanent incremental match-market
  hourly fact with no age deletion.

Schema: `wc2026_intermediate`

- `int_wc2026_knockout_fixtures`: the 31 advancement fixtures keyed by official
  FIFA match number; excludes the third-place match.

## dbt Marts

Schema: `wc2026_marts`

- `wc2026_knockout_match_hourly_odds`: dense raw hourly team-advance closes for
  official home/away teams across Polymarket and Kalshi. Nulls preserve exact
  missing observations; the mart never fills or normalizes prices.

Schema: `polymarket_wc2026_marts`

- `polymarket_wc2026_match_minute_odds`: dense, inclusive in-game UTC minute
  rows at `(odds_minute_utc, market_id)` for all 104 matches. It contains 216
  group moneyline markets and 32 knockout advance/win markets. Yes/No OHLC,
  average, counts, and observation times remain null when a minute has no
  source point; no value is filled or derived as `1 - price`. Team identity and
  home/away orientation are reconciled to one pinned international-results
  revision before publication. Timing deltas, boundary flags/status, raw close
  sums/deviations, anomaly flags, source revision/hash/load time, and the matched
  international-results ID are included without changing the grain.
- `polymarket_wc2026_knockout_market_tokens`: progression-side token universe for real WC2026 team knockout
  markets at or above the WC2026 contract volume floor, plus derived `market_status`, source live flag,
  active-team live flag, and explicit price semantics.
- `polymarket_wc2026_knockout_token_hourly_odds`: trailing 30-day hourly OHLC odds for real-team progression-side
  knockout tokens (dbt view over the incremental hourly fact), including current market status, tournament status,
  active-team live flag, and `price_represents = 'progression'`.
- `polymarket_wc2026_graph_token_hourly_odds`: trailing 30-day hourly OHLC odds
  for both tokens of each real-team knockout market, with dbt-clean stage, team,
  progression-token, and opposite-token semantics for graph builds.
- `polymarket_wc2026_knockout_markets`: latest real-team progression-side knockout snapshot with explicit
  current-price status and progression outcome labels. Use `is_actionable_live_market` for current live consumption;
  use `is_active_team_live_market` when stale/missing live rows should remain visible. Closed/resolved rows are
  retained as historical rows.

Schema: `international_results_wc2026_marts`

- `international_results_wc2026_matches`: clean FIFA World Cup 2026 fixtures/results with stage mapping and tied-knockout advancer inference from later fixtures when possible.
- `international_results_wc2026_team_status`: canonical team roster and current tournament status used to filter Polymarket public marts.

Schema: `polymarket_us_midterms_2026_marts`

- `polymarket_us_midterms_2026_market_token_hourly_odds`: trailing 30-day hourly
  OHLC odds for admitted US midterms market tokens joined to source metadata. This
  is the only public midterms mart in v0.1.x.

Schema: `kalshi_wc2026_marts`

- `kalshi_wc2026_stage_markets`: latest stage-of-elimination market snapshots.
- `kalshi_wc2026_stage_market_hourly_odds`: trailing contract-window hourly odds
  for stage markets.
- `kalshi_wc2026_group_winner_markets`: latest group-winner market snapshots.
- `kalshi_wc2026_group_winner_market_hourly_odds`: trailing contract-window
  hourly odds for group-winner markets.

Schema: `polymarket_wc2026_observability`

- `polymarket_wc2026_match_minute_odds_data_quality`: expected-versus-mapped
  games, results provenance, markets, tokens, timing, audit status, minute rows,
  boundary/interior completeness, pair deviations, cadence, warning/error
  counts, and publication-blocking issue keys.
- `polymarket_wc2026_match_minute_token_coverage`: one row per mapped token with
  expected/observed buckets, raw and fetch counts, first/last offsets, maximum
  gap, distinct prices, ratio, and latest fetch provenance.
- `polymarket_wc2026_match_minute_odds_quality_issues`: stable current warning or
  error keys with entity IDs, measured values, thresholds, and explanations.
- `polymarket_wc2026_sync_run_observability`: run-level ingestion, market-discovery provenance, and odds-sync telemetry.
- `polymarket_wc2026_knockout_stage_coverage`: raw classified market coverage vs public scoped tokens by stage,
  direction, and market status, including hourly completeness metrics.
- `polymarket_wc2026_knockout_data_quality`: DQ findings for aggregated source-state anomalies, sparse stage/team
  coverage, actionable stale or missing odds, upstream eliminated-team live lag, and live-team alignment.

Schema: `polymarket_us_midterms_2026_observability`

- `polymarket_us_midterms_2026_sync_run_observability`: run-level ingestion and
  odds-sync telemetry for US midterms jobs.

Schema: `kalshi_wc2026_observability`

- `kalshi_wc2026_sync_run_observability`: run-level Kalshi ingestion telemetry.
- `kalshi_wc2026_stage_coverage`: classified market coverage and hourly
  completeness metrics.
- `kalshi_wc2026_data_quality`: DQ findings for sparse coverage and stale or
  missing live odds.

Schema: `international_results_wc2026_observability`

- `international_results_wc2026_data_quality`: warning findings for unresolved
  tied-knockout advancers or stale source loads, plus an error when a populated
  snapshot does not share one valid immutable revision and payload SHA-256.

Schema: `wc2026_observability`

- `wc2026_knockout_match_odds_coverage`: fixture, provider mapping, side,
  observed-hour, and freshness coverage per advancement match.
- `wc2026_knockout_match_odds_data_quality`: mapping/fixture/price errors and
  missing/stale provider warnings.

## dlt Landing And Canonical Tables

Canonical raw and ops table names and schemas remain stable. dlt owns batch
landing for `markets`, `market_tokens`, `odds_history`,
`market_scope_registry`, and `pipeline_run_events`; stage tables and `_dlt*`
metadata tables are internal implementation details.

`international_results_wc2026_raw.match_results` is custom SQL storage, not dlt,
because the source is a single CSV and a full WC2026 replacement is simpler than
batch finalization.

`openfootball_wc2026_raw.knockout_fixtures` is also custom SQL storage. Its
parser validates the complete 73–104 match-number/stage contract before an
atomic full replacement.

`kalshi_wc2026_raw.events` and `kalshi_wc2026_raw.markets` are created by
`kalshi_wc2026_raw_markets`. `kalshi_wc2026_raw.market_candlesticks_hourly` is
custom SQL storage updated by the hourly candlestick sync asset.

`polymarket_wc2026_raw.match_minute_odds_history` is custom dlt-staged storage
with primary key `(clobTokenId, timestamp)`. Every row records its selected
market, fixed fidelity `1`, exact Gamma timing window, and ingestion timestamp.
The stage is loaded before a transaction replaces the canonical table and marks
all matching fetch-audit rows published; either both changes commit or neither
does.

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

This release changes strict raw schemas for results provenance and the minute
fetch audit. Reset an existing local warehouse (`rm oddsfox.duckdb*`) before
rerunning the pipeline; no compatibility or migration path is provided.
