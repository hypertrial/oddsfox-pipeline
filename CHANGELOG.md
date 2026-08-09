# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Unified minute-odds landing is parquet-first: immutable partitioned snapshots
  under `${ODDSFOX_RUNTIME_ROOT}/minute-odds-snapshots/<match|futures>/` retain
  every CLOB token in `raw/` and publish-time primary-token minute OHLC in
  `primary_ohlc/`. DuckDB registers stable views over the active snapshot;
  `int_polymarket_wc2026_futures_token_minute_odds` and
  `int_polymarket_wc2026_token_minute_odds` are pass-through views (no global
  377M-row aggregate/table rewrite). Reruns skip CLOB for tokens whose prior
  published window still matches and rebuild only dirty token buckets; snapshot
  retention keeps the active + predecessor snapshot after a successful DuckDB
  register. Failed register rolls `CURRENT` back to the predecessor. Live smoke
  uses a disposable `ODDSFOX_RUNTIME_ROOT` under
  `.cache/runtime/smoke/minute-odds-live` so sampled publishes cannot GC
  operator snapshots. dbt liveness treats growth
  in `${ODDSFOX_RUNTIME_ROOT}/duckdb-temp` (and warehouse `.tmp`/WAL) as progress
  so a long active DuckDB query is not killed as idle. Measure with
  `uv run make minute-odds-dbt-benchmark`
  (`MINUTE_ODDS_DBT_BENCHMARK_TIER=performance` for ~10M rows;
  `production-shaped` for the opt-in ~377M acceptance tier).

- `init_duck_db()` wraps its multi-schema DDL bootstrap in a single DuckDB
  transaction instead of per-statement autocommit/fsync. Every isolated-DuckDB
  test fixture and production `ensure_duck_db()` call pays this path once per
  fresh warehouse. Measured ~4.6x faster under 10-way concurrent bootstrap
  (matching `pytest -n auto`); suite cumulative pytest `setup` phase dropped
  from ~630s to ~414s on a local `make test --durations=0` comparison. Failure
  still relies on connection close discarding an uncommitted transaction.

### Fixed

- `oddsfox_dbt` now terminates the dbt subprocess on Dagster cancellation /
  generator close, not only on the no-progress hard timeout, so a canceled
  run cannot leave an orphaned dbt child holding the DuckDB warehouse lock.
- The `oddsfox_dbt` build's no-progress hard timeout now escalates to `SIGKILL`
  if the dbt subprocess outlives a bounded grace period after `SIGTERM`. A
  single large dbt node (for example the unsampled full-backfill
  `int_polymarket_wc2026_futures_token_minute_odds` table rebuild) can run for
  hours with zero streamed events; the prior single-`SIGTERM` termination
  could leave the dbt process running as an orphan that held an exclusive lock
  on the DuckDB warehouse file, wedging every subsequent run against it even
  after Dagster reported the run as failed/canceled.
- Match-minute and unified minute-odds catalog refresh skip the exhaustive Gamma
  slug-prefix recall partition (now default-off on
  `MarketScopeRegistryConfig` / `collect_wc2026_event_catalog`). Only
  `polymarket_wc2026_event_catalog_recall_audit` enables unlimited recall.
  Tag/series discovery and 104/248/496 inventory fail-closed checks remain.
- `polymarket_wc2026_minute_odds_live_smoke` catalog refresh skips the exhaustive
  Gamma slug-prefix recall partition (use
  `polymarket_wc2026_event_catalog_recall_audit` for that completeness scan).
  Smoke still runs routine tag/series discovery before sampling.
- Minute-odds match/futures syncs borrow DuckDB only for plan selection, audit,
  and publish, releasing the warehouse lock during long CLOB fetches and during
  temporary Parquet shard construction so other Dagster steps (for example the
  sibling minute relation) are not blocked by spill. Do not overlap two
  publishers of the same minute raw table; that can leave multiple
  `raw_published=true` audits for one surviving snapshot. After spill, syncs
  drop in-memory history tuples and DuckDB publish caps `memory_limit` (default
  `12GB`, override `ODDSFOX_MINUTE_PUBLISH_MEMORY_LIMIT`) so snapshot publish
  spills to `${ODDSFOX_RUNTIME_ROOT}/duckdb-temp` instead of SIGKILL.
- Futures-minute sync no longer fail-closes on empty in-window CLOB history;
  empty tokens are audited and skipped while success tokens publish. Hard
  `error`/`cancelled` (or all-empty) still fail the run. Publish inventory and
  market-minute DQ treat empty audit siblings as healthy.
- Permanent mid-plan CLOB errors in `sync_token_plan` flush already-fetched odds
  and advance the ledger cursor from contiguous progress instead of jumping to
  `end_ts` while discarding the buffer.
- Unified minute-odds backfill selects
  `+polymarket_wc2026_market_minute_odds_data_quality` so publication DQ is built
  with the mart (matching the recreate guide and CI contract).

### Added

- `make minute-odds-dbt-benchmark` /
  `scripts/benchmark_polymarket_wc2026_minute_odds_dbt.py`: disposable synthetic
  dbt rebuild harness for the unified minute mart (tiers `smoke` / `tune` /
  `performance` / `production-shaped`). Reports wall time, primary vs all-token
  counts, DQ blockers, peak RSS, and DuckDB temp bytes. Never opens the
  operator warehouse.
- `make minute-odds-live-smoke` /
  `polymarket_wc2026_minute_odds_live_smoke`: disposable unified minute-odds
  live smoke that samples ~5% of match markets and ~5% of futures markets
  independently (all tokens retained per selected market), caps sampled futures
  windows to their final 24 hours, builds
  `+polymarket_wc2026_market_minute_odds_data_quality`, and validates
  audits/raw/mart health into `.cache/runtime/smoke/minute-odds/`. Never opens
  the operator warehouse. Production
  `polymarket_wc2026_minute_odds_backfill` defaults and the full match
  publication gate remain unchanged.
- `make futures-minute-publish-benchmark` /
  `scripts/benchmark_polymarket_wc2026_futures_minute_publish.py`: disposable
  baseline-versus-candidate publish harness with exact SQL equality and ignored
  JSON evidence under `.cache/runtime/benchmarks/futures-minute-publish/`.
- `POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG` (default `true`): when `false`,
  `polymarket_wc2026_minute_odds_backfill` skips markets/event-catalog/registry
  and reuses the warehouse catalog on odds/dbt reruns (restart Dagster after
  changing).
- `POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH` / `_REFRESH_FUTURES` (default
  `true`): when `false`, skip that raw minute leg (and, for match, its
  international-results/OpenFootball inputs) so operators can resume without
  refetching completed stages. Both may be `false` for a dbt-only rebuild.
- Isolated Polymarket WC2026 unified minute-odds pipeline
  (`polymarket_wc2026_minute_odds_backfill`, `make minute-odds-backfill`,
  `dbt-minute-odds-ci`): match-window minute history plus futures tournament-span
  minute history into `polymarket_wc2026_market_minute_odds` (`tag:minute_odds`).
- Futures-minute raw/audit tables and shared `odds/minute_batch.py` fetch stack
  (CLOB batch POST, 24h window chunks, RPS auto-tune, Parquet candidate/swap
  publish).
- `scripts/cleanup_polymarket_wc2026_registry_hygiene.py`
  (`make cleanup-polymarket-wc2026-registry-hygiene`) dry-runs or applies deletion
  of synthetic catalog contamination (`evt-A` / `evt-B` / `m-shared`) and
  ineligible `events_api` / `markets_api` registry orphans.
- `is_numeric_polymarket_id` helper; catalog merge and sticky registry build skip
  non-numeric Polymarket event/market IDs.

### Changed

- Minute-odds publish no longer double-materializes through a persistent DuckDB
  stage plus global `row_number()` window. Per-token timestamp dedupe happens
  before spill (audit `window_history_sha256` / row counts follow the published
  post-dedupe history); bounded typed Arrow batches write temporary Parquet
  shards plus a `manifest.json` of exact `token_ids`; DuckDB bulk-loads a
  candidate table, builds the primary key once, validates constraints and
  audit/manifest token-id set equality, and atomically renames the candidate
  into the canonical raw snapshot. Defaults: 4M rows/shard, 256k-row Arrow
  batches, Snappy compression (frozen via the disposable publish benchmark
  matrix on equality-correct runs). Same-machine streamed 10M-row performance
  tier measured about 1.4x DuckDB publish speedup with exact raw/audit
  equality; do not claim 10x unless a later report reaches ≥10x. Smoke-tier
  ratios are not the speed claim. Use
  `make futures-minute-publish-benchmark` with
  `FUTURES_MINUTE_PUBLISH_BENCHMARK_TIER=performance` or `production-shaped`.
- Troubleshooting DuckDB lock errors documents how to find and kill orphan
  warehouse holders after a canceled run or `dagster-dev` restart (`lsof` +
  kill; prefer a new launch over auto-retry).
- Futures-minute sync logs DuckDB audit/publish phases (fetch handoff, audit
  write, Parquet spill, candidate load, PK build, swap, cleanup) so large
  publishes are not silent in Dagster.
- Related-tag event-catalog recall still expands Gamma page breadth, but local
  membership now requires tag / series / slug-prefix match (no related-only
  short-circuit).
- Catalog and scan market payloads inherit enclosing-event tags when Gamma omits
  market-level tags.
- Catalog refresh prunes ineligible `events_api` / `markets_api` registry rows.
- Observability `market_tokens_without_history` casts token IDs to `VARCHAR` so
  the anti-join no longer over-counts.
- Data contracts and troubleshooting document resolution/status nulls, empty
  CLOB history, and synthetic warehouse contamination cleanup.

## [0.2.0] - 2026-08-05

### Added

- `scripts/export_marts_parquet.py` (`make export-marts-parquet`) exports every
  present table or view in the shipped `*_marts` schemas to Parquet under
  `artifacts/marts_exports/<utc>/`.
- Kalshi candlestick sync and series-scope registry refresh surface partial
  failure counts (`markets_failed` / `failed_market_tickers`, `events_failed`)
  without changing fail-open partial-success behavior.
- Polymarket asset helpers split into
  `polymarket_asset_helpers_{markets,registry,odds}` with a barrel re-export.

- `scripts/bootstrap_dbt_ci_duckdb.py`, `scripts/gate_timing.py`, Playwright
  browser caching in Manual Full Validation, and unified uv cache path
  `.cache/runtime/uv`.

### Changed

- Focused Mutmut gate drops `polygon_settlement_normalize` so the gate matches
  the documented mutation surface (outbound URL, raw snapshots, market-scope
  predicates, market persistence, and odds planning).
- Dagster dbt warehouse snapshots expand `+` selectors via the dbt manifest
  parent graph (missing manifest falls back to non-expanded matching).
- Path-keyed Polymarket/Kalshi dlt pipeline caches live in
  `storage/duckdb/dlt_batch.py` (landing callers no longer import orchestration).
- Docs: `configuration.md` documents the remaining
  `POLYMARKET_WC2026_SCOPE_*` tag-discovery / keyset / registry-cap env vars.
- Golden mart `polymarket_wc2026_market_hourly_odds` now selects a primary CLOB
  token for every admitted market: Yes when present, otherwise `outcome_index`
  0. Adds `primary_outcome_label` so rows state what the price represents
  (Yes, Over, team name, etc.). Closes coverage for advance, totals, spreads,
  corners, and other non-Yes market types that previously had upstream hourly
  history but never reached the mart.
- Polymarket hourly odds sync (`polymarket_wc2026_raw_token_odds_history_hourly`)
  fetches CLOB price history via `POST /batch-prices-history` (up to 20 tokens
  per call) instead of one `GET /prices-history` per token window. Per-token
  ledger, telemetry, and full-lifetime collection semantics are unchanged.
  Configurable as `batch_group_size` (default `20`). Hourly
  `auto_tune_max_rps` default raised to `90`.
- Polymarket odds planning and staging odds/ledger/skips/daily models now use
  the latest `event_market_payload_snapshots` token catalog (same SoT as
  `stg_polymarket_wc2026_market_tokens`), so registry/enrichment-only tokens
  without payload coverage no longer break dbt relationships.

- Routine Polymarket WC2026 registry refresh / full pipeline event-catalog runs
  skip the platform-wide slug-prefix recall scan (`include_slug_prefix_recall=
  false`). Tag and series partitions stay exhaustive. Use the unscheduled
  `polymarket_wc2026_event_catalog_recall_audit` job (`make event-catalog-recall-audit`)
  for a rare completeness re-check. Partition-level checkpoints resume interrupted
  crawls; slug-prefix recall can early-stop after consecutive empty match pages.

- Polymarket WC2026 hourly odds default Dagster config now sets `force=false`,
  so routine runs skip fully-checked closed tokens and only revisit due gaps.

- **Breaking:** `polymarket_wc2026_dbt_build` and `polymarket_wc2026_full_pipeline`
  now build only the golden mart closure (`+polymarket_wc2026_market_hourly_odds`).
  They no longer ingest `international_results` or build match-minute, order-book,
  portrait, or other sibling Polymarket WC2026 marts. Use the dedicated backfill
  jobs for those surfaces.

- **Breaking:** Polymarket WC2026 refocuses on one documented golden mart:
  `polymarket_wc2026_market_hourly_odds` at grain `(market_id, odds_hour_epoch)`.
  Admission uses sticky event lifetime volume
  (`event_min_lifetime_volume_usd = 100000`) from
  `dbt/seeds/polymarket_wc2026_pipeline_policy.csv`. Export with
  `scripts/export_polymarket_wc2026_market_hourly_odds.py`.

- `polymarket_wc2026_market_scope_registry_refresh` now materializes the
  `event_catalog` multi-asset neighborhood (plus OpenFootball schedule fixtures)
  so registry refresh aligns with the declared `event_catalog` dependency.
- Polymarket odds writer flushes now stage rows with Arrow bulk loads on the
  writer connection instead of per-flush `dlt.pipeline.run` replace loads.
- Kalshi hourly candlestick sync now fetches concurrently and persists candlestick
  rows plus ledger state in one batched write after the worker pool joins.
- Split `polygon_settlement` ingest into `polygon_settlement_{types,normalize,scan,sync}`
  modules with a facade preserving public imports; widened focused mutation coverage
  to `polygon_settlement_normalize.py`.
- Split `market_portrait` into `market_portrait_story` and `market_portrait_export`
  modules with a facade preserving public imports.
- Extracted `merge_event_catalog_batch` into `dlt_batch_event_catalog.py`.
- Deduplicated polygon audit/export JSON writes via `publishing._bundle_io.write_json`.
- Extracted shared registry helpers (`registry_common.py`) and lifted Kalshi
  registry skip-if-refreshed materialization into `kalshi_asset_helpers.py`.
- Added a bootstrap lock around DuckDB schema initialization in `connection.py`.
- Removed `[project.optional-dependencies].dev`; contributor tooling installs
  exclusively through uv dependency groups (`uv sync --group dev`).
- Split the root `Makefile` into include fragments (`Makefile.gates`,
  `Makefile.dbt`, `Makefile.lint`, `Makefile.test`, `Makefile.ops`).

- Split the former "Advanced match analysis (experimental)" registry row into
  three mature, isolated pipelines: match-minute odds, match order book, and
  market portrait. Added `dbt-match-order-book-ci`, `dbt-market-portrait-ci`,
  and `market-portrait-target-validate` release-gate lanes plus synthetic
  portrait/trades replay fixtures.

- Routine scoped dbt jobs (`polymarket_wc2026_dbt_build`,
  `kalshi_wc2026_dbt_build`, and
  `polymarket_wc2026_logical_atlas`) default to incremental builds
  (`full_refresh=False`). Set `full_refresh=True` in Dagster run config when a
  one-off full rebuild is required.
- Polymarket market-scope discovery and registry refresh now apply the scan
  helper's built-in page-budget guard (25 pages without progress) when Dagster
  run config leaves `max_pages_without_progress` unset, instead of disabling the
  guard.
- Kalshi market-scope registry refresh and hourly candlestick sync use bounded
  concurrent fetch with the shared `KALSHI_REQUESTS_PER_SECOND` rate limiter.
- `int_polymarket_wc2026_logical_markets` and
  `int_polymarket_wc2026_fixture_events` materialize as tables; Polymarket
  markets and token-working-set twin models share parameterized dbt macros.
- Warehouse observability snapshots batch raw-table counts and scope dbt model
  snapshots to the models selected by the active `dbt_select` /
  `dbt_exclude`.
- `release/logical_bundle` calls the exporter in-process instead of shelling
  out to `scripts/export_polymarket_wc2026_logical_bundle.py` (the script
  remains the CLI wrapper).
- Removed unused single-field Polymarket metadata backfill entry points
  (`backfill_tokens`, `backfill_slugs`, `backfill_end_dates`,
  `backfill_event_slugs`); `enrich_market_metadata` is the sole path.
- Consolidated Polymarket raw dlt column specs with DuckDB DDL, shared
  publishing bundle I/O helpers, and orchestration raw-snapshot / dlt-cache
  helpers; decomposed the highest-complexity ingestion, contracts, publishing,
  and odds-planning functions without behavior change.

- Pipeline clarity docs: [Pipeline registry](docs/reference/orchestration.md#pipeline-registry)
  with maturity tiers (production, mature composed/isolated, experimental);
  entry-point jobs vs steps in [Terminology](docs/reference/terminology.md#execution);
  advanced match-analysis family (order book → market portrait; minute odds
  optional and independent) grouped in operator and scope guides. Docs-only; no job or code changes.
- Compact terminology cutover: normative vocabulary is exactly **34** core
  terms in `docs/reference/terminology.md`, gated by
  `config/terminology_policy.toml` and `make check-terminology`. Breaking
  renames include ScopeStep / job surface `market_scope_registry`, dbt
  `*_working_set` models, `int_wc2026_advancement_fixtures`, and symlink
  activation via `activate_current` (replacing `publish_current`).
- `release-gate` (and Manual Full Validation) is required only before publishing
  a major version; ordinary PRs, including dependency/Dagster/dbt/data-quality
  work, use `ci-fast` plus focused Make targets.
- Local `ci-fast` and `release-gate` use one Make jobserver (`GATE_JOBS`) over a
  prerequisite DAG; `ci-fast-core` / `release-gate-core` run the same graph with
  `-j1`. Coverage shards write distinct `COVERAGE_FILE`s and combine once;
  subprocess pools are capped with `RELEASE_PYTEST_WORKERS`, `DBT_TEST_WORKERS`,
  and `MUTMUT_MAX_CHILDREN`. dbt profile threads are environment-configurable
  (`DBT_THREADS`). PR CI and Manual Full Validation install narrower uv
  dependency groups per worker (`--no-default-groups`) while keeping full
  Python 3.10 and 3.13 suites. Manual Full Validation's static lane is
  `static-docs` (no container worker).
- Test ownership is path-based: `tests/repository/`, `tests/docs/`, and
  `tests/package/` hold policy checks; `make check-repository` is the canonical
  repo-check entrypoint wired into lint and CI static lanes. Ordinary unit
  collection ignores those directories.
- Orchestration unit fixtures no longer reload settings modules per test; market
  scope tests live under `tests/unit/ingestion/market_scope/`. Polygon audit
  release validation uses focused helpers and minimal row factories, with one
  full 39,120-row aggregate check and one complete audit-bundle build.
- Polygon settlement data-quality SQL is split into tagged seed/scan/raw/minute
  summary seams. `dbt-polygon-settlement-ci` runs Polygon-tagged dbt unit tests
  covering every hard blocker key and all seven normalization-pair branches,
  then a slim integration suite (exact dense mart, representative scan failure,
  representative raw-pair failure).
- Dagster integration is layered: mocked registered-job smoke, recording-dbt
  wiring for the twelve shipped scoped jobs, one real disposable-DuckDB/dbt E2E
  per shipped scope, and writer recovery without repeated real dbt builds.
- Golden mart fixtures remain available via `make golden-dbt` but are no longer
  duplicated in the release / Manual Full `dbt-quality` sequence because
  `integration-dbt-cov` already executes them.

### Removed

- `polymarket_wc2026_hourly_odds_schedule` and
  `POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED`. WC2026 Polymarket events are
  complete; use manual `polymarket_wc2026_hourly_odds_ingest` or
  `polymarket_wc2026_full_pipeline` for one-off refreshes.
- Unused `POLYMARKET_WC2026_HOURLY_WINDOW_DAYS` setting export and the dead
  Polymarket hourly dbt macro `contract_ref` / `hourly_window_days` branch
  (lifetime history; Kalshi retention window is unchanged).
- Unused `validate_git_sha` helper and unused Polygon seed split constants
  `EXPECTED_GROUP_PROPOSITIONS` / `EXPECTED_KNOCKOUT_PROPOSITIONS`.

- **Breaking:** Polymarket WC2026 knockout and catalog marts
  (`polymarket_wc2026_markets`, `polymarket_wc2026_knockout_market_tokens`,
  `polymarket_wc2026_knockout_markets`, `polymarket_wc2026_knockout_token_hourly_odds`),
  knockout observability marts/tests, and export scripts
  `export_polymarket_markets.py` and
  `export_polymarket_wc2026_knockout_hourly_odds.py`. Delete local warehouse
  files (`rm oddsfox.duckdb*`) and rerun quickstart after upgrading.

- **Breaking:** Polymarket WC2026 logical atlas — the seven
  `polymarket_wc2026_logical_*` marts, `polymarket-wc2026-logical-v1` export
  bundle, `polymarket_wc2026_logical_atlas` job, `release/logical_bundle` asset,
  `raw/reviewed_event_membership` asset, related dbt models/seeds/tests, and
  scripts `export_polymarket_wc2026_logical_bundle.py`,
  `materialize_polymarket_wc2026_logical_fixture.py`, and
  `build_hosted_artifacts.py`. Shared event-catalog ingestion
  (`raw/event_catalog`, `raw/event_snapshots`, `raw/event_market_memberships`)
  remains for market scope registry refresh.

- Polymarket US midterms 2026 pipeline (`polymarket_us_midterms_2026_*` jobs,
  schedules, dbt graph, and `POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SCHEDULE_ENABLED`).
- Cross-platform WC2026 knockout match pipeline (`wc2026_knockout_match_odds_full_pipeline`,
  `wc2026_marts.wc2026_knockout_match_hourly_odds`, related observability, and
  `WC2026_KNOCKOUT_MATCH_ODDS_HOURLY_SCHEDULE_ENABLED`).
- Docker packaging and publication: Dockerfile(s), `.dockerignore`, container
  smoke Make targets, GHCR multi-arch publish/sign steps, and the Docker image
  guide. OddsFox Pipeline is macOS-first; distribution smoke stays on
  `make package-smoke`.
- Retired first-party terminology identifiers and prose from the compact
  cutover (see [Terminology](docs/reference/terminology.md) deprecated table):
  `token_universe` / `match_market_universe` model names and “market/token/
  validated universe” phrases, ScopeStep `market_registry`, `publish_current`,
  `scope_class`, and related graph-export product names. Operators with older
  warehouses delete `oddsfox.duckdb*` and rebuild.

### Fixed

- Match-minute CI seed writes `event_market_payload_snapshots` so
  `stg_polymarket_wc2026_markets` (payload SoT) can build the synthetic
  104/248/496 contract after the markets staging cutover.
- Restore 100% statement/branch coverage for `release-gate` (unit + Dagster +
  dbt shards) after post-0.1.13 gaps in registry collect, odds fetch/execution,
  jobs config merge, and related helpers.
- Kalshi candlestick due filtering compares naive-UTC `next_check_at` walls to
  `CURRENT_TIMESTAMP AT TIME ZONE 'UTC'` so session timezones no longer skip or
  pull markets early.
- Polymarket `token_sync_ledger` NULL/NULL cursor upserts keep `NULL` instead of
  materializing BIGINT min (which broke `to_timestamp` in staging).
- Metadata enrich and markets-API scan prefer `is_enclosing_event` the same way
  market transform does.
- Gamma/odds ISO datetime parsing truncates >6 fractional digits before
  `fromisoformat` so trailing offsets are not dropped.
- `wc2026.v1` `contract_fingerprint` includes `team_ratings_pre_match`,
  `base_camp_venues`, `international_matches`, and `third_place_slot_assignments`.
- Match order-book inventory requires FIFA match 95 with 1 market / 2 tokens
  (aligned with the documented mart contract and singular inventory test).
- Orchestration docs: only Kalshi has a schedule env enable flag; international
  results stays hard-stopped at definition load.

- Kalshi hourly/full-pipeline jobs default `force=false` so candlestick ledger
  due filtering and `routine_interval_hours` apply (matching Polymarket).
- Polymarket hourly/daily/ledger staging timestamps use
  `to_timestamp(...) at time zone 'UTC'` so hour buckets stay UTC under
  half-hour session timezones.
- Event-catalog market payloads mark enclosing events; market transform prefers
  `is_enclosing_event` when extracting `event_id` / `event_slug`.
- Polymarket odds pool group failures schedule ledger retry without writing
  permanent `token_sync_skips`.
- Polymarket Gamma datetime parsing converts offset-aware values to naive UTC
  (aligned with Kalshi/odds planning).
- Historical international-results shootout/goalscorer joins fail closed on
  ambiguous `(date, home, away)` matches instead of picking `min(match_id)`.
- Kalshi candlestick/registry asset helpers persist hard-failure sync metrics.
- Observability `tag:match_minute` inference covers `match_working_set` and
  `match_token_minute` intermediates.

- `export_marts_parquet` discovers DuckDB views as well as base tables, so Kalshi
  marts (materialized as views) are included in the all-marts dump.
- Kalshi `_parse_ts` converts offset-aware datetimes to naive UTC instead of
  stripping the offset and keeping local wall time.
- Event-catalog partition checkpoints reuse only `complete=true` caches;
  incomplete early-stop partitions are rescanned on retry (including exhaustive
  recall audits).

- Dagster dbt snapshot tag inference no longer treats `match_trade*` models as
  `tag:pmxt_order_book` (they are `tag:market_portrait`); also infers
  `match_minute` and `wc2026_strategy` so excludes match real dbt tags.
- `snapshot_dbt_models` looks up strategy marts by their DuckDB aliases
  (`team_ratings_current`, `fixtures`, …) instead of always reporting the
  `wc2026_*` node names as missing.

- Kalshi market/candlestick normalize maps live `volume_fp` /
  `open_interest_fp` (fixed-point contract counts) into warehouse `volume` /
  `open_interest`, with legacy integer-field fallback. Replay cassette updated
  to the live field shape.
- Docs: orchestration pipeline registry CI dbt gate column now matches Make /
  GitHub (`ci-fast` → `dbt-lint`; model builds / inventory in `dbt-build-ci` and
  isolated lanes; market-portrait exclusion wording corrected).
- Docs: README and day-two live-column guidance scoped to Kalshi current marts
  (`is_actionable_live_market` / `current_price_status`).

- Sticky Polymarket market-scope registry admission no longer drops an enclosing
  event when a newer non-enclosing related-event bridge exists for the same
  `market_id`.
- Kalshi HTTP client builds with `retries=0` so urllib3 `status_forcelist`
  cannot turn 429 responses into status-less `RetryError` before Kalshi's own
  backoff sees them. `APIClient(retries<=0)` mounts a plain zero-retry adapter.
- Wrapped `requests.exceptions.ChunkedEncodingError` is classified as a
  transient pipeline error for Dagster retry.
- Kalshi dbt source `events` `asset_key` points at `kalshi/wc2026/raw/events`
  (was incorrectly wired to `raw/markets`).
- Event-catalog partition checkpoints clear immediately after a successful
  warehouse merge, before sync-run metrics, so a metrics failure cannot leave
  stale recovery checkpoints.
- Docs: CLOB `fidelity=60` is one observation bucket per 60 minutes (hourly),
  not per minute.

- Polymarket Gamma market transform now accepts RFC3339 offset timestamps,
  coalesces `volumeNum` from `volume`, and honors `endDateIso` when `endDate` is
  absent.
- Event-catalog registry refresh stamps per-market `scraped_at`, admits membership
  from the latest enclosing snapshot only, and prunes stale `event_catalog`
  registry rows after rebuild.
- Routine `raw/markets` no longer mutates the event-catalog registry
  (`refresh_registry=false`); metadata enrichment runs inline with guardrail
  checks instead of a daemon side thread.
- Kalshi market landing stamps one shared `scraped_at` per sync batch.
- Elo pre-kickoff export filters `snapshot_scope = '2025'` so duplicate year-end
  scopes do not emit duplicate team rows.

- `init_duck_db()` again syncs the active DuckDB path before the initialized
  fast-path return, so `DUCKDB_PATH` / `DUCKDB_NAME` swaps still re-bootstrap
  under the schema bootstrap lock.
- `profile_warehouse.py` `--refresh` now propagates `--duckdb-path` to Polymarket
  sync and dbt subprocesses instead of mutating the settings-default warehouse.
- Polygon settlement scan status JSON is stored under
  `BASE_DIR/.cache/polygon_settlement/status/` instead of a hardcoded checkout
  path.
- `polymarket_wc2026_full_pipeline` (and other jobs using `_merge_run_configs`)
  now unions `dbt_select` and `dbt_exclude` when combining `oddsfox_dbt` run
  configs instead of last-write-wins over the whole op config.
- Kalshi hourly candlestick sync now honors Dagster `history_backfill_days` and
  `routine_interval_hours` run-config fields instead of ignoring them.
- `parse_created_at()` accepts ISO-8601 timestamps ending in `Z` or `+00:00`
  without fractional seconds.
- Empty `DUCKDB_PATH` now falls back to `DUCKDB_NAME` like the connection
  resolver, instead of treating the empty string as a literal path.
- Unrecognized `POLYMARKET_WC2026_SCOPE_KEYSET_CLOSED` values now omit the
  closed filter instead of coercing to `false`.
- Kalshi `map_bounded` now skips per-item transient failures instead of
  aborting the whole candlestick sync or registry refresh batch.
- Polymarket market-discovery progress guardrails now read `events_page` and
  record per-page deltas instead of always reporting zero work.
- Kalshi and Polymarket progress guardrails now record per-callback work
  deltas instead of inflating cumulative totals.
- `wc2026_results` now tolerates a one-day fixture/result date drift when
  joining international results.
- `fetch_token_history()` honors explicit `start_ts=0` epoch timestamps.
- Warehouse profiler numeric classification now covers DuckDB unsigned integer
  types (`USMALLINT`, `UINTEGER`, `UBIGINT`).
- Match-minute knockout working-set joins now allow up to one day of kickoff
  drift instead of a 60-second cutoff that silently dropped mapped markets.
- Kalshi `hour_start_utc` now stores the hourly candlestick bucket start
  (`end_period_ts - 3600`) instead of the inclusive period end timestamp.
- `wc2026_results` keeps one-day date tolerance for team-identity joins but
  requires exact dates for knockout city-only attribution.
- Kalshi QF/SF/FL `progression_outcome_label` values now use the
  `not_eliminated_in_*` convention aligned with price inversion.
- Order-book backfill progress callbacks now record progress before enforcing
  hard no-progress timeouts.
- Release-gate match-order-book and market-portrait lanes now use isolated
  dbt runtime roots instead of sharing one target directory.
- `count_polymarket_wc2026_gamma_tag_events.py` now forwards
  `keyset_related_tags` to Gamma keyset requests.
- `release-gate-coverage-prep` now prepares dbt state under the coverage
  runtime root used by coverage shards.
- `join_under_base()` now rejects relative hrefs that escape the base path via
  `../` segments.
## [0.1.13] - 2026-08-02

### Added

- The manifest-bound `polymarket-wc2026-logical-v1` event, market, membership,
  proposition, entity, and scope bundle for the local WC2026 Logical Market
  Atlas. Event admission uses Polymarket's source-reported lifetime event volume
  with a reviewed final-tournament membership policy and sticky eligibility.
- Append-only Polymarket event, tag, and event-market snapshots; reviewed
  membership decisions; bundle quality checks; and atomic paired Pipeline/Graph
  release publication with exact source revisions and file hashes. Builds are
  shadow-only; activation additionally requires Graph's manifest-bound browser
  smoke receipt and validates it under the activation lock before repointing
  `current`. Content-sealed pre-atlas rollback releases remain activatable
  without fabricating unavailable historical code revisions.
- Logical market membership is derived from each event's latest complete
  catalog observation, so source corrections and zero-child removals do not
  leave stale relationships in the served logical atlas while raw history remains
  append-only.
- Public Polymarket market catalog marts `polymarket_wc2026_markets` and
  `polymarket_us_midterms_2026_markets`: one row per platform-wide Gamma market
  with volume at or above $100,000 USD (`/markets/keyset` catalog sync; no
  tag/registry filter), exposing event/market identity, question, description, outcomes,
  CLOB token IDs, reported USD volume, start/end times (nulling `start_time` when
  Gamma reports a start after `end_time`), category, and tags.
- `scripts/sync_polymarket_markets_catalog.py` to land that platform-wide catalog
  into `polymarket_catalog_raw.markets`.
- `scripts/export_polymarket_markets.py` to export those catalog marts to parquet
  under `artifacts/polymarket_markets_exports/` with fail-closed grain, volume-floor,
  timing, and outcomes/CLOB JSON checks.

### Changed

- Breaking terminology cutover (no aliases): canonical vocabulary in
  `docs/reference/terminology.md` — product-path **pipeline** (not flow);
  **logical atlas / logical bundle / logical contract** (not graph odds/export);
  public marts as the supported query API; **`wc2026.v1` is the private strategy
  clean-data contract only**; minute-grain / match-minute (not minutely);
  ingestion-run observability (not sync-run); schedule fixtures for OpenFootball
  1–104; market scope registry refresh / step `market_registry`; metadata
  enrichment; pipeline policy for threshold seeds. Identifier renames:
  `shipped_scopes`; `*_market_scope_registry_refresh`; ScopeStep
  `market_registry`; `membership_class`; `market_metadata_enrichment`;
  `ingestion_run_events` / `*_ingestion_run_observability`; match-minute
  `observation_gap`. Delete `oddsfox.duckdb*` and rebuild before using the new
  layout.

- Breaking: logical-atlas identifiers rename `graph_*` eligibility/usability
  fields and models to `logical_*` (`int_polymarket_wc2026_logical_markets`,
  `polymarket_wc2026_logical_contract`, `logical_usable`,
  `event_logical_eligible`, tag `wc2026_logical_atlas`). No compatibility
  aliases.

- Breaking: the WC2026 release path now exports the logical-v1 bundle and
  invokes `oddsfox-graph discover --input-profile polymarket-wc2026-logical-v1`.
  The prior hourly graph mart/export is removed. Recreate local DuckDB
  warehouses before running the new full-pipeline job.

## [0.1.12] - 2026-07-29

### Fixed

- Football stories now use exact half-open 60-second UTC bands, infer missing
  stoppage labels from actual period duration with a one-millisecond tolerance,
  and clamp only the final band to the actual period boundary. Validation
  tolerates only the sanitizer's possible two-microsecond inversion between
  equal final-period and game-end timestamps.
- Football annotations now carry chronological post-event scores. Scoring
  transitions are validated one goal at a time, final scores must agree with
  `MatchFacts`, and score checkpoints become effective at the uncertain
  event-minute band's end.
- Minute-aligned reaction windows now select the last strictly pre-window and
  first guaranteed post-window observations, return null when unavailable, and
  never cross a halftime or extra-time break. Directional millisecond rounding
  uses ceilings for `< S` and `>= E` thresholds and floors for same-period
  upper bounds, preserving those predicates at sanitized micro-epsilon
  boundaries.
- Story publication now fails closed on derived band, annotation, score, and
  reaction invariant violations without changing the
  `oddsfox.market-portrait.v1` wire shape.

## [0.1.11] - 2026-07-29

### Changed

- Market portraits now begin at actual kickoff and use a continuous 45-second
  regulation timeline with uniform football-minute pacing.
- Extra time extends portraits to 60 seconds and penalty shootouts add one
  five-second phase without changing the v1 bundle contract.

## [0.1.10] - 2026-07-29

### Changed

- Added sanitized UTC kickoff to the neutral market-portrait fact boundary.
- Market-portrait export now rejects implausible or shifted period timelines,
  inconsistent validated-universe timing, and PMXT root windows that do not
  strictly cover the complete football interval before serializing a bundle.
- Preserved the byte-stable `oddsfox.market-portrait.v1` bundle contract.

## [0.1.9] - 2026-07-28

### Added

- Added the private `oddsfox.market-portrait.v1` contract, neutral football
  fact API, group/knockout target generation, resumable PMXT trade acquisition,
  exact complete-state marts, and explicit portrait backfill workflow.
- Renamed the optional public match-event placeholder and asset identity to the
  provider-neutral `private_match_events` boundary.

### Fixed

- `wc2026_team_ratings_current` now selects only EloRatings
  `snapshot_scope = current` (live World scrape). It previously preferred
  year-end rows via `snapshot_year desc nulls last`, so the “current” mart and
  latest freeze CSV mostly echoed 2025 year-end ratings.

### Added

- Added an unscheduled, resumable hosted-PMXT historical L2 backfill for the
  pinned Argentina–Egypt WC2026 round-of-16 team-to-advance market, including
  both outcome-token snapshot streams, conservative credit accounting,
  fail-closed adaptive range splitting, dlt landing, transactional publication,
  and the long-form
  `polymarket_wc2026_marts.polymarket_wc2026_match_order_book` mart.
- Added PMXT order-book identity/completeness/depth observability, an isolated
  Dagster/dbt graph, replay and integration coverage, and the opt-in
  `make match-order-book-live-smoke` operator path.
- Added `wc2026_marts.team_ratings_pre_match`: match×team pre-match EloRatings
  reconstructed from canonical `wc2026_raw.eloratings__match_results`
  (`pre = post ∓ change`, all competitions on/after 2026-01-01). Requires a
  fresh EloRatings collector snapshot that publishes `match_results.parquet`
  alongside `team_ratings.parquet`.
- Added `scripts/export_eloratings_wc2026_team_ratings_freezes.py` and
  `make export-wc2026-elo-freezes` to export national-team Elo CSV freezes
  (`pre_kickoff` = year-end 2025 history; `latest_current` = live World scrape)
  under `artifacts/wc2026_elo_exports/`.
- Added strict focused Mutmut coverage for outbound URL safety, raw snapshot
  contracts, Polymarket scope predicates, market persistence, and odds
  planning, enforced by ordinary policy tests plus local and manual full
  release gates.
- Added incremental/full-refresh equivalence coverage for all five incremental
  odds models, repeat-run tests for all shipped refresh paths, and transactional
  Polymarket writer failure/recovery validation.
- Added a required Python 3.13 package and ordinary-test compatibility worker
  while retaining Python 3.10 as the supported floor and full-release runtime.
- Added an operator guide for the cross-platform knockout job
  (`wc2026_knockout_match_odds_full_pipeline`) and linked it from Choose a
  scope, Operators, Enable schedules, and Orchestration.
- Split private `oddsfox.raw.v1` / strategy clean-data docs into
  `docs/reference/strategy-contracts.md`; public mart contracts remain on
  `docs/reference/data-contracts.md`.
- Split the WC2026 minute-mart recreation runbook into an index plus
  match-minute and Polygon settlement child guides.
- Expanded the Integrators hub with a consume/pin/export/Polygon-boundary
  checklist.
- Documented audience hubs (analysts, operators, contributors, integrators),
  FAQ, glossary, scope and non-goals, design decisions, integration guidance,
  day-two operations, and the signed Docker image as an advanced path.
- Added an operator-responsibilities page covering data-rights checklist,
  non-advice and non-venue disclaimers, export redistribution matrix, privacy
  caveats, and non-authoritative third-party terms pointers.
- Expanded `SECURITY.md` scope notes for RPC secrets, wallet material, and
  operator-local Polygon audit artifacts.

### Changed

- Removed the unused Great Expectations-style report layer; dbt build/tests and
  observability relations remain the data-quality authority.
- Moved the Polygon settlement complete column contract from the data
  dictionary into public data contracts; dictionary keeps analyst summary.
- Slimmed the Analysts hub to a warehouse branch and join map; added a shared
  reference-ladder note on the chooser, dictionary, contracts, and warehouse
  pages.
- Aligned Development schedule snippets with all four disabled-by-default
  hourly flags and pointed README first-run readers at Quickstart.
- Reshaped the docs site and README toward progressive disclosure: thinner
  README portal, quality-gate SSOT in `AGENTS.md`, contributor checklists in the
  Development guide, and Polygon settlement framed as optional/advanced.
- Reordered the analyst data dictionary so common WC2026 marts lead and the
  Polygon settlement mart follows.
- Clarified that local success checks are technical verification, not
  Hypertrial certification of data rights or trading fitness, and added a docs
  site copyright footer linking Scope, operator responsibilities, and
  Third-Party Notices.
- Strengthened contribution IP hygiene in `CONTRIBUTING.md` and the
  Contributors hub.

### Fixed

- Corrected the pinned Neg Risk adapter provenance notice: its repository
  contains no licence file, so the project infers no licence permission.
- Extended ownership and production-use wording enforcement across the complete
  tracked tree.

## [0.1.8] - 2026-07-24

### Added

- Enforced the reviewed licence mapping for every direct runtime dependency
  while retaining the release SBOM for transitive and base-image inventory.

### Changed

- Established `THIRD_PARTY_NOTICES.md` as the authoritative statement that
  Hypertrial owns and MIT-licenses the first-party project, operates no hosted
  production pipeline or data service, and does not restrict recipients' MIT
  rights.
- Documented independently authored Polygon event-interface provenance,
  third-party source and Contributor Covenant attribution, nominative use of
  third-party marks, and contributor-retained MIT licensing.
- Made GitHub Private Vulnerability Reporting the sole private security channel
  and scoped the container's MIT label to the Hypertrial-owned application.
- Split automatic validation into parallel static/docs, fast-test/contract, and
  dbt-lint workers behind the stable `fast-gate` check. Fast unit tests now use
  xdist while DuckDB, Dagster, dbt integration, replay contract, and browser
  suites remain serial.
- Split manual full validation into parallel coverage, dbt/data-quality, and
  static/docs/container workers behind `full-gate`, and removed the duplicate
  ordinary test pass before coverage.
- Made SQLFluff dbt compilation fail closed and removed redundant automatic dbt
  parsing after mutation checks proved malformed project/schema YAML, undefined
  macros, missing refs, and invalid SQL all fail lint.

## [0.1.7] - 2026-07-23

### Added

- Independent WC2026 Polygon V2 settlement-log flow with an operator-supplied
  248-row on-chain market manifest, resumable finalized-block backfill, wallet- and
  order-payload-redacted fill snapshot, isolated Dagster/dbt graph, and dense
  39,120-row
  `polymarket_wc2026_polygon_settlement_minute_odds` mart.
- Immutable internal Polygon settlement audit releases with complete
  source/provenance/quality evidence and checksums, plus a standalone offline
  exporter for the allowlisted **WC2026 Polygon Settlement Minute Aggregates**
  CSV and operator-local technical quality dossier.
- Developer-only Polygon seed authoring and validation commands, replay-backed
  Polygon dbt validation, and an opt-in live Polygon smoke target.
- `polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds`, a dense,
  null-preserving in-game minute mart for all 104 FIFA World Cup 2026 matches,
  with 248 selected markets, 496 literal source tokens, minute Yes/No OHLC, and
  primary Gamma event timing.
- Dedicated, unscheduled
  `polymarket_wc2026_match_minute_odds_backfill` ingestion/publication job and
  match-minute inventory/completeness observability.
- A DuckDB-native script to export and summarize the match-minute mart as
  Parquet.
- Immutable `international_results` revision and payload provenance, append-only
  per-token minute-fetch audits, token-cadence coverage, and detailed current
  match-minute quality issues.
- `THIRD_PARTY_NOTICES.md`, PEP 639 MIT package metadata, distribution-policy
  checks, and explicit synthetic-fixture provenance.
- An SSD-rooted runtime contract plus `local-marts-rebuild`, which full-refreshes
  and verifies both WC2026 minute marts from operator-local raw warehouses.

### Changed

- Polygon settlement audit releases now live below
  `artifacts/polygon_settlement/audit/`; allowlisted technical exports live below
  `artifacts/polygon_settlement/exports/`. The exporter verifies the immutable
  audit bundle and copies the primary CSV byte-for-byte without querying the
  warehouse
  or calling a network service.
- Polygon settlement collection now uses the `polygon-v2-settlement-v4`
  pipeline: ranges are planned per authored exchange, exact token/block bounds
  reject irrelevant discoveries before receipt fetch, and complete leaves run
  concurrently through receipt/header validation and normalization. Adaptive
  250–20,000-block and 5–50-receipt work records safe per-chunk RPC metrics,
  writes atomic redacted status, and inserts fills through explicit Arrow
  batches. Published compatible reruns short-circuit before credentials or RPC
  construction; all live-smoke state remains below the SSD-backed `.cache`.
- Ordinary Polymarket/dbt jobs and `make dbt-build` exclude the isolated
  `polygon_settlement` graph; the full release gate validates it independently
  with synthetic replay fixtures. The new backfill and release jobs have no
  schedules and do not modify the existing Gamma/CLOB flow.
- Match-minute CLOB publication now replaces the complete raw snapshot in one
  transaction only after all 496 token fetches succeed; failed runs retain audit
  evidence and leave the previous raw snapshot and public mart unchanged.
- The minute mart now exposes scheduled-versus-actual timing, boundary status,
  a zero-based uncapped `elapsed_window_minute` wall-clock axis, raw Yes/No
  close-pair diagnostics, and pinned results provenance. Source-price, cadence,
  timing, and incomplete interior-minute anomalies are nonblocking warnings;
  structural contract failures still block atomic publication.
- Match-minute Parquet export now validates a temporary artifact before atomic
  replacement and reports structural inventory, completeness, boundary, pair,
  provenance, size, and SHA-256 metrics. Existing local warehouses must be reset
  for the new results-provenance and fetch-audit schemas; no migration shim is
  provided.
- Make child processes now place temporary files, uv/XDG/browser caches, Python
  bytecode, and dbt output below `.cache/runtime` by default. Polygon and local
  mart rebuild targets also use SSD-local DuckDB extension directories. Local
  overlays are permitted in the working tree while distribution checks continue
  to validate the committed header shells.

### Fixed

- Polygon V2 normalization now accounts for the exchange's post-settlement
  refund of unused active maker assets in mixed MINT/MERGE matches while still
  requiring exact passive-leg and received-asset conservation.
- Match-minute team names and home/away orientation now follow the latest
  international-results snapshot. The dedicated backfill refreshes that source
  and blocks publication unless all 104 FIFA-numbered games reconcile uniquely.
- Corrected FIFA match 31's scheduled kickoff to June 19 at 11:00 PM ET
  (`2026-06-20 03:00 UTC`).
- Strategy-facing WC2026 marts now use only each private source's latest
  ledger-declared complete snapshot. Repeated full snapshots no longer duplicate
  model grains or retain rows removed by a newer complete snapshot, and an empty
  latest payload now blocks readiness instead of inheriting older raw rows.
- Completed knockout outcomes now align to FIFA match IDs by the schedule's
  unique date and host city when bracket-slot fixtures do not yet contain team
  names.

### Removed

- Removed populated external/reference seeds and the reviewed Polygon
  resolution attestation from repository distributions. Existing seed paths
  are retained as header-only schema shells, and the attestation is
  operator-local.
- Removed external-publication framing from current project documentation. No
  repository command uploads local audit or export artifacts.

## [0.1.6] - 2026-07-17

### Added

- Manual live-readiness workflow for offline source contracts and a disposable
  live WC2026 cross-platform pipeline smoke. The smoke uses a bounded 24-hour
  odds window and no historical backfill without changing production defaults.
- Static Vercel deployment for the MkDocs documentation site at
  `https://data.oddsfox.io/`.
- `wc2026_marts.wc2026_knockout_match_hourly_odds`, keyed by published FIFA
  match number, with dense nullable raw team-advance closes from Polymarket and
  Kalshi for matches 73–102 and 104.
- OpenFootball WC2026 knockout fixture ingestion, permanent incremental
  platform match-hour facts, cross-provider coverage/data-quality relations,
  `wc2026_knockout_match_odds_full_pipeline`, and the stopped-by-default
  `wc2026_knockout_match_odds_hourly_schedule`.

### Changed

- Consolidated GitHub Actions into one offline runner capped at five minutes
  total; it runs lint, fast tests, saved HTTP contracts, dbt parse, and a
  strict documentation build. The exhaustive release gate remains local.
- Reorganized the documentation into operator-first getting-started, guide,
  reference, concept, and development sections.
- Replaced the oversized landing page with a compact responsive homepage, a
  consistent dark palette, rendered Mermaid diagrams, and
  permanent redirects for moved public documentation URLs.
- Refreshed the MkDocs site with the OddsFox website palette, locally hosted
  Inter and JetBrains Mono fonts, a fox favicon, responsive task navigation,
  and a logo-led technical homepage.
- Breaking: added neutral `wc2026_intermediate`, `wc2026_marts`, and
  `wc2026_observability` schemas plus source match facts. Existing local
  warehouses must be reset; no compatibility aliases or migration are provided.
- Kalshi WC2026 scope now includes `KXWCADVANCE`; Polymarket discovery includes
  `fifwc-` exact match events. Exact `soccer_team_to_advance` match markets
  bypass the progression-futures volume floor without changing existing marts.

### Removed

- Removed GitHub-hosted live ingestion. `make live-smoke` remains the
  operator-owned local readiness path.

### Fixed

- Combined WC2026 Dagster/dbt runs now preserve the exact Dagster model subset
  and leave indirect tests to dbt's `buildable` policy, preventing unselected
  model emissions and relationship tests against unbuilt staging relations.
- Documentation browser policy tests fulfill GitHub metadata requests locally,
  so rate limits cannot make otherwise offline render checks fail.

## [0.1.5] - 2026-07-11

### Added

- Kalshi WC2026 pipeline for winner, knockout stage-of-elimination, and group-winner
  markets (`kalshi/wc2026/...` Dagster assets, `kalshi_wc2026_*` DuckDB schemas,
  and public `kalshi_wc2026_marts` relations). Additive; Polymarket contracts are
  unchanged.

### Removed

- Unused CLOB authenticated-request path (`ClobAuth`, `CLOB_API_KEY` /
  `CLOB_API_SECRET` / `CLOB_API_PASSPHRASE`). Polymarket and Kalshi ingestion
  use unauthenticated public API endpoints only.
- `scripts/build_hosted_artifacts.py --refresh-command`; hosted artifact refreshes
  now use the fixed Dagster pipeline command or `--skip-refresh` only.

### Fixed

- Kalshi market-registry-refresh job now lands the `events` raw table (previously
  omitted, breaking standalone registry refreshes).
- Scoped `kalshi_wc2026_full_pipeline` dbt builds exclude `cross_domain` tests
  that reference Polymarket or international-results observability models outside
  the Kalshi ancestor closure (prevents missing-relation errors under
  `indirect_selection: buildable`). Kalshi jobs pass `dbt_exclude=tag:cross_domain`
  at dbt CLI time so schema tests pulled in via indirect selection are skipped too.
  Scoped builds also exclude `tag:polymarket` nodes outside the Kalshi ancestor
  closure.
- Scoped Polymarket market queries respect `active_polymarket_scope` through full
  query execution (midterms metadata backfill).
- DuckDB unit test isolation when local `.env` sets `DUCKDB_PATH`.
- Dagster dbt build syncs `DUCKDB_PATH` to the active warehouse path before
  `dbt build`.
- Scoped `pipeline_run_events` writes for US midterms 2026 ingestion runs.

### Changed

- Breaking: renamed the shared Dagster dbt asset/op and run-config key from
  `polymarket_wc2026_dbt` to `oddsfox_dbt`. Existing job names such as
  `polymarket_wc2026_dbt_build` are unchanged.
- Docs: midterms quickstart path, warehouse schemas, operations validation SQL,
  data-contract analyst caveats, and local `.env`/test isolation guidance.
- Breaking: renamed WC2026 knockout observability columns
  `raw_classified_markets_ge_5000` to `raw_classified_markets_ge_floor` and
  `minimum_raw_markets_ge_5000` to `minimum_raw_markets_ge_floor`.
- Added `international_results_wc2026_match_results_ingest`, raw
  `international_results_wc2026_raw.match_results`, and public
  `international_results_wc2026_matches` / `international_results_wc2026_team_status`
  marts from the `martj42/international_results` FIFA World Cup CSV slice.
- Breaking: public Polymarket WC2026 knockout marts now require extracted teams
  to match the FIFA World Cup 2026 fixture/result roster, removing non-team
  regional futures and non-participant teams such as Italy from public odds
  output while retaining live and historical real-team rows.
- WC2026 knockout classification now recognizes Polymarket elimination-framed
  Round of 16/32 questions (`Will % be eliminated in the Round of X of the World Cup?`),
  so `round_of_16` and `round_of_32` rows populate knockout marts when those markets
  cross the volume floor.
- Breaking: public WC2026 marts now expose only knockout-related Polymarket markets
  with reported `volume >= $5,000` USD. The public mart surface is
  `polymarket_wc2026_knockout_market_tokens`,
  `polymarket_wc2026_knockout_token_hourly_odds`,
  `polymarket_wc2026_knockout_markets`, and run observability.
- Breaking: removed the broad public WC2026 marts for all-token hourly odds, daily odds,
  token coverage, market coverage, market universe, and market-token universe.
  Operators should reset local DuckDB warehouses or drop old dbt schemas before rebuilding.
- Knockout public odds now normalize to the progression side: winner/reach markets
  use the Yes token, elimination-framed markets use the No token, and marts expose
  `market_direction` plus `source_outcome_label` for source-framing metadata.
- Default Gamma keyset discovery now uses `keyset_volume_min=5000`.
- `polymarket_wc2026_hourly_odds_ingest` and `polymarket_wc2026_full_pipeline` now
  default odds sync to the trailing 30 days (`history_backfill_days=30`,
  `window_hours=720`, `min_volume=5000`).
- Breaking: removed minutely ingestion, minutely odds marts, minutely schedule
  flags, the standalone knockout Dagster job, and the unused Dagster odds repair
  asset. `polymarket_wc2026_dbt_build` and `polymarket_wc2026_full_pipeline` still build the knockout
  dbt marts.
- Breaking v0.1.x namespace reset: source/scope names now use source-first
  `polymarket_wc2026` instead of `wc2026_polymarket`. Dagster asset keys are
  hierarchical under `polymarket/wc2026/...`; jobs, schedules, op config keys,
  env vars, scripts, DuckDB/dbt schemas, and marts use flat
  `polymarket_wc2026_*` names. Delete old local warehouses with
  `rm oddsfox.duckdb*` and rerun quickstart.
- Removed the remaining list-shaped Dagster scope config surface and fixed all
  orchestration ingestion/backfill/odds calls to the single `wc2026` market
  scope.
- `int_polymarket_wc2026_market_tokens` now materializes as a dbt table to avoid
  repeated high-fanout downstream view expansion.
- Breaking: `polymarket_wc2026_knockout_token_hourly_odds` is now a dbt view
  over the private incremental `int_polymarket_wc2026_token_hourly_odds` fact
  table, preserving public columns while keeping hourly prices incremental and
  market/team metadata current at query time. Operators with old local relation
  types should reset their DuckDB warehouse or drop the affected dbt schemas
  before rebuilding.
- Breaking: odds history run config renamed `rebuild_minutely` to
  `rebuild_history` and `minutely_backfill_days` to `history_backfill_days`.
  No aliases are provided.
- Polymarket scope helpers now load any slug-like scope present in
  `market_scopes.yml`; the packaged v0.1.x Dagster/dbt graph remains fixed to
  WC2026.
## [0.1.4] - 2026-07-03

### Added

- Live-current hourly odds mart and export option for graph-ready OddsGraph
  inputs.

### Fixed

- Hourly forced sync planning keeps ended-market grace filters instead of
  re-planning stale ended markets.

## [0.1.3] - 2026-07-03

### Fixed

- Python 3.10 CI coverage now exercises the dbt profiles fallback and
  `polymarket_wc2026_raw.markets` index creation branches, restoring the required
  100% coverage gate after the `v0.1.2` release.

## [0.1.2] - 2026-07-02

### Added

- Generic dbt test macros for grain uniqueness and price bounds (replacing
  duplicated singular tests).
- Regression tests ensuring dbt source `meta.dagster.asset_key` values match
  Dagster asset keys and that resolved dbt model deps wire to ingestion assets.
- Schedule mutual-exclusion guard when both minutely odds schedule env flags
  are enabled.
- `outcome_label` on selected-scope minutely, daily, and whale odds marts so
  analysts can interpret `outcome_index` without joining to `polymarket_wc2026_markets`.
- Companion markdown data spec written alongside selected-scope minutely odds
  parquet exports (`export_selected_minutely_odds.py`; use `--no-spec` to skip).
- Seven new Polymarket scope presets: `us-politics`, `geopolitics`, `crypto`,
  `economy`, `nba`, `nfl`, `champions-league`.

### Changed

- `PolymarketDagsterDbtTranslator` now honors `meta.dagster.asset_key` on dbt
  sources (with duplicate-source keys enabled) so the shared dbt asset waits for
  ingestion assets instead of running immediately after `polymarket_wc2026_raw_markets`.
- Removed tautological dbt tests on selected-scope minutely/daily/whale marts
  (`mart_matches_selected_scope`, redundant `no_duplicate_grain`, whale subset
  singular test) that scanned ~54M view rows and added ~10 minutes to local dbt
  builds; grain and reconciliation coverage remains on sources and upstream
  models.
- Breaking: `POLYMARKET_MARKET_SCOPE` replaced by CSV
  `POLYMARKET_WC2026_MARKET_SCOPES` (one or more preset names). dbt var
  `active_market_scope` replaced by `active_market_scopes` (list).
  `polymarket_wc2026_markets` grain is now `(scope_name, market_id)`. Dagster-run dbt
  passes `active_market_scopes` from env automatically. Warehouse reset
  recommended (`rm oddsfox.duckdb*`).
- Breaking v0.1.x warehouse and orchestration contract change: WC2026-specific
  marts, registry tables, env vars, scripts, assets, and jobs were replaced by
  generic selected-market-scope surfaces. WC2026 remains the default preset in
  `market_scopes.yml`; operators with old local DuckDB files should delete
  `oddsfox.duckdb*` and rerun quickstart.
- GitHub Actions CI now runs `integration-dagster` and `make coverage`
  alongside the existing lint, test, dbt, docs, and costguard gates.
- selected-scope full-keyset discovery now defaults `keyset_volume_min` to
  `POLYMARKET_WC2026_SCOPE_KEYSET_VOLUME_MIN` (10_000) for both dlt and markets sync
  entrypoints.
- dlt Dagster asset name aligned to `polymarket_wc2026_raw_markets` (matches deps and
  dbt sources).
- CLOB odds HTTP retries happen only in the app-level backoff loop (urllib3
  status retries disabled for the CLOB client).
- Settings consumers in `market_scope` predicates/scan and DuckDB connection
  read config lazily so `reload_all_settings_modules()` propagates without
  extra `importlib.reload` per module.
- Orchestration ops facade collapsed through `polymarket_wc2026_ops.py`.
- Orphan `market_tokens` cleanup now runs after metadata/token backfill instead
  of inside the dbt asset, keeping the shared dbt asset read-only against raw
  tables.
- DuckDB market storage internals split into query and mutation modules while
  preserving the `oddsfox_pipeline.storage.duckdb.markets` facade.
- Odds sync now exposes `default_odds_sync_runtime()` as the supported runtime
  factory for tests and injected callables.
- selected-scope keyset scan tag-closure queueing moved into a pure helper with
  the same strict scope gates and telemetry output.
- `int_polymarket_wc2026_token_universe` now materializes as a dbt table after
  profile-backed validation showed neutral-or-better build behavior.

### Fixed

- Circular import between `scope_sql` and `storage.duckdb._market_queries` that
  prevented `dagster dev` from loading definitions.
- Minutely odds sync no longer re-fetches full history for already-closed,
  fully-checked tokens on every run when `force=True`; only explicit rebuild
  (`rebuild_minutely` or `minutely_backfill_days`) reopens them.
- Pool worker exceptions now enqueue skip/state ledger updates instead of
  silently dropping tokens.
- Writer flush wraps odds + ledger upserts in one transaction (dlt stage load
  happens before `BEGIN`).
- Markets sync progress guardrail now calls `.check()` during discovery.
- dbt build raises on non-zero process exit code after stream completion.
- Due-token count queries apply consistent volume/ended-market filters.
- `market_tokens` backfill and sync share one dlt-batch write path.
- Multi-statement DuckDB writes wrapped in transactions (`save_event_slugs_batch`,
  `delete_orphan_market_tokens`, `refresh_token_odds_daily`).
- Latest sync metrics now surface `pipeline_run_event_append_failed` and
  `pipeline_run_event_append_error` when append-only run telemetry cannot land.

### Removed

- WC2026-specific public marts, dbt intermediates, Dagster asset/job names,
  env-var names, and operator scripts. No compatibility views, env aliases, or
  migration shims were added.
- Dead parallel odds planning/fetch module (`process.py`, `build_token_plans`,
  `set_status_hook`).
- Unused snapshot HTTP client/cache surface and degraded snapshot result helper.

## [0.1.1] - 2026-07-01

### Added

- Full WC2026 odds time-series marts: `polymarket_wc2026_token_minutely_odds` and
  `polymarket_wc2026_token_daily_odds` (dbt views).
- `scripts/prune_odds_history.py` and `make prune-odds-history` for raw
  minutely retention (default 365 days).
- MkDocs Material theme; architecture, data-contracts, community, and
  development docs.
- GitHub issue and PR templates; AGENTS.md and Ponytail Cursor rule.
- Shared transient HTTP retry helper (`http_retry.py`).

### Changed

- `wc2026_whale_minutely_odds` is now a filtered view over
  `polymarket_wc2026_token_minutely_odds`.
- dlt is the sole owner of `polymarket_wc2026_raw.markets` rows; snapshot upserts
  populate dlt metadata columns.
- Due-token SQL deduplicated; backfill progress and slug handling aligned with
  shared storage helpers.

### Fixed

- dlt-owned markets snapshot upserts in DuckDB (`idx_markets_id`, metadata
  columns).
- Backfill slug tuple order, empty scheduler snapshot returns, and post-save
  progress accounting.
- Daily `avg_price` float drift breaking OHLC dbt test.

### Removed

- `token_latest_odds` mart (use time-series marts; see
  `docs/reference/data-contracts.md`).
- Redundant `odds_history` indexes (~1.45 GiB legacy index footprint on
  upgrade).
- Dead `wc2026_event_tags` dbt var.

## [0.1.0] - 2026-06-30

### Added

- Local Python pipeline foundation for prediction-market data, initially
  focused on FIFA World Cup 2026 Polymarket markets and odds.
- Dagster orchestration with WC2026 ingest, minutely odds, and dbt refresh jobs.
- dlt landing for Polymarket Gamma markets into DuckDB raw schemas.
- Python odds sync engine with ledgers, retries, and token-level planning.
- dbt staging, intermediate, mart, and observability models for WC2026 scope.
- DuckDB warehouse bootstrap, ops schemas, and profiling utilities.
- MkDocs documentation site with CI `docs-check` validation.
- GitHub Actions CI: lint, tests, docs build, dbt parse, and dbt build.
- Schedules disabled by default; opt-in via `.env` for live ingestion.

[Unreleased]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.13...v0.2.0
[0.1.13]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.12...v0.1.13
[0.1.12]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.11...v0.1.12
[0.1.11]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.10...v0.1.11
[0.1.10]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.9...v0.1.10
[0.1.9]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.8...v0.1.9
[0.1.8]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.7...v0.1.8
[0.1.7]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/hypertrial/oddsfox-pipeline/releases/tag/v0.1.1
[0.1.0]: https://github.com/hypertrial/oddsfox-pipeline/releases/tag/v0.1.0
