# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Static Vercel deployment for the MkDocs documentation site at
  `https://data.oddsfox.io/`.

### Changed

- Refreshed the MkDocs site with the OddsFox website palette, locally hosted
  Inter and JetBrains Mono fonts, a fox favicon, responsive task navigation,
  and a logo-led technical homepage.

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

- `token_latest_odds` mart (use time-series marts; see docs/data-contracts.md).
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

[Unreleased]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.5...HEAD
[0.1.5]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/hypertrial/oddsfox-pipeline/releases/tag/v0.1.1
[0.1.0]: https://github.com/hypertrial/oddsfox-pipeline/releases/tag/v0.1.0
