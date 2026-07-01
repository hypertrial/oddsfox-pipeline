# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Generic dbt test macros for grain uniqueness, price bounds, and mart/int
  reconciliation (replacing 14 duplicated singular tests).
- Regression test ensuring dbt source `meta.dagster.asset_key` values match
  Dagster Definitions asset keys.
- Schedule mutual-exclusion guard when both minutely odds schedule env flags
  are enabled.

### Changed

- WC2026 full-keyset discovery now defaults `keyset_volume_min` to
  `POLYMARKET_WC2026_KEYSET_VOLUME_MIN` (10_000) for both dlt and markets sync
  entrypoints.
- dlt Dagster asset name aligned to `dlt_polymarket_markets` (matches deps and
  dbt sources).
- CLOB odds HTTP retries happen only in the app-level backoff loop (urllib3
  status retries disabled for the CLOB client).
- Settings consumers in `wc2026_scope` predicates/scan and DuckDB connection
  read config lazily so `reload_all_settings_modules()` propagates without
  extra `importlib.reload` per module.
- Orchestration ops facade collapsed through `polymarket_ops.py`.

### Fixed

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

### Removed

- Dead parallel odds planning/fetch module (`process.py`, `build_token_plans`,
  `set_status_hook`).

## [0.1.1] - 2026-07-01

### Added

- Full WC2026 odds time-series marts: `wc2026_token_minutely_odds` and
  `wc2026_token_daily_odds` (dbt views).
- `scripts/prune_odds_history.py` and `make prune-odds-history` for raw
  minutely retention (default 365 days).
- MkDocs Material theme; architecture, data-contracts, community, and
  development docs.
- GitHub issue and PR templates; AGENTS.md and Ponytail Cursor rule.
- Shared transient HTTP retry helper (`http_retry.py`).

### Changed

- `wc2026_whale_minutely_odds` is now a filtered view over
  `wc2026_token_minutely_odds`.
- dlt is the sole owner of `polymarket_raw.markets` rows; snapshot upserts
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

[0.1.1]: https://github.com/hypertrial/oddsfox/releases/tag/v0.1.1
[0.1.0]: https://github.com/hypertrial/oddsfox/releases/tag/v0.1.0
