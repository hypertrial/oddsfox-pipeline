# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
