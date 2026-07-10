# Configuration

Use `.env.example` as the source of local overrides.
For first-run steps, see [Quickstart](quickstart.md).

Most settings are adapter-specific. In v0.1.x, that means the shipped WC2026
and US midterms 2026 Polymarket pipelines, the Kalshi WC2026 pipeline, plus the
fixed FIFA results CSV used for WC2026 team validation.

## Warehouse and dbt

- `DUCKDB_NAME`: warehouse filename or path. Default: `oddsfox.duckdb`.
- `DUCKDB_PATH`: optional path override. When set, it takes precedence over
  `DUCKDB_NAME`; hosted graph deployments set this to the SSD-backed warehouse
  path. Dagster dbt builds also sync this env var to the active Python warehouse
  path before invoking `dbt build`, so ingestion and analytics share one DuckDB
  file even when `DUCKDB_PATH` is unset.
- `DBT_PROFILES_DIR`: optional dbt profiles directory override.

Most operators should leave `DBT_PROFILES_DIR` unset and use the packaged `dbt/profiles`.

## Local development

- `DUCKDB_PATH` takes precedence over `DUCKDB_NAME`. If `.env` points
  `DUCKDB_PATH` at your real warehouse (`oddsfox.duckdb`), unit tests can write
  to that file unless they isolate the path.
- For local dev, either leave `DUCKDB_PATH` unset (use `DUCKDB_NAME` only) or
  use a disposable warehouse filename while iterating.
- `reload_all_settings_modules()` re-loads `.env` during tests. Storage tests
  use `isolate_duckdb_test_env()` in
  `tests/unit/storage/duckdb_storage_test_support.py` to clear `DUCKDB_PATH`
  before and after reload.
- US midterms hourly jobs default to the same trailing 30-day window and volume
  floor as WC2026, aligned with `dbt/seeds/polymarket_us_midterms_2026_contract.csv`.

## API Pacing

- `MARKETS_REQUESTS_PER_SECOND`: Gamma market/event request pace.
- `ODDS_REQUESTS_PER_SECOND`: CLOB odds request pace.
- `HTTP_CONNECT_TIMEOUT_SECONDS`: HTTP connection timeout.
- `HTTP_READ_TIMEOUT_SECONDS`: HTTP read timeout.

Lower request rates when Polymarket APIs return transient failures or timeouts.
The `international_results` CSV refresh uses the shared HTTP timeout settings
and has no source-specific env override.

## Polymarket scopes

| Preset | Focus |
| --- | --- |
| `wc2026` | FIFA World Cup 2026 |
| `us_midterms_2026` | Balance of Power, Senate control, and House control 2026 midterms |

`src/oddsfox_pipeline/ingestion/polymarket/seeds/market_scopes.yml` is the
scope source. The packaged seed contains `wc2026` and `us_midterms_2026`, and
the shipped Dagster jobs, assets, and dbt graphs are fixed per scope in v0.1.x.

Polymarket scope helper code accepts any slug-like scope that exists in the
seed file, which keeps tests and future adapter work seed-backed instead of
hard-coded. That does not add a runtime scope selector.

### WC2026 field overrides (advanced)

These override the packaged WC2026 seed when set. They apply only to `wc2026`;
additional seed-backed helper scopes do not read `POLYMARKET_WC2026_*`
overrides.

- `POLYMARKET_WC2026_SCOPE_EVENT_SLUGS`
- `POLYMARKET_WC2026_SCOPE_EVENT_SLUG_PREFIXES`
- `POLYMARKET_WC2026_SCOPE_EVENT_TAGS`
- `POLYMARKET_WC2026_SCOPE_KEYSET_CLOSED`
- `POLYMARKET_WC2026_SCOPE_KEYSET_VOLUME_MIN`: minimum Gamma keyset volume filter (default
  `5000`, aligned with the WC2026 knockout universe floor); shared by dlt and markets sync entrypoints.
- `POLYMARKET_WC2026_SCOPE_KEYSET_RELATED_TAGS`
- `POLYMARKET_WC2026_SCOPE_TAG_DISCOVERY`
- `POLYMARKET_WC2026_SCOPE_TAG_CLOSURE_ROUNDS`
- `POLYMARKET_WC2026_SCOPE_TAG_CRAWL_MAX`

The seed file `src/oddsfox_pipeline/ingestion/polymarket/seeds/market_scopes.yml`
is the default scope source.

Shared dbt contract values such as the knockout volume floor, trailing hourly
window, and freshness windows live in `dbt/seeds/polymarket_wc2026_contract.csv`
and `dbt/seeds/polymarket_us_midterms_2026_contract.csv`.
Python defaults are checked against those seeds in unit tests.

## Kalshi WC2026

- `KALSHI_REQUESTS_PER_SECOND`: Kalshi trade API request pace (default `5`).
- `KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED`: enables the hourly
  `kalshi_wc2026_hourly_odds_ingest` schedule (`fidelity=60`).

Kalshi uses the public HTTPS trade API at `external-api.kalshi.com`. No API key,
secret, or passphrase is required for local docs, dbt, or mocked tests.

`src/oddsfox_pipeline/ingestion/kalshi/seeds/market_scopes.yml` is the scope
source for the fixed `wc2026` Kalshi graph. Shared contract values such as the
trailing hourly window and freshness windows live in
`dbt/seeds/kalshi_wc2026_contract.csv`; Python defaults are checked against that
seed in unit tests.

## Odds History Run Config

Dagster hourly odds config uses history-oriented option names:

- `rebuild_history`: bypass routine skip planning and rebuild token history.
- `history_backfill_days`: rebuild only the trailing N days of history. The packaged
  `polymarket_wc2026_hourly_odds_ingest` and `polymarket_wc2026_full_pipeline` jobs
  default this to `30`.
- `window_hours`: maximum CLOB fetch window per request. Hourly/full-pipeline jobs
  default this to `720` (30 days), aligned with the default backfill window.

The old minutely-oriented names are not accepted in v0.1.x.

## Schedules

- `POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED`: enables the hourly `polymarket_wc2026_hourly_odds_ingest` schedule (`fidelity=60`).
- `POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SCHEDULE_ENABLED`: enables the hourly `polymarket_us_midterms_2026_hourly_odds_ingest` schedule (`fidelity=60`).
- `KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED`: enables the hourly `kalshi_wc2026_hourly_odds_ingest` schedule (`fidelity=60`).

All schedule flags default to `false`.
