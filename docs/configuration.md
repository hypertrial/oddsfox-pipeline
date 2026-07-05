# Configuration

Use `.env.example` as the source of local overrides.
For first-run steps, see [Quickstart](quickstart.md).

Most settings are adapter-specific. In v0.1.x, that means the shipped WC2026
Polymarket pipeline plus the fixed FIFA results CSV used for team validation.

## Warehouse and dbt

- `DUCKDB_NAME`: warehouse filename or path. Default: `oddsfox.duckdb`.
- `DUCKDB_PATH`: optional absolute path override.
- `DBT_PROFILES_DIR`: optional dbt profiles directory override.

Most operators should leave `DBT_PROFILES_DIR` unset and use the packaged `dbt/profiles`.

## API Pacing

- `MARKETS_REQUESTS_PER_SECOND`: Gamma market/event request pace.
- `ODDS_REQUESTS_PER_SECOND`: CLOB odds request pace.
- `HTTP_CONNECT_TIMEOUT_SECONDS`: HTTP connection timeout.
- `HTTP_READ_TIMEOUT_SECONDS`: HTTP read timeout.

Lower request rates when Polymarket APIs return transient failures or timeouts.
The `international_results` CSV refresh uses the shared HTTP timeout settings
and has no source-specific env override.

## WC2026 Scope

| Preset | Focus |
| --- | --- |
| `wc2026` | FIFA World Cup 2026 |

`src/oddsfox_pipeline/ingestion/polymarket/seeds/market_scopes.yml` is the
scope source. The packaged seed contains only `wc2026`, and the shipped
Dagster jobs, assets, env vars, and dbt graph remain fixed to WC2026 in
v0.1.x.

Polymarket scope helper code accepts any slug-like scope that exists in the
seed file, which keeps tests and future adapter work seed-backed instead of
hard-coded. That does not add a runtime scope selector or non-WC2026 marts.

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
window, and freshness windows live in `dbt/seeds/polymarket_wc2026_contract.csv`.
Python defaults are checked against that seed in unit tests.

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

All schedule flags default to `false`.

## CLOB Credentials

- `CLOB_API_KEY`
- `CLOB_API_SECRET`
- `CLOB_API_PASSPHRASE`

Leave unset for local docs, dbt, and mocked tests.
