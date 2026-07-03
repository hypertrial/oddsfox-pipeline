# Configuration

Use `.env.example` as the source of local overrides.
For first-run steps, see [Quickstart](quickstart.md).

Most settings are adapter-specific. In v0.1.x, that means Polymarket and selected
market-scope controls.

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

## Current Market Scopes

- `POLYMARKET_MARKET_SCOPES`: comma-separated preset names from
  `src/oddsfox/ingestion/polymarket/seeds/market_scopes.yml` (default `wc2026`).
  At least one scope is required. Examples: `wc2026`, `wc2026,us-politics`,
  `nba,nfl,champions-league`.
- Dagster-run `polymarket_dbt` passes the same scope list to dbt as
  `active_market_scopes`; standalone `dbt build` uses the default in
  `dbt/dbt_project.yml` unless you pass `--vars`.

### Available presets

| Preset | Focus |
| --- | --- |
| `wc2026` | FIFA World Cup 2026 (default) |
| `us-politics` | US elections and domestic politics |
| `geopolitics` | International relations and conflict |
| `crypto` | Bitcoin, Ethereum, and crypto markets |
| `economy` | Macro and Fed rates |
| `nba` | NBA basketball |
| `nfl` | NFL football |
| `champions-league` | UEFA Champions League and Premier League |

### Per-scope field overrides (advanced)

These apply to every selected scope when set; prefer seed YAML presets instead.

- `POLYMARKET_SCOPE_EVENT_SLUGS`
- `POLYMARKET_SCOPE_EVENT_SLUG_PREFIXES`
- `POLYMARKET_SCOPE_EVENT_TAGS`
- `POLYMARKET_SCOPE_KEYSET_CLOSED`
- `POLYMARKET_SCOPE_KEYSET_VOLUME_MIN`: minimum Gamma keyset volume filter (default
  `10000`); shared by dlt and markets sync entrypoints.
- `POLYMARKET_SCOPE_KEYSET_RELATED_TAGS`
- `POLYMARKET_SCOPE_TAG_DISCOVERY`
- `POLYMARKET_SCOPE_TAG_CLOSURE_ROUNDS`
- `POLYMARKET_SCOPE_TAG_CRAWL_MAX`

The seed file `src/oddsfox/ingestion/polymarket/seeds/market_scopes.yml` is the default scope source.

## Schedules

- `POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED`: enables the five-minute schedule and hourly cold trigger for `polymarket_minutely_odds_ingest`.
- `POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED`: enables the one-minute live schedule for `polymarket_minutely_odds_ingest`.
- `POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED`: enables the hourly `polymarket_hourly_odds_ingest` schedule (`fidelity=60`).

All schedule flags default to `false`.

## CLOB Credentials

- `CLOB_API_KEY`
- `CLOB_API_SECRET`
- `CLOB_API_PASSPHRASE`

Leave unset for local docs, dbt, and mocked tests.
