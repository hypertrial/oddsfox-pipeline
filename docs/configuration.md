# Configuration

Use `.env.example` as the source of local overrides.
For first-run steps, see [Quickstart](quickstart.md).

Most settings are adapter-specific. In v0.1.x, that means the WC2026-only
Polymarket pipeline.

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

## WC2026 Scope

| Preset | Focus |
| --- | --- |
| `wc2026` | FIFA World Cup 2026 |

`src/oddsfox_pipeline/ingestion/polymarket/seeds/market_scopes.yml` contains
only `wc2026`. Non-WC2026 presets and multi-scope dbt vars are not supported in
v0.1.x.

### WC2026 field overrides (advanced)

These override the fixed WC2026 seed when set.

- `POLYMARKET_WC2026_SCOPE_EVENT_SLUGS`
- `POLYMARKET_WC2026_SCOPE_EVENT_SLUG_PREFIXES`
- `POLYMARKET_WC2026_SCOPE_EVENT_TAGS`
- `POLYMARKET_WC2026_SCOPE_KEYSET_CLOSED`
- `POLYMARKET_WC2026_SCOPE_KEYSET_VOLUME_MIN`: minimum Gamma keyset volume filter (default
  `10000`); shared by dlt and markets sync entrypoints.
- `POLYMARKET_WC2026_SCOPE_KEYSET_RELATED_TAGS`
- `POLYMARKET_WC2026_SCOPE_TAG_DISCOVERY`
- `POLYMARKET_WC2026_SCOPE_TAG_CLOSURE_ROUNDS`
- `POLYMARKET_WC2026_SCOPE_TAG_CRAWL_MAX`

The seed file `src/oddsfox_pipeline/ingestion/polymarket/seeds/market_scopes.yml` is the default scope source.

## Schedules

- `POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED`: enables the hourly `polymarket_wc2026_hourly_odds_ingest` schedule (`fidelity=60`).

All schedule flags default to `false`.

## CLOB Credentials

- `CLOB_API_KEY`
- `CLOB_API_SECRET`
- `CLOB_API_PASSPHRASE`

Leave unset for local docs, dbt, and mocked tests.
