# Configuration

Use `.env.example` as the source of local overrides.
For first-run steps, see [Quickstart](../getting-started/index.md).

Most settings are adapter-specific. In v0.1.x, that means the shipped WC2026
and US midterms 2026 Polymarket pipelines, the Kalshi WC2026 pipeline, the fixed
FIFA results CSV used for team validation, and the OpenFootball mirror of FIFA
knockout match numbers.

## Warehouse and dbt

- `DUCKDB_NAME`: warehouse filename or path. Default: `oddsfox.duckdb`.
- `DUCKDB_PATH`: optional path override. When set, it takes precedence over
  `DUCKDB_NAME`. Dagster dbt builds sync this env var to the active Python
  warehouse path before invoking `dbt build`, so ingestion and analytics share
  one DuckDB file even when `DUCKDB_PATH` is unset.
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
and has no source-specific env override. The OpenFootball fixture refresh uses
the same timeout settings and a fixed public source URL.

## WC2026 Polygon settlement history

The independent Polygon flow has no schedule and does not reuse Gamma/CLOB
configuration.

- `POLYGON_RPC_URL` (required for live backfill and seed authoring): Polygon
  JSON-RPC endpoint. It must support chain ID 137 and the `finalized` block tag.
  Seed authoring additionally needs archive-capable historical event-block
  calls.
- `POLYGON_RPC_PROVIDER_LABEL` (required with the primary URL): non-secret
  provider/plan label stored in provenance.
- `POLYGON_VERIFY_RPC_URL` (optional): independent Polygon endpoint used only
  during release verification.
- `POLYGON_VERIFY_RPC_PROVIDER_LABEL` (required when the verification URL is
  set): non-secret second-provider label.

Full endpoint values can contain credentials. They are validated before use but
are never logged or persisted; only the label and sanitized HTTPS origin enter
the local audit tables. The default Dagster run config uses five requests per
second, five complete-leaf workers, 8,000-block initial chunks, 20-receipt
initial batches, and four transient retries. Typed one-off run config may
override those four tuning values. Log chunks adapt within 250–20,000 blocks
and receipt batches within 5–50 transactions. Polygon chain ID 137, finalized
head semantics, contract addresses, event layouts, window lengths, and the
`polygon-v2-settlement-v4` normalizer are code-fixed invariants. There are no
tuning env vars for this flow.

All Polygon live-smoke runtime state is rooted below the repository's
`.cache/polygon_settlement/`: the v4 warehouse under `benchmarks/v4/`, Dagster
state, dbt target/logs, temp/XDG/Python caches, DuckDB extensions, and redacted
checkpoint status. Place the repository on the intended SSD before running.

Missing, disagreeing, or failed secondary verification is reported as an
advisory warning and does not block a technically valid audit release. An
invalid or non-finalized primary scan fails closed.

`POLYGON_DATASET_VERSION` selects the immutable audit/export version for the
manual Make targets. `POLYGON_AUDIT_OUTPUT_ROOT` defaults to
`artifacts/polygon_settlement/audit`; the sanitized exporter defaults to
`artifacts/polygon_settlement/exports`. These paths contain local artifacts and
are ignored by Git. The software accepts no publisher, dataset-licence,
legal-review, provider-terms, distribution, or upload configuration.

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

The packaged WC2026 event prefixes include `fifwc-` so exact match events are
discovered deterministically alongside tag discovery. The combined match job
sets the Gamma keyset volume floor to zero and the odds volume filter to null for exact
`soccer_team_to_advance` markets; source-specific progression jobs keep their
normal $5,000 defaults.

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

The packaged Kalshi series include `KXWCADVANCE`. Only the two team-advance
binary markets under a recognized event are eligible for the neutral match
mart.

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
- `WC2026_KNOCKOUT_MATCH_ODDS_HOURLY_SCHEDULE_ENABLED`: enables the atomic
  hourly `wc2026_knockout_match_odds_full_pipeline` schedule.

All schedule flags default to `false`.

`polymarket_wc2026_polygon_settlement_backfill` and
`polymarket_wc2026_polygon_settlement_release` are manual-only jobs. They have
no schedule or enable flag, and the release job writes only a local immutable
internal audit bundle. The sanitized exporter is standalone and unscheduled;
neither path uploads or distributes data.

The neutral `wc2026_*` schemas are a breaking local warehouse layout change.
v0.1.x has no compatibility aliases or migration path; delete
`oddsfox.duckdb*` and rerun quickstart when upgrading an older warehouse.
