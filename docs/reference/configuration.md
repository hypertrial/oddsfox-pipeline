# Configuration

Use `.env.example` as the source of local overrides.
For first-run steps, see [Quickstart](../getting-started/index.md).

Most settings are adapter-specific. In v0.1.x, that means the shipped WC2026
Polymarket pipeline, the Kalshi WC2026 pipeline, the fixed FIFA results CSV
used for team validation, and the OpenFootball mirror of FIFA schedule
fixtures.

## Warehouse and dbt

- `DUCKDB_NAME`: warehouse filename or path. Default: `oddsfox.duckdb`.
- `DUCKDB_PATH`: optional path override. When set, it takes precedence over
  `DUCKDB_NAME`. Dagster dbt builds sync this env var to the active Python
  warehouse path before invoking `dbt build`, so ingestion and analytics share
  one DuckDB file even when `DUCKDB_PATH` is unset.
- `DBT_PROFILES_DIR`: optional dbt profiles directory override.

Most operators should leave `DBT_PROFILES_DIR` unset and use the packaged `dbt/profiles`.

## Operator-supplied seed data

Six external/reference seed paths under `dbt/seeds/` are distributed as
header-only schema shells. Populate them locally only with data you are entitled
to use. Do not commit local rows. Restore the shells with
`git restore dbt/seeds` after local work.

The Polygon candidate generator writes below ignored `artifacts/`; after review,
operators may copy its manifest to the existing seed path and supply the
matching ignored
`config/polygon-settlement-resolution-attestation.yml`. The repository includes
only a placeholder attestation example. See `dbt/seeds/README.md` for the
repository seed policy.

dbt parse and the ordinary dbt graph remain valid with empty shells. Models that
depend on these inputs are empty, and Polygon readiness fails closed until the
complete local manifest and attestation are present.

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

## API Pacing

- `MARKETS_REQUESTS_PER_SECOND`: Gamma market/event request pace.
- `ODDS_REQUESTS_PER_SECOND`: CLOB odds request pace.
- `HTTP_CONNECT_TIMEOUT_SECONDS`: HTTP connection timeout.
- `HTTP_READ_TIMEOUT_SECONDS`: HTTP read timeout.

Lower request rates when Polymarket APIs return transient failures or timeouts.
The `international_results` CSV refresh uses the shared HTTP timeout settings
and has no source-specific env override. The OpenFootball fixture refresh uses
the same timeout settings and a fixed public source URL.

## Local storage root

Make targets default all child-process temporary files and caches to
`.cache/runtime/` below the checkout. `ODDSFOX_RUNTIME_ROOT` overrides that
location. `ODDSFOX_STORAGE_ROOT` defaults to the repository and defines the
allowed warehouse boundary for `make local-marts-rebuild`.

When the checkout is on an SSD, export the parent-process `TMPDIR`,
`UV_CACHE_DIR`, `UV_PYTHON_INSTALL_DIR`, `XDG_CACHE_HOME`, and
`PYTHONPYCACHEPREFIX` and `PLAYWRIGHT_BROWSERS_PATH` before the first `uv`
command. The complete setup and both mart workflows are in
[Recreate local marts](../guides/recreate-local-marts.md)
([match-minute](../guides/recreate-match-minute-mart.md),
[Polygon settlement](../guides/recreate-polygon-settlement-mart.md)).

## WC2026 PMXT order-book history

`PMXT_API_KEY` is optional for ordinary scopes and required only when an
unpublished `polymarket_wc2026_match_order_book_backfill` scan needs hosted
PMXT network access. The value is sent solely as an `Authorization: Bearer`
header. It is never logged, persisted, or included in Dagster metadata.

The dedicated run config defaults to 50 requests per minute, a conservative
20,000-attempt UTC-month local ceiling, four bounded transient retries, and an
expiring single-writer lease. Typed one-off Dagster run config may lower those
limits or set `force=true` to create a separate scan. It cannot provide
arbitrary event, market, condition, token, or timestamp values; reviewed
targets live in
`src/oddsfox_pipeline/ingestion/polymarket/seeds/order_book_targets.yml`.
The initial manifest contains only FIFA match 95, Argentina–Egypt.

The pipeline is backfill-only. It has no schedule flag and is excluded from
ordinary dbt and full-pipeline jobs with `tag:pmxt_order_book`. See
[Recreate the PMXT order-book mart](../guides/recreate-match-order-book-mart.md).

## WC2026 Polygon settlement history

The independent Polygon pipeline has no schedule and does not reuse Gamma/CLOB
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
tuning env vars for this pipeline.

All Polygon live-smoke runtime state is rooted below the repository's
`.cache/polygon_settlement/`: the v4 warehouse under `benchmarks/v4/`, Dagster
state, dbt target/logs, temp/XDG/Python caches, DuckDB extensions, and redacted
checkpoint status. Place the repository on the intended SSD before running.

Missing, disagreeing, or failed secondary verification is reported as an
advisory warning and does not block a technically valid audit release. An
invalid or non-finalized primary scan fails closed.

`POLYGON_DATASET_VERSION` selects the immutable audit/export version for the
manual Make targets. `POLYGON_AUDIT_OUTPUT_ROOT` defaults to
`artifacts/polygon_settlement/audit`; the technical exporter defaults to
`artifacts/polygon_settlement/exports`. These paths contain operator-local
artifacts and are ignored by Git. The software accepts no upload
configuration.

## API fidelity and pipeline policy thresholds

Upstream observation buckets are configuration, not ontology terms:

- Hourly CLOB odds jobs use `fidelity=60` (one observation bucket per 60 minutes
  of wall clock inside each request window; the warehouse fact is still hourly).
- Match-minute odds use fixed CLOB `fidelity=1` for exact game windows.
- Kalshi hourly candlesticks align to the same hourly schedule cadence.

Shared volume floors, trailing hourly windows, and freshness windows live in
the `<namespace>_pipeline_policy.csv` seeds (see [Naming](naming.md)):

- `dbt/seeds/polymarket_wc2026_pipeline_policy.csv` — Polymarket WC2026 sticky
  event admission via `event_min_lifetime_volume_usd` (currently `100000`) for
  the golden mart.
- `dbt/seeds/kalshi_wc2026_pipeline_policy.csv`

Python defaults are checked against those seeds in unit tests.

## Polymarket scopes

| Preset | Focus |
| --- | --- |
| `wc2026` | FIFA World Cup 2026 |

`src/oddsfox_pipeline/ingestion/polymarket/seeds/market_scopes.yml` is the
scope source. The packaged seed contains `wc2026`, and the shipped jobs,
assets, and dbt graphs are fixed per scope in v0.1.x.

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
- `POLYMARKET_WC2026_SCOPE_KEYSET_CLOSED`: Gamma `/events/keyset` closed filter.
  Unset defaults to `false` (open events only). `false`/`0`/`open` → `false`;
  `true`/`1`/`closed` → `true`. Empty string or `any`/`all`/`none`/`null`, or
  any other unrecognized value, omits the `closed` parameter entirely (filter
  not applied; scope not narrowed).
- `POLYMARKET_WC2026_SCOPE_KEYSET_VOLUME_MIN`: optional Gamma `/events/keyset`
  volume filter. Unset omits `volume_min`; event admission uses the event catalog
  and sticky `event_min_lifetime_volume_usd` floor (currently $100,000 USD).
- `POLYMARKET_WC2026_SCOPE_KEYSET_RELATED_TAGS`
- `POLYMARKET_WC2026_SCOPE_TAG_DISCOVERY`
- `POLYMARKET_WC2026_SCOPE_TAG_DISCOVERY_KEYWORDS`: comma-separated keyword
  overrides for tag discovery. Unset uses the packaged seed keywords.
- `POLYMARKET_WC2026_SCOPE_TAG_CLOSURE_ROUNDS`
- `POLYMARKET_WC2026_SCOPE_TAG_CRAWL_MAX`
- `POLYMARKET_WC2026_SCOPE_TAG_CLOSURE_KEYWORD_GATE`: when true (default), tag
  closure only keeps crawled tags that match discovery keywords.
- `POLYMARKET_WC2026_SCOPE_TAG_CRAWL_DENYLIST`: comma-separated tag slugs
  excluded from tag crawl expansion.
- `POLYMARKET_WC2026_SCOPE_REGISTRY_MAX_EVENT_PAGES`: optional hard cap on
  registry event-catalog pagination. Unset leaves the default uncapped
  (operator/job config may still bound pages).

The seed file `src/oddsfox_pipeline/ingestion/polymarket/seeds/market_scopes.yml`
is the default scope source. Pipeline-policy thresholds for this namespace are
covered under
[API fidelity and pipeline policy thresholds](#api-fidelity-and-pipeline-policy-thresholds).

The packaged WC2026 event prefixes include `fifwc-` so exact match events are
discovered deterministically alongside tag discovery. The isolated match-minute
job sets the Gamma keyset volume floor to zero and the odds volume filter to
null for exact `soccer_team_to_advance` markets; the golden-mart full pipeline
keeps normal event-volume defaults.

## Kalshi WC2026

- `KALSHI_REQUESTS_PER_SECOND`: Kalshi trade API request pace (default `5`).

Kalshi uses the public HTTPS trade API at `external-api.kalshi.com`. No API key,
secret, or passphrase is required for local docs, dbt, or mocked tests.

`src/oddsfox_pipeline/ingestion/kalshi/seeds/market_scopes.yml` is the scope
source for the fixed `wc2026` Kalshi dbt graph. Shared pipeline policy values such as
the trailing hourly window and freshness windows live in
`dbt/seeds/kalshi_wc2026_pipeline_policy.csv`; Python defaults are checked against
that seed in unit tests.

The packaged Kalshi series include `KXWCADVANCE` for raw ingestion only; no
Kalshi dbt mart currently classifies or publishes match-advance markets.

## dbt build defaults

Scoped dbt jobs (`polymarket_wc2026_dbt_build` and `kalshi_wc2026_dbt_build`)
ship with `full_refresh=False` in their
default Dagster run config. Routine runs therefore use dbt incremental
materializations where defined. Override with `full_refresh=True` in Dagster run
config when a one-off full rebuild is required.

## Market scope page budget

Polymarket market discovery and `ops/market_scope_registry` run configs expose
`max_pages_without_progress`. When the field is omitted or explicitly `null` in
Dagster run config, the orchestration layer passes the scan helper's built-in
default of **25** pages without progress. Set a positive integer in run config
to tighten or relax the guard for a one-off run.

## Event catalog recall and checkpoints

`polymarket_wc2026_raw_event_catalog` (and the shared
`MarketScopeRegistryConfig` used by registry refresh) also exposes:

- `include_slug_prefix_recall` (default `true` on the config class; routine
  `full_pipeline` / registry refresh set `false`): when `false`, skip the
  unfiltered Gamma slug-prefix recall partition. Tag and series scans still
  run.
- `slug_prefix_recall_max_pages_without_progress` (default `500`; `null` =
  exhaustive): early-stop the slug-prefix partition after this many consecutive
  pages with no local prefix matches. Match-minute and the recall-audit job pin
  `null`.
- `reset_event_catalog_checkpoint` (default `false`): clear
  `polymarket_wc2026_ops.event_catalog_scan_checkpoint` before crawling.

Run the exhaustive recall completeness check with:

```bash
uv run make event-catalog-recall-audit
```

## Odds History Run Config

Dagster hourly odds config uses history-oriented option names:

- `force`: when `false` (Polymarket WC2026 hourly/full-pipeline default), routine
  runs plan only due tokens and skip fully-checked closed markets. Set `true` to
  revisit every registry token on that run.
- `rebuild_history`: bypass routine skip planning and rebuild token history.
- `history_backfill_days`: rebuild only the trailing N days of history. The packaged
  `polymarket_wc2026_hourly_odds_ingest` and `polymarket_wc2026_full_pipeline` jobs
  default this to `0` (collect raw history from market creation).
- `window_hours`: maximum CLOB fetch window per request. Hourly/full-pipeline jobs
  default this to `720` (30 days) for CLOB chunk sizing.
- `batch_group_size`: number of token IDs per CLOB `POST /batch-prices-history`
  call (default `20`, max `20`). Hourly odds groups due tokens into shared
  window fetches to cut HTTP round-trips.
- `auto_tune_max_rps`: upper bound for odds RPS auto-tuning. Polymarket hourly /
  full-pipeline jobs default this to `90` (under the documented CLOB
  `/prices-history` ceiling of 100 req/s).

Kalshi hourly odds (`kalshi_wc2026_hourly_odds_ingest`,
`kalshi_wc2026_full_pipeline`) use the same field names with different defaults:

- `force`: when `false` (Kalshi WC2026 hourly/full-pipeline default), routine
  runs select only ledger-due markets. Set `true` to revisit every registry
  market on that run.
- `history_backfill_days`: rebuild only the trailing N days of candlestick
  history. Kalshi hourly/full-pipeline jobs default this to `63` (aligned with
  `KALSHI_WC2026_HOURLY_WINDOW_DAYS` and the Kalshi pipeline-policy seed),
  unlike Polymarket's `0` (collect from market creation).
- `routine_interval_hours`: expected hours between routine runs for skip
  planning and ledger state. Kalshi hourly jobs default this to `1`.

The old minute-grain schedule-oriented names are not accepted in v0.1.x.

## Schedules

- `KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED`: enables the hourly `kalshi_wc2026_hourly_odds_ingest` schedule (hourly `period_interval=60` candlesticks).

The schedule flag defaults to `false`. Polymarket WC2026 has no Dagster schedule;
use `polymarket_wc2026_hourly_odds_ingest` or `polymarket_wc2026_full_pipeline`
manually when needed.

`polymarket_wc2026_polygon_settlement_backfill` and
`polymarket_wc2026_polygon_settlement_release` are manual-only jobs. They have
no schedule or enable flag, and the release job writes only a local immutable
internal audit bundle. The technical exporter is standalone and unscheduled;
neither path uploads data.

`polymarket_wc2026_match_order_book_backfill` is also manual-only and has no
schedule or enable flag. Its only credential is the optional-until-needed
`PMXT_API_KEY`.

The neutral `wc2026_*` schemas are a breaking local warehouse layout change.
v0.1.x has no compatibility aliases or migration path; see
[Terminology](terminology.md).
