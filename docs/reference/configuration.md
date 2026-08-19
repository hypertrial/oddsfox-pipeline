# Configuration

Use `.env.example` as the source of local overrides.
For first-run steps, see [Quickstart](../getting-started/index.md).

Most settings are adapter-specific. In v0.2.x, that means the shipped WC2026
Polymarket pipeline, the Kalshi WC2026 pipeline, Polygon settlement, and the
source-neutral transport of an immutable Scraper reference bundle.

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

## Scraper reference bundles

`REFERENCE_BUNDLE_DIR` selects a local immutable `oddsfox.reference.v1`
directory for `make reference-bundle-load`. The loader validates producer
provenance, the exact table inventory, primary grains, schema version, manifest,
and every checksum before replacing the active `oddsfox_reference` schema in a
transaction. An HTTPS bundle URL requires an explicit
`ODDSFOX_REFERENCE_ARTIFACT_HOSTS` allowlist.

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

## Local storage root

Make targets default all child-process temporary files and caches to
`.cache/runtime/` below the checkout. `ODDSFOX_RUNTIME_ROOT` overrides that
location. `ODDSFOX_STORAGE_ROOT` defaults to the repository and defines the
allowed warehouse boundary for `make local-marts-rebuild`.

`make dagster-dev` always uses a private, UID-scoped `/tmp/oddsfox-dg-<uid>/`
directory for its ephemeral gRPC socket files, keeping below the macOS
Unix-domain socket path limit even when `ODDSFOX_RUNTIME_ROOT` is deeply nested.
Warehouses, snapshots, dbt output, and caches remain under the configured SSD
runtime paths.

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

The separate WC2026 stage-execution study defaults to the public PMXT v2 archive
plus one credentialed historical seed request per target token-hour.
`make stage-execution-plan` is network-free and reports archive hours, token
hours, storage bounds, and the minimum seed request count;
`make stage-execution-release` refuses to start when that count exceeds
`STAGE_EXECUTION_REQUEST_BUDGET` (20,000 by default). Every seed attempt reserves
from the shared UTC-month PMXT ledger at `STAGE_EXECUTION_CREDIT_LEDGER` (the
warehouse by default), so separate checkpoints cannot each consume the full
allowance. `STAGE_EXECUTION_SOURCE=api-range` selects the legacy range-query
mode explicitly. Checkpoints and immutable outputs remain under ignored
operator-local artifact paths.

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
assets, and dbt graphs are fixed per scope in v0.2.x.

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

- `include_slug_prefix_recall` (default `false` on the config class): when
  `false`, skip the unfiltered Gamma slug-prefix recall partition. Tag and
  series scans still run. Only
  `polymarket_wc2026_event_catalog_recall_audit` enables this (`true` with
  unlimited pages).
- `slug_prefix_recall_max_pages_without_progress` (default `500`; `null` =
  exhaustive): early-stop the slug-prefix partition after this many consecutive
  pages with no local prefix matches. The recall-audit job pins `null`.
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

The old minute-grain schedule-oriented names are not accepted in v0.2.x.

## Schedules

- `KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED`: enables the hourly `kalshi_wc2026_hourly_odds_ingest` schedule (hourly `period_interval=60` candlesticks).

The schedule flag defaults to `false`. Polymarket WC2026 has no Dagster schedule;
use `polymarket_wc2026_hourly_odds_ingest` or `polymarket_wc2026_full_pipeline`
manually when needed.

## Unified minute odds

- `POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG`: when `true` (default),
  `polymarket_wc2026_minute_odds_backfill` refreshes markets, event catalog, and
  registry before minute fetch. Set `false` to reuse an already-landed warehouse
  catalog on odds/dbt reruns. Restart `uv run make dagster-dev` after changing.
- `POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH`: when `true` (default), runs
  match-minute raw using the already active Scraper reference bundle. Set
  `false` to reuse warehouse match-minute rows. This setting never refreshes
  non-market sources.
- `POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES`: when `true` (default), runs
  futures-minute raw. Set `false` to reuse warehouse futures-minute rows.
  Both match and futures may be `false` for a dbt-only unified minute rebuild.
- `ODDSFOX_MINUTE_PUBLISH_MEMORY_LIMIT`: DuckDB `memory_limit` during minute-odds
  snapshot publish / view registration (default `12GB`). Use values DuckDB
  accepts (`8GB`, `50%`). Keeps large publishes spilling under
  `${ODDSFOX_RUNTIME_ROOT}/duckdb-temp` instead of host SIGKILL.
- `ODDSFOX_MINUTE_PUBLISH_THREADS`: optional DuckDB `threads` override for the
  same publish connection.
- `ODDSFOX_DBT_MEMORY_LIMIT`: DuckDB `memory_limit` for dbt builds (default
  `20GB` via `dbt/profiles/profiles.yml`). Spill directory is
  `${ODDSFOX_RUNTIME_ROOT}/duckdb-temp`.
- `ODDSFOX_DBT_FORCE_PREPARE`: when `true`/`1`/`yes`/`on`, `dagster-dev`
  re-runs `dbt deps` + `dbt parse` on every code-location load even if
  `manifest.json` under `DBT_TARGET_PATH` is still newer than dbt inputs.
  Default is off so warm restarts skip prepare.
- Minute-odds immutable snapshots live under
  `${ODDSFOX_RUNTIME_ROOT}/minute-odds-snapshots/<scope>/<match|futures>/` with an
  atomic `CURRENT` pointer; temporary fetch spill remains under
  `minute-odds-publish/<fetch_run_id>/`.

The soccer daily schedule is controlled by
`POLYMARKET_SOCCER_DAILY_SCHEDULE_ENABLED` and is `false` by default. When
enabled it runs `polymarket_soccer_full_pipeline` at 04:00 UTC. Soccer catch-up
uses `completion_grace_minutes=60` and `empty_retry_hours=72` by default; these
are Dagster run-config fields. `force=true` retries terminal-empty exact
windows. When an empty window reaches its configured retry deadline, the
pipeline records that exact token window in
`polymarket_soccer_ops.match_minute_odds_terminal_unavailable`; observability
therefore preserves the run-specific deadline instead of assuming 72 hours.
Set `retry_empty_only=true` for a bounded recovery run that retries only current
exact-window tokens whose latest attempt was empty and which have no reusable
published success. It cannot be combined with `force=true`; expired histories
that remain empty are terminalized before that recovery run completes.

Soccer production monitoring uses local `POLYMARKET_SOCCER_MONITOR_*` settings.
Defaults are: successful full-run freshness `30` hours, stale-running age `6`
hours, retry warning/critical ages `24`/`72` hours, event or market count drop
`0.10`, completion grace `60` minutes, mapping coverage drop `5.0` percentage
points, duration/CPU/RSS
regression ratio `2.0`, free-disk warning/critical floors `10`/`2` GiB, and
completed-history retention `400` days. The critical disk floor blocks
preflight; warning thresholds do not block valid publication.

Soccer minute fetches retain only bounded in-flight work and Parquet buffers;
the current catalog projections and private incremental mart state live in the
configured DuckDB warehouse. For this v0.2 change, use new versioned
`DUCKDB_PATH` and `ODDSFOX_RUNTIME_ROOT` paths on the same external SSD. Run the
catalog, live smoke, backfill, dbt build, and health check before switching the
Dagster environment. The repository intentionally provides no destructive reset
target or in-place state bootstrap.

!!! warning "v0.2 local layout reset"

    The scope segment in the snapshot path is a breaking local-state layout
    change. Delete old `${ODDSFOX_RUNTIME_ROOT}/minute-odds-snapshots/` state
    and rebuild it; no dual-path compatibility reader is provided.

Smoke-only knobs consumed by `polymarket_wc2026_minute_odds_live_smoke` /
`make minute-odds-live-smoke` (production backfill ignores them):

- `POLYMARKET_WC2026_MINUTE_ODDS_SMOKE_FRACTION`: per-leg market sample fraction
  (default `0.05`). Match and futures are sampled independently after full
  inventory validation.
- `POLYMARKET_WC2026_MINUTE_ODDS_SMOKE_SEED`: deterministic hash-rank seed
  (default `wc2026-minute-smoke-v1`).
- `POLYMARKET_WC2026_MINUTE_ODDS_SMOKE_FUTURES_WINDOW_HOURS`: cap each sampled
  futures plan to its final N hours (default `24`). Requires sampling.
- `MINUTE_ODDS_LIVE_SMOKE_RESET`: Make-only; default `true` deletes the disposable
  `.cache/minute_odds_live_smoke.duckdb` and the disposable smoke runtime root
  `.cache/runtime/smoke/minute-odds-live` (which holds Parquet snapshots) before
  the cold run.
- `MINUTE_ODDS_LIVE_SMOKE_REFRESH_CATALOG`: Make-only; default `true` so cold
  smoke refreshes markets/catalog/registry even when operator `.env` has
  `POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG=false`. Set `false` with
  `MINUTE_ODDS_LIVE_SMOKE_RESET=false` for warm odds/dbt reruns that reuse the
  disposable catalog. The Make target always forces match and futures refresh
  and always sets `ODDSFOX_RUNTIME_ROOT` to the smoke runtime root so sampled
  publishes cannot touch the operator snapshot tree.

`make match-minute-live-smoke` likewise uses a disposable DuckDB path and sets
`ODDSFOX_RUNTIME_ROOT` to `.cache/runtime/smoke/match-minute-live` so full
match-minute publishes cannot GC operator minute-odds snapshots.

`polymarket_wc2026_polygon_settlement_backfill` and
`polymarket_wc2026_polygon_settlement_release` are manual-only jobs. They have
no schedule or enable flag, and the release job writes only a local immutable
internal audit bundle. The technical exporter is standalone and unscheduled;
neither path uploads data.

`polymarket_wc2026_match_order_book_backfill` is also manual-only and has no
schedule or enable flag. Its only credential is the optional-until-needed
`PMXT_API_KEY`.

The neutral `wc2026_*` schemas are a breaking local warehouse layout change.
v0.2.x has no compatibility aliases or migration path; see
[Terminology](terminology.md).

## Global Polymarket graph catalog

The global graph catalog is configured through Dagster run config or the Make
targets documented in the
[catalog runbook](../guides/polymarket-graph-catalog.md). Production crawls have
no page cap. A bounded `max_pages` is accepted only for tests and explicit
diagnostics; such a truncated crawl is not activated or publishable.

`DUCKDB_NAME` selects the operator-local warehouse,
`POLYMARKET_CATALOG_RELEASE_ROOT` selects the ignored immutable release root,
and `RELEASE_VERSION` is required for publication. There is no schedule-enable
setting because acquisition and release are intentionally manual.
