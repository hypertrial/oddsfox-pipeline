# Orchestration reference

This reference lists the fixed Dagster assets, jobs, scope behavior, schedules,
and persistence boundaries shipped by OddsFox Pipeline `v0.2.x`.

For procedures, use [Run a scope](../guides/run-a-scope.md),
[Enable schedules](../guides/enable-schedules.md), and
[Validate and recover](../guides/validate-and-recover.md).

Canonical vocabulary lives in [Terminology](terminology.md).

## Pipeline registry

Entry-point jobs are pipelines; narrower jobs run one step of a pipeline. See
[Terminology](terminology.md#execution) for the distinction.

**Maturity tiers:** **Production** — scheduled-capable, included in automatic
`ci-fast` (Python/docs checks + `dbt-lint`), primary quickstart path. Model
builds and inventory proofs live in `dbt-build-ci` / isolated `dbt-*-ci` lanes
and `release-gate`, not under `ci-fast`. **Mature, isolated** — own CI lane and
documented data-boundary isolation, not immaturity. **Experimental** — opt-in
backfill, paid or narrow credentials, single-target manifests.

| Pipeline | Entry job(s) | Steps | Schedule | CI dbt gate | Maturity |
| --- | --- | --- | --- | --- | --- |
| Polymarket WC2026 | `polymarket_wc2026_full_pipeline` | `market_scope_registry`, `odds`, `dbt` | None | `ci-fast` → `dbt-lint`; model build in `dbt-build-ci` (excludes `tag:polygon_settlement` / `tag:pmxt_order_book` / `tag:minute_odds`) | Production |
| Kalshi WC2026 | `kalshi_wc2026_full_pipeline` | `market_scope_registry`, `odds`, `dbt` | Hourly odds (stopped) | `ci-fast` → `dbt-lint`; `+tag:kalshi` builds in `dbt-build-ci` / `release-gate` | Production |
| Polygon settlement history | `polymarket_wc2026_polygon_settlement_backfill` → `_release` → standalone exporter | Backfill scan, audit release, offline export | None | `dbt-polygon-settlement-ci` (excluded from ordinary `dbt-build-ci`) | Mature, isolated |
| Match-minute odds | `polymarket_wc2026_match_minute_odds_backfill` | Results refresh, minute fetch, dbt | None | `dbt-match-minute-ci` (also compiles in ordinary `dbt-build-ci`; inventory proofs are the isolated lane) | Mature, isolated |
| Minute odds (unified) | `polymarket_wc2026_minute_odds_backfill` | Match-minute + futures-minute fetch, unified dbt | None | `dbt-minute-odds-ci` (excluded from ordinary `dbt-build-ci` via `tag:minute_odds`) | Mature, isolated |
| Match order book | `polymarket_wc2026_match_order_book_backfill` | PMXT order-book scan, dbt | None | `dbt-match-order-book-ci` (excluded from ordinary `dbt-build-ci`) | Mature, isolated |
| Market portrait | `polymarket_wc2026_market_portrait_backfill` | Order book + trades scan, portrait bundle build | None | `dbt-market-portrait-ci` (`tag:market_portrait` trade marts still compile in ordinary `dbt-build-ci`; order-book dual-tagged models follow `tag:pmxt_order_book` exclusion) | Mature, isolated |

Supporting ingestion jobs (`international_results_historical_ingest`,
`international_results_wc2026_match_results_ingest`) feed Kalshi and match-minute
WC2026 pipelines but are not separate product pipelines.

## Pipeline outputs

Marts are defined once in [Data contracts](data-contracts.md#documented-marts);
this list maps each pipeline to what it builds.

- **Polymarket WC2026** (`polymarket_wc2026_full_pipeline`):
  `polymarket_wc2026_market_hourly_odds`. Dagster `dbt` and `full` steps build
  only the golden mart closure (`+polymarket_wc2026_market_hourly_odds`); use
  dedicated backfill jobs for match-minute, order-book, portrait, and Polygon
  settlement marts.
- **Kalshi WC2026** (`kalshi_wc2026_full_pipeline`): `kalshi_wc2026_stage_markets`,
  `kalshi_wc2026_stage_market_hourly_odds`, `kalshi_wc2026_group_winner_markets`,
  `kalshi_wc2026_group_winner_market_hourly_odds`. Rebuilds the same shared
  `international_results_wc2026_*` marts.
- **Polygon settlement history** (`polymarket_wc2026_polygon_settlement_backfill`):
  `polymarket_wc2026_polygon_settlement_minute_odds`. The `_release` job and
  standalone exporter read this mart to write audit/export artifact bundles;
  they build no additional mart.
- **Match-minute odds** (`polymarket_wc2026_match_minute_odds_backfill`):
  `polymarket_wc2026_match_minute_odds`. Rebuilds the shared
  `international_results_wc2026_matches` mart as an input.
- **Minute odds (unified)** (`polymarket_wc2026_minute_odds_backfill`):
  `polymarket_wc2026_market_minute_odds`. Reuses the match-minute raw path for
  game markets and adds a futures-minute raw path for all other
  registry-eligible WC2026 markets over the tournament span.
- **Match order book** (`polymarket_wc2026_match_order_book_backfill`):
  `polymarket_wc2026_match_order_book`.
- **Market portrait** (`polymarket_wc2026_market_portrait_backfill`): no
  documented mart. Builds `polymarket_wc2026_match_order_book` (shared with the
  match order book pipeline), `polymarket_wc2026_match_order_book_states`, and
  `polymarket_wc2026_match_trades` as bundle inputs to
  `oddsfox.market-portrait.v1`. See [Market portrait](market-portrait.md).

## Asset order

1. `polymarket/wc2026/raw/markets`
2. `polymarket/wc2026/raw/markets_snapshot`
3. `polymarket/wc2026/raw/event_catalog`
4. `polymarket/wc2026/raw/event_snapshots`
5. `polymarket/wc2026/raw/event_market_memberships`
6. `polymarket/wc2026/ops/market_scope_registry`
7. `polymarket/wc2026/raw/market_metadata_enrichment`
8. `polymarket/wc2026/raw/token_odds_history_hourly`
9. `polymarket/wc2026/raw/match_token_odds_history_minute` (dedicated backfill only)
10. `polymarket/wc2026/raw/futures_token_odds_history_minute` (dedicated unified minute backfill only)
11. `polymarket/wc2026/raw/match_order_book_snapshots` (dedicated PMXT backfill only)
12. `polymarket/wc2026/raw/polygon_settlement_fills` (dedicated finalized backfill only)
13. `international_results/historical/raw/snapshot`
14. `international_results/wc2026/raw/match_results`
15. `openfootball/wc2026/raw/schedule_fixtures`
16. `kalshi/wc2026/raw/events` (landed with the markets dlt source)
17. `kalshi/wc2026/raw/markets`
18. `kalshi/wc2026/raw/markets_snapshot`
19. `kalshi/wc2026/ops/market_scope_registry`
20. `kalshi/wc2026/raw/market_candlesticks_hourly`
21. dbt model assets under the matching
    `{staging,intermediate,marts,observability}` namespaces.
22. `polymarket/wc2026/release/polygon_settlement_odds_bundle` (internal audit release only)

Flat Dagster op names preserve the same source-first order, for example
`polymarket_wc2026_raw_token_odds_history_hourly`.

## Jobs

Entry-point jobs are pipelines; narrower jobs run one step. See
[Pipeline registry](#pipeline-registry) and [Terminology](terminology.md#execution).

### Polymarket WC2026

**Entry point**

- `polymarket_wc2026_full_pipeline`: registry, odds, and golden-mart dbt.

**Steps**

- `polymarket_wc2026_market_scope_registry_refresh`: market discovery, event
  catalog (with OpenFootball fixtures), market scope registry refresh, and
  metadata enrichment. Routine event-catalog runs skip the exhaustive
  slug-prefix recall scan (`include_slug_prefix_recall=false`).
- `polymarket_wc2026_event_catalog_recall_audit`: same registry selection as
  `market_scope_registry_refresh`, but forces exhaustive slug-prefix recall
  (`include_slug_prefix_recall=true`,
  `slug_prefix_recall_max_pages_without_progress=null`). Unscheduled;
  run via `uv run make event-catalog-recall-audit` for a rare completeness
  check.
- `polymarket_wc2026_hourly_odds_ingest`: trailing hourly token-odds refresh.
- `polymarket_wc2026_dbt_build`: golden-mart dbt build
  (`+polymarket_wc2026_market_hourly_odds`). Default run config uses
  incremental dbt (`full_refresh=False`); set `full_refresh=True` in Dagster run
  config for a one-off full rebuild.

**Isolated: Polygon settlement**

- `polymarket_wc2026_polygon_settlement_backfill`: validates the committed
  248-proposition seed, resolves each unique window once, and scans only the
  authored exchange for each range through the finalized head. Five bounded
  workers execute complete discovery/receipt/header/normalization leaves while
  DuckDB commits remain on the main thread. The job resumes successful chunks,
  atomically replaces the wallet- and order-payload-redacted snapshot only after
  exchange-specific gap-free coverage, and builds only the dedicated
  `polygon_settlement` dbt ancestors. A valid published v4 scan returns offline
  before credentials or RPC construction. It makes no Gamma, CLOB,
  international-results, or runtime OpenFootball request.
- `polymarket_wc2026_polygon_settlement_release`: requires an already valid
  39,120-row mart, optionally compares scoped hashes through a second RPC, and
  writes one immutable internal audit bundle below
  `artifacts/polygon_settlement/audit/releases/`. It never refreshes the primary
  scan. The standalone technical exporter is not a Dagster asset or job; it
  reads a completed audit bundle offline and writes below
  `artifacts/polygon_settlement/exports/releases/`.

**Isolated: Match-minute odds**

- `polymarket_wc2026_match_minute_odds_backfill`: one-time or rerunnable
  completed-match backfill for all 104 FIFA-numbered games and the dedicated
  minute mart. It refreshes the latest 104 international-results rows and the
  OpenFootball schedule fixtures (knockout subset 73–104), discovers closed
  Gamma events without a volume floor, validates result alignment and the
  104/248/496 inventory, fetches exact game windows at CLOB `fidelity=1`, then
  runs dbt. The results refresh first resolves and downloads an immutable Git
  revision.   Minute fetches append 496 audit rows; only an all-success run
  atomically replaces raw history and marks those audits published.
  Fetch throughput matches the hourly odds path: CLOB batch POST (≤20
  tokens), 24h preemptive window chunks, workers/RPS 40 with auto-tune to 90,
  and Arrow staging for the publish replace (still no hourly ledger; each run
  is a full bounded refetch).
  Run `uv run make match-minute-live-smoke` for the disposable live acceptance
  check; it is intentionally absent from CI and all schedules.

**Isolated: Minute odds (unified)**

- `polymarket_wc2026_minute_odds_backfill`: one-time or rerunnable unified
  minute-grain backfill. It runs the match-minute raw path for game markets and
  a separate futures-minute raw path for every other registry-eligible WC2026
  market (tournament span `[2026-06-11, 2026-07-19]`, capped by each market's
  close/resolution time). Neither path shares the hourly `token_sync_ledger`.
  Match-minute still fail-closes unless every in-game token succeeds; futures
  minute audits empty in-window CLOB history and publishes success tokens only
  (hard `error`/`cancelled`, or an all-empty run, still fail). Futures publish
  logs audit write, Arrow stage load, and raw snapshot replace/commit so large
  DuckDB publishes are visible in Dagster. Both legs borrow
  DuckDB only for plan selection and publish (warehouse lock released during
  CLOB fetch) and share the same batch/auto-tune/window-chunk stack as hourly
  odds (via `odds/minute_batch.py`); publish builds the Arrow stage table in one
  columnar pass from fetch results, broadcasting per-token scalars with Arrow
  `take` / `repeat` (no per-row dict or Python list materialization for broadcast
  columns). Futures
  spans are pre-chunked into 24h windows before CLOB calls so tournament-length
  fidelity=1 history does not rely on deep recursive auto-split alone.
  dbt builds `+polymarket_wc2026_market_minute_odds_data_quality`
  (`tag:minute_odds`), producing `polymarket_wc2026_market_minute_odds` and its
  data-quality observability row. By default the job also refreshes the shared
  markets/event-catalog/registry; set
  `POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG=false` (and restart Dagster) to
  reuse an already-landed warehouse catalog on odds/dbt reruns. Run
  `uv run make minute-odds-backfill` after the schedule overlay is validated. No
  schedule.

**Isolated: Match order book**

- `polymarket_wc2026_match_order_book_backfill`: validates the reviewed
  Argentina–Egypt match-95 manifest against one exact Gamma market lookup,
  retrieves both independent outcome-token snapshot streams from PMXT, and
  builds only `+tag:pmxt_order_book` and excludes `tag:match_minute` alongside
  `tag:polygon_settlement`. Saturated 1,000-snapshot ranges split
  recursively with a one-millisecond overlap; terminal loads merge
  idempotently before their window checkpoints. Compatible published runs
  return without Gamma, PMXT, or credential access. Credit exhaustion pauses
  the scan for a later resume. The job has no schedule.

**Isolated: Market portrait**

- `polymarket_wc2026_market_portrait_backfill`: resumable PMXT books and
  trades backfill for a reviewed target manifest; builds
  `+tag:pmxt_order_book +tag:market_portrait` (trade marts are
  `tag:market_portrait` only) and the `oddsfox.market-portrait.v1` bundle.
  Requires `TARGET_MANIFEST` and a PMXT API key. Portrait publication
  requires a completed order-book scan and trade scan for the same manifest.
  See [Market portrait](market-portrait.md).

**Supporting ingestion**

- `international_results_historical_ingest`: public 2006+ matches, shootouts,
  and goalscorers for strategy model fitting.
- `international_results_wc2026_match_results_ingest`: FIFA fixture/results
  refresh.

### Kalshi WC2026

- `kalshi_wc2026_market_scope_registry_refresh`
- `kalshi_wc2026_hourly_odds_ingest`
- `kalshi_wc2026_dbt_build`
- `kalshi_wc2026_full_pipeline`

The full pipeline refreshes FIFA results, Kalshi markets and candlesticks, then
builds `+tag:kalshi` including `international_results` parents while excluding
unrelated Polymarket tests. Dagster warehouse snapshots for scoped dbt jobs
expand the same `+` ancestry from the local dbt manifest when present.

## Scope behavior

### Polymarket WC2026

- `raw/markets` fetches Gamma markets for the current event-catalog registry
  (`discovery_mode=targeted`, `refresh_registry=false`) and lands them through
  dlt without mutating registry admission. Full keyset discovery with registry
  refresh remains available via explicit Dagster run config (match-minute and
  full-refresh jobs).
- `raw/markets_snapshot` records local lineage and does not call Gamma.
- `raw/event_catalog`, `raw/event_snapshots`,
  and `raw/event_market_memberships` land event-catalog inputs used by market
  scope registry refresh. Tag and series Gamma partitions remain exhaustive;
  the platform-wide slug-prefix recall partition is optional (off on routine
  `full_pipeline` / registry refresh, on for
  `polymarket_wc2026_event_catalog_recall_audit` and match-minute). When
  slug-prefix recall runs with
  `slug_prefix_recall_max_pages_without_progress` set, it stops after that many
  consecutive pages with no local slug-prefix matches and marks the partition
  `complete=false`. Partition-level checkpoints in
  `polymarket_wc2026_ops.event_catalog_scan_checkpoint` let a retried crawl
  skip already-complete partitions (`complete=true`); incomplete early-stop
  caches are rescanned. Checkpoints clear after a successful warehouse merge.
  Set `reset_event_catalog_checkpoint=true` to discard them
  before a crawl.
- `ops/market_scope_registry` rebuilds sticky event-volume admission from
  landed event-catalog snapshots, prunes stale `event_catalog` registry rows,
  and materializes admitted markets into raw tables. When Dagster run config
  leaves `max_pages_without_progress` unset on legacy keyset refresh paths,
  discovery and registry refresh apply the scan helper's built-in guard (25
  pages without progress).
- Metadata enrichment and hourly odds operate over the fixed WC2026 registry.
- The match-minute asset writes a separate raw table and never reads or updates
  the hourly token-sync ledger. Any missing token history aborts before dbt. A
  failed run keeps its append-only audit evidence while leaving the previous raw
  snapshot and public table intact.
- The PMXT order-book asset uses separate raw snapshots and ops scan/window
  ledgers. It neither joins the two outcome streams by time nor enters the
  routine hourly/full selections. Empty books remain auditable raw snapshots
  but generate no artificial public levels.
- Kalshi and match-minute dbt paths use `international_results_wc2026_*` marts for
  real-team validation; the Polymarket golden-mart closure does not.
- The Polygon settlement asset is a parallel historical path. Its market and
  fixture semantics come only from the reviewed dbt seed at runtime. It scans
  finalized Polygon logs and stores normalized economic legs without wallets,
  order hashes, signatures, raw event payloads, oracle prose, or RPC URLs.
- The ordinary Polymarket dbt/full jobs build only
  `+polymarket_wc2026_market_hourly_odds` and exclude
  `tag:match_minute tag:minute_odds tag:wc2026_strategy wc2026_fixtures wc2026_schedule_matches
  wc2026_team_canonical_aliases tag:polygon_settlement tag:pmxt_order_book
  tag:market_portrait` (see `POLYMARKET_WC2026_SCOPE.dbt_exclude` in
  `src/oddsfox_pipeline/orchestration/shipped_scopes.py`); only dedicated
  backfills or replay-backed validation targets build those graphs.

### Kalshi WC2026

- `raw/markets` discovers series, events, and markets and lands events and
  markets through dlt.
- `raw/markets_snapshot` is local lineage and does not call Kalshi.
- The registry admits fixed WC2026 stage and group-winner markets. `KXWCADVANCE`
  is registered for raw ingestion only; no Kalshi dbt mart consumes it yet.
- `raw/market_candlesticks_hourly` syncs hourly public-trade-API candlesticks.
  Successful sync metrics include `markets_failed` / `failed_market_tickers`
  when individual markets error under bounded concurrency (partial success is
  still accepted). Registry refresh metrics similarly surface `events_failed`.

### Canonical WC2026 fixtures

- `openfootball/wc2026/raw/schedule_fixtures` refreshes the dependency-free
  OpenFootball `cup.txt`/`cup_finals.txt` mirror of the FIFA schedule and
  retains all FIFA match numbers 1–104. Knockout consumers filter 73–104
  explicitly (`int_polymarket_wc2026_match_working_set` and related models).
- The parser fails closed on invalid pinned file identity, exact bytes,
  reviewed group-fixture slice hashes, IDs, stages, groups, dates, UTC offsets,
  teams, or venues. Each stored fixture includes the final source line number
  in the legacy `source_line_number` field and the exact source-slice SHA-256.
  The bundle manifest also pins the FIFA schedule document used to review the
  numeric IDs. Neutral dbt models exclude match 103 and map both vendors by
  unique normalized team pair.

## Schedules

| Schedule | Job | Default |
| --- | --- | --- |
| `international_results_daily_schedule` | `international_results_historical_ingest` | Stopped |
| `kalshi_wc2026_hourly_odds_schedule` | `kalshi_wc2026_hourly_odds_ingest` | Stopped |

`polymarket_wc2026_hourly_odds_ingest` remains a manual job only; WC2026
Polymarket events are complete and the hourly schedule was removed in v0.2.x.

The match-minute backfill has no schedule or environment enable flag.
The unified minute-odds backfill has no schedule or environment enable flag.
The PMXT match-order-book backfill has no schedule or environment enable flag.
The market-portrait backfill has no schedule or environment enable flag.
The Polygon settlement backfill and audit-release jobs likewise have no schedule
or environment enable flag. The technical exporter is standalone and
unscheduled. None of these paths uploads or distributes data.

The international-results schedule runs daily at 02:15 UTC and is always
stopped at definition load (start it in the Dagster UI if needed). The Kalshi
hourly schedule runs on the hour and remains stopped unless
`KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=true`.

## Run monitoring and retries

Local Dagster instances load `dagster_instance.yaml` with:

- `run_monitoring.enabled: true` — orphaned runs (SIGKILL, OOM, laptop sleep) are
  marked failed instead of staying in `STARTED` forever. No instance-level
  `max_runtime_seconds` cap so long backfills are not killed mid-run.
- `run_retries.enabled: true` with `max_retries: 2` — failed runs retry from the
  last successful step (`FROM_FAILURE`). Raw ingest upserts and the odds
  `token_sync_ledger` make replay safe. Step-level `RetryRequested` retries can
  stack with instance run retries on transient network failures.

Long-running assets emit `progress_guardrail` heartbeats via `ProgressGuardrail`.
`polymarket_wc2026_raw_event_catalog`, `sync_markets`, metadata enrichment, and
`oddsfox_dbt` are guarded; hourly odds logs guardrail output through Dagster
`context.log`.

Failed asset runs persist failure metrics (canonical `status=failed`, with any
prior summary status under `failure_status`) to
`{source}_{scope}_ops.sync_run_metrics` and append to `ingestion_run_events`.
Inspect recent health with:

```bash
uv run python scripts/run_health.py
```

## Landing and finalization

Canonical raw and ops table schemas are the operator and dbt boundary. dlt
lands market, odds-history, registry, ingestion-event, and bounded PMXT snapshot
batches; stage tables and `_dlt*` metadata are internal. PMXT terminal batches
use a dlt-owned replaceable stage followed by an idempotent canonical merge, so
a crash before the window checkpoint can safely replay the same content.

International-results CSV storage, canonical snapshot loading, OpenFootball
fixture storage, and Kalshi candlesticks use custom transactional SQL.
Polygon settlement scans also use custom transactional SQL: successful leaf
chunks are resumable audit evidence, while publication replaces the canonical
fill snapshot and marks the scan published in one transaction. A failed retry
leaves the previous good snapshot available.
Scheduler ledger rows, skip state, and daily odds aggregates also remain custom
SQL finalizers because they preserve monotonic cursors, first-seen timestamps,
scheduler state, and aggregate rebuild semantics.

Next, see the [warehouse reference](warehouse.md) for relation ownership or
[data contracts](data-contracts.md) for the documented mart query surface.
