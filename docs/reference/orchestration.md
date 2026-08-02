# Orchestration reference

This reference lists the fixed Dagster assets, jobs, scope behavior, schedules,
and persistence boundaries shipped by OddsFox Pipeline `v0.1.x`.

For procedures, use [Run a scope](../guides/run-a-scope.md),
[Enable schedules](../guides/enable-schedules.md), and
[Validate and recover](../guides/validate-and-recover.md).

Canonical vocabulary lives in [Terminology](terminology.md).

## Pipeline registry

Entry-point jobs are pipelines; narrower jobs run one step of a pipeline. See
[Terminology](terminology.md#execution) for the distinction.

**Maturity tiers:** **Production** — scheduled-capable, full `ci-fast` dbt gate,
primary quickstart path. **Mature, composed** — composes existing scope assets
into a cross-provider mart. **Mature, isolated** — own CI lane and documented
data-boundary isolation, not immaturity. **Experimental** — opt-in backfill,
paid or narrow credentials, single-target manifests.

| Pipeline | Entry job(s) | Steps | Schedule | CI dbt gate | Maturity |
| --- | --- | --- | --- | --- | --- |
| Polymarket WC2026 | `polymarket_wc2026_full_pipeline` | `market_scope_registry`, `odds`, `dbt`, `logical_atlas` | Hourly odds (stopped) | `ci-fast` (`+tag:polymarket,tag:wc2026`) | Production |
| Polymarket US midterms 2026 | `polymarket_us_midterms_2026_full_pipeline` | `market_scope_registry`, `odds`, `dbt` | Hourly odds (stopped) | `ci-fast` (`+tag:us_midterms_2026`) | Production |
| Kalshi WC2026 | `kalshi_wc2026_full_pipeline` | `market_scope_registry`, `odds`, `dbt` | Hourly odds (stopped) | `ci-fast` (`+tag:kalshi`) | Production |
| Cross-platform WC2026 knockout | `wc2026_knockout_match_odds_full_pipeline` | OpenFootball fixtures + both provider registries and hourly odds + `+tag:cross_domain` dbt | Hourly (stopped) | `ci-fast` (`+tag:cross_domain`) | Mature, composed |
| Polygon settlement history | `polymarket_wc2026_polygon_settlement_backfill` → `_release` → standalone exporter | Backfill scan, audit release, offline export | None | `dbt-polygon-settlement-ci` (excluded from ordinary `dbt-build-ci`) | Mature, isolated |
| Advanced match analysis | `polymarket_wc2026_match_order_book_backfill` → `polymarket_wc2026_market_portrait_backfill`; `polymarket_wc2026_match_minute_odds_backfill` (independent) | Order book, then portrait (portrait requires order book + trades); minute odds is a separate optional path in the same family | None | Minute mart in `ci-fast`; PMXT-tagged models excluded (`tag:pmxt_order_book`) | Experimental |

Supporting ingestion jobs (`international_results_historical_ingest`,
`international_results_wc2026_match_results_ingest`) feed WC2026 production
pipelines but are not separate product pipelines.

## Asset order

1. `polymarket/wc2026/raw/markets`
2. `polymarket/wc2026/raw/markets_snapshot`
3. `polymarket/wc2026/raw/reviewed_event_membership`
4. `polymarket/wc2026/raw/event_catalog`
5. `polymarket/wc2026/raw/event_snapshots`
6. `polymarket/wc2026/raw/event_market_memberships`
7. `polymarket/wc2026/ops/market_scope_registry`
8. `polymarket/wc2026/raw/market_metadata_enrichment`
9. `polymarket/wc2026/raw/token_odds_history_hourly`
10. `polymarket/wc2026/raw/match_token_odds_history_minute` (dedicated backfill only)
11. `polymarket/wc2026/raw/match_order_book_snapshots` (dedicated PMXT backfill only)
12. `polymarket/wc2026/raw/polygon_settlement_fills` (dedicated finalized backfill only)
13. `polymarket/us_midterms_2026/raw/markets`
14. `polymarket/us_midterms_2026/raw/markets_snapshot`
15. `polymarket/us_midterms_2026/ops/market_scope_registry`
16. `polymarket/us_midterms_2026/raw/market_metadata_enrichment`
17. `polymarket/us_midterms_2026/raw/token_odds_history_hourly`
18. `international_results/historical/raw/snapshot`
19. `international_results/wc2026/raw/match_results`
20. `openfootball/wc2026/raw/schedule_fixtures`
21. `kalshi/wc2026/raw/events` (landed with the markets dlt source)
22. `kalshi/wc2026/raw/markets`
23. `kalshi/wc2026/raw/markets_snapshot`
24. `kalshi/wc2026/ops/market_scope_registry`
25. `kalshi/wc2026/raw/market_candlesticks_hourly`
26. dbt model assets under the matching
    `{staging,intermediate,marts,observability}` namespaces.
27. `polymarket/wc2026/release/logical_bundle`
28. `polymarket/wc2026/release/polygon_settlement_odds_bundle` (internal audit release only)

Flat Dagster op names preserve the same source-first order, for example
`polymarket_wc2026_raw_token_odds_history_hourly`.

## Jobs

Entry-point jobs are pipelines; narrower jobs run one step. See
[Pipeline registry](#pipeline-registry) and [Terminology](terminology.md#execution).

### Polymarket WC2026

**Entry point**

- `polymarket_wc2026_full_pipeline`: results, registry, odds, dbt, and logical
  atlas release.

**Steps**

- `polymarket_wc2026_market_scope_registry_refresh`: market discovery, market
  scope registry refresh, and metadata enrichment.
- `polymarket_wc2026_hourly_odds_ingest`: trailing hourly token-odds refresh.
- `polymarket_wc2026_dbt_build`: WC2026 and international-results dbt build.
  Default run config uses incremental dbt (`full_refresh=False`); set
  `full_refresh=True` in Dagster run config for a one-off full rebuild.
- `polymarket_wc2026_logical_atlas`: builds logical marts and publishes the
  `polymarket/wc2026/release/logical_bundle` for the static WC2026 logical atlas
  (no odds). See
  [Build the WC2026 logical atlas](../guides/build-wc2026-logical-atlas.md).

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

**Advanced match analysis (experimental)**

Portrait requires order book and trades; minute odds is an independent path in
the same family. See [Pipeline registry](#pipeline-registry).

1. `polymarket_wc2026_match_minute_odds_backfill` (optional, independent): one-time or rerunnable
   completed-match backfill for all 104 FIFA-numbered games and the dedicated
   minute mart. It refreshes the latest 104 international-results rows and the
   OpenFootball schedule fixtures (knockout subset 73–104), discovers closed
   Gamma events without a volume floor, validates result alignment and the
   104/248/496 inventory, fetches exact game windows at CLOB `fidelity=1`, then
   runs dbt. The results refresh first resolves and downloads an immutable Git
   revision. Minute fetches append 496 audit rows; only an all-success run
   atomically replaces raw history and marks those audits published.
   Run `uv run make match-minute-live-smoke` for the disposable live acceptance
   check; it is intentionally absent from CI and all schedules.
2. `polymarket_wc2026_match_order_book_backfill` (required before portrait): validates the reviewed
   Argentina–Egypt match-95 manifest against one exact Gamma market lookup,
   retrieves both independent outcome-token snapshot streams from PMXT, and
   builds only `+tag:pmxt_order_book`. Saturated 1,000-snapshot ranges split
   recursively with a one-millisecond overlap; terminal loads merge
   idempotently before their window checkpoints. Compatible published runs
   return without Gamma, PMXT, or credential access. Credit exhaustion pauses
   the scan for a later resume. The job has no schedule.
3. `polymarket_wc2026_market_portrait_backfill` (requires step 2): resumable PMXT books and
   trades backfill for a reviewed target manifest; builds the
   `oddsfox.market-portrait.v1` bundle. Requires `TARGET_MANIFEST` and a PMXT
   API key. See [Market portrait](market-portrait.md).

**Supporting ingestion**

- `international_results_historical_ingest`: public 2006+ matches, shootouts,
  and goalscorers for strategy model fitting.
- `international_results_wc2026_match_results_ingest`: FIFA fixture/results
  refresh.

### Polymarket US midterms 2026

- `polymarket_us_midterms_2026_market_scope_registry_refresh`
- `polymarket_us_midterms_2026_hourly_odds_ingest`
- `polymarket_us_midterms_2026_dbt_build`
- `polymarket_us_midterms_2026_full_pipeline`

The dbt jobs select `+tag:us_midterms_2026` so their shared catalog ancestors
are materialized; there is no FIFA results input.

### Kalshi WC2026

- `kalshi_wc2026_market_scope_registry_refresh`
- `kalshi_wc2026_hourly_odds_ingest`
- `kalshi_wc2026_dbt_build`
- `kalshi_wc2026_full_pipeline`

The full pipeline refreshes FIFA results, Kalshi markets and candlesticks, then
builds `+tag:kalshi` including `international_results` parents while excluding
unrelated Polymarket tests.

### Cross-platform WC2026 knockout match odds

- `wc2026_knockout_match_odds_full_pipeline`: refreshes the OpenFootball
  schedule fixtures mirror, both provider registries, both hourly odds sources,
  permanent provider facts, and the neutral mart/observability models in one job.

The combined job selects `+tag:cross_domain`. Source-specific Polymarket and
Kalshi dbt jobs exclude that tag, so they cannot publish a partially refreshed
cross-provider comparison. Operator recipe:
[Run cross-platform knockout](../guides/run-cross-platform-knockout.md).

## Scope behavior

### Polymarket WC2026

- `raw/markets` performs one Gamma discovery pass, lands raw markets through
  dlt, and persists token mappings from the same payload.
- `raw/markets_snapshot` records local lineage and does not call Gamma.
- `raw/reviewed_event_membership`, `raw/event_catalog`, `raw/event_snapshots`,
  and `raw/event_market_memberships` land reviewed membership and event-catalog
  inputs used by the logical atlas.
- `ops/market_scope_registry` writes only when discovery did not already
  refresh the market scope registry. When Dagster run config leaves
  `max_pages_without_progress` unset, discovery and registry refresh apply the
  scan helper's built-in guard (25 pages without progress).
- Metadata enrichment and hourly odds operate over the fixed WC2026 registry.
- The match-minute asset writes a separate raw table and never reads or updates
  the hourly token-sync ledger. Any missing token history aborts before dbt. A
  failed run keeps its append-only audit evidence while leaving the previous raw
  snapshot and public table intact.
- The PMXT order-book asset uses separate raw snapshots and ops scan/window
  ledgers. It neither joins the two outcome streams by time nor enters the
  routine hourly/full selections. Empty books remain auditable raw snapshots
  but generate no artificial public levels.
- FIFA results supply the real-team validation inputs used by dbt.
- The Polygon settlement asset is a parallel historical path. Its market and
  fixture semantics come only from the reviewed dbt seed at runtime. It scans
  finalized Polygon logs and stores normalized economic legs without wallets,
  order hashes, signatures, raw event payloads, oracle prose, or RPC URLs.
- The ordinary Polymarket dbt/full jobs exclude `tag:polygon_settlement` and
  `tag:pmxt_order_book`; only their dedicated backfills or replay-backed
  validation targets build them.
- `release/logical_bundle` exports the seven-file `polymarket-wc2026-logical-v1`
  logical bundle for `oddsfox-graph`.

### Polymarket US midterms 2026

- Discovery targets Balance of Power, Senate control, and House control event
  slugs.
- Raw, ops, registry, and odds assets mirror the WC2026 pipeline in a separate
  namespace.
- The public dbt surface is a markets-plus-hourly-odds mart without office-type
  classification.

### Kalshi WC2026

- `raw/markets` discovers series, events, and markets and lands events and
  markets through dlt.
- `raw/markets_snapshot` is local lineage and does not call Kalshi.
- The registry admits fixed WC2026 stage, group-winner, and `KXWCADVANCE`
  match-advance markets.
- `raw/market_candlesticks_hourly` syncs hourly public-trade-API candlesticks.

### Canonical WC2026 fixtures

- `openfootball/wc2026/raw/schedule_fixtures` refreshes the dependency-free
  OpenFootball `cup.txt`/`cup_finals.txt` mirror of the FIFA schedule and
  retains all FIFA match numbers 1–104. Knockout consumers filter 73–104
  explicitly (`int_wc2026_advancement_fixtures` and related models).
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
| `polymarket_wc2026_hourly_odds_schedule` | `polymarket_wc2026_hourly_odds_ingest` | Stopped |
| `polymarket_us_midterms_2026_hourly_odds_schedule` | `polymarket_us_midterms_2026_hourly_odds_ingest` | Stopped |
| `kalshi_wc2026_hourly_odds_schedule` | `kalshi_wc2026_hourly_odds_ingest` | Stopped |
| `wc2026_knockout_match_odds_hourly_schedule` | `wc2026_knockout_match_odds_full_pipeline` | Stopped |

The match-minute backfill has no schedule or environment enable flag.
The PMXT match-order-book backfill has no schedule or environment enable flag.
The Polygon settlement backfill and audit-release jobs likewise have no schedule
or environment enable flag. The technical exporter is standalone and
unscheduled. None of these paths uploads or distributes data.

The international-results schedule runs daily at 02:15 UTC; the other four run
hourly. The combined schedule uses Polymarket CLOB
`fidelity=60`, bypasses the progression-futures volume floor for exact match
markets, and remains stopped unless its dedicated env flag is enabled.

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
