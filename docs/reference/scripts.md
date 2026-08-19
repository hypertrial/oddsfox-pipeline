# Scripts

Operator scripts live under `scripts/`.
Run them through `uv run python` so they use the repo environment.

## Warehouse

- `run_scope.py`: run a fixed Dagster step for one or more shipped scopes, such as `polymarket:wc2026`, `polymarket:soccer`, or `kalshi:wc2026`. Namespace aliases such as `polymarket_wc2026`, `polymarket_soccer`, and `kalshi_wc2026` are accepted.

Soccer operator targets:

- `make soccer-catalog-audit`: converge open and closed exact-tag Gamma scans
  and refresh the strict match-result registry.
- `make soccer-minute-live-smoke`: use a disposable warehouse and runtime root
  to sample early, middle, and recent admitted games.
- `make soccer-minute-backfill`: run the resumable full soccer pipeline.
- `make dbt-soccer-minute-ci`: build and test the isolated synthetic dbt graph.
- `make soccer-minute-performance-benchmark`: run the disposable SSD-local
  cold/warm incremental benchmark and write JSON below
  `${ODDSFOX_RUNTIME_ROOT}/benchmarks/polymarket-soccer/`.
- `make soccer-production-health`: print current local soccer health and return
  `0` for healthy/warning-only, `1` for critical, or `2` for unreadable or
  invalid monitoring state. For JSON automation, invoke `scripts/run_health.py`
  with `--scope polymarket:soccer --fail-on critical --format json`.
- `load_reference_bundle.py`: validate and transactionally activate a complete
  Scraper `oddsfox.reference.v1` bundle. The script has no source-specific
  parser or endpoint knowledge.
- `profile_warehouse.py`: inspect schemas, relations, row counts, and stats.
- `sync_polymarket_markets_catalog.py`: sync every Gamma market with volume ≥ $100k via `/markets/keyset` (`volume_num_min`, `after_cursor`; open + closed passes) into `polymarket_catalog_raw.markets`. Optional operator utility; not required for the golden WC2026 hourly mart.
- `export_polymarket_wc2026_market_hourly_odds.py`: export `polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds` to Parquet under `artifacts/polymarket_wc2026_exports/`.
- `cleanup_polymarket_wc2026_registry_hygiene.py`: dry-run (default) or `--apply` deletion of synthetic catalog contamination (`evt-A` / `evt-B` / `m-shared`) and ineligible `events_api` / `markets_api` registry orphans. Prefer `make cleanup-polymarket-wc2026-registry-hygiene` (set `APPLY=1` to write). Stop Dagster and other DuckDB writers first.
- `export_marts_parquet.py`: export every present table or view in the shipped `*_marts` schemas (Polymarket, Kalshi, international-results, `wc2026_marts`) to Parquet under `artifacts/marts_exports/<utc>/`. Prefer `make export-marts-parquet`. Includes isolated marts when built; for the allowlisted Polygon technical dossier use the dedicated Polygon exporter.
- `export_polymarket_wc2026_match_minute_odds.py`: write the 104-game match-minute mart to a temporary Parquet, validate its grain, 104/248/496 inventory, proposition mix, timing, elapsed-axis invariants, and immutable results provenance, then atomically replace the prior artifact. It prints completeness, boundary nulls, pair warnings, elapsed range and over-120-minute games, revision/hash, file size, and SHA-256; quality warnings do not fail export.
- `validate_polymarket_wc2026_minute_odds_live_smoke.py`: read-only acceptance
  checks for the disposable unified minute-odds live smoke warehouse. Confirms
  both latest fetch audits, sample counts of `ceil(5%)` markets per leg with
  all tokens retained, raw PK/CHECK health, unified mart rows from both
  `match` and `futures`, and null `blocking_issue_keys`. Prefer
  `make minute-odds-live-smoke` (writes
  `.cache/runtime/smoke/minute-odds/minute_odds_live_smoke.json`).
- `generate_polymarket_wc2026_market_portrait_target.py`: generate a non-credit-consuming PMXT target candidate YAML from the warehouse working set for operator review. Calls Gamma for fresh identities. Output defaults to `.cache/market_portrait_targets/match-<fifa_match_id>.yml`.
- `generate_polymarket_wc2026_polygon_settlement_seed.py`: developer-only
  authoring tool. It validates and reads the Scraper-owned
  `oddsfox.reference.v1` fixture table, derives condition/question/token evidence
  from Polygon without Gamma/CLOB/UI inputs, verifies resolution and token
  orientation, and writes a candidate CSV, `EVIDENCE.json`, and
  `resolution_attestation.yml` below ignored `artifacts/`. It refuses existing
  output directories and never updates the reviewed dbt seed or committed
  attestation.
- `build_polymarket_wc2026_polygon_settlement_release.py`: validate an already
  materialized Polygon mart and build a complete immutable internal SemVer audit
  bundle with schema, provenance, sources, issue-level quality evidence,
  changelog, a `DO_NOT_PUBLISH.md` marker, and SHA-256 checksums. It refuses
  version collisions.
- `export_polymarket_wc2026_polygon_settlement_minute_odds.py`: verify an
  immutable audit release and create the allowlisted **WC2026 Polygon Settlement
  Minute Aggregates** operator-local technical dossier entirely offline. It copies the
  allowlisted CSV byte-for-byte, emits only redacted aggregate metadata, and
  refuses version collisions or unexpected input/output files.
- `benchmark_polymarket_wc2026_polygon_settlement.py`: optional exact comparator
  for two completed v3/v4 benchmark warehouses. It hard-fails on economic-fill
  or full-mart differences, non-39,120 marts, failed v4 publication gates, or
  incomplete scans, then writes only aggregate durations/counts, database
  hashes, v4 RPC metrics, and the advisory speed ratio. It refuses a partial or
  missing baseline.
- `benchmark_polymarket_wc2026_futures_minute_publish.py`: disposable
  baseline-versus-candidate publish benchmark for futures-minute raw replace.
  Synthetic token histories only; writes JSON under
  `.cache/runtime/benchmarks/futures-minute-publish/`. Prefer
  `make futures-minute-publish-benchmark`. Never opens the operator warehouse.
- `benchmark_polymarket_wc2026_minute_odds_dbt.py` — disposable synthetic
  dbt rebuild timing for the unified minute mart (`make minute-odds-dbt-benchmark`).
  Never opens the operator warehouse.
- `build_polymarket_wc2026_stage_execution_release.py`: offline-plan or resume
  targeted PMXT v2 archive reconstruction for the pinned WC2026 stage-minute
  report, using one shared-budget API seed per token-hour, then atomically
  publish the ignored stage-execution evidence bundle. The explicit
  `--source api-range` path retains direct range queries.
  Prefer `make stage-execution-plan` before the credit-consuming
  `make stage-execution-release`; release mode requires a clean pipeline Git
  tree and checks deterministic publication blockers before acquisition.
- `compact_warehouse.py`: rewrite the DuckDB file into a compact copy and swap it into place.
- `prune_odds_history.py`: delete `polymarket_wc2026_raw.odds_history` rows older
  than a retention window (default 365 days). Destructive pruning is protected
  through the WC2026 tournament plus its 90-day review window; inspect with
  `--dry-run`.
- `repair_polymarket_wc2026_token_sync_ledger.py`: rebuild a corrupted token sync ledger.
- `count_polymarket_wc2026_gamma_tag_events.py`: count Gamma events for WC2026 tags.
- `bootstrap_dbt_ci_duckdb.py`: shared disposable DuckDB bootstrap for
  `dbt-build-ci` / `dbt-unit` (and the base layer of source-freshness seeding).
- `gate_timing.py`: opt-in Make-target timing harness that writes ignored JSON
  under `.cache/runtime/benchmarks/`. Prefer `make gate-timing`.
- `seed_dbt_source_freshness.py`: seed freshness-source rows on top of the shared
  CI bootstrap, then used by `make dbt-source-freshness-ci`.

Makefile shortcuts (stop Dagster and other writers first):

```bash
make prune-odds-history          # default 365-day retention; add --dry-run via script directly
make cleanup-polymarket-wc2026-registry-hygiene  # dry-run; APPLY=1 to delete synthetic/API orphans
make compact-warehouse           # reclaim dead space after rebuilds or pruning
make runtime-dirs                # create SSD-local temp and cache directories
make dbt-prepare                 # shared dbt deps/parse into DBT_TARGET_PATH
make gate-timing                 # opt-in cold/warm gate timing JSON
make match-minute-inputs-validate # require a loaded 104-match Scraper reference
make minute-odds-live-smoke       # disposable 5%/leg unified minute live smoke
make futures-minute-publish-benchmark # disposable publish speed/equality report
make minute-odds-dbt-benchmark    # disposable synthetic dbt rebuild timing
make polygon-settlement-seed-validate # operator-local seed + resolution attestation
make dbt-polygon-settlement-ci    # replay-only; no RPC credentials
make polygon-settlement-benchmark # requires completed v3 and v4 warehouses
```

To full-refresh and verify both real minute marts from completed local raw
warehouses, use:

```bash
uv run make local-marts-rebuild \
  MATCH_MINUTE_REBUILD_DUCKDB_PATH="$PWD/.cache/operator-marts/match.duckdb" \
  POLYGON_SETTLEMENT_REBUILD_DUCKDB_PATH="$PWD/.cache/operator-marts/polygon.duckdb"
```

The command requires the operator-local schedule, Polygon manifest, and
attestation at their existing paths, and requires both warehouses below
`ODDSFOX_STORAGE_ROOT`.

Author a seed candidate with an archive-capable primary RPC. Review the
candidate and evidence before separately promoting it to `dbt/seeds/`; the Make
target never performs that promotion:

```bash
POLYGON_SEED_MANIFEST_VERSION=1.0.0 \
POLYGON_SEED_REVIEWED_AT=2026-07-22T12:00:00Z \
POLYGON_SEED_OUTPUT_DIR=artifacts/polygon_settlement_seed_candidates/1.0.0 \
uv run make polygon-settlement-seed-candidate
```

If a local seed or resolution attestation needs a correction, regenerate and
review its evidence and use a new SemVer for the next immutable local
audit/export.

Run the unscheduled historical pipeline only after configuring
`POLYGON_RPC_URL` and `POLYGON_RPC_PROVIDER_LABEL`:

```bash
uv run make polygon-settlement-live-smoke
```

The target keeps its resumable v4 warehouse under
`.cache/polygon_settlement/benchmarks/v4/` by default. Set
`POLYGON_SETTLEMENT_LIVE_SMOKE_RESET=true` only when an intentional clean scan
is required. Its DuckDB/WAL/spill, Dagster state, dbt target/logs, Python temp
files, XDG cache, and child-process uv cache are rooted below
`.cache/polygon_settlement/` (including DuckDB extensions; the project uv cache
is `.cache/runtime/uv`). Providers with a lower `eth_getLogs` range ceiling can start
with smaller leaves without discarding the checkpoint, for example:

```bash
POLYGON_SETTLEMENT_LIVE_SMOKE_INITIAL_BLOCK_CHUNK_SIZE=2000 \
  uv run make polygon-settlement-live-smoke
```

The equivalent live-only overrides for request rate, workers, and initial
receipt batch size use the same `POLYGON_SETTLEMENT_LIVE_SMOKE_` prefix. Core
defaults remain 5 requests/second, 5 workers, 8,000 blocks, and 20 receipts.
Because uv
starts before Make, run from the repository root so `pyproject.toml` also keeps
the outer `uv run` cache on the SSD-backed repository volume.

`make polygon-settlement-benchmark` remains available for a future completed
v3 baseline, but it deliberately fails for the preserved partial v3 run. A v4
live run does not claim a measured v3 speed ratio without that baseline.

Build an internal audit release from a populated, valid warehouse:

```bash
POLYGON_DATASET_VERSION=1.0.0 \
uv run make polygon-settlement-release
```

The audit lands below
`artifacts/polygon_settlement/audit/releases/<version>/`. It contains the market
sidecar, full provenance, and issue-level quality evidence and is marked
`DO_NOT_PUBLISH.md`.

Create the separate allowlisted technical export without opening the warehouse or
making network requests:

```bash
POLYGON_DATASET_VERSION=1.0.0 \
uv run make polygon-settlement-export
```

The equivalent direct command is:

```bash
uv run python \
  scripts/export_polymarket_wc2026_polygon_settlement_minute_odds.py \
  --audit-release artifacts/polygon_settlement/audit/releases/1.0.0 \
  --output-root artifacts/polygon_settlement/exports
```

It writes
`artifacts/polygon_settlement/exports/releases/<version>/`, verifies that the
CSV SHA-256 is identical to the audit copy, and includes no market sidecar,
full provenance, exact warning rows, credentials, upload configuration, or
upload action. The output remains under operator control.

Run scripts through the project environment:

```bash
uv run python scripts/run_scope.py --list
uv run python scripts/run_scope.py polymarket:wc2026 --step full
uv run python scripts/run_scope.py polymarket:wc2026 kalshi:wc2026 --step dbt
uv run python scripts/profile_warehouse.py --snapshot-copy
uv run python scripts/export_polymarket_wc2026_market_hourly_odds.py
uv run make export-marts-parquet
uv run python scripts/export_polymarket_wc2026_match_minute_odds.py
export ODDSFOX_DATA_DIR="${ODDSFOX_DATA_DIR:-.runtime}"
mkdir -p "$ODDSFOX_DATA_DIR/exports"
uv run python scripts/export_polymarket_wc2026_market_hourly_odds.py --snapshot-copy --output "$ODDSFOX_DATA_DIR/exports/wc2026_market_hourly.parquet"
# writes "$ODDSFOX_DATA_DIR/exports/wc2026_market_hourly.parquet"
```

Scripts that call Polymarket APIs need network access and should use conservative request-rate settings.
The Polygon seed authoring/backfill paths call only the configured JSON-RPC.
Fixture evidence is read from a previously published Scraper reference bundle;
Pipeline does not contact its upstream source, Gamma, or CLOB.
