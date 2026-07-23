# Scripts

Operator scripts live under `scripts/`.
Run them through `uv run python` so they use the repo environment.

## Warehouse

- `run_scope.py`: run a fixed Dagster step for one or more shipped scopes, such as `polymarket:wc2026`, `polymarket:us_midterms_2026`, or `kalshi:wc2026`. Namespace aliases such as `polymarket_wc2026`, `polymarket_us_midterms_2026`, and `kalshi_wc2026` are accepted.
- `profile_warehouse.py`: inspect schemas, relations, row counts, and stats.
- `export_polymarket_wc2026_knockout_hourly_odds.py`: export `polymarket_wc2026_marts.polymarket_wc2026_knockout_token_hourly_odds` to parquet for progression-only WC2026 knockout audits.
- `export_polymarket_wc2026_graph_hourly_odds.py`: export `polymarket_wc2026_marts.polymarket_wc2026_graph_token_hourly_odds` to portable parquet for `oddsfox-graph`; it includes both Yes/No tokens.
- `export_polymarket_wc2026_match_minute_odds.py`: write the 104-game match-minute mart to a temporary Parquet, validate its grain, 104/248/496 inventory, proposition mix, timing, elapsed-axis invariants, and immutable results provenance, then atomically replace the prior artifact. It prints completeness, boundary nulls, pair warnings, elapsed range and over-120-minute games, revision/hash, file size, and SHA-256; quality warnings do not fail export.
- `generate_polymarket_wc2026_polygon_settlement_seed.py`: developer-only
  authoring tool. It downloads the hash-pinned CC0 OpenFootball fixture files
  and pinned official FIFA schedule PDF, derives condition/question/token
  evidence from Polygon without Gamma/CLOB/UI inputs, verifies resolution and
  token orientation, and writes a candidate CSV, `EVIDENCE.json`, and
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
- `build_hosted_artifacts.py`: local artifact helper that runs refresh, dbt, graph export, graph build, validation, and atomic publication into `$ODDSFOX_DATA_DIR/artifacts/releases/<UTC_BUILD_ID>` plus `$ODDSFOX_DATA_DIR/artifacts/current`.
- `compact_warehouse.py`: rewrite the DuckDB file into a compact copy and swap it into place.
- `prune_odds_history.py`: delete `polymarket_wc2026_raw.odds_history` rows older than a retention window (default 365 days).
- `repair_polymarket_wc2026_token_sync_ledger.py`: rebuild a corrupted token sync ledger.
- `count_polymarket_wc2026_gamma_tag_events.py`: count Gamma events for WC2026 tags.

Makefile shortcuts (stop Dagster and other writers first):

```bash
make prune-odds-history          # default 365-day retention; add --dry-run via script directly
make compact-warehouse           # reclaim dead space after rebuilds or pruning
make runtime-dirs                # create SSD-local temp and cache directories
make match-minute-inputs-validate # require a complete local 104-match schedule
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

Run the unscheduled historical flow only after configuring
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
is `.cache/uv`). Providers with a lower `eth_getLogs` range ceiling can start
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
uv run python scripts/export_polymarket_wc2026_knockout_hourly_odds.py
uv run python scripts/export_polymarket_wc2026_match_minute_odds.py
export ODDSFOX_DATA_DIR="${ODDSFOX_DATA_DIR:-.runtime}"
mkdir -p "$ODDSFOX_DATA_DIR/exports"
uv run python scripts/export_polymarket_wc2026_knockout_hourly_odds.py --snapshot-copy --output "$ODDSFOX_DATA_DIR/exports/wc2026_knockout_hourly.parquet"
uv run python scripts/export_polymarket_wc2026_graph_hourly_odds.py --snapshot-copy --output "$ODDSFOX_DATA_DIR/exports/wc2026_graph_hourly.parquet"
# writes "$ODDSFOX_DATA_DIR/exports/wc2026_knockout_hourly.parquet"
# writes "$ODDSFOX_DATA_DIR/exports/wc2026_graph_hourly.parquet"
```

Local hosted-artifact build:

```bash
export ODDSFOX_DATA_DIR="${ODDSFOX_DATA_DIR:-.runtime}"
mkdir -p "$ODDSFOX_DATA_DIR"/{warehouse,artifacts,exports,dagster-home,dlt,logs}
export DUCKDB_PATH="$ODDSFOX_DATA_DIR/warehouse/oddsfox.duckdb"
export DAGSTER_HOME="$ODDSFOX_DATA_DIR/dagster-home"
export DLT_DATA_DIR="$ODDSFOX_DATA_DIR/dlt"
uv run python scripts/build_hosted_artifacts.py \
  --artifact-dir "$ODDSFOX_DATA_DIR/artifacts" \
  --graph-repo ../oddsfox-graph
```

For a local fixture run without network or dbt:

```bash
export ODDSFOX_DATA_DIR="${ODDSFOX_DATA_DIR:-.runtime}"
uv run python scripts/build_hosted_artifacts.py \
  --artifact-dir "$ODDSFOX_DATA_DIR/artifacts" \
  --graph-repo ../oddsfox-graph \
  --skip-refresh \
  --skip-dbt \
  --input-parquet "$ODDSFOX_DATA_DIR/exports/wc2026_graph_hourly.parquet" \
  --allow-stale-current
```

Scripts that call Polymarket APIs need network access and should use conservative request-rate settings.
The Polygon seed authoring/backfill paths call only the configured JSON-RPC and
the authoring tool's pinned OpenFootball and FIFA evidence URLs; they do not
call Gamma or CLOB.
