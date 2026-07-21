# Scripts

Operator scripts live under `scripts/`.
Run them through `uv run python` so they use the repo environment.

## Warehouse

- `run_scope.py`: run a fixed Dagster step for one or more shipped scopes, such as `polymarket:wc2026`, `polymarket:us_midterms_2026`, or `kalshi:wc2026`. Namespace aliases such as `polymarket_wc2026`, `polymarket_us_midterms_2026`, and `kalshi_wc2026` are accepted.
- `profile_warehouse.py`: inspect schemas, relations, row counts, and stats.
- `export_polymarket_wc2026_knockout_hourly_odds.py`: export `polymarket_wc2026_marts.polymarket_wc2026_knockout_token_hourly_odds` to parquet for progression-only WC2026 knockout audits.
- `export_polymarket_wc2026_graph_hourly_odds.py`: export `polymarket_wc2026_marts.polymarket_wc2026_graph_token_hourly_odds` to portable parquet for `oddsfox-graph`; it includes both Yes/No tokens.
- `export_polymarket_wc2026_match_minute_odds.py`: export the 104-game match-minute mart to Parquet and print inventory, time-range, observation, completeness, and file-size statistics.
- `build_hosted_artifacts.py`: local artifact helper that runs refresh, dbt, graph export, graph build, validation, and atomic publication into `$ODDSFOX_DATA_DIR/artifacts/releases/<UTC_BUILD_ID>` plus `$ODDSFOX_DATA_DIR/artifacts/current`.
- `compact_warehouse.py`: rewrite the DuckDB file into a compact copy and swap it into place.
- `prune_odds_history.py`: delete `polymarket_wc2026_raw.odds_history` rows older than a retention window (default 365 days).
- `repair_polymarket_wc2026_token_sync_ledger.py`: rebuild a corrupted token sync ledger.
- `count_polymarket_wc2026_gamma_tag_events.py`: count Gamma events for WC2026 tags.

Makefile shortcuts (stop Dagster and other writers first):

```bash
make prune-odds-history          # default 365-day retention; add --dry-run via script directly
make compact-warehouse           # reclaim dead space after rebuilds or pruning
```

Run scripts through the project environment:

```bash
uv run python scripts/run_scope.py --list
uv run python scripts/run_scope.py polymarket:wc2026 --step full
uv run python scripts/run_scope.py polymarket:wc2026 kalshi:wc2026 --step dbt
uv run python scripts/profile_warehouse.py --snapshot-copy
uv run python scripts/export_polymarket_wc2026_knockout_hourly_odds.py
uv run python scripts/export_polymarket_wc2026_match_minute_odds.py
export ODDSFOX_DATA_DIR="${ODDSFOX_DATA_DIR:-/Volumes/Mac SSD/hypertrial_trilemma/hypertrial/OddsFox/.runtime}"
mkdir -p "$ODDSFOX_DATA_DIR/exports"
uv run python scripts/export_polymarket_wc2026_knockout_hourly_odds.py --snapshot-copy --output "$ODDSFOX_DATA_DIR/exports/wc2026_knockout_hourly.parquet"
uv run python scripts/export_polymarket_wc2026_graph_hourly_odds.py --snapshot-copy --output "$ODDSFOX_DATA_DIR/exports/wc2026_graph_hourly.parquet"
# writes "$ODDSFOX_DATA_DIR/exports/wc2026_knockout_hourly.parquet"
# writes "$ODDSFOX_DATA_DIR/exports/wc2026_graph_hourly.parquet"
```

Hosted artifact publish:

```bash
export ODDSFOX_DATA_DIR="${ODDSFOX_DATA_DIR:-/Volumes/Mac SSD/hypertrial_trilemma/hypertrial/OddsFox/.runtime}"
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
export ODDSFOX_DATA_DIR="${ODDSFOX_DATA_DIR:-/Volumes/Mac SSD/hypertrial_trilemma/hypertrial/OddsFox/.runtime}"
uv run python scripts/build_hosted_artifacts.py \
  --artifact-dir "$ODDSFOX_DATA_DIR/artifacts" \
  --graph-repo ../oddsfox-graph \
  --skip-refresh \
  --skip-dbt \
  --input-parquet "$ODDSFOX_DATA_DIR/exports/wc2026_graph_hourly.parquet" \
  --allow-stale-current
```

Scripts that call Polymarket APIs need network access and should use conservative request-rate settings.
