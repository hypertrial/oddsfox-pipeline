# Scripts

Operator scripts live under `scripts/`.
Run them through `uv run python` so they use the repo environment.

## Warehouse

- `profile_warehouse.py`: inspect schemas, relations, row counts, and stats.
- `export_polymarket_wc2026_knockout_hourly_odds.py`: export `polymarket_wc2026_marts.polymarket_wc2026_knockout_token_hourly_odds` to parquet for progression-only WC2026 knockout audits.
- `export_polymarket_wc2026_graph_hourly_odds.py`: export `polymarket_wc2026_marts.polymarket_wc2026_graph_token_hourly_odds` to parquet for `oddsfox-graph`; this is the hosted graph input and includes both Yes/No tokens.
- `build_hosted_artifacts.py`: run refresh, dbt, graph export, graph build, validation, and atomic publish into `/artifacts/releases/<UTC_BUILD_ID>` plus `/artifacts/current`.
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
uv run python scripts/profile_warehouse.py --snapshot-copy
uv run python scripts/export_polymarket_wc2026_knockout_hourly_odds.py
uv run python scripts/export_polymarket_wc2026_knockout_hourly_odds.py --snapshot-copy --output /tmp/wc2026_knockout_hourly.parquet
uv run python scripts/export_polymarket_wc2026_graph_hourly_odds.py --snapshot-copy --output /tmp/wc2026_graph_hourly.parquet
# writes /tmp/wc2026_knockout_hourly.parquet
# writes artifacts/polymarket_wc2026_exports/polymarket_wc2026_knockout_token_hourly_odds_<UTC>.parquet
```

Hosted artifact publish:

```bash
uv run python scripts/build_hosted_artifacts.py \
  --artifact-dir /artifacts \
  --graph-repo ../oddsfox-graph
```

For a local fixture run without network or dbt:

```bash
uv run python scripts/build_hosted_artifacts.py \
  --artifact-dir /tmp/oddsfox-artifacts \
  --graph-repo ../oddsfox-graph \
  --skip-refresh \
  --skip-dbt \
  --input-parquet /tmp/wc2026_graph_hourly.parquet \
  --allow-stale-current
```

Scripts that call Polymarket APIs need network access and should use conservative request-rate settings.
