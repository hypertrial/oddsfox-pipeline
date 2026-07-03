# Scripts

Operator scripts live under `scripts/`.
Run them through `uv run python` so they use the repo environment.

## Warehouse

- `profile_warehouse.py`: inspect schemas, relations, row counts, and stats.
- `export_selected_minutely_odds.py`: export `polymarket_marts.selected_token_minutely_odds` to parquet plus a companion `.md` data spec.
- `export_selected_hourly_odds.py`: export `polymarket_marts.selected_token_hourly_odds` to parquet plus a companion `.md` data spec; pass `--live-current` to export `polymarket_marts.selected_token_live_hourly_odds` for graph builds.
- `export_wc2026_knockout_markets.py`: export `polymarket_marts.wc2026_knockout_markets` to parquet for WC2026 knockout artifact builds.
- `compact_warehouse.py`: rewrite the DuckDB file into a compact copy and swap it into place.
- `prune_odds_history.py`: delete `polymarket_raw.odds_history` rows older than a retention window (default 365 days).
- `repair_polymarket_token_sync_ledger.py`: rebuild a corrupted token sync ledger.

Makefile shortcuts (stop Dagster and other writers first):

```bash
make prune-odds-history          # default 365-day retention; add --dry-run via script directly
make compact-warehouse           # reclaim dead space after rebuilds or pruning
```

## Current Polymarket Scope

- `audit_polymarket_selected_scope.py`: compare registry, allowlist, and strict selected-scope predicates.
- `audit_selected_scope_tag_coverage.py`: crawl Gamma tag/search discovery and report registry gaps.
- `count_gamma_tag_events.py`: count Gamma events for selected-scope tags.

These audit scripts take a single `--scope-name` (default `wc2026`). When
`POLYMARKET_MARKET_SCOPES` selects more than one scope, re-run each script once
per scope.

Run scripts through the project environment:

```bash
uv run python scripts/profile_warehouse.py --snapshot-copy
uv run python scripts/export_selected_minutely_odds.py
uv run python scripts/export_selected_minutely_odds.py --snapshot-copy --output /tmp/selected_minutely.parquet
# writes /tmp/selected_minutely.parquet and /tmp/selected_minutely.md
uv run python scripts/export_selected_hourly_odds.py
uv run python scripts/export_selected_hourly_odds.py --snapshot-copy --output /tmp/selected_hourly.parquet
# writes /tmp/selected_hourly.parquet and /tmp/selected_hourly.md
uv run python scripts/export_selected_hourly_odds.py --live-current
# writes artifacts/selected_scope_exports/selected_token_live_hourly_odds_<UTC>.parquet
uv run python scripts/export_wc2026_knockout_markets.py
# writes artifacts/selected_scope_exports/wc2026_knockout_markets_<UTC>.parquet
```

Scripts that call Polymarket APIs need network access and should use conservative request-rate settings.
