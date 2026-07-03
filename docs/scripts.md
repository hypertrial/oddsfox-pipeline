# Scripts

Operator scripts live under `scripts/`.
Run them through `uv run python` so they use the repo environment.

## Warehouse

- `profile_warehouse.py`: inspect schemas, relations, row counts, and stats.
- `export_wc2026_minutely_odds.py`: export `wc2026_polymarket_marts.wc2026_token_minutely_odds` to parquet plus a companion `.md` data spec.
- `export_wc2026_hourly_odds.py`: export `wc2026_polymarket_marts.wc2026_token_hourly_odds` to parquet plus a companion `.md` data spec.
- `export_wc2026_knockout_markets.py`: export `wc2026_polymarket_marts.wc2026_knockout_token_hourly_odds` to parquet for WC2026 knockout artifact builds.
- `compact_warehouse.py`: rewrite the DuckDB file into a compact copy and swap it into place.
- `prune_odds_history.py`: delete `wc2026_polymarket_raw.odds_history` rows older than a retention window (default 365 days).
- `repair_wc2026_polymarket_token_sync_ledger.py`: rebuild a corrupted token sync ledger.
- `count_wc2026_gamma_tag_events.py`: count Gamma events for WC2026 tags.

Makefile shortcuts (stop Dagster and other writers first):

```bash
make prune-odds-history          # default 365-day retention; add --dry-run via script directly
make compact-warehouse           # reclaim dead space after rebuilds or pruning
```

Run scripts through the project environment:

```bash
uv run python scripts/profile_warehouse.py --snapshot-copy
uv run python scripts/export_wc2026_minutely_odds.py
uv run python scripts/export_wc2026_minutely_odds.py --snapshot-copy --output /tmp/wc2026_minutely.parquet
# writes /tmp/wc2026_minutely.parquet and /tmp/wc2026_minutely.md
uv run python scripts/export_wc2026_hourly_odds.py
uv run python scripts/export_wc2026_hourly_odds.py --snapshot-copy --output /tmp/wc2026_hourly.parquet
# writes /tmp/wc2026_hourly.parquet and /tmp/wc2026_hourly.md
uv run python scripts/export_wc2026_knockout_markets.py
# writes artifacts/wc2026_exports/wc2026_knockout_token_hourly_odds_<UTC>.parquet
```

Scripts that call Polymarket APIs need network access and should use conservative request-rate settings.
